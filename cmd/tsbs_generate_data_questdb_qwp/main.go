// tsbs_generate_data_questdb_qwp simulates a TSBS use case and captures the
// post-upgrade QWP WebSocket frames that go-questdb-client would send over
// the wire. The HTTP upgrade handshake is stripped so the dump is purely a
// sequence of WebSocket frames — tsbs_load_questdb_qwp opens its own
// upgrade against a live server and then streams these bytes verbatim.
package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"math/rand"
	"os"
	"time"

	"github.com/blagojts/viper"
	qdb "github.com/questdb/go-questdb-client/v4"
	"github.com/questdb/tsbs/internal/utils"
	"github.com/questdb/tsbs/pkg/data"
	"github.com/questdb/tsbs/pkg/data/usecases"
	"github.com/questdb/tsbs/pkg/data/usecases/common"
	"github.com/spf13/pflag"
)

const (
	defaultFlushBytes = 1 << 20 // 1 MiB
)

func main() {
	config := &common.DataGeneratorConfig{}
	config.AddToFlagSet(pflag.CommandLine)
	pflag.Int("flush-bytes", defaultFlushBytes,
		"QWP auto-flush byte threshold; batches fire once the buffer exceeds this size")
	pflag.Int("max-buffer-size", 0,
		"Hard cap on the sender buffer in bytes (0 = unlimited)")
	pflag.Int("parallel", 1,
		"Number of output files. When >1, --file is a prefix and outputs are {file}.0..{file}.N-1; "+
			"file k gets every timestamp shifted by k ms so all files carry distinct rows.")
	pflag.Parse()

	if err := utils.SetupConfigFile(); err != nil {
		log.Fatalf("config file: %v", err)
	}
	if err := viper.Unmarshal(&config.BaseConfig); err != nil {
		log.Fatalf("unmarshal base config: %v", err)
	}
	if err := viper.Unmarshal(config); err != nil {
		log.Fatalf("unmarshal config: %v", err)
	}
	// Format is fixed for this binary; pin it to a known value so the
	// shared validator accepts it without requiring --format on the CLI.
	config.Format = "questdb"
	if err := config.Validate(); err != nil {
		log.Fatalf("invalid config: %v", err)
	}

	flushBytes := viper.GetInt("flush-bytes")
	if flushBytes <= 0 {
		log.Fatalf("--flush-bytes must be positive, got %d", flushBytes)
	}
	maxBufSize := viper.GetInt("max-buffer-size")
	parallel := viper.GetInt("parallel")
	if parallel < 1 {
		log.Fatalf("--parallel must be >= 1, got %d", parallel)
	}

	outs, closeOuts, err := openOutputs(config.File, parallel)
	if err != nil {
		log.Fatalf("open output: %v", err)
	}
	defer closeOuts()

	rand.Seed(config.Seed)
	scfg, err := usecases.GetSimulatorConfig(config)
	if err != nil {
		log.Fatalf("simulator config: %v", err)
	}
	sim := scfg.NewSimulator(config.LogInterval, config.Limit)

	ctx := context.Background()
	senders := make([]qdb.LineSender, parallel)
	closeAll := func() {
		for _, s := range senders {
			if s != nil {
				_ = s.Close(ctx)
			}
		}
	}
	for i := 0; i < parallel; i++ {
		dumpSink := &postHandshakeWriter{inner: outs[i]}
		opts := []qdb.LineSenderOption{
			qdb.WithQwp(),
			qdb.WithQwpDumpWriter(dumpSink),
			qdb.WithAutoFlushBytes(flushBytes),
			qdb.WithAutoFlushRows(0),
			qdb.WithAutoFlushInterval(0),
			qdb.WithInFlightWindow(1),
		}
		if maxBufSize > 0 {
			opts = append(opts, qdb.WithMaxBufferSize(maxBufSize))
		}
		s, err := qdb.NewLineSender(ctx, opts...)
		if err != nil {
			closeAll()
			log.Fatalf("create sender[%d]: %v", i, err)
		}
		senders[i] = s
	}

	started := time.Now()
	rows, err := runSimulation(ctx, senders, sim)
	if err != nil {
		closeAll()
		log.Fatalf("generate: %v", err)
	}
	for i, s := range senders {
		if err := s.Flush(ctx); err != nil {
			closeAll()
			log.Fatalf("final flush[%d]: %v", i, err)
		}
	}
	for i, s := range senders {
		if err := s.Close(ctx); err != nil {
			log.Fatalf("close sender[%d]: %v", i, err)
		}
	}
	fmt.Fprintf(os.Stderr, "wrote %d rows × %d file(s) in %s\n",
		rows, parallel, time.Since(started).Round(time.Millisecond))
}

func openOutputs(path string, parallel int) ([]io.Writer, func(), error) {
	if parallel <= 1 {
		if path == "" {
			return []io.Writer{os.Stdout}, func() {}, nil
		}
		f, err := os.Create(path)
		if err != nil {
			return nil, nil, err
		}
		return []io.Writer{f}, func() { _ = f.Close() }, nil
	}
	if path == "" {
		return nil, nil, fmt.Errorf("--file is required when --parallel > 1")
	}
	files := make([]*os.File, 0, parallel)
	outs := make([]io.Writer, 0, parallel)
	closer := func() {
		for _, f := range files {
			_ = f.Close()
		}
	}
	for i := 0; i < parallel; i++ {
		name := fmt.Sprintf("%s.%d", path, i)
		f, err := os.Create(name)
		if err != nil {
			closer()
			return nil, nil, fmt.Errorf("create %s: %w", name, err)
		}
		files = append(files, f)
		outs = append(outs, f)
	}
	return outs, closer, nil
}

func runSimulation(ctx context.Context, senders []qdb.LineSender, sim common.Simulator) (uint64, error) {
	point := data.NewPoint()
	var rows uint64
	for !sim.Finished() {
		write := sim.Next(point)
		if write {
			for i, s := range senders {
				offset := time.Duration(i) * time.Millisecond
				if err := emitPoint(ctx, s, point, offset); err != nil {
					return rows, err
				}
			}
			rows++
		}
		point.Reset()
	}
	return rows, nil
}

// emitPoint maps one simulator Point onto a QWP row. Tag values that are not
// strings are demoted to typed columns, matching the convention used by the
// existing QuestDB ILP serializer (pkg/targets/questdb/serializer.go), so the
// resulting QuestDB table schema is identical regardless of wire protocol.
func emitPoint(ctx context.Context, s qdb.LineSender, p *data.Point, offset time.Duration) error {
	s = s.Table(string(p.MeasurementName()))

	tagKeys := p.TagKeys()
	for i, v := range p.TagValues() {
		if v == nil {
			continue
		}
		name := string(tagKeys[i])
		if sv, ok := v.(string); ok {
			s = s.Symbol(name, sv)
			continue
		}
		if err := appendColumn(s, name, v); err != nil {
			return fmt.Errorf("tag %q: %w", name, err)
		}
	}

	fieldKeys := p.FieldKeys()
	for i, v := range p.FieldValues() {
		if v == nil {
			continue
		}
		name := string(fieldKeys[i])
		if err := appendColumn(s, name, v); err != nil {
			return fmt.Errorf("field %q: %w", name, err)
		}
	}

	ts := p.Timestamp()
	if ts == nil {
		return s.AtNow(ctx)
	}
	return s.At(ctx, ts.Add(offset))
}

func appendColumn(s qdb.LineSender, name string, v interface{}) error {
	switch vv := v.(type) {
	case int:
		s.Int64Column(name, int64(vv))
	case int8:
		s.Int64Column(name, int64(vv))
	case int16:
		s.Int64Column(name, int64(vv))
	case int32:
		s.Int64Column(name, int64(vv))
	case int64:
		s.Int64Column(name, vv)
	case uint:
		s.Int64Column(name, int64(vv))
	case uint8:
		s.Int64Column(name, int64(vv))
	case uint16:
		s.Int64Column(name, int64(vv))
	case uint32:
		s.Int64Column(name, int64(vv))
	case float32:
		s.Float64Column(name, float64(vv))
	case float64:
		s.Float64Column(name, vv)
	case bool:
		s.BoolColumn(name, vv)
	case string:
		s.StringColumn(name, vv)
	default:
		return fmt.Errorf("unsupported value type %T", v)
	}
	return nil
}

// postHandshakeWriter buffers writes until it has observed the first
// "\r\n\r\n" (end of the HTTP upgrade request written by the WebSocket
// client), then drops everything up to and including the delimiter and
// passes subsequent writes straight through. The resulting dump
// contains only post-upgrade WebSocket frames, so a replayer can
// perform its own handshake against a live server.
type postHandshakeWriter struct {
	inner    io.Writer
	buf      []byte
	upgraded bool
}

func (w *postHandshakeWriter) Write(p []byte) (int, error) {
	if w.upgraded {
		if _, err := w.inner.Write(p); err != nil {
			return 0, err
		}
		return len(p), nil
	}
	w.buf = append(w.buf, p...)
	idx := bytes.Index(w.buf, []byte("\r\n\r\n"))
	if idx < 0 {
		return len(p), nil
	}
	tail := w.buf[idx+4:]
	w.buf = nil
	w.upgraded = true
	if len(tail) > 0 {
		if _, err := w.inner.Write(tail); err != nil {
			return len(p), err
		}
	}
	return len(p), nil
}
