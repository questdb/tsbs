package main

import (
	"bufio"
	"context"
	"crypto/rand"
	"crypto/sha1"
	"crypto/tls"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"strings"
	"time"

	qdb "github.com/questdb/go-questdb-client/v4"
	"github.com/questdb/tsbs/load"
)

const (
	qwpReplayClientID = "tsbs_load_questdb/replay-1"
	qwpReplayPath     = "/write/v4"
	qwpReplayVersion  = "1"
)

type qwpReplayDump struct {
	dir     string
	paths   []string
	frames  []uint64
	rows    uint64
	metrics uint64
	bytes   int64
}

func (d *qwpReplayDump) close() {
	if d != nil && d.dir != "" {
		_ = os.RemoveAll(d.dir)
	}
}

type qwpReplayEncoder struct {
	file        *os.File
	writer      *bufio.Writer
	sender      qdb.LineSender
	qwp         qdb.QwpSender
	processor   *qwpProcessor
	rowsPending uint
	frames      uint64
}

// runQwpPreencodedReplay deliberately separates producer work from the timed
// interval. The input is decoded and encoded through the real Go QWP encoder
// first; the timer starts only after the resulting WebSocket frame streams are
// complete. This is a server-capacity diagnostic, not an end-to-end client
// benchmark.
func runQwpPreencodedReplay(dec *qwpDecoder, config load.BenchmarkRunnerConfig) error {
	workers := int(config.Workers)
	if workers < 1 {
		workers = 1
	}
	batchSize := config.BatchSize
	if batchSize == 0 {
		batchSize = 10_000
	}

	preencodeStart := time.Now()
	dump, err := preencodeQwpReplay(dec, workers, batchSize, config.Limit)
	if err != nil {
		return err
	}
	defer dump.close()
	preencodeElapsed := time.Since(preencodeStart)

	fmt.Printf(
		"pre-encoded %d rows (%d metrics, %d bytes, %d frames) with %d encoder(s) in %.3fsec; excluded from replay rate\n",
		dump.rows,
		dump.metrics,
		dump.bytes,
		sumUint64(dump.frames),
		workers,
		preencodeElapsed.Seconds(),
	)
	if !config.DoLoad {
		return nil
	}

	endpoints, err := qwpReplayEndpoints(questdbQWPAddr)
	if err != nil {
		return err
	}

	type result struct {
		bytes int64
		acks  uint64
		err   error
	}
	results := make(chan result, workers)
	start := time.Now()
	for i := range dump.paths {
		i := i
		go func() {
			n, acks, err := replayQwpDump(
				endpoints[i%len(endpoints)],
				dump.paths[i],
				dump.frames[i],
				time.Duration(qwpCloseTimeoutMs)*time.Millisecond,
			)
			results <- result{bytes: n, acks: acks, err: err}
		}()
	}

	var replayedBytes int64
	var ackedFrames uint64
	var replayErr error
	for range dump.paths {
		result := <-results
		replayedBytes += result.bytes
		ackedFrames += result.acks
		if result.err != nil && replayErr == nil {
			replayErr = result.err
		}
	}
	elapsed := time.Since(start)
	if replayErr != nil {
		return replayErr
	}
	if replayedBytes != dump.bytes {
		return fmt.Errorf("replayed %d bytes, expected %d", replayedBytes, dump.bytes)
	}

	fmt.Printf(
		"replayed %d bytes and acknowledged %d QWP frames across %d connection(s)\n",
		replayedBytes,
		ackedFrames,
		workers,
	)
	fmt.Printf("\nSummary:\n")
	fmt.Printf(
		"loaded %d metrics in %.3fsec with %d workers (mean rate %.2f metrics/sec)\n",
		dump.metrics,
		elapsed.Seconds(),
		workers,
		float64(dump.metrics)/elapsed.Seconds(),
	)
	fmt.Printf(
		"loaded %d rows in %.3fsec with %d workers (mean rate %.2f rows/sec)\n",
		dump.rows,
		elapsed.Seconds(),
		workers,
		float64(dump.rows)/elapsed.Seconds(),
	)
	return nil
}

func preencodeQwpReplay(
	dec *qwpDecoder,
	workers int,
	batchSize uint,
	limit uint64,
) (_ *qwpReplayDump, retErr error) {
	dir, err := os.MkdirTemp("", "tsbs-qwp-replay-")
	if err != nil {
		return nil, fmt.Errorf("create replay directory: %w", err)
	}
	dump := &qwpReplayDump{
		dir:    dir,
		paths:  make([]string, workers),
		frames: make([]uint64, workers),
	}
	defer func() {
		if retErr != nil {
			dump.close()
		}
	}()

	ctx := context.Background()
	encoders := make([]*qwpReplayEncoder, workers)
	for i := range encoders {
		path := fmt.Sprintf("%s/worker-%03d.qwp-frames", dir, i)
		encoder, err := newQwpReplayEncoder(ctx, path)
		if err != nil {
			closeQwpReplayEncoders(ctx, encoders)
			return nil, err
		}
		dump.paths[i] = path
		encoders[i] = encoder
	}

	worker := 0
	rowsInDispatchBatch := uint(0)
	for limit == 0 || dump.rows < limit {
		schemaID, row, err := dec.next()
		if err == io.EOF {
			break
		}
		if err != nil {
			closeQwpReplayEncoders(ctx, encoders)
			return nil, fmt.Errorf("decode input row %d: %w", dump.rows, err)
		}
		if schemaID >= uint64(len(dec.schemas)) {
			closeQwpReplayEncoders(ctx, encoders)
			return nil, errors.New("QWP row references an undefined schema")
		}

		encoder := encoders[worker]
		schema := dec.schemas[schemaID]
		if err := encoder.processor.writeBinaryRow(schema, row, dec.strings); err != nil {
			closeQwpReplayEncoders(ctx, encoders)
			return nil, fmt.Errorf("encode input row %d: %w", dump.rows, err)
		}
		encoder.rowsPending++
		dump.rows++
		dump.metrics += uint64(len(schema.fieldKeys))
		rowsInDispatchBatch++

		if rowsInDispatchBatch == batchSize {
			if err := encoder.flush(ctx); err != nil {
				closeQwpReplayEncoders(ctx, encoders)
				return nil, fmt.Errorf("flush replay encoder %d: %w", worker, err)
			}
			rowsInDispatchBatch = 0
			worker = (worker + 1) % workers
		}
	}
	if rowsInDispatchBatch > 0 {
		if err := encoders[worker].flush(ctx); err != nil {
			closeQwpReplayEncoders(ctx, encoders)
			return nil, fmt.Errorf("flush final replay encoder %d: %w", worker, err)
		}
	}

	if err := closeQwpReplayEncoders(ctx, encoders); err != nil {
		return nil, err
	}
	for i, encoder := range encoders {
		dump.frames[i] = encoder.frames
		info, err := os.Stat(dump.paths[i])
		if err != nil {
			return nil, fmt.Errorf("stat replay stream %d: %w", i, err)
		}
		dump.bytes += info.Size()
	}
	return dump, nil
}

func newQwpReplayEncoder(ctx context.Context, path string) (*qwpReplayEncoder, error) {
	file, err := os.Create(path)
	if err != nil {
		return nil, fmt.Errorf("create replay stream %s: %w", path, err)
	}
	writer := bufio.NewWriterSize(file, 4*1024*1024)
	sink := &qwpPostHandshakeWriter{inner: writer}
	sender, err := qdb.NewLineSender(
		ctx,
		qdb.WithQwp(),
		qdb.WithQwpDumpWriter(sink),
		qdb.WithAutoFlushRows(0),
		qdb.WithAutoFlushBytes(0),
		qdb.WithAutoFlushInterval(0),
		// The pre-encode encoder flushes and awaits the fake dump endpoint's
		// ACK once per batch, so in-flight is already bounded to one frame.
		// (The client's old WithInFlightWindow option was removed in the
		// cursor-mode refactor; this loop never needed it.)
		qdb.WithCloseFlushTimeout(2*time.Minute),
	)
	if err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("create replay encoder: %w", err)
	}
	qwp, ok := sender.(qdb.QwpSender)
	if !ok {
		_ = sender.Close(ctx)
		_ = file.Close()
		return nil, errors.New("QWP dump sender does not implement QwpSender")
	}
	processor := &qwpProcessor{
		ctx:    ctx,
		sender: sender,
		qwp:    qwp,
		intern: make(map[string]string),
	}
	return &qwpReplayEncoder{
		file:      file,
		writer:    writer,
		sender:    sender,
		qwp:       qwp,
		processor: processor,
	}, nil
}

func (e *qwpReplayEncoder) flush(ctx context.Context) error {
	if e == nil || e.rowsPending == 0 {
		return nil
	}
	fsn, err := e.qwp.FlushAndGetSequence(ctx)
	if err != nil {
		return err
	}
	// The fake dump endpoint ACKs the frame. Waiting here keeps the untimed
	// pre-encode phase bounded and guarantees the complete frame reached disk.
	if err := e.qwp.AwaitAckedFsn(ctx, fsn); err != nil {
		return err
	}
	e.rowsPending = 0
	e.frames++
	return nil
}

func closeQwpReplayEncoders(ctx context.Context, encoders []*qwpReplayEncoder) error {
	var firstErr error
	for _, encoder := range encoders {
		if encoder == nil || encoder.sender == nil {
			continue
		}
		if err := encoder.sender.Close(ctx); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("close replay encoder: %w", err)
		}
		encoder.sender = nil
	}
	for _, encoder := range encoders {
		if encoder == nil {
			continue
		}
		if encoder.writer != nil {
			if err := encoder.writer.Flush(); err != nil && firstErr == nil {
				firstErr = fmt.Errorf("flush replay stream: %w", err)
			}
			encoder.writer = nil
		}
		if encoder.file != nil {
			// Keep dump writeback out of the timed replay interval. The file
			// remains resident in the page cache, but it is clean before the
			// benchmark starts.
			if err := encoder.file.Sync(); err != nil && firstErr == nil {
				firstErr = fmt.Errorf("sync replay stream: %w", err)
			}
			if err := encoder.file.Close(); err != nil && firstErr == nil {
				firstErr = fmt.Errorf("close replay stream: %w", err)
			}
			encoder.file = nil
		}
	}
	return firstErr
}

// qwpPostHandshakeWriter removes the fake endpoint's HTTP request from a QWP
// client dump. Everything after the first header terminator is already masked,
// client-to-server WebSocket framing and can be replayed verbatim.
type qwpPostHandshakeWriter struct {
	inner    io.Writer
	pending  []byte
	upgraded bool
}

func (w *qwpPostHandshakeWriter) Write(p []byte) (int, error) {
	if w.upgraded {
		if _, err := w.inner.Write(p); err != nil {
			return 0, err
		}
		return len(p), nil
	}
	w.pending = append(w.pending, p...)
	idx := strings.Index(string(w.pending), "\r\n\r\n")
	if idx < 0 {
		return len(p), nil
	}
	tail := w.pending[idx+4:]
	w.pending = nil
	w.upgraded = true
	if len(tail) > 0 {
		if _, err := w.inner.Write(tail); err != nil {
			return len(p), err
		}
	}
	return len(p), nil
}

func qwpReplayEndpoints(raw string) ([]string, error) {
	parts := strings.Split(raw, ",")
	endpoints := make([]string, 0, len(parts))
	for _, part := range parts {
		endpoint := strings.TrimSpace(part)
		if endpoint == "" {
			continue
		}
		if _, _, err := net.SplitHostPort(endpoint); err != nil {
			return nil, fmt.Errorf("invalid QWP address %q: %w", endpoint, err)
		}
		endpoints = append(endpoints, endpoint)
	}
	if len(endpoints) == 0 {
		return nil, errors.New("QWP address is empty")
	}
	return endpoints, nil
}

func replayQwpDump(
	endpoint string,
	path string,
	expectedFrames uint64,
	drainTimeout time.Duration,
) (int64, uint64, error) {
	file, err := os.Open(path)
	if err != nil {
		return 0, 0, fmt.Errorf("open replay stream: %w", err)
	}
	defer file.Close()

	conn, err := dialQwpReplay(endpoint)
	if err != nil {
		return 0, 0, err
	}
	defer conn.Close()

	reader, err := upgradeQwpReplay(conn, endpoint)
	if err != nil {
		return 0, 0, err
	}

	type ackResult struct {
		count uint64
		err   error
	}
	ackCh := make(chan ackResult, 1)
	go func() {
		count, err := readQwpReplayResponses(reader)
		ackCh <- ackResult{count: count, err: err}
	}()

	n, err := io.Copy(conn, file)
	if err != nil {
		return n, 0, fmt.Errorf("send replay stream: %w", err)
	}
	// Dump mode records data frames, not the client's transport shutdown.
	// Half-close the write side so QuestDB knows no more frames are coming while
	// leaving the read side open for the final cumulative ACK.
	if closeWriter, ok := conn.(interface{ CloseWrite() error }); ok {
		if err := closeWriter.CloseWrite(); err != nil {
			return n, 0, fmt.Errorf("close replay write side: %w", err)
		}
	}
	if drainTimeout <= 0 {
		drainTimeout = 60 * time.Second
	}
	if err := conn.SetReadDeadline(time.Now().Add(drainTimeout)); err != nil {
		return n, 0, fmt.Errorf("set replay drain deadline: %w", err)
	}

	result := <-ackCh
	if result.err != nil && !(errors.Is(result.err, io.EOF) && result.count == expectedFrames) {
		return n, result.count, result.err
	}
	if result.count != expectedFrames {
		return n, result.count, fmt.Errorf(
			"server acknowledged %d frames from %s, expected %d",
			result.count,
			path,
			expectedFrames,
		)
	}
	return n, result.count, nil
}

func dialQwpReplay(endpoint string) (net.Conn, error) {
	if !useTLS {
		conn, err := net.Dial("tcp", endpoint)
		if err != nil {
			return nil, fmt.Errorf("dial %s: %w", endpoint, err)
		}
		return conn, nil
	}
	conn, err := tls.Dial("tcp", endpoint, &tls.Config{
		InsecureSkipVerify: true,
		MinVersion:         tls.VersionTLS12,
	})
	if err != nil {
		return nil, fmt.Errorf("dial %s: %w", endpoint, err)
	}
	return conn, nil
}

func upgradeQwpReplay(conn net.Conn, endpoint string) (*bufio.Reader, error) {
	keyBytes := make([]byte, 16)
	if _, err := rand.Read(keyBytes); err != nil {
		return nil, fmt.Errorf("generate WebSocket key: %w", err)
	}
	key := base64.StdEncoding.EncodeToString(keyBytes)

	authorization := ""
	if qwpUser != "" {
		token := base64.StdEncoding.EncodeToString([]byte(qwpUser + ":" + qwpPassword))
		authorization = "Authorization: Basic " + token + "\r\n"
	} else if qwpToken != "" {
		authorization = "Authorization: Bearer " + qwpToken + "\r\n"
	}
	request := "GET " + qwpReplayPath + " HTTP/1.1\r\n" +
		"Host: " + endpoint + "\r\n" +
		"Upgrade: websocket\r\n" +
		"Connection: Upgrade\r\n" +
		"Sec-WebSocket-Version: 13\r\n" +
		"Sec-WebSocket-Key: " + key + "\r\n" +
		"X-QWP-Max-Version: " + qwpReplayVersion + "\r\n" +
		"X-QWP-Client-Id: " + qwpReplayClientID + "\r\n" +
		authorization +
		"\r\n"
	if _, err := io.WriteString(conn, request); err != nil {
		return nil, fmt.Errorf("send QWP upgrade: %w", err)
	}

	reader := bufio.NewReader(conn)
	response, err := http.ReadResponse(reader, &http.Request{Method: http.MethodGet})
	if err != nil {
		return nil, fmt.Errorf("read QWP upgrade: %w", err)
	}
	if response.Body != nil {
		defer response.Body.Close()
	}
	if response.StatusCode != http.StatusSwitchingProtocols {
		return nil, fmt.Errorf("QWP upgrade returned %s", response.Status)
	}
	if !headerHasToken(response.Header, "Upgrade", "websocket") ||
		!headerHasToken(response.Header, "Connection", "upgrade") {
		return nil, errors.New("QWP upgrade response is missing WebSocket headers")
	}
	if got, want := response.Header.Get("Sec-WebSocket-Accept"), webSocketAccept(key); got != want {
		return nil, fmt.Errorf("invalid Sec-WebSocket-Accept: got %q", got)
	}
	return reader, nil
}

func headerHasToken(header http.Header, name, token string) bool {
	for _, value := range header.Values(name) {
		for _, part := range strings.Split(value, ",") {
			if strings.EqualFold(strings.TrimSpace(part), token) {
				return true
			}
		}
	}
	return false
}

func webSocketAccept(key string) string {
	hash := sha1.Sum([]byte(key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"))
	return base64.StdEncoding.EncodeToString(hash[:])
}

// readQwpReplayResponses consumes unmasked server WebSocket frames until the
// close response. It validates every QWP ACK and returns the number of data
// frames cumulatively acknowledged by the server.
func readQwpReplayResponses(reader *bufio.Reader) (uint64, error) {
	var acked uint64
	for {
		opcode, payload, err := readServerWebSocketFrame(reader)
		if err != nil {
			return acked, fmt.Errorf("read QWP replay response: %w", err)
		}
		switch opcode {
		case 0x2: // binary QWP ACK
			if len(payload) < 1 {
				return acked, errors.New("empty QWP ACK")
			}
			switch payload[0] {
			case 0x00: // STATUS_OK
				if len(payload) < 11 {
					return acked, fmt.Errorf("short QWP OK ACK: %d bytes", len(payload))
				}
				sequence := int64(binary.LittleEndian.Uint64(payload[1:9]))
				if sequence < 0 {
					return acked, fmt.Errorf("negative QWP ACK sequence %d", sequence)
				}
				acked = uint64(sequence) + 1
			case 0x02: // unsolicited durable watermark; not a frame ACK
				continue
			default:
				message := "server rejected QWP frame"
				if len(payload) >= 11 {
					messageLen := int(binary.LittleEndian.Uint16(payload[9:11]))
					if len(payload) >= 11+messageLen {
						message = string(payload[11 : 11+messageLen])
					}
				}
				return acked, fmt.Errorf("QWP status 0x%02x: %s", payload[0], message)
			}
		case 0x8: // close
			return acked, nil
		case 0x9, 0xA: // ping/pong
			continue
		default:
			return acked, fmt.Errorf("unexpected server WebSocket opcode 0x%x", opcode)
		}
	}
}

func readServerWebSocketFrame(reader *bufio.Reader) (byte, []byte, error) {
	var header [2]byte
	if _, err := io.ReadFull(reader, header[:]); err != nil {
		return 0, nil, err
	}
	fin := header[0]&0x80 != 0
	opcode := header[0] & 0x0F
	if !fin {
		return 0, nil, errors.New("fragmented server WebSocket frame")
	}
	if header[1]&0x80 != 0 {
		return 0, nil, errors.New("masked server WebSocket frame")
	}

	length := uint64(header[1] & 0x7F)
	switch length {
	case 126:
		var extended [2]byte
		if _, err := io.ReadFull(reader, extended[:]); err != nil {
			return 0, nil, err
		}
		length = uint64(binary.BigEndian.Uint16(extended[:]))
	case 127:
		var extended [8]byte
		if _, err := io.ReadFull(reader, extended[:]); err != nil {
			return 0, nil, err
		}
		length = binary.BigEndian.Uint64(extended[:])
	}
	if length > 16*1024*1024 {
		return 0, nil, fmt.Errorf("server WebSocket frame too large: %d", length)
	}
	payload := make([]byte, int(length))
	if _, err := io.ReadFull(reader, payload); err != nil {
		return 0, nil, err
	}
	return opcode, payload, nil
}

func sumUint64(values []uint64) uint64 {
	var total uint64
	for _, value := range values {
		total += value
	}
	return total
}
