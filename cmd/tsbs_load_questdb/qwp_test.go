package main

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	qdb "github.com/questdb/go-questdb-client/v4"
	"github.com/questdb/tsbs/pkg/data"
)

// recordingSender captures the row-builder calls the loader makes. It
// embeds qdb.QwpSender so it satisfies the interface without spelling out
// the column types the loader never uses; calling one of those panics,
// which is what we want in a test.
type recordingSender struct {
	qdb.QwpSender

	sb   strings.Builder
	rows []string

	flushes       int
	awaited       []int64
	awaitDeadline []time.Time
	nextFsn       int64
	closed        bool
	closeErr      error
	events        []string
}

func (s *recordingSender) Table(name string) qdb.LineSender {
	s.sb.Reset()
	s.sb.WriteString(name)
	return s
}

func (s *recordingSender) Symbol(name, val string) qdb.LineSender {
	fmt.Fprintf(&s.sb, " sym:%s=%s", name, val)
	return s
}

func (s *recordingSender) Int64Column(name string, val int64) qdb.LineSender {
	fmt.Fprintf(&s.sb, " i64:%s=%d", name, val)
	return s
}

func (s *recordingSender) Float64Column(name string, val float64) qdb.LineSender {
	fmt.Fprintf(&s.sb, " f64:%s=%v", name, val)
	return s
}

func (s *recordingSender) StringColumn(name, val string) qdb.LineSender {
	fmt.Fprintf(&s.sb, " str:%s=%s", name, val)
	return s
}

func (s *recordingSender) BoolColumn(name string, val bool) qdb.LineSender {
	fmt.Fprintf(&s.sb, " bool:%s=%t", name, val)
	return s
}

func (s *recordingSender) At(_ context.Context, ts time.Time) error {
	fmt.Fprintf(&s.sb, " ts:%d", ts.UnixNano())
	s.rows = append(s.rows, s.sb.String())
	s.sb.Reset()
	return nil
}

func (s *recordingSender) AtNano(_ context.Context, ts time.Time) error {
	fmt.Fprintf(&s.sb, " tsnano:%d", ts.UnixNano())
	s.rows = append(s.rows, s.sb.String())
	s.sb.Reset()
	return nil
}

func (s *recordingSender) Flush(_ context.Context) error {
	s.flushes++
	return nil
}

func (s *recordingSender) FlushAndGetSequence(_ context.Context) (int64, error) {
	s.flushes++
	s.nextFsn++
	return s.nextFsn, nil
}

func (s *recordingSender) AwaitAckedFsn(ctx context.Context, target int64) error {
	s.awaited = append(s.awaited, target)
	deadline, _ := ctx.Deadline()
	s.awaitDeadline = append(s.awaitDeadline, deadline)
	s.events = append(s.events, fmt.Sprintf("await:%d", target))
	return nil
}

func (s *recordingSender) Close(_ context.Context) error {
	s.closed = true
	s.events = append(s.events, "close")
	return s.closeErr
}

func newTestQwpProcessor() (*qwpProcessor, *recordingSender) {
	s := &recordingSender{}
	return &qwpProcessor{
		ctx:    context.Background(),
		sender: s,
		qwp:    s,
		intern: make(map[string]string),
	}, s
}

// newTestILPHTTPProcessor builds the processor as the ILP-over-HTTP path
// does: a plain LineSender with no QWP capabilities.
func newTestILPHTTPProcessor() (*qwpProcessor, *recordingSender) {
	s := &recordingSender{}
	return &qwpProcessor{
		ctx:    context.Background(),
		sender: s,
		intern: make(map[string]string),
	}, s
}

func TestIngestionProtocolContract(t *testing.T) {
	if defaultIngestionProtocol != protocolILP {
		t.Fatalf("default ingestion protocol = %q", defaultIngestionProtocol)
	}
	for _, value := range []string{"ilp", "ilp-http", "qwip"} {
		if err := validateIngestionProtocol(value); err != nil {
			t.Errorf("validateIngestionProtocol(%q): %v", value, err)
		}
	}
	for _, value := range []string{"", "qwp", "QWIP", " qwip", "qwip "} {
		if err := validateIngestionProtocol(value); err == nil {
			t.Errorf("validateIngestionProtocol(%q) succeeded", value)
		}
	}
}

func TestQwpWriteRow(t *testing.T) {
	cases := []struct {
		desc string
		line string
		want string
	}{
		{
			desc: "devops cpu row",
			line: "cpu,hostname=host_0,region=eu-west-1 usage_user=58i,usage_system=2i 1451606400000000000",
			want: "cpu sym:hostname=host_0 sym:region=eu-west-1 i64:usage_user=58 i64:usage_system=2 ts:1451606400000000000",
		},
		{
			desc: "float fields",
			line: "readings,name=truck_0 latitude=72.45,longitude=-159.15 1451606400000000000",
			want: "readings sym:name=truck_0 f64:latitude=72.45 f64:longitude=-159.15 ts:1451606400000000000",
		},
		{
			desc: "no tags",
			line: "cpu usage_user=1i 140",
			want: "cpu i64:usage_user=1 ts:140",
		},
		{
			desc: "boolean and quoted string fields",
			line: "diagnostics,name=truck_0 status=true,model=\"F-150\" 140",
			want: "diagnostics sym:name=truck_0 bool:status=true str:model=F-150 ts:140",
		},
		{
			desc: "bare string field",
			line: "diagnostics,name=truck_0 fleet=South 140",
			want: "diagnostics sym:name=truck_0 str:fleet=South ts:140",
		},
		{
			desc: "negative and exponent values",
			line: "cpu,hostname=host_1 a=-3i,b=-1.5,c=1e-05 140",
			want: "cpu sym:hostname=host_1 i64:a=-3 f64:b=-1.5 f64:c=1e-05 ts:140",
		},
	}

	for _, c := range cases {
		t.Run(c.desc, func(t *testing.T) {
			p, s := newTestQwpProcessor()
			if err := p.writeRow([]byte(c.line)); err != nil {
				t.Fatalf("writeRow(%q) returned error: %v", c.line, err)
			}
			if len(s.rows) != 1 {
				t.Fatalf("expected 1 row, got %d", len(s.rows))
			}
			if s.rows[0] != c.want {
				t.Errorf("got  %q\nwant %q", s.rows[0], c.want)
			}
		})
	}
}

func TestQwpWriteRowNanoTimestamps(t *testing.T) {
	nanoTimestamps = true
	defer func() { nanoTimestamps = false }()

	p, s := newTestQwpProcessor()
	line := "cpu,hostname=host_0 usage_user=1i 1451606400000000000"
	if err := p.writeRow([]byte(line)); err != nil {
		t.Fatalf("writeRow(%q) returned error: %v", line, err)
	}
	want := "cpu sym:hostname=host_0 i64:usage_user=1 tsnano:1451606400000000000"
	if len(s.rows) != 1 || s.rows[0] != want {
		t.Errorf("got  %v\nwant %q", s.rows, want)
	}
}

func TestQwpWriteRowErrors(t *testing.T) {
	cases := []struct {
		desc string
		line string
	}{
		{desc: "no fields or timestamp", line: "cpu,hostname=host_0"},
		{desc: "no timestamp", line: "cpu,hostname=host_0 usage_user=1i"},
		{desc: "malformed tag", line: "cpu,hostname usage_user=1i 140"},
		{desc: "malformed field", line: "cpu,hostname=host_0 usage_user 140"},
		{desc: "non-numeric timestamp", line: "cpu,hostname=host_0 usage_user=1i abc"},
	}

	for _, c := range cases {
		t.Run(c.desc, func(t *testing.T) {
			p, _ := newTestQwpProcessor()
			if err := p.writeRow([]byte(c.line)); err == nil {
				t.Errorf("writeRow(%q) expected an error, got nil", c.line)
			}
		})
	}
}

func TestQwpProcessBatch(t *testing.T) {
	bufPool = sync.Pool{
		New: func() interface{} {
			return bytes.NewBuffer(make([]byte, 0, 4*1024*1024))
		},
	}
	f := &factory{}
	b := f.New().(*batch)
	b.Append(data.LoadedPoint{
		Data: []byte("cpu,hostname=host_0 usage_user=1i,usage_system=2i 140"),
	})
	b.Append(data.LoadedPoint{
		Data: []byte("cpu,hostname=host_1 usage_user=3i,usage_system=4i 150"),
	})

	fatal = func(format string, args ...interface{}) {
		t.Errorf("fatal called unexpectedly: "+format, args...)
	}
	defer func() { fatal = t.Fatalf }()

	awaitAck = true
	defer func() { awaitAck = false }()

	p, s := newTestQwpProcessor()
	mCnt, rCnt := p.ProcessBatch(b, true)

	if mCnt != 4 {
		t.Errorf("metric count: got %d want 4", mCnt)
	}
	if rCnt != 2 {
		t.Errorf("row count: got %d want 2", rCnt)
	}
	if len(s.rows) != 2 {
		t.Fatalf("expected 2 rows sent, got %d", len(s.rows))
	}
	if s.flushes != 1 {
		t.Errorf("expected 1 flush, got %d", s.flushes)
	}
	if len(s.awaited) != 1 || s.awaited[0] != 1 {
		t.Errorf("expected the batch fsn to be awaited, got %v", s.awaited)
	}
}

// TestILPHTTPProcessBatch checks the ILP-over-HTTP path: same rows, but a
// plain Flush rather than publish-and-ack, and microsecond timestamps even
// when nanoseconds are requested, since AtNano is QWP-only.
func TestILPHTTPProcessBatch(t *testing.T) {
	bufPool = sync.Pool{
		New: func() interface{} {
			return bytes.NewBuffer(make([]byte, 0, 4*1024*1024))
		},
	}
	f := &factory{}
	b := f.New().(*batch)
	b.Append(data.LoadedPoint{
		Data: []byte("cpu,hostname=host_0 usage_user=1i,usage_system=2i 140"),
	})

	fatal = func(format string, args ...interface{}) {
		t.Errorf("fatal called unexpectedly: "+format, args...)
	}
	defer func() { fatal = t.Fatalf }()

	nanoTimestamps = true
	defer func() { nanoTimestamps = false }()

	p, s := newTestILPHTTPProcessor()
	mCnt, rCnt := p.ProcessBatch(b, true)

	if mCnt != 2 || rCnt != 1 {
		t.Errorf("counts: got %d metrics %d rows, want 2 and 1", mCnt, rCnt)
	}
	if s.flushes != 1 {
		t.Errorf("expected 1 flush, got %d", s.flushes)
	}
	if len(s.awaited) != 0 {
		t.Errorf("ILP/HTTP must not await an fsn, got %v", s.awaited)
	}
	want := "cpu sym:hostname=host_0 i64:usage_user=1 i64:usage_system=2 ts:140"
	if len(s.rows) != 1 || s.rows[0] != want {
		t.Errorf("rows:\n got  %v\n want %q", s.rows, want)
	}
}

func TestQwpProcessBatchNoLoad(t *testing.T) {
	bufPool = sync.Pool{
		New: func() interface{} {
			return bytes.NewBuffer(make([]byte, 0, 4*1024*1024))
		},
	}
	f := &factory{}
	b := f.New().(*batch)
	b.Append(data.LoadedPoint{
		Data: []byte("cpu,hostname=host_0 usage_user=1i 140"),
	})

	p, s := newTestQwpProcessor()
	if _, rCnt := p.ProcessBatch(b, false); rCnt != 1 {
		t.Errorf("row count: got %d want 1", rCnt)
	}
	if len(s.rows) != 0 || s.flushes != 0 {
		t.Errorf("nothing should have been sent with doLoad=false: %d rows, %d flushes", len(s.rows), s.flushes)
	}
}

func TestILPHTTPConfIgnoresQwpOverride(t *testing.T) {
	oldConf, oldAddr, oldTLS := qwpConfString, questdbILPHTTPAddr, useTLS
	defer func() {
		qwpConfString, questdbILPHTTPAddr, useTLS = oldConf, oldAddr, oldTLS
	}()

	qwpConfString = "ws::addr=qwp.example:9000;"
	questdbILPHTTPAddr = "http.example:9000"
	for _, tc := range []struct {
		tls  bool
		want string
	}{
		{tls: false, want: "http::addr=http.example:9000;auto_flush=off;"},
		{tls: true, want: "https::addr=http.example:9000;auto_flush=off;tls_verify=unsafe_off;"},
	} {
		useTLS = tc.tls
		if got := ilpHTTPConf(); got != tc.want {
			t.Errorf("ilpHTTPConf() = %q, want %q", got, tc.want)
		}
	}
}

func TestQwpCloseAwaitsLastPublishedFsnBeforeSenderClose(t *testing.T) {
	oldTimeout, oldConf, oldAwait := qwpCloseTimeoutMs, qwpConfString, awaitAck
	defer func() {
		qwpCloseTimeoutMs, qwpConfString, awaitAck = oldTimeout, oldConf, oldAwait
	}()
	awaitAck = false

	for _, tc := range []struct {
		name      string
		conf      string
		timeoutMs uint
	}{
		{name: "default config", timeoutMs: 60000},
		{name: "custom config cannot disable barrier", conf: "ws::addr=other:9000;close_flush_timeout_millis=0;", timeoutMs: 25},
	} {
		t.Run(tc.name, func(t *testing.T) {
			qwpConfString, qwpCloseTimeoutMs = tc.conf, tc.timeoutMs
			p, s := newTestQwpProcessor()
			if err := p.flush(); err != nil {
				t.Fatal(err)
			}
			if err := p.flush(); err != nil {
				t.Fatal(err)
			}

			before := time.Now()
			if err := p.closeSender(); err != nil {
				t.Fatalf("closeSender: %v", err)
			}
			if got, want := s.events, []string{"await:2", "close"}; fmt.Sprint(got) != fmt.Sprint(want) {
				t.Fatalf("events = %v, want %v", got, want)
			}
			if len(s.awaitDeadline) != 1 || s.awaitDeadline[0].IsZero() {
				t.Fatalf("await deadline = %v, want one bounded deadline", s.awaitDeadline)
			}
			remaining := s.awaitDeadline[0].Sub(before)
			want := time.Duration(tc.timeoutMs) * time.Millisecond
			if remaining <= 0 || remaining > want+10*time.Millisecond {
				t.Errorf("deadline remaining = %v, want approximately %v", remaining, want)
			}
		})
	}
}

func TestQwpZeroCloseTimeoutIsRejectedAndCannotSkipAckSilently(t *testing.T) {
	if err := validateQwpAckTimeout(protocolQWIP, 0); err == nil {
		t.Fatal("validateQwpAckTimeout(qwip, 0) succeeded")
	}
	if err := validateQwpAckTimeout(protocolILPHTTP, 0); err != nil {
		t.Fatalf("ILP/HTTP should not require a QWIP ack timeout: %v", err)
	}

	oldTimeout := qwpCloseTimeoutMs
	defer func() { qwpCloseTimeoutMs = oldTimeout }()
	qwpCloseTimeoutMs = 0
	p, s := newTestQwpProcessor()
	if err := p.flush(); err != nil {
		t.Fatal(err)
	}
	if err := p.closeSender(); err == nil {
		t.Fatal("closeSender with zero timeout succeeded")
	}
	if len(s.awaited) != 0 {
		t.Errorf("zero-timeout close unexpectedly awaited with an unbounded context: %v", s.awaited)
	}
	if !s.closed {
		t.Error("sender resources were not closed after zero-timeout error")
	}
}

func TestQwpConf(t *testing.T) {
	defer func() {
		qwpConfString, questdbQWPAddr, qwpUser, qwpPassword, qwpToken, qwpSFDir = "", "", "", "", "", ""
		useTLS = false
	}()

	cases := []struct {
		desc string
		set  func()
		want string
	}{
		{
			desc: "plain",
			set: func() {
				questdbQWPAddr = "127.0.0.1:9000"
			},
			want: "ws::addr=127.0.0.1:9000;auto_flush=off;close_flush_timeout_millis=60000;",
		},
		{
			desc: "tls with basic auth",
			set: func() {
				questdbQWPAddr = "host:9000"
				useTLS = true
				qwpUser = "admin"
				qwpPassword = "quest"
			},
			want: "wss::addr=host:9000;auto_flush=off;close_flush_timeout_millis=60000;tls_verify=unsafe_off;username=admin;password=quest;",
		},
		{
			desc: "store and forward",
			set: func() {
				questdbQWPAddr = "a:9000,b:9000"
				qwpSFDir = "/tmp/sf"
			},
			want: "ws::addr=a:9000,b:9000;auto_flush=off;close_flush_timeout_millis=60000;sf_dir=/tmp/sf;sender_id=tsbs-3;",
		},
		{
			desc: "explicit conf string wins",
			set: func() {
				questdbQWPAddr = "127.0.0.1:9000"
				qwpConfString = "ws::addr=other:9000;"
			},
			want: "ws::addr=other:9000;",
		},
	}

	for _, c := range cases {
		t.Run(c.desc, func(t *testing.T) {
			qwpConfString, questdbQWPAddr, qwpUser, qwpPassword, qwpToken, qwpSFDir = "", "", "", "", "", ""
			useTLS = false
			qwpCloseTimeoutMs = 60000
			c.set()
			if got := qwpConf(3); got != c.want {
				t.Errorf("got  %q\nwant %q", got, c.want)
			}
		})
	}
}
