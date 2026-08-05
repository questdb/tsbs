package main

import (
	"context"
	"errors"
	"io"
	"os"
	"reflect"
	"strings"
	"testing"

	"github.com/InfluxCommunity/influxdb3-go/v2/influxdb3"
	"github.com/questdb/tsbs/pkg/query"
)

type fakeRawFlightReader struct {
	batches      int
	nextCalls    int
	err          error
	releaseCalls int
}

func (r *fakeRawFlightReader) Next() bool {
	r.nextCalls++
	return r.nextCalls <= r.batches
}

func (r *fakeRawFlightReader) Err() error { return r.err }
func (r *fakeRawFlightReader) Release()   { r.releaseCalls++ }

type fakeFlightQueryClient struct {
	reader     rawFlightReader
	queryErr   error
	closeErr   error
	gotOptions *influxdb3.QueryOptions
	gotQuery   string
	queryCalls int
	closeCalls int
}

func (c *fakeFlightQueryClient) QueryWithOptions(_ context.Context, options *influxdb3.QueryOptions, queryText string) (rawFlightReader, error) {
	c.queryCalls++
	c.gotOptions = options
	c.gotQuery = queryText
	return c.reader, c.queryErr
}

func (c *fakeFlightQueryClient) Close() error {
	c.closeCalls++
	return c.closeErr
}

func TestValidateInfluxVersion(t *testing.T) {
	for _, version := range []string{"v1", "v2", "v3"} {
		if err := validateInfluxVersion(version); err != nil {
			t.Fatalf("validateInfluxVersion(%q): %v", version, err)
		}
	}
	for _, version := range []string{"", "3", "v4", "latest"} {
		if err := validateInfluxVersion(version); err == nil {
			t.Fatalf("validateInfluxVersion(%q) succeeded", version)
		}
	}
}

func TestValidateQueryOptions(t *testing.T) {
	tests := []struct {
		name          string
		version       string
		chunkSize     uint64
		printResponse bool
		debug         int
		wantErr       string
	}{
		{name: "v1 options unchanged", version: "v1", chunkSize: 10, printResponse: true, debug: 4},
		{name: "v2 options unchanged", version: "v2", chunkSize: 10, printResponse: true, debug: 4},
		{name: "v3 defaults", version: "v3"},
		{name: "v3 timing debug", version: "v3", debug: 1},
		{name: "v3 label debug", version: "v3", debug: 2},
		{name: "v3 request debug", version: "v3", debug: 3},
		{name: "v3 chunking", version: "v3", chunkSize: 1, wantErr: "chunk-response-size"},
		{name: "v3 printed responses", version: "v3", printResponse: true, wantErr: "print-responses"},
		{name: "v3 response debug", version: "v3", debug: 4, wantErr: "debug level 4"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateQueryOptions(tt.version, tt.chunkSize, tt.printResponse, tt.debug)
			if tt.wantErr == "" {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("error = %v, want substring %q", err, tt.wantErr)
			}
		})
	}
}

func TestNewFlightClientAllowsAuthenticationDisabledCore(t *testing.T) {
	client, err := NewFlightClient("http://127.0.0.1:8086", "benchmark", "")
	if err != nil {
		t.Fatal(err)
	}
	if client.database != "benchmark" || client.client == nil {
		t.Fatalf("bad client: %+v", client)
	}
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestFlightClientDoConsumesRawBatchesAndReleasesReader(t *testing.T) {
	reader := &fakeRawFlightReader{batches: 3}
	client := &fakeFlightQueryClient{reader: reader}
	flight := &FlightClient{client: client, database: "benchmark"}
	q := &query.HTTP{
		Path:             []byte("/query?epoch=ms&q=SELECT+mean%28%22usage_user%22%29+FROM+%22cpu%22"),
		HumanLabel:       []byte("mean cpu"),
		HumanDescription: []byte("mean CPU usage"),
		Method:           []byte("GET"),
	}

	stderr := captureStderr(t, func() {
		lag, err := flight.Do(q, &HTTPClientDoOptions{Debug: 3})
		if err != nil {
			t.Fatal(err)
		}
		if lag < 0 {
			t.Fatalf("lag = %f", lag)
		}
	})

	if client.gotQuery != `SELECT mean("usage_user") FROM "cpu"` {
		t.Fatalf("query = %q", client.gotQuery)
	}
	if client.gotOptions == nil || client.gotOptions.Database != "benchmark" || client.gotOptions.QueryType != influxdb3.InfluxQL {
		t.Fatalf("options = %+v", client.gotOptions)
	}
	if reader.nextCalls != 4 {
		t.Fatalf("Next calls = %d, want 4", reader.nextCalls)
	}
	if reader.releaseCalls != 1 {
		t.Fatalf("Release calls = %d, want 1", reader.releaseCalls)
	}
	for _, want := range []string{"mean cpu", "mean CPU usage", "request:"} {
		if !strings.Contains(stderr, want) {
			t.Fatalf("stderr %q does not contain %q", stderr, want)
		}
	}
}

func TestFlightClientDoFailures(t *testing.T) {
	queryFailure := errors.New("dial failed")
	terminalFailure := errors.New("stream failed")

	tests := []struct {
		name         string
		path         string
		client       *fakeFlightQueryClient
		wantErr      string
		wantReleases int
	}{
		{
			name:    "invalid path",
			path:    "/api/v3/query_influxql?q=SELECT+1",
			client:  &fakeFlightQueryClient{},
			wantErr: "invalid query path",
		},
		{
			name:    "missing query",
			path:    "/query?epoch=ms",
			client:  &fakeFlightQueryClient{},
			wantErr: "exactly one non-empty q parameter",
		},
		{
			name:    "duplicate query",
			path:    "/query?q=SELECT+1&q=SELECT+2",
			client:  &fakeFlightQueryClient{},
			wantErr: "exactly one non-empty q parameter",
		},
		{
			name:    "empty query",
			path:    "/query?q=",
			client:  &fakeFlightQueryClient{},
			wantErr: "exactly one non-empty q parameter",
		},
		{
			name:    "query call",
			path:    "/query?q=SELECT+1",
			client:  &fakeFlightQueryClient{queryErr: queryFailure},
			wantErr: "query failed: dial failed",
		},
		{
			name:    "nil raw reader",
			path:    "/query?q=SELECT+1",
			client:  &fakeFlightQueryClient{},
			wantErr: "query returned a nil raw reader",
		},
		{
			name:         "terminal reader",
			path:         "/query?q=SELECT+1",
			client:       &fakeFlightQueryClient{reader: &fakeRawFlightReader{batches: 2, err: terminalFailure}},
			wantErr:      "error reading results: stream failed",
			wantReleases: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			flight := &FlightClient{client: tt.client, database: "benchmark"}
			_, err := flight.Do(&query.HTTP{Path: []byte(tt.path)}, &HTTPClientDoOptions{})
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("error = %v, want substring %q", err, tt.wantErr)
			}
			if reader, ok := tt.client.reader.(*fakeRawFlightReader); ok && reader.releaseCalls != tt.wantReleases {
				t.Fatalf("Release calls = %d, want %d", reader.releaseCalls, tt.wantReleases)
			}
		})
	}
}

func TestFlightClientDoRejectsNilClientAndQuery(t *testing.T) {
	validQuery := &query.HTTP{Path: []byte("/query?q=SELECT+1")}
	tests := []struct {
		name    string
		flight  *FlightClient
		query   *query.HTTP
		wantErr string
	}{
		{name: "nil receiver", query: validQuery, wantErr: "Flight client is nil"},
		{name: "nil client", flight: &FlightClient{}, query: validQuery, wantErr: "Flight client is nil"},
		{name: "nil query", flight: &FlightClient{client: &fakeFlightQueryClient{}}, wantErr: "InfluxDB query is nil"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer func() {
				if recovered := recover(); recovered != nil {
					t.Fatalf("Do panicked: %v", recovered)
				}
			}()
			_, err := tt.flight.Do(tt.query, nil)
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("error = %v, want substring %q", err, tt.wantErr)
			}
		})
	}
}

func TestFlightClientClosePropagatesError(t *testing.T) {
	closeErr := errors.New("close failed")
	client := &fakeFlightQueryClient{closeErr: closeErr}
	flight := &FlightClient{client: client}
	if err := flight.Close(); !errors.Is(err, closeErr) {
		t.Fatalf("Close error = %v", err)
	}
	if client.closeCalls != 1 {
		t.Fatalf("Close calls = %d, want 1", client.closeCalls)
	}
}

func TestV3ProcessorUsesFlightFactoryAndPropagatesQueryAndClose(t *testing.T) {
	oldRunner, oldURLs, oldVersion, oldToken, oldFactory := runner, daemonUrls, influxVersion, authToken, newFlightClient
	t.Cleanup(func() {
		runner, daemonUrls, influxVersion, authToken, newFlightClient = oldRunner, oldURLs, oldVersion, oldToken, oldFactory
	})

	reader := &fakeRawFlightReader{batches: 1}
	fakeClient := &fakeFlightQueryClient{reader: reader, closeErr: errors.New("close failed")}
	factoryCalls := 0
	newFlightClient = func(hostURL, database, token string) (*FlightClient, error) {
		factoryCalls++
		if hostURL != "http://influx:8086" || database != "benchmark" || token != "token" {
			t.Fatalf("factory args = %q, %q, %q", hostURL, database, token)
		}
		return &FlightClient{client: fakeClient, database: database}, nil
	}
	runner = query.NewBenchmarkRunner(query.BenchmarkRunnerConfig{DBName: "benchmark"})
	daemonUrls = []string{"http://influx:8086"}
	influxVersion = "v3"
	authToken = "token"

	p := &processor{}
	p.Init(0)
	if factoryCalls != 1 || p.flightClient == nil || p.httpClient != nil {
		t.Fatalf("processor did not select Flight: calls=%d processor=%+v", factoryCalls, p)
	}
	stats, err := p.ProcessQuery(&query.HTTP{Path: []byte("/query?q=SELECT+1"), HumanLabel: []byte("label")}, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(stats) != 1 {
		t.Fatalf("stats = %+v", stats)
	}
	if latency := reflect.ValueOf(stats[0]).Elem().FieldByName("value").Float(); latency < 0 {
		t.Fatalf("stat latency = %f", latency)
	}
	if err := p.Close(); err == nil || err.Error() != "close failed" {
		t.Fatalf("Close error = %v", err)
	}
}

func TestProcessorImplementsCloser(t *testing.T) {
	var closer query.ProcessorCloser = &processor{}
	if err := closer.Close(); err != nil {
		t.Fatal(err)
	}
}

func captureStderr(t *testing.T, fn func()) string {
	t.Helper()
	old := os.Stderr
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	os.Stderr = w
	defer func() { os.Stderr = old }()

	fn()
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	output, err := io.ReadAll(r)
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	return string(output)
}
