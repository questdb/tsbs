package main

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/InfluxCommunity/influxdb3-go/v2/influxdb3"
	"github.com/questdb/tsbs/pkg/query"
)

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

func TestFlightClientDoUsesGeneratedInfluxQLPathAndOptionsAndDrainsRows(t *testing.T) {
	iterator := &fakeFlightIterator{rows: []map[string]any{{"value": 1}, {"value": 2}, {"value": 3}}}
	executor := &fakeFlightQueryExecutor{iterator: iterator}
	client := &FlightClient{client: executor, database: "benchmark"}
	generated := &query.HTTP{Path: []byte("/query?q=SELECT+max%28usage_user%29+FROM+cpu+WHERE+hostname+%3D+%27host_1%27")}

	if _, err := client.Do(generated, nil); err != nil {
		t.Fatal(err)
	}
	if executor.query != "SELECT max(usage_user) FROM cpu WHERE hostname = 'host_1'" {
		t.Fatalf("query = %q", executor.query)
	}
	wantOptions := influxdb3.QueryOptions{Database: "benchmark", QueryType: influxdb3.InfluxQL}
	if executor.options == nil || !reflect.DeepEqual(*executor.options, wantOptions) {
		t.Fatalf("options = %#v, want %#v", executor.options, wantOptions)
	}
	if iterator.values != len(iterator.rows) {
		t.Fatalf("consumed rows = %d, want %d", iterator.values, len(iterator.rows))
	}
}

func TestFlightClientDoPropagatesTerminalIteratorError(t *testing.T) {
	marker := errors.New("terminal Arrow Flight error")
	iterator := &fakeFlightIterator{rows: []map[string]any{{"value": 1}, {"value": 2}}, err: marker}
	client := &FlightClient{client: &fakeFlightQueryExecutor{iterator: iterator}, database: "benchmark"}

	_, err := client.Do(&query.HTTP{Path: []byte("/query?q=SELECT+%2A+FROM+cpu")}, nil)
	if !errors.Is(err, marker) {
		t.Fatalf("error = %v, want marker", err)
	}
	if iterator.values != len(iterator.rows) {
		t.Fatalf("consumed rows before terminal error = %d, want %d", iterator.values, len(iterator.rows))
	}
}

func TestProcessorImplementsCloser(t *testing.T) {
	var closer query.ProcessorCloser = &processor{}
	if err := closer.Close(); err != nil {
		t.Fatal(err)
	}
}

type fakeFlightQueryExecutor struct {
	iterator flightIterator
	options  *influxdb3.QueryOptions
	query    string
}

func (executor *fakeFlightQueryExecutor) QueryWithOptions(_ context.Context, options *influxdb3.QueryOptions, query string) (flightIterator, error) {
	executor.options = options
	executor.query = query
	return executor.iterator, nil
}

func (*fakeFlightQueryExecutor) Close() error { return nil }

type fakeFlightIterator struct {
	rows   []map[string]any
	index  int
	values int
	err    error
}

func (iterator *fakeFlightIterator) Next() bool {
	if iterator.index >= len(iterator.rows) {
		return false
	}
	iterator.index++
	return true
}
func (iterator *fakeFlightIterator) Value() map[string]any {
	iterator.values++
	return iterator.rows[iterator.index-1]
}
func (iterator *fakeFlightIterator) Err() error { return iterator.err }
