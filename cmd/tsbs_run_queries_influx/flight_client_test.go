package main

import (
	"testing"

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

func TestProcessorImplementsCloser(t *testing.T) {
	var closer query.ProcessorCloser = &processor{}
	if err := closer.Close(); err != nil {
		t.Fatal(err)
	}
}
