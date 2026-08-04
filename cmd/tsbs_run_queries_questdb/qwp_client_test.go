package main

import (
	"errors"
	"reflect"
	"testing"
)

func TestQueryProtocolContract(t *testing.T) {
	if defaultQueryProtocol != protocolPGWire {
		t.Fatalf("default query protocol = %q", defaultQueryProtocol)
	}
	for _, value := range []string{"pgwire", "http", "qwep"} {
		got, err := resolveQueryProtocol(value, false)
		if err != nil || got != value {
			t.Errorf("resolveQueryProtocol(%q, false) = %q, %v", value, got, err)
		}
	}
	if got, err := resolveQueryProtocol("pgwire", true); err != nil || got != "http" {
		t.Fatalf("legacy use-http = %q, %v", got, err)
	}
	for _, value := range []string{"", "pg", "qwp", "QWEP", " qwep"} {
		if _, err := resolveQueryProtocol(value, false); err == nil {
			t.Errorf("resolveQueryProtocol(%q, false) succeeded", value)
		}
	}
}

func TestDrainQwpRowsConsumesAllBatchesAndPropagatesErrors(t *testing.T) {
	visited := 0
	rows, err := drainQwpRows(func(yield func(int, error) bool) {
		for _, count := range []int{2, 3, 5} {
			visited++
			if !yield(count, nil) {
				return
			}
		}
	})
	if err != nil || rows != 10 || visited != 3 {
		t.Fatalf("rows=%d visited=%d err=%v", rows, visited, err)
	}

	marker := errors.New("terminal QWEP batch error")
	rows, err = drainQwpRows(func(yield func(int, error) bool) {
		yield(4, nil)
		yield(0, marker)
	})
	if rows != 4 || !errors.Is(err, marker) {
		t.Fatalf("rows=%d err=%v", rows, err)
	}
}

func TestQwpBindActionsPreserveOrderAndTypes(t *testing.T) {
	got := qwpBindActions([]interface{}{"host", float64(42), 3.5, true, nil, struct{ X int }{7}})
	want := []qwpBindAction{
		{index: 0, kind: qwpBindVarchar, stringValue: "host"},
		{index: 1, kind: qwpBindLong, longValue: 42},
		{index: 2, kind: qwpBindDouble, doubleValue: 3.5},
		{index: 3, kind: qwpBindBoolean, boolValue: true},
		{index: 4, kind: qwpBindNullVarchar},
		{index: 5, kind: qwpBindVarchar, stringValue: "{7}"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("actions = %#v, want %#v", got, want)
	}
}
