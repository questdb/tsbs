package main

import (
	"testing"

	"github.com/questdb/tsbs/pkg/query"
)

func TestSQLFromQuery(t *testing.T) {
	cases := []struct {
		desc       string
		q          *query.HTTP
		wantSQL    string
		wantParams []interface{}
		wantErr    bool
	}{
		{
			desc: "array parameter is inlined and the rest renumbered",
			q: &query.HTTP{
				RawQuery: []byte("SELECT max(usage_user) FROM cpu WHERE hostname IN $1 AND timestamp >= $2 AND timestamp < $3"),
				Body:     []byte(`[["host_1","host_2"],"2016-01-01T00:00:00Z","2016-01-01T01:00:00Z"]`),
			},
			wantSQL:    "SELECT max(usage_user) FROM cpu WHERE hostname IN ('host_1','host_2') AND timestamp >= $1 AND timestamp < $2",
			wantParams: []interface{}{"2016-01-01T00:00:00Z", "2016-01-01T01:00:00Z"},
		},
		{
			desc: "no array parameters leaves the placeholders alone",
			q: &query.HTTP{
				RawQuery: []byte("SELECT max(usage_user) FROM cpu WHERE timestamp < $1"),
				Body:     []byte(`["2016-01-01T01:00:00Z"]`),
			},
			wantSQL:    "SELECT max(usage_user) FROM cpu WHERE timestamp < $1",
			wantParams: []interface{}{"2016-01-01T01:00:00Z"},
		},
		{
			desc: "query with no parameters at all",
			q: &query.HTTP{
				Path: []byte("/exec?count=false&query=SELECT+*+FROM+cpu+latest+by+hostname"),
			},
			wantSQL: "SELECT * FROM cpu latest by hostname",
		},
		{
			desc:    "path without a query string",
			q:       &query.HTTP{Path: []byte("/exec")},
			wantErr: true,
		},
		{
			desc:    "path with no query parameter",
			q:       &query.HTTP{Path: []byte("/exec?count=false")},
			wantErr: true,
		},
		{
			desc: "malformed parameter JSON",
			q: &query.HTTP{
				RawQuery: []byte("SELECT 1 FROM cpu WHERE timestamp < $1"),
				Body:     []byte("not json"),
			},
			wantErr: true,
		},
	}

	for _, c := range cases {
		t.Run(c.desc, func(t *testing.T) {
			sql, params, err := sqlFromQuery(c.q)
			if c.wantErr {
				if err == nil {
					t.Fatalf("expected an error, got sql %q", sql)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if sql != c.wantSQL {
				t.Errorf("sql:\n got  %q\n want %q", sql, c.wantSQL)
			}
			if len(params) != len(c.wantParams) {
				t.Fatalf("params: got %v want %v", params, c.wantParams)
			}
			for i := range params {
				if params[i] != c.wantParams[i] {
					t.Errorf("param %d: got %v want %v", i, params[i], c.wantParams[i])
				}
			}
		})
	}
}
