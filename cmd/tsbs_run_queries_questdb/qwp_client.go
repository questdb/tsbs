package main

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	qdb "github.com/questdb/go-questdb-client/v4"
	"github.com/questdb/tsbs/pkg/query"
)

// QwpClient runs queries over the QuestDB Wire Protocol, which streams
// results back as columnar batches on the same WebSocket the loader uses
// for ingestion.
//
// A client is not safe for concurrent Query/Exec, so each worker owns one.
type QwpClient struct {
	client *qdb.QwpQueryClient
	ctx    context.Context
	opts   *QwpClientDoOptions
}

// QwpClientDoOptions mirrors HTTPClientDoOptions so both transports report
// the same debug output.
type QwpClientDoOptions struct {
	Debug                int
	PrettyPrintResponses bool
}

// NewQwpClient connects a query client using the given configuration
// string, for example "ws::addr=localhost:9000;".
func NewQwpClient(conf string, opts *QwpClientDoOptions) (*QwpClient, error) {
	ctx := context.Background()
	client, err := qdb.QwpQueryClientFromConf(ctx, conf)
	if err != nil {
		return nil, err
	}
	return &QwpClient{client: client, ctx: ctx, opts: opts}, nil
}

func (c *QwpClient) Close() {
	if c.client != nil {
		c.client.Close(c.ctx)
	}
}

// Do runs one query and returns its latency in milliseconds. Like the
// other transports it consumes the whole result set, so the measurement
// covers streaming every row back, not just the server's first response.
func (c *QwpClient) Do(hq *query.HTTP) (float64, error) {
	sql, params, err := sqlFromQuery(hq)
	if err != nil {
		return 0, err
	}

	start := time.Now()

	q := c.client.Query(c.ctx, sql, qwpBinds(params))
	defer q.Close()

	var rows int64
	for batch, err := range q.Batches() {
		if err != nil {
			return 0, fmt.Errorf("query failed: %v (sql: %s)", err, sql)
		}
		rows += int64(batch.RowCount())
	}

	lag := float64(time.Since(start).Nanoseconds()) / 1e6

	switch c.opts.Debug {
	case 1:
		fmt.Fprintf(os.Stderr, "debug: %s in %7.2fms\n", hq.HumanLabel, lag)
	case 2, 3:
		fmt.Fprintf(os.Stderr, "debug: %s in %7.2fms -- %s\n", hq.HumanLabel, lag, hq.HumanDescription)
	default:
		if c.opts.Debug >= 4 {
			fmt.Fprintf(os.Stderr, "debug: %s in %7.2fms -- %s\n", hq.HumanLabel, lag, hq.HumanDescription)
			fmt.Fprintf(os.Stderr, "debug:   sql: %s\n", sql)
			fmt.Fprintf(os.Stderr, "debug:   rows: %d\n", rows)
		}
	}
	if c.opts.PrettyPrintResponses {
		fmt.Printf("%s -- %d rows\n", hq.HumanLabel, rows)
	}

	return lag, nil
}

// qwpBinds binds the scalar parameters left after array inlining. The
// generator emits them as JSON, so numbers arrive as float64 and
// timestamps as strings; QuestDB casts a varchar bind to the column type
// it is compared against, which is what the PostgreSQL path relies on
// too.
func qwpBinds(params []interface{}) qdb.QwpQueryOption {
	return qdb.WithQwpQueryBinds(func(b *qdb.QwpBinds) {
		for i, p := range params {
			// Bind indexes are zero-based: $1 in the SQL is index 0,
			// and they must be set in order.
			idx := i
			switch v := p.(type) {
			case string:
				b.VarcharBind(idx, v)
			case float64:
				// JSON has no integer type: bind whole numbers as
				// longs so they compare against integer columns.
				if v == float64(int64(v)) {
					b.LongBind(idx, int64(v))
				} else {
					b.DoubleBind(idx, v)
				}
			case bool:
				b.BooleanBind(idx, v)
			case nil:
				b.NullVarcharBind(idx)
			default:
				b.VarcharBind(idx, fmt.Sprintf("%v", v))
			}
		}
	})
}

// qwpConf builds the query client configuration string.
func qwpConf() string {
	if qwpQueryConf != "" {
		return qwpQueryConf
	}

	var sb strings.Builder
	if qwpUseTLS {
		sb.WriteString("wss::")
	} else {
		sb.WriteString("ws::")
	}
	sb.WriteString("addr=")
	sb.WriteString(qwpAddr)
	sb.WriteString(";")
	if qwpUseTLS {
		sb.WriteString("tls_verify=unsafe_off;")
	}
	if username != "" {
		sb.WriteString("username=")
		sb.WriteString(username)
		sb.WriteString(";password=")
		sb.WriteString(password)
		sb.WriteString(";")
	}
	return sb.String()
}
