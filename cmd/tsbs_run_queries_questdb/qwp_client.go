package main

import (
	"context"
	"fmt"
	"iter"
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

func (c *QwpClient) Close() error {
	if c.client == nil {
		return nil
	}
	return c.client.Close(c.ctx)
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

	rows, err := drainQwpRows(func(yield func(int, error) bool) {
		for batch, err := range q.Batches() {
			count := 0
			if err == nil {
				count = batch.RowCount()
			}
			if !yield(count, err) {
				return
			}
		}
	})
	if err != nil {
		return 0, fmt.Errorf("query failed: %v (sql: %s)", err, sql)
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

func drainQwpRows(batches iter.Seq2[int, error]) (int64, error) {
	var rows int64
	for count, err := range batches {
		if err != nil {
			return rows, err
		}
		rows += int64(count)
	}
	return rows, nil
}

type qwpBindKind uint8

const (
	qwpBindVarchar qwpBindKind = iota
	qwpBindLong
	qwpBindDouble
	qwpBindBoolean
	qwpBindNullVarchar
)

type qwpBindAction struct {
	index       int
	kind        qwpBindKind
	stringValue string
	longValue   int64
	doubleValue float64
	boolValue   bool
}

// qwpBindActions converts scalar parameters into ordered bind operations.
// JSON numbers arrive as float64; whole numbers are bound as longs.
func qwpBindActions(params []interface{}) []qwpBindAction {
	actions := make([]qwpBindAction, 0, len(params))
	for i, p := range params {
		action := qwpBindAction{index: i}
		switch v := p.(type) {
		case string:
			action.kind = qwpBindVarchar
			action.stringValue = v
		case float64:
			if v == float64(int64(v)) {
				action.kind = qwpBindLong
				action.longValue = int64(v)
			} else {
				action.kind = qwpBindDouble
				action.doubleValue = v
			}
		case bool:
			action.kind = qwpBindBoolean
			action.boolValue = v
		case nil:
			action.kind = qwpBindNullVarchar
		default:
			action.kind = qwpBindVarchar
			action.stringValue = fmt.Sprintf("%v", v)
		}
		actions = append(actions, action)
	}
	return actions
}

// qwpBinds applies the tested ordered bind actions to the client bind buffer.
func qwpBinds(params []interface{}) qdb.QwpQueryOption {
	return qdb.WithQwpQueryBinds(func(b *qdb.QwpBinds) {
		for _, action := range qwpBindActions(params) {
			switch action.kind {
			case qwpBindVarchar:
				b.VarcharBind(action.index, action.stringValue)
			case qwpBindLong:
				b.LongBind(action.index, action.longValue)
			case qwpBindDouble:
				b.DoubleBind(action.index, action.doubleValue)
			case qwpBindBoolean:
				b.BooleanBind(action.index, action.boolValue)
			case qwpBindNullVarchar:
				b.NullVarcharBind(action.index)
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
