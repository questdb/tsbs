package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	qdb "github.com/questdb/go-questdb-client/v4"
	"github.com/questdb/tsbs/pkg/targets"
)

// errNotThreeTuples is returned for a line that is not made of the three
// space-separated tuples the influx line protocol requires.
var errNotThreeTuples = errors.New("parse error: line does not have 3 tuples")

// maxInternedValues caps the per-worker string intern table so that a
// high-cardinality data set cannot grow it without bound.
const maxInternedValues = 1 << 16

// qwpProcessor ingests over the QuestDB Wire Protocol (QWP) instead of
// writing raw ILP text to a socket. The data source is still ILP text
// (that is what tsbs_generate_data emits for the questdb format), so each
// line is parsed back into table / symbols / typed columns and handed to
// the client's row builder.
//
// One sender per worker: QWP has no sender pool, a single sender already
// pipelines transmission through its cursor engine and I/O goroutine.
type qwpProcessor struct {
	ctx context.Context

	// sender is the row builder every path writes through. qwp is the
	// same object when the transport is QWP, and nil for ILP over HTTP,
	// which has no publish/ack split: its Flush is the round trip.
	sender qdb.LineSender
	qwp    qdb.QwpSender

	// lastFsn is the most recently published QWIP sequence. Close uses
	// it for an explicit acknowledgement barrier that is independent
	// of the sender's own close configuration.
	lastFsn      int64
	hasPublished bool

	// intern keeps a single copy of every table name, column name and
	// symbol value seen by this worker, so parsing a line does not
	// allocate a string per token.
	intern map[string]string
}

func (p *qwpProcessor) Init(numWorker int, doLoad, _ bool) {
	if !doLoad {
		return
	}

	p.ctx = context.Background()
	p.intern = make(map[string]string)

	conf := senderConf(numWorker)
	sender, err := qdb.LineSenderFromConf(p.ctx, conf)
	if err != nil {
		fatal("failed to create sender: %v", err)
		return
	}
	p.sender = sender

	if protocol == protocolQWIP {
		qwp, ok := sender.(qdb.QwpSender)
		if !ok {
			fatal("configured sender did not yield a QWP sender, use the ws:: or wss:: scheme")
			return
		}
		p.qwp = qwp
	}
}

// Close explicitly waits for the last published QWIP sequence before
// closing the client. The benchmark's acknowledgement guarantee therefore
// does not depend on close_flush_timeout_millis in a custom client config.
func (p *qwpProcessor) Close(doLoad bool) {
	if !doLoad || p.sender == nil {
		return
	}
	if err := p.closeSender(); err != nil {
		fatal("failed to close sender: %v", err)
	}
}

func (p *qwpProcessor) closeSender() error {
	closeCtx := p.ctx
	cancel := func() {}
	var ackErr error
	if p.qwp != nil {
		if err := validateQwpAckTimeout(protocolQWIP, qwpCloseTimeoutMs); err != nil {
			ackErr = err
			closeCtx, cancel = context.WithCancel(p.ctx)
			cancel()
		} else {
			closeCtx, cancel = context.WithTimeout(p.ctx, time.Duration(qwpCloseTimeoutMs)*time.Millisecond)
		}
	}
	defer cancel()

	if p.qwp != nil && p.hasPublished && ackErr == nil {
		if err := p.qwp.AwaitAckedFsn(closeCtx, p.lastFsn); err != nil {
			ackErr = fmt.Errorf("failed to await final QWIP ack for fsn %d: %w", p.lastFsn, err)
		}
	}

	closeErr := p.sender.Close(closeCtx)
	if closeErr != nil {
		closeErr = fmt.Errorf("failed to close client: %w", closeErr)
	}
	return errors.Join(ackErr, closeErr)
}

func (p *qwpProcessor) ProcessBatch(b targets.Batch, doLoad bool) (uint64, uint64) {
	// A binary QWP data file needs no parsing, so it takes its own path.
	if binBatch, ok := b.(*qwpBatch); ok {
		return p.processBinaryBatch(binBatch, doLoad)
	}
	batch := b.(*batch)

	if doLoad {
		buf := batch.buf.Bytes()
		for len(buf) > 0 {
			line := buf
			if i := bytes.IndexByte(buf, '\n'); i >= 0 {
				line, buf = buf[:i], buf[i+1:]
			} else {
				buf = nil
			}
			if len(line) == 0 {
				continue
			}
			if err := p.writeRow(line); err != nil {
				fatal("failed to write row %q: %v", line, err)
				return 0, 0
			}
		}

		if err := p.flush(); err != nil {
			fatal("%v", err)
			return 0, 0
		}
	}

	metricCnt := batch.metrics
	rowCnt := batch.rows

	// Return the batch buffer to the pool.
	batch.buf.Reset()
	bufPool.Put(batch.buf)
	return metricCnt, uint64(rowCnt)
}

func (p *qwpProcessor) processBinaryBatch(b *qwpBatch, doLoad bool) (uint64, uint64) {
	if doLoad {
		if err := p.writeRows(b); err != nil {
			fatal("failed to write batch: %v", err)
			return 0, 0
		}
		if err := p.flush(); err != nil {
			fatal("%v", err)
			return 0, 0
		}
	}

	metricCnt := b.metrics
	rowCnt := b.rows

	// Return the batch buffer to the pool.
	qwpBufPool.Put(b.buf[:0])
	b.buf = nil
	return metricCnt, uint64(rowCnt)
}

// flush sends the buffered rows. Over ILP/HTTP that is a round trip, so a
// clean return means the server processed the batch. Over QWP it is a
// publish: the batch is durably queued and a background goroutine
// delivers it, and only AwaitAckedFsn blocks on server confirmation.
func (p *qwpProcessor) flush() error {
	if p.qwp == nil {
		if err := p.sender.Flush(p.ctx); err != nil {
			return fmt.Errorf("failed to flush batch: %v", err)
		}
		return nil
	}

	fsn, err := p.qwp.FlushAndGetSequence(p.ctx)
	if err != nil {
		return fmt.Errorf("failed to flush QWP batch: %v", err)
	}
	p.lastFsn = fsn
	p.hasPublished = true
	if awaitAck {
		if err := p.qwp.AwaitAckedFsn(p.ctx, fsn); err != nil {
			return fmt.Errorf("failed to await ack for fsn %d: %v", fsn, err)
		}
	}
	return nil
}

// at closes the current row. AtNano is QWP-only, so the nanosecond option
// applies only there; ILP/HTTP always sends microseconds, which is what
// the ILP/TCP path produces too.
func (p *qwpProcessor) at(ts int64) error {
	if nanoTimestamps && p.qwp != nil {
		return p.qwp.AtNano(p.ctx, time.Unix(0, ts))
	}
	return p.sender.At(p.ctx, time.Unix(0, ts))
}

// writeRow parses a single ILP line and emits it through the QWP row
// builder. The line layout is:
//
//	<measurement>[,<tag>=<val>]* <field>=<val>[,<field>=<val>]* <timestamp>
func (p *qwpProcessor) writeRow(line []byte) error {
	sep := bytes.IndexByte(line, ' ')
	if sep < 0 {
		return errNotThreeTuples
	}
	tags := line[:sep]
	rest := line[sep+1:]

	// The timestamp is the last space-separated token; splitting from
	// the right keeps unquoted string field values with spaces in one
	// piece rather than eating the fields section.
	tsSep := bytes.LastIndexByte(rest, ' ')
	if tsSep < 0 {
		return errNotThreeTuples
	}
	fields := rest[:tsSep]
	tsRaw := rest[tsSep+1:]

	name := tags
	if i := bytes.IndexByte(tags, ','); i >= 0 {
		name = tags[:i]
		tags = tags[i+1:]
	} else {
		tags = nil
	}
	sender := p.sender.Table(p.interned(name))

	for len(tags) > 0 {
		var pair []byte
		pair, tags = nextToken(tags)
		key, val, ok := splitPair(pair)
		if !ok {
			return fmt.Errorf("malformed tag %q", pair)
		}
		sender = sender.Symbol(p.interned(key), p.interned(val))
	}

	for len(fields) > 0 {
		var pair []byte
		pair, fields = nextToken(fields)
		key, val, ok := splitPair(pair)
		if !ok {
			return fmt.Errorf("malformed field %q", pair)
		}
		var err error
		sender, err = p.appendField(sender, key, val)
		if err != nil {
			return err
		}
	}

	ts, err := strconv.ParseInt(string(tsRaw), 10, 64)
	if err != nil {
		return fmt.Errorf("malformed timestamp %q: %v", tsRaw, err)
	}
	// At sends a microsecond timestamp, which is what the ILP path
	// produces, so every transport creates the same table. AtNano keeps
	// the generator's nanosecond timestamps instead, at the cost of a
	// TIMESTAMP_NS designated column that an ILP-created table does not
	// have. A table's resolution is fixed by its first row, so the
	// choice has to be the same for every row.
	return p.at(ts)
}

// appendField maps an ILP field value onto a typed QWP column, following
// the influx line protocol conventions the TSBS serializer emits: an 'i'
// suffix for integers, bare true/false for booleans, double quotes for
// strings, everything else a double.
func (p *qwpProcessor) appendField(sender qdb.LineSender, key, val []byte) (qdb.LineSender, error) {
	if len(val) == 0 {
		return nil, fmt.Errorf("empty value for field %q", key)
	}

	switch val[len(val)-1] {
	case 'i':
		if i, err := strconv.ParseInt(string(val[:len(val)-1]), 10, 64); err == nil {
			return sender.Int64Column(p.interned(key), i), nil
		}
	case '"':
		if len(val) > 1 && val[0] == '"' {
			return sender.StringColumn(p.interned(key), string(val[1:len(val)-1])), nil
		}
	}

	switch string(val) {
	case "t", "T", "true", "True", "TRUE":
		return sender.BoolColumn(p.interned(key), true), nil
	case "f", "F", "false", "False", "FALSE":
		return sender.BoolColumn(p.interned(key), false), nil
	}

	f, err := strconv.ParseFloat(string(val), 64)
	if err != nil {
		// Not a number and not quoted: the TSBS serializer writes
		// string values bare, so treat it as a string.
		return sender.StringColumn(p.interned(key), string(val)), nil
	}
	return sender.Float64Column(p.interned(key), f), nil
}

// interned returns a string copy of b, reusing a previously created copy
// when there is one. Table names, column names and symbol values all
// repeat on every row, so this keeps row parsing allocation-free in the
// steady state.
func (p *qwpProcessor) interned(b []byte) string {
	if s, ok := p.intern[string(b)]; ok {
		return s
	}
	s := string(b)
	if len(p.intern) < maxInternedValues {
		p.intern[s] = s
	}
	return s
}

// nextToken splits off the first comma-separated token of b, returning it
// along with the remainder.
func nextToken(b []byte) (token, rest []byte) {
	if i := bytes.IndexByte(b, ','); i >= 0 {
		return b[:i], b[i+1:]
	}
	return b, nil
}

// splitPair splits a key=value pair on its first equals sign.
func splitPair(b []byte) (key, val []byte, ok bool) {
	i := bytes.IndexByte(b, '=')
	if i <= 0 || i == len(b)-1 {
		return nil, nil, false
	}
	return b[:i], b[i+1:], true
}
