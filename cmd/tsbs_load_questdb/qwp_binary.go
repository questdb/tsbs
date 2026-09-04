package main

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"sync"

	"github.com/questdb/tsbs/pkg/data"
	"github.com/questdb/tsbs/pkg/data/usecases/common"
	"github.com/questdb/tsbs/pkg/targets"
	"github.com/questdb/tsbs/pkg/targets/questdb"
)

// qwpSchema is the decoded form of a schema definition record: the shape
// every row referencing it takes.
type qwpSchema struct {
	table     string
	tagKeys   []string
	fieldKeys []string
	fieldType []byte
}

// qwpDecoder reads the binary QWP data format. It is driven by the single
// scanning goroutine, so its dictionary and schema tables need no locking:
// they only grow, entries are never rewritten, and a batch takes a
// snapshot of both before it is handed to a worker.
type qwpDecoder struct {
	r               *bufio.Reader
	strings         []string
	schemas         []*qwpSchema
	row             []byte
	dictionaryBytes uint64
	schemaSlots     uint64
}

var errQwpTruncated = errors.New("truncated QWP data file")

// The aggregate dictionary budget leaves more than 5 MiB of headroom above
// the 10,888,890 hostname bytes produced by a one-million-host TSBS data set.
// Real TSBS schemas use tens of slots; the aggregate slot budget is deliberately
// much larger while still bounding retained slice storage.
const (
	maxQwpRowBytes          = 4 << 20
	maxQwpStringBytes       = 1 << 20
	maxQwpDictionaryEntries = 1 << 20
	maxQwpDictionaryBytes   = 16 << 20
	maxQwpSchemas           = 1 << 16
	maxQwpTagsPerSchema     = 1 << 16
	maxQwpFieldsPerSchema   = 1 << 16
	maxQwpSchemaSlots       = 1 << 16
)

func checkQwpLimit(name string, value, limit uint64) error {
	if value > limit {
		return fmt.Errorf("QWP %s %d exceeds limit %d", name, value, limit)
	}
	return nil
}

func addQwpBudget(name string, used, additional, limit uint64) (uint64, error) {
	if used > limit || additional > limit-used {
		return used, fmt.Errorf("QWP aggregate %s %d+%d exceeds limit %d", name, used, additional, limit)
	}
	return used + additional, nil
}

// readHeader consumes the file header and reports whether the reader holds
// a QWP binary file at all.
func qwpDetect(r *bufio.Reader) (bool, error) {
	magic, err := r.Peek(len(questdb.QwpMagic))
	if err != nil {
		if err == io.EOF {
			return false, nil
		}
		return false, err
	}
	return string(magic) == questdb.QwpMagic, nil
}

func newQwpDecoder(r *bufio.Reader) (*qwpDecoder, error) {
	header := make([]byte, len(questdb.QwpMagic)+1)
	if _, err := io.ReadFull(r, header); err != nil {
		return nil, err
	}
	if string(header[:len(questdb.QwpMagic)]) != questdb.QwpMagic {
		return nil, errors.New("not a QWP data file")
	}
	if v := header[len(questdb.QwpMagic)]; v != questdb.QwpVersion {
		return nil, errors.New("unsupported QWP data file version")
	}
	return &qwpDecoder{r: r}, nil
}

// next returns the next row: its schema and its payload. The payload is
// only valid until the following call, so callers must copy it.
func (d *qwpDecoder) next() (uint64, []byte, error) {
	for {
		kind, err := d.r.ReadByte()
		if err != nil {
			return 0, nil, err // io.EOF ends the stream
		}
		switch kind {
		case questdb.QwpRecString:
			if err := d.readString(); err != nil {
				return 0, nil, err
			}
		case questdb.QwpRecSchema:
			if err := d.readSchema(); err != nil {
				return 0, nil, err
			}
		case questdb.QwpRecRow:
			schemaID, err := binary.ReadUvarint(d.r)
			if err != nil {
				return 0, nil, errQwpTruncated
			}
			if schemaID >= uint64(len(d.schemas)) {
				return 0, nil, errors.New("QWP row references an undefined schema")
			}
			n, err := binary.ReadUvarint(d.r)
			if err != nil {
				return 0, nil, errQwpTruncated
			}
			if err := checkQwpLimit("row length", n, maxQwpRowBytes); err != nil {
				return 0, nil, err
			}
			rowLen := int(n)
			if cap(d.row) < rowLen {
				d.row = make([]byte, rowLen)
			}
			d.row = d.row[:rowLen]
			if _, err := io.ReadFull(d.r, d.row); err != nil {
				return 0, nil, errQwpTruncated
			}
			return schemaID, d.row, nil
		default:
			return 0, nil, errors.New("unknown QWP record kind")
		}
	}
}

func (d *qwpDecoder) readString() error {
	if len(d.strings) >= maxQwpDictionaryEntries {
		return fmt.Errorf("QWP dictionary entries %d exceeds limit %d", len(d.strings)+1, maxQwpDictionaryEntries)
	}
	n, err := binary.ReadUvarint(d.r)
	if err != nil {
		return errQwpTruncated
	}
	if err := checkQwpLimit("string length", n, maxQwpStringBytes); err != nil {
		return err
	}
	nextDictionaryBytes, err := addQwpBudget("dictionary bytes", d.dictionaryBytes, n, maxQwpDictionaryBytes)
	if err != nil {
		return err
	}
	buf := make([]byte, int(n))
	if _, err := io.ReadFull(d.r, buf); err != nil {
		return errQwpTruncated
	}
	d.strings = append(d.strings, string(buf))
	d.dictionaryBytes = nextDictionaryBytes
	return nil
}

func (d *qwpDecoder) readSchema() error {
	if len(d.schemas) >= maxQwpSchemas {
		return fmt.Errorf("QWP schemas %d exceeds limit %d", len(d.schemas)+1, maxQwpSchemas)
	}
	tableID, err := binary.ReadUvarint(d.r)
	if err != nil {
		return errQwpTruncated
	}
	s := &qwpSchema{}
	if tableID >= uint64(len(d.strings)) {
		return errors.New("QWP schema references an undefined string")
	}
	s.table = d.strings[tableID]

	tagCount, err := binary.ReadUvarint(d.r)
	if err != nil {
		return errQwpTruncated
	}
	if err := checkQwpLimit("schema tag count", tagCount, maxQwpTagsPerSchema); err != nil {
		return err
	}
	nextSchemaSlots, err := addQwpBudget("schema slots", d.schemaSlots, tagCount, maxQwpSchemaSlots)
	if err != nil {
		return err
	}
	s.tagKeys = make([]string, int(tagCount))
	for i := range s.tagKeys {
		id, err := binary.ReadUvarint(d.r)
		if err != nil {
			return errQwpTruncated
		}
		if id >= uint64(len(d.strings)) {
			return errors.New("QWP schema references an undefined string")
		}
		s.tagKeys[i] = d.strings[id]
	}

	fieldCount, err := binary.ReadUvarint(d.r)
	if err != nil {
		return errQwpTruncated
	}
	if err := checkQwpLimit("schema field count", fieldCount, maxQwpFieldsPerSchema); err != nil {
		return err
	}
	nextSchemaSlots, err = addQwpBudget("schema slots", nextSchemaSlots, fieldCount, maxQwpSchemaSlots)
	if err != nil {
		return err
	}
	s.fieldKeys = make([]string, int(fieldCount))
	s.fieldType = make([]byte, int(fieldCount))
	for i := range s.fieldKeys {
		id, err := binary.ReadUvarint(d.r)
		if err != nil {
			return errQwpTruncated
		}
		if id >= uint64(len(d.strings)) {
			return errors.New("QWP schema references an undefined string")
		}
		s.fieldKeys[i] = d.strings[id]
		code, err := d.r.ReadByte()
		if err != nil {
			return errQwpTruncated
		}
		s.fieldType[i] = code
	}

	d.schemas = append(d.schemas, s)
	d.schemaSlots = nextSchemaSlots
	return nil
}

// qwpDataSource feeds the loader from a binary QWP data file.
type qwpDataSource struct {
	dec *qwpDecoder
}

// qwpPoint is one row as handed to a batch: the schema id and the row
// bytes, which the batch copies.
type qwpPoint struct {
	schemaID uint64
	payload  []byte
}

func (s *qwpDataSource) NextItem() data.LoadedPoint {
	schemaID, payload, err := s.dec.next()
	if err != nil {
		if err == io.EOF {
			return data.LoadedPoint{}
		}
		fatal("scan error: %v", err)
		return data.LoadedPoint{}
	}
	return data.NewLoadedPoint(qwpPoint{schemaID: schemaID, payload: payload})
}

func (s *qwpDataSource) Headers() *common.GeneratedDataHeaders { return nil }

// qwpBatch holds rows in their wire-adjacent binary form, prefixed with
// their schema id and length, plus the dictionary snapshot needed to
// resolve them. The snapshots are safe to read from a worker: both tables
// only ever grow, and the slice headers captured here cover every row the
// batch contains.
type qwpBatch struct {
	dec       *qwpDecoder
	buf       []byte
	pooledBuf *[]byte
	rows      uint
	metrics   uint64
	schemas   []*qwpSchema
	dict      []string
	scratch   []byte
}

func (b *qwpBatch) Len() uint {
	return b.rows
}

func (b *qwpBatch) Append(item data.LoadedPoint) {
	p := item.Data.(qwpPoint)
	b.rows++
	b.metrics += uint64(len(b.dec.schemas[p.schemaID].fieldKeys))

	b.scratch = b.scratch[:0]
	b.scratch = binary.AppendUvarint(b.scratch, p.schemaID)
	b.scratch = binary.AppendUvarint(b.scratch, uint64(len(p.payload)))
	b.buf = append(b.buf, b.scratch...)
	b.buf = append(b.buf, p.payload...)

	// Snapshot after appending, so the tables definitely cover every row
	// in this batch. Both tables only grow and their entries are never
	// rewritten, so a worker reading through these slice headers sees a
	// consistent view of everything its rows reference.
	b.schemas = b.dec.schemas
	b.dict = b.dec.strings
}

type qwpFactory struct {
	dec *qwpDecoder
}

// qwpBufPool recycles batch buffers: the loader allocates a fresh batch
// for every fill, so without a pool a run churns one 4 MiB buffer per
// batch.
var qwpBufPool = sync.Pool{
	New: func() interface{} {
		buf := make([]byte, 0, 4*1024*1024)
		return &buf
	},
}

func (f *qwpFactory) New() targets.Batch {
	pooledBuf := qwpBufPool.Get().(*[]byte)
	return &qwpBatch{dec: f.dec, buf: (*pooledBuf)[:0], pooledBuf: pooledBuf}
}

// writeRows decodes the batch and emits every row through the QWP row
// builder. This is the counterpart of the ILP text path's writeRow, minus
// the parsing: names are already strings and numbers are already numbers.
func (p *qwpProcessor) writeRows(b *qwpBatch) error {
	buf := b.buf
	for len(buf) > 0 {
		schemaID, n := binary.Uvarint(buf)
		if n <= 0 {
			return errQwpTruncated
		}
		buf = buf[n:]
		size, n := binary.Uvarint(buf)
		if n <= 0 {
			return errQwpTruncated
		}
		buf = buf[n:]
		if uint64(len(buf)) < size {
			return errQwpTruncated
		}
		row := buf[:size]
		buf = buf[size:]

		if schemaID >= uint64(len(b.schemas)) {
			return errors.New("QWP row references an undefined schema")
		}
		if err := p.writeBinaryRow(b.schemas[schemaID], row, b.dict); err != nil {
			return err
		}
	}
	return nil
}

func (p *qwpProcessor) writeBinaryRow(s *qwpSchema, row []byte, dict []string) error {
	sender := p.sender.Table(s.table)

	for _, key := range s.tagKeys {
		id, n := binary.Uvarint(row)
		if n <= 0 {
			return errQwpTruncated
		}
		row = row[n:]
		if id >= uint64(len(dict)) {
			return errors.New("QWP row references an undefined string")
		}
		sender = sender.Symbol(key, dict[id])
	}

	for i, key := range s.fieldKeys {
		switch s.fieldType[i] {
		case questdb.QwpTypeInt64:
			if len(row) < 8 {
				return errQwpTruncated
			}
			sender = sender.Int64Column(key, int64(binary.LittleEndian.Uint64(row)))
			row = row[8:]
		case questdb.QwpTypeFloat64:
			if len(row) < 8 {
				return errQwpTruncated
			}
			sender = sender.Float64Column(key, math.Float64frombits(binary.LittleEndian.Uint64(row)))
			row = row[8:]
		case questdb.QwpTypeBool:
			if len(row) < 1 {
				return errQwpTruncated
			}
			var value bool
			switch row[0] {
			case 0:
				value = false
			case 1:
				value = true
			default:
				return errors.New("invalid QWP boolean value")
			}
			sender = sender.BoolColumn(key, value)
			row = row[1:]
		case questdb.QwpTypeString:
			size, n := binary.Uvarint(row)
			if n <= 0 || uint64(len(row[n:])) < size {
				return errQwpTruncated
			}
			row = row[n:]
			sender = sender.StringColumn(key, string(row[:size]))
			row = row[size:]
		default:
			return errors.New("unknown QWP column type")
		}
	}

	if len(row) < 8 {
		return errQwpTruncated
	}
	if len(row) > 8 {
		return errors.New("QWP row has trailing data")
	}
	return p.at(int64(binary.LittleEndian.Uint64(row)))
}

// compile-time assertion that the batch satisfies the loader interface.
var _ targets.Batch = (*qwpBatch)(nil)
