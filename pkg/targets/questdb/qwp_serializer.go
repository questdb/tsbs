package questdb

import (
	"encoding/binary"
	"fmt"
	"io"
	"math"

	"github.com/questdb/tsbs/pkg/data"
)

// QwpSerializer writes points in the binary QWP data format described in
// qwp_format.go. It is stateful: string and schema definitions are emitted
// the first time they are needed and referenced by id afterwards, so one
// serializer must write one whole file, which is how the generator uses it.
type QwpSerializer struct {
	strings map[string]uint64
	schemas map[string]uint64

	// scratch buffers, reused across points to keep serialization
	// allocation-free in the steady state.
	schemaKey []byte
	header    []byte
	payload   []byte

	// tags and fields hold the indexes of the non-null tags and fields
	// of the point being serialized. A point that omits a value gets a
	// schema of its own rather than a null placeholder.
	tags   []int
	fields []int

	wroteMagic bool
}

// Serialize writes one point. The first call also writes the file header.
func (s *QwpSerializer) Serialize(p *data.Point, w io.Writer) error {
	if !s.wroteMagic {
		if _, err := w.Write([]byte(QwpMagic)); err != nil {
			return err
		}
		if _, err := w.Write([]byte{QwpVersion}); err != nil {
			return err
		}
		s.wroteMagic = true
		s.strings = make(map[string]uint64)
		s.schemas = make(map[string]uint64)
	}

	tagKeys, tagValues := p.TagKeys(), p.TagValues()
	fieldKeys, fieldValues := p.FieldKeys(), p.FieldValues()

	// A tag whose value is not a string is not a symbol; it is a typed
	// column, exactly as the ILP serializer treats it.
	s.tags = s.tags[:0]
	s.fields = s.fields[:0]
	for i := range tagKeys {
		if tagValues[i] == nil {
			continue
		}
		if _, ok := tagValues[i].(string); ok {
			s.tags = append(s.tags, i)
		} else {
			s.fields = append(s.fields, ^i) // ^i marks "from the tag list"
		}
	}
	for i := range fieldKeys {
		if fieldValues[i] == nil {
			continue
		}
		s.fields = append(s.fields, i)
	}
	if len(s.fields) == 0 {
		// No columns at all: the row would be rejected downstream, and
		// the ILP serializer drops it too.
		return nil
	}

	schemaID, err := s.schemaFor(w, p, tagKeys, tagValues, fieldKeys, fieldValues)
	if err != nil {
		return err
	}

	s.payload = s.payload[:0]
	for _, i := range s.tags {
		id, err := s.stringID(w, tagValues[i].(string))
		if err != nil {
			return err
		}
		s.payload = binary.AppendUvarint(s.payload, id)
	}
	for _, i := range s.fields {
		v := fieldValues[i]
		if i < 0 {
			v = tagValues[^i]
		}
		s.payload, err = appendValue(s.payload, v)
		if err != nil {
			return err
		}
	}
	s.payload = binary.LittleEndian.AppendUint64(s.payload, uint64(p.Timestamp().UTC().UnixNano()))

	s.header = s.header[:0]
	s.header = append(s.header, QwpRecRow)
	s.header = binary.AppendUvarint(s.header, schemaID)
	s.header = binary.AppendUvarint(s.header, uint64(len(s.payload)))
	if _, err := w.Write(s.header); err != nil {
		return err
	}
	_, err = w.Write(s.payload)
	return err
}

// schemaFor returns the id of the schema describing this point's shape,
// defining it (and any strings it needs) first if it is new.
func (s *QwpSerializer) schemaFor(w io.Writer, p *data.Point, tagKeys [][]byte, tagValues []interface{}, fieldKeys [][]byte, fieldValues []interface{}) (uint64, error) {
	s.schemaKey = s.schemaKey[:0]
	s.schemaKey = append(s.schemaKey, p.MeasurementName()...)
	for _, i := range s.tags {
		s.schemaKey = append(s.schemaKey, 0)
		s.schemaKey = append(s.schemaKey, tagKeys[i]...)
	}
	for _, i := range s.fields {
		key, v := fieldKeys, fieldValues
		idx := i
		if i < 0 {
			key, v, idx = tagKeys, tagValues, ^i
		}
		code, err := typeCode(v[idx])
		if err != nil {
			return 0, err
		}
		s.schemaKey = append(s.schemaKey, 1, code)
		s.schemaKey = append(s.schemaKey, key[idx]...)
	}
	if id, ok := s.schemas[string(s.schemaKey)]; ok {
		return id, nil
	}

	tableID, err := s.stringID(w, string(p.MeasurementName()))
	if err != nil {
		return 0, err
	}
	tagIDs := make([]uint64, 0, len(s.tags))
	for _, i := range s.tags {
		id, err := s.stringID(w, string(tagKeys[i]))
		if err != nil {
			return 0, err
		}
		tagIDs = append(tagIDs, id)
	}
	type fieldDef struct {
		id   uint64
		code byte
	}
	fieldDefs := make([]fieldDef, 0, len(s.fields))
	for _, i := range s.fields {
		key, v := fieldKeys, fieldValues
		idx := i
		if i < 0 {
			key, v, idx = tagKeys, tagValues, ^i
		}
		id, err := s.stringID(w, string(key[idx]))
		if err != nil {
			return 0, err
		}
		code, err := typeCode(v[idx])
		if err != nil {
			return 0, err
		}
		fieldDefs = append(fieldDefs, fieldDef{id: id, code: code})
	}

	buf := []byte{QwpRecSchema}
	buf = binary.AppendUvarint(buf, tableID)
	buf = binary.AppendUvarint(buf, uint64(len(tagIDs)))
	for _, id := range tagIDs {
		buf = binary.AppendUvarint(buf, id)
	}
	buf = binary.AppendUvarint(buf, uint64(len(fieldDefs)))
	for _, f := range fieldDefs {
		buf = binary.AppendUvarint(buf, f.id)
		buf = append(buf, f.code)
	}
	if _, err := w.Write(buf); err != nil {
		return 0, err
	}

	id := uint64(len(s.schemas))
	s.schemas[string(s.schemaKey)] = id
	return id, nil
}

// stringID returns the dictionary id of v, defining it if it is new.
func (s *QwpSerializer) stringID(w io.Writer, v string) (uint64, error) {
	if id, ok := s.strings[v]; ok {
		return id, nil
	}
	buf := []byte{QwpRecString}
	buf = binary.AppendUvarint(buf, uint64(len(v)))
	buf = append(buf, v...)
	if _, err := w.Write(buf); err != nil {
		return 0, err
	}
	id := uint64(len(s.strings))
	s.strings[v] = id
	return id, nil
}

func typeCode(v interface{}) (byte, error) {
	switch v.(type) {
	case int, int64, int32, int16, int8:
		return QwpTypeInt64, nil
	case float64, float32:
		return QwpTypeFloat64, nil
	case bool:
		return QwpTypeBool, nil
	case string, []byte:
		return QwpTypeString, nil
	}
	return 0, fmt.Errorf("unsupported field type %T", v)
}

func appendValue(buf []byte, v interface{}) ([]byte, error) {
	switch val := v.(type) {
	case int:
		return binary.LittleEndian.AppendUint64(buf, uint64(int64(val))), nil
	case int64:
		return binary.LittleEndian.AppendUint64(buf, uint64(val)), nil
	case int32:
		return binary.LittleEndian.AppendUint64(buf, uint64(int64(val))), nil
	case int16:
		return binary.LittleEndian.AppendUint64(buf, uint64(int64(val))), nil
	case int8:
		return binary.LittleEndian.AppendUint64(buf, uint64(int64(val))), nil
	case float64:
		return binary.LittleEndian.AppendUint64(buf, math.Float64bits(val)), nil
	case float32:
		return binary.LittleEndian.AppendUint64(buf, math.Float64bits(float64(val))), nil
	case bool:
		if val {
			return append(buf, 1), nil
		}
		return append(buf, 0), nil
	case string:
		buf = binary.AppendUvarint(buf, uint64(len(val)))
		return append(buf, val...), nil
	case []byte:
		buf = binary.AppendUvarint(buf, uint64(len(val)))
		return append(buf, val...), nil
	}
	return nil, fmt.Errorf("unsupported field type %T", v)
}
