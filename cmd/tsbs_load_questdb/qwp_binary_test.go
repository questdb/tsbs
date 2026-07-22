package main

import (
	"bufio"
	"bytes"
	"context"
	"io"
	"testing"
	"time"

	"github.com/questdb/tsbs/pkg/data"
	"github.com/questdb/tsbs/pkg/data/serialize"
	"github.com/questdb/tsbs/pkg/targets/questdb"
)

// decodeAll runs every row of a serialized QWP file through the loader's
// binary path and returns what reached the row builder.
func decodeAll(t *testing.T, encoded []byte) []string {
	t.Helper()

	dec, err := newQwpDecoder(bufio.NewReader(bytes.NewReader(encoded)))
	if err != nil {
		t.Fatalf("newQwpDecoder: %v", err)
	}
	p, s := newTestQwpProcessor()
	for {
		schemaID, row, err := dec.next()
		if err == io.EOF {
			return s.rows
		}
		if err != nil {
			t.Fatalf("decode: %v", err)
		}
		if err := p.writeBinaryRow(dec.schemas[schemaID], row, dec.strings); err != nil {
			t.Fatalf("writeBinaryRow: %v", err)
		}
	}
}

// textAll runs the same points through the ILP text path.
func textAll(t *testing.T, encoded []byte) []string {
	t.Helper()

	p, s := newTestQwpProcessor()
	for _, line := range bytes.Split(bytes.TrimRight(encoded, "\n"), []byte("\n")) {
		if len(line) == 0 {
			continue
		}
		if err := p.writeRow(line); err != nil {
			t.Fatalf("writeRow(%q): %v", line, err)
		}
	}
	return s.rows
}

// TestQwpBinaryMatchesText is the load-bearing test for the binary format:
// for the same points, the binary path must hand the client exactly the
// rows the ILP text path does.
func TestQwpBinaryMatchesText(t *testing.T) {
	cases := []struct {
		desc  string
		point *data.Point
	}{
		{"default point", serialize.TestPointDefault()},
		{"int field", serialize.TestPointInt()},
		{"multiple fields", serialize.TestPointMultiField()},
		{"no tags", serialize.TestPointNoTags()},
		{"nil tag", serialize.TestPointWithNilTag()},
		{"nil field", serialize.TestPointWithNilField()},
	}

	for _, c := range cases {
		t.Run(c.desc, func(t *testing.T) {
			var textBuf, binBuf bytes.Buffer
			if err := (&questdb.Serializer{}).Serialize(c.point, &textBuf); err != nil {
				t.Fatalf("ILP serialize: %v", err)
			}
			if err := (&questdb.QwpSerializer{}).Serialize(c.point, &binBuf); err != nil {
				t.Fatalf("QWP serialize: %v", err)
			}

			want := textAll(t, textBuf.Bytes())
			got := decodeAll(t, binBuf.Bytes())
			if len(got) != len(want) {
				t.Fatalf("row count: got %d want %d", len(got), len(want))
			}
			for i := range want {
				if got[i] != want[i] {
					t.Errorf("row %d:\n got  %q\n want %q", i, got[i], want[i])
				}
			}
		})
	}
}

// TestQwpBinaryMultiplePoints checks that the dictionary and schema tables
// are reused across points rather than redefined per row, and that a
// second measurement gets its own schema.
func TestQwpBinaryMultiplePoints(t *testing.T) {
	var buf bytes.Buffer
	s := &questdb.QwpSerializer{}
	for i := 0; i < 5; i++ {
		if err := s.Serialize(serialize.TestPointMultiField(), &buf); err != nil {
			t.Fatalf("serialize: %v", err)
		}
	}
	if err := s.Serialize(serialize.TestPointNoTags(), &buf); err != nil {
		t.Fatalf("serialize: %v", err)
	}

	dec, err := newQwpDecoder(bufio.NewReader(bytes.NewReader(buf.Bytes())))
	if err != nil {
		t.Fatalf("newQwpDecoder: %v", err)
	}
	rows := 0
	for {
		_, _, err := dec.next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("decode: %v", err)
		}
		rows++
	}
	if rows != 6 {
		t.Errorf("rows: got %d want 6", rows)
	}
	if len(dec.schemas) != 2 {
		t.Errorf("schemas: got %d want 2", len(dec.schemas))
	}
	// cpu, hostname, host_0, region, eu-west-1, datacenter, eu-west-1b,
	// big_usage_guest, usage_guest, usage_guest_nice, usage_guest_nice is
	// already defined, so 10 distinct strings.
	if len(dec.strings) != 10 {
		t.Errorf("strings: got %d want 10: %v", len(dec.strings), dec.strings)
	}
}

func TestQwpDetect(t *testing.T) {
	textInput := bufio.NewReader(bytes.NewReader([]byte("cpu,hostname=host_0 usage_user=1i 140\n")))
	if ok, err := qwpDetect(textInput); err != nil || ok {
		t.Errorf("ILP text detected as binary: %v %v", ok, err)
	}

	var buf bytes.Buffer
	if err := (&questdb.QwpSerializer{}).Serialize(serialize.TestPointDefault(), &buf); err != nil {
		t.Fatalf("serialize: %v", err)
	}
	binInput := bufio.NewReader(bytes.NewReader(buf.Bytes()))
	if ok, err := qwpDetect(binInput); err != nil || !ok {
		t.Errorf("binary not detected: %v %v", ok, err)
	}
	// Detection must not consume anything.
	if _, err := newQwpDecoder(binInput); err != nil {
		t.Errorf("newQwpDecoder after detect: %v", err)
	}
}

func TestQwpDecoderRejectsGarbage(t *testing.T) {
	cases := []struct {
		desc string
		in   []byte
	}{
		{"empty", nil},
		{"bad magic", []byte("XXXX\x01")},
		{"bad version", []byte("QWPB\x7f")},
	}
	for _, c := range cases {
		t.Run(c.desc, func(t *testing.T) {
			if _, err := newQwpDecoder(bufio.NewReader(bytes.NewReader(c.in))); err == nil {
				t.Error("expected an error, got nil")
			}
		})
	}

	truncated := []byte("QWPB\x01\x03")
	dec, err := newQwpDecoder(bufio.NewReader(bytes.NewReader(truncated)))
	if err != nil {
		t.Fatalf("newQwpDecoder: %v", err)
	}
	if _, _, err := dec.next(); err == nil {
		t.Error("expected an error for a truncated row, got nil")
	}
}

// benchPoint is the point BenchmarkQwpWriteRow's line describes: a
// cpu-only row with ten tags and ten integer fields. Both benchmarks use
// it, so their numbers compare directly.
func benchPoint() *data.Point {
	p := data.NewPoint()
	p.SetMeasurementName([]byte("cpu"))
	tags := [][2]string{
		{"hostname", "host_0"}, {"region", "eu-central-1"},
		{"datacenter", "eu-central-1a"}, {"rack", "6"},
		{"os", "Ubuntu15.10"}, {"arch", "x86"},
		{"team", "SF"}, {"service", "19"},
		{"service_version", "1"}, {"service_environment", "test"},
	}
	for _, tag := range tags {
		p.AppendTag([]byte(tag[0]), tag[1])
	}
	fields := []struct {
		key string
		val int64
	}{
		{"usage_user", 58}, {"usage_system", 2}, {"usage_idle", 24},
		{"usage_nice", 61}, {"usage_iowait", 22}, {"usage_irq", 63},
		{"usage_softirq", 6}, {"usage_steal", 44}, {"usage_guest", 80},
		{"usage_guest_nice", 38},
	}
	for _, f := range fields {
		p.AppendField([]byte(f.key), f.val)
	}
	ts := time.Unix(0, 1451606400000000000)
	p.SetTimestamp(&ts)
	return p
}

var benchBinaryEncoded = func() []byte {
	var buf bytes.Buffer
	if err := (&questdb.QwpSerializer{}).Serialize(benchPoint(), &buf); err != nil {
		panic(err)
	}
	return buf.Bytes()
}()

// BenchmarkQwpWriteBinaryRow is the counterpart of BenchmarkQwpWriteRow:
// same row, decoded from the binary format instead of parsed from text.
func BenchmarkQwpWriteBinaryRow(b *testing.B) {
	dec, err := newQwpDecoder(bufio.NewReader(bytes.NewReader(benchBinaryEncoded)))
	if err != nil {
		b.Fatal(err)
	}
	schemaID, row, err := dec.next()
	if err != nil {
		b.Fatal(err)
	}
	schema := dec.schemas[schemaID]
	dict := dec.strings
	rowCopy := append([]byte(nil), row...)

	p := &qwpProcessor{
		ctx:    context.Background(),
		sender: &noopSender{},
		intern: make(map[string]string),
	}

	b.ReportAllocs()
	b.SetBytes(int64(len(rowCopy)))
	for i := 0; i < b.N; i++ {
		if err := p.writeBinaryRow(schema, rowCopy, dict); err != nil {
			b.Fatal(err)
		}
	}
}
