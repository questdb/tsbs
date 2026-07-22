package main

import (
	"context"
	"testing"
	"time"

	qdb "github.com/questdb/go-questdb-client/v4"
)

// noopSender satisfies the row-builder calls without doing any work, so a
// benchmark against it measures the loader's own parsing and dispatch
// cost, with the client's encoding and I/O excluded.
type noopSender struct {
	qdb.QwpSender
}

func (s *noopSender) Table(string) qdb.LineSender              { return s }
func (s *noopSender) Symbol(string, string) qdb.LineSender     { return s }
func (s *noopSender) Int64Column(string, int64) qdb.LineSender { return s }
func (s *noopSender) Float64Column(string, float64) qdb.LineSender {
	return s
}
func (s *noopSender) StringColumn(string, string) qdb.LineSender { return s }
func (s *noopSender) BoolColumn(string, bool) qdb.LineSender     { return s }
func (s *noopSender) At(context.Context, time.Time) error        { return nil }
func (s *noopSender) AtNano(context.Context, time.Time) error    { return nil }

var benchLine = []byte("cpu,hostname=host_0,region=eu-central-1,datacenter=eu-central-1a,rack=6,os=Ubuntu15.10,arch=x86,team=SF,service=19,service_version=1,service_environment=test usage_user=58i,usage_system=2i,usage_idle=24i,usage_nice=61i,usage_iowait=22i,usage_irq=63i,usage_softirq=6i,usage_steal=44i,usage_guest=80i,usage_guest_nice=38i 1451606400000000000")

// BenchmarkQwpWriteRow measures parsing one generated cpu-only line and
// dispatching it into the row builder.
func BenchmarkQwpWriteRow(b *testing.B) {
	p := &qwpProcessor{
		ctx:    context.Background(),
		sender: &noopSender{},
		intern: make(map[string]string),
	}

	b.ReportAllocs()
	b.SetBytes(int64(len(benchLine)))
	for i := 0; i < b.N; i++ {
		if err := p.writeRow(benchLine); err != nil {
			b.Fatal(err)
		}
	}
}
