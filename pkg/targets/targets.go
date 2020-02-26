package targets

import (
	"github.com/spf13/viper"
	"github.com/timescale/tsbs/load"
)

// Formats supported for generation
const (
	FormatCassandra   = "cassandra"
	FormatClickhouse  = "clickhouse"
	FormatInflux      = "influx"
	FormatMongo       = "mongo"
	FormatSiriDB      = "siridb"
	FormatTimescaleDB = "timescaledb"
	FormatAkumuli     = "akumuli"
	FormatCrateDB     = "cratedb"
	FormatPrometheus  = "prometheus"
	FormatVictoriaMetrics = "victoriametrics"
)

func SupportedFormats() []string {
	return []string{
		FormatCassandra,
		FormatClickhouse,
		FormatInflux,
		FormatMongo,
		FormatSiriDB,
		FormatTimescaleDB,
		FormatAkumuli,
		FormatCrateDB,
		FormatPrometheus,
		FormatVictoriaMetrics,
	}
}

type ImplementedTarget interface {
	Benchmark() load.Benchmark
	ParseLoaderConfig(v *viper.Viper) (interface{}, error)
}