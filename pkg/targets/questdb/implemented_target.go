package questdb

import (
	"github.com/blagojts/viper"
	"github.com/questdb/tsbs/pkg/data/serialize"
	"github.com/questdb/tsbs/pkg/data/source"
	"github.com/questdb/tsbs/pkg/targets"
	"github.com/questdb/tsbs/pkg/targets/constants"
	"github.com/spf13/pflag"
)

func NewTarget() targets.ImplementedTarget {
	return &influxTarget{}
}

// NewQwpTarget returns the target that generates data in the binary QWP
// format. It is the same database with the same flags; only the on-disk
// point encoding differs, so the loader can ingest without parsing text.
func NewQwpTarget() targets.ImplementedTarget {
	return &qwpTarget{}
}

type qwpTarget struct {
	influxTarget
}

func (t *qwpTarget) TargetName() string {
	return constants.FormatQuestDBQWP
}

func (t *qwpTarget) Serializer() serialize.PointSerializer {
	return &QwpSerializer{}
}

type influxTarget struct {
}

func (t *influxTarget) TargetSpecificFlags(flagPrefix string, flagSet *pflag.FlagSet) {
	flagSet.String(flagPrefix+"url", "http://localhost:9000/", "QuestDB REST end point")
	flagSet.String(flagPrefix+"ilp-bind-to", "127.0.0.1:9009", "QuestDB influx line protocol TCP ip:port")
	flagSet.String(flagPrefix+"qwp-addr", "127.0.0.1:9000", "QuestDB wire protocol WebSocket ip:port. Comma-separated list enables failover")
}

func (t *influxTarget) TargetName() string {
	return constants.FormatQuestDB
}

func (t *influxTarget) Serializer() serialize.PointSerializer {
	return &Serializer{}
}

func (t *influxTarget) Benchmark(string, *source.DataSourceConfig, *viper.Viper) (targets.Benchmark, error) {
	panic("not implemented")
}
