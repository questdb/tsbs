package akumuli

import (
	"github.com/blagojts/viper"
	"github.com/questdb/tsbs/pkg/data/serialize"
	"github.com/questdb/tsbs/pkg/data/source"
	"github.com/questdb/tsbs/pkg/targets"
	"github.com/questdb/tsbs/pkg/targets/constants"
	"github.com/spf13/pflag"
)

func NewTarget() targets.ImplementedTarget {
	return &akumuliTarget{}
}

type akumuliTarget struct {
}

func (t *akumuliTarget) TargetSpecificFlags(flagPrefix string, flagSet *pflag.FlagSet) {
	flagSet.String(flagPrefix+"endpoint", "http://localhost:8282", "Akumuli RESP endpoint IP address.")
}

func (t *akumuliTarget) TargetName() string {
	return constants.FormatAkumuli
}

func (t *akumuliTarget) Serializer() serialize.PointSerializer {
	return &Serializer{}
}

func (t *akumuliTarget) Benchmark(string, *source.DataSourceConfig, *viper.Viper) (targets.Benchmark, error) {
	panic("not implemented")
}
