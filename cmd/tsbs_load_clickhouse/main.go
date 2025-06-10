// tsbs_load_clickhouse loads a ClickHouse instance with data from stdin.
//
// If the database exists beforehand, it will be *DROPPED*.
package main

import (
	"fmt"

	"github.com/blagojts/viper"
	"github.com/questdb/tsbs/internal/utils"
	"github.com/questdb/tsbs/load"
	"github.com/questdb/tsbs/pkg/targets"
	"github.com/questdb/tsbs/pkg/targets/clickhouse"
	"github.com/spf13/pflag"
)

// Global vars
var (
	target targets.ImplementedTarget
)

var loader load.BenchmarkRunner
var loaderConf load.BenchmarkRunnerConfig
var conf *clickhouse.ClickhouseConfig

// Parse args:
func init() {
	loaderConf = load.BenchmarkRunnerConfig{}
	target := clickhouse.NewTarget()
	loaderConf.AddToFlagSet(pflag.CommandLine)
	target.TargetSpecificFlags("", pflag.CommandLine)
	pflag.Parse()

	err := utils.SetupConfigFile()

	if err != nil {
		panic(fmt.Errorf("fatal error config file: %s", err))
	}

	if err := viper.Unmarshal(&loaderConf); err != nil {
		panic(fmt.Errorf("unable to decode config: %s", err))
	}
	conf = &clickhouse.ClickhouseConfig{
		Host:       viper.GetString("host"),
		User:       viper.GetString("user"),
		Password:   viper.GetString("password"),
		LogBatches: viper.GetBool("log-batches"),
		Debug:      viper.GetInt("debug"),
		DbName:     loaderConf.DBName,
	}

	loader = load.GetBenchmarkRunner(loaderConf)
}

func main() {
	loader.RunBenchmark(clickhouse.NewBenchmark(loaderConf.FileName, loaderConf.HashWorkers, conf))
}
