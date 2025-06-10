// bulk_load_cassandra loads a Cassandra daemon with data from stdin.
//
// The caller is responsible for assuring that the database is empty before
// bulk load.
package main

import (
	"fmt"

	"github.com/blagojts/viper"
	"github.com/questdb/tsbs/internal/utils"
	"github.com/questdb/tsbs/load"
	"github.com/questdb/tsbs/pkg/data/source"
	"github.com/questdb/tsbs/pkg/targets/cassandra"
	"github.com/questdb/tsbs/pkg/targets/constants"
	"github.com/questdb/tsbs/pkg/targets/initializers"
	"github.com/spf13/pflag"
)

// Parse args:
func initProgramOptions() (*cassandra.SpecificConfig, *load.BenchmarkRunnerConfig, load.BenchmarkRunner) {
	config := load.BenchmarkRunnerConfig{}
	target := initializers.GetTarget(constants.FormatCassandra)
	config.AddToFlagSet(pflag.CommandLine)
	target.TargetSpecificFlags("", pflag.CommandLine)
	pflag.Parse()

	err := utils.SetupConfigFile()

	if err != nil {
		panic(fmt.Errorf("fatal error config file: %s", err))
	}

	if err := viper.Unmarshal(&config); err != nil {
		panic(fmt.Errorf("unable to decode config: %s", err))
	}

	dbConfig := &cassandra.SpecificConfig{
		Hosts:             viper.GetString("hosts"),
		ReplicationFactor: viper.GetInt("replication-factor"),
		ConsistencyLevel:  viper.GetString("consistency"),
		WriteTimeout:      viper.GetDuration("write-timeout"),
	}

	config.HashWorkers = false
	config.BatchSize = 100
	loader := load.GetBenchmarkRunner(config)
	return dbConfig, &config, loader
}

func main() {
	dbConfig, loaderConf, loader := initProgramOptions()
	benchmark, err := cassandra.NewBenchmark(dbConfig, &source.DataSourceConfig{
		Type: source.FileDataSourceType,
		File: &source.FileDataSourceConfig{Location: loaderConf.FileName},
	})
	if err != nil {
		panic(err)
	}
	loader.RunBenchmark(benchmark)
}
