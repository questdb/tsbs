// tsbs_load_victoriametrics loads a VictoriaMetrics with data from stdin or file.
package main

import (
	"fmt"
	"log"
	"strings"

	"github.com/blagojts/viper"
	"github.com/questdb/tsbs/internal/utils"
	"github.com/questdb/tsbs/load"
	"github.com/questdb/tsbs/pkg/data/source"
	"github.com/questdb/tsbs/pkg/targets/victoriametrics"
	"github.com/spf13/pflag"
)

// Parse args:
func initProgramOptions() (*victoriametrics.SpecificConfig, load.BenchmarkRunner, *load.BenchmarkRunnerConfig) {
	target := victoriametrics.NewTarget()

	loaderConf := load.BenchmarkRunnerConfig{}
	loaderConf.AddToFlagSet(pflag.CommandLine)
	target.TargetSpecificFlags("", pflag.CommandLine)
	pflag.Parse()

	if err := utils.SetupConfigFile(); err != nil {
		panic(fmt.Errorf("fatal error config file: %s", err))
	}
	if err := viper.Unmarshal(&loaderConf); err != nil {
		panic(fmt.Errorf("unable to decode config: %s", err))
	}

	urls := viper.GetString("urls")
	if len(urls) == 0 {
		log.Fatalf("missing `urls` flag")
	}
	vmURLs := strings.Split(urls, ",")

	loader := load.GetBenchmarkRunner(loaderConf)
	return &victoriametrics.SpecificConfig{ServerURLs: vmURLs}, loader, &loaderConf
}

func main() {
	vmConf, loader, loaderConf := initProgramOptions()

	benchmark, err := victoriametrics.NewBenchmark(vmConf, &source.DataSourceConfig{
		Type: source.FileDataSourceType,
		File: &source.FileDataSourceConfig{Location: loaderConf.FileName},
	})
	if err != nil {
		panic(err)
	}
	loader.RunBenchmark(benchmark)
}
