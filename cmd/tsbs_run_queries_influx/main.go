// tsbs_run_queries_influx speed tests InfluxDB using requests from stdin.
//
// It reads encoded Query objects from stdin, and makes concurrent requests
// to the provided HTTP endpoint. This program has no knowledge of the
// internals of the endpoint.
package main

import (
	"fmt"
	"log"
	"strings"

	"github.com/blagojts/viper"
	"github.com/questdb/tsbs/internal/utils"
	"github.com/questdb/tsbs/pkg/query"
	"github.com/spf13/pflag"
)

// Program option vars:
var (
	daemonUrls    []string
	chunkSize     uint64
	authToken     string
	influxVersion string // "v1", "v2", or "v3"
)

// Global vars:
var (
	runner *query.BenchmarkRunner
)

// Parse args:
func init() {
	var config query.BenchmarkRunnerConfig
	config.AddToFlagSet(pflag.CommandLine)
	var csvDaemonUrls string

	pflag.String("urls", "http://localhost:8086", "Daemon URLs, comma-separated. Will be used in a round-robin fashion.")
	pflag.Uint64("chunk-response-size", 0, "Number of series to chunk results into. 0 means no chunking.")
	pflag.String("auth-token", "", "Use the Authorization header with the Token scheme to provide your token to InfluxDB. If empty will not send the Authorization header.")
	pflag.String("influx-version", "v1", "InfluxDB version: v1, v2, or v3. Determines which query client to use.")

	pflag.Parse()

	err := utils.SetupConfigFile()

	if err != nil {
		panic(fmt.Errorf("fatal error config file: %s", err))
	}

	if err := viper.Unmarshal(&config); err != nil {
		panic(fmt.Errorf("unable to decode config: %s", err))
	}

	csvDaemonUrls = viper.GetString("urls")
	authToken = viper.GetString("auth-token")
	chunkSize = viper.GetUint64("chunk-response-size")
	influxVersion = viper.GetString("influx-version")
	if err := validateInfluxVersion(influxVersion); err != nil {
		log.Fatal(err)
	}
	if err := validateQueryOptions(influxVersion, chunkSize, config.PrintResponses, config.Debug); err != nil {
		log.Fatal(err)
	}
	log.Printf("Using InfluxDB %s API", influxVersion)
	if authToken != "" {
		log.Println("Using Authorization header in benchmark")
	} else {
		log.Println("Given no Authorization header was provided will not send it in benchmark")
	}
	daemonUrls = strings.Split(csvDaemonUrls, ",")
	if len(daemonUrls) == 0 {
		log.Fatal("missing 'urls' flag")
	}

	runner = query.NewBenchmarkRunner(config)
}

func validateInfluxVersion(version string) error {
	switch version {
	case "v1", "v2", "v3":
		return nil
	default:
		return fmt.Errorf("invalid influx-version %q; expected v1, v2, or v3", version)
	}
}

func validateQueryOptions(version string, chunkSize uint64, printResponses bool, debug int) error {
	if version != "v3" {
		return nil
	}
	if chunkSize != 0 {
		return fmt.Errorf("--chunk-response-size is not supported for InfluxDB v3 Arrow queries")
	}
	if printResponses {
		return fmt.Errorf("--print-responses is not supported for InfluxDB v3 Arrow queries")
	}
	if debug == 4 {
		return fmt.Errorf("debug level 4 response-body output is not supported for InfluxDB v3 Arrow queries")
	}
	return nil
}

func main() {
	runner.Run(&query.HTTPPool, newProcessor)
}

type processor struct {
	httpClient   *HTTPClient
	flightClient *FlightClient
	opts         *HTTPClientDoOptions
}

var newFlightClient = NewFlightClient

func newProcessor() query.Processor { return &processor{} }

func (p *processor) Init(workerNumber int) {
	p.opts = &HTTPClientDoOptions{
		Debug:                runner.DebugLevel(),
		PrettyPrintResponses: runner.DoPrintResponses(),
		chunkSize:            chunkSize,
		database:             runner.DatabaseName(),
	}
	url := daemonUrls[workerNumber%len(daemonUrls)]

	if influxVersion == "v3" {
		// Use Flight client for v3
		var err error
		p.flightClient, err = newFlightClient(url, runner.DatabaseName(), authToken)
		if err != nil {
			log.Fatalf("Failed to create Flight client: %v", err)
		}
	} else {
		// Use HTTP client for v1/v2
		p.httpClient = NewHTTPClient(url, authToken)
	}
}

func (p *processor) ProcessQuery(q query.Query, _ bool) ([]*query.Stat, error) {
	hq := q.(*query.HTTP)
	var lag float64
	var err error

	if influxVersion == "v3" {
		lag, err = p.flightClient.Do(hq, p.opts)
	} else {
		lag, err = p.httpClient.Do(hq, p.opts)
	}

	if err != nil {
		return nil, err
	}
	stat := query.GetStat()
	stat.Init(q.HumanLabelName(), lag)
	return []*query.Stat{stat}, nil
}

func (p *processor) Close() error {
	if p.flightClient != nil {
		return p.flightClient.Close()
	}
	return nil
}
