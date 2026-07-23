// bulk_load_questdb loads an QuestDB daemon with data from stdin.
//
// The caller is responsible for assuring that the database is empty before
// bulk load.
package main

import (
	"bufio"
	"bytes"
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/blagojts/viper"
	"github.com/questdb/tsbs/internal/utils"
	"github.com/questdb/tsbs/load"
	"github.com/questdb/tsbs/pkg/targets"
	"github.com/questdb/tsbs/pkg/targets/constants"
	"github.com/questdb/tsbs/pkg/targets/initializers"
	"github.com/spf13/pflag"
)

// Ingestion protocols supported by the loader.
const (
	protocolQWP     = "qwp"
	protocolILP     = "ilp"
	protocolILPHTTP = "ilp-http"
)

// Program option vars:
var (
	protocol           string
	questdbILPBindTo   string
	questdbILPHTTPAddr string
	questdbQWPAddr     string
	qwpConfString      string
	qwpUser            string
	qwpPassword        string
	qwpToken           string
	qwpSFDir           string
	awaitAck           bool
	nanoTimestamps     bool
	qwpCloseTimeoutMs  uint
	qwpPreencodeReplay bool
	useTLS             bool
	authTokenId        string
	authToken          string
)

// Global vars
var (
	loader  load.BenchmarkRunner
	config  load.BenchmarkRunnerConfig
	bufPool sync.Pool
	target  targets.ImplementedTarget

	// input is the data stream, and qwpDec is non-nil when that stream
	// holds a binary QWP data file rather than ILP text. Both are set up
	// once in main, before the loader starts, so the data source and the
	// batch factory agree on the input format.
	input  *bufio.Reader
	qwpDec *qwpDecoder
)

// allows for testing
var fatal = log.Fatalf

// Parse args:
func init() {
	target = initializers.GetTarget(constants.FormatQuestDB)
	config = load.BenchmarkRunnerConfig{}
	// Not all the default flags apply to QuestDB
	// config.AddToFlagSet(pflag.CommandLine)
	pflag.CommandLine.Uint("batch-size", 10000, "Number of items to batch together in a single insert")
	pflag.CommandLine.Uint("workers", 1, "Number of parallel clients inserting")
	pflag.CommandLine.Uint64("limit", 0, "Number of items to insert (0 = all of them).")
	pflag.CommandLine.Bool("do-load", true, "Whether to write data. Set this flag to false to check input read speed.")
	pflag.CommandLine.Duration("reporting-period", 10*time.Second, "Period to report write stats")
	pflag.CommandLine.String("file", "", "File name to read data from")
	pflag.CommandLine.Int64("seed", 0, "PRNG seed (default: 0, which uses the current timestamp)")
	pflag.CommandLine.String("insert-intervals", "", "Time to wait between each insert, default '' => all workers insert ASAP. '1,2' = worker 1 waits 1s between inserts, worker 2 and others wait 2s")
	pflag.CommandLine.Bool("hash-workers", false, "Whether to consistently hash insert data to the same workers (i.e., the data for a particular host always goes to the same worker)")
	pflag.CommandLine.Bool("tls", false, "Whether to use TLS encryption for database connection. The certificate check is disabled, so the client will trust any server")
	pflag.CommandLine.String("auth-id", "", "ILP authentication token id")
	pflag.CommandLine.String("auth-token", "", "ILP authentication token")
	pflag.CommandLine.String("protocol", protocolQWP, "Ingestion protocol: 'qwp' (QuestDB Wire Protocol over WebSocket), 'ilp-http' (influx line protocol over HTTP) or 'ilp' (influx line protocol over TCP)")
	pflag.CommandLine.String("ilp-http-addr", "127.0.0.1:9000", "QuestDB HTTP ip:port for --protocol=ilp-http")
	pflag.CommandLine.String("qwp-conf", "", "Full QWP client configuration string. Overrides every other QWP connection flag")
	pflag.CommandLine.String("qwp-user", "", "QWP basic auth user name")
	pflag.CommandLine.String("qwp-password", "", "QWP basic auth password")
	pflag.CommandLine.String("qwp-token", "", "QWP bearer token")
	pflag.CommandLine.String("qwp-sf-dir", "", "QWP store-and-forward directory. Empty means memory mode, which is what a throughput benchmark wants")
	pflag.CommandLine.Bool("qwp-await-ack", false, "Wait for the server to acknowledge every batch before counting it. Slower, but every reported row is server-confirmed when counted")
	pflag.CommandLine.Bool("qwp-nano-timestamps", false, "Send nanosecond designated timestamps over QWP. Off by default so that the table matches the one the ILP path creates, which is microsecond resolution")
	pflag.CommandLine.Uint("qwp-close-timeout-ms", 60000, "How long Close waits for the server to acknowledge outstanding batches. Close is the loader's ack barrier, so this bounds the wait for the last batches of a run")
	pflag.CommandLine.Bool("qwp-preencode-replay", false, "Pre-encode binary TSBS input into QWP WebSocket frames outside the timed interval, then replay those frames. This measures server ingestion without row-builder CPU")
	target.TargetSpecificFlags("", pflag.CommandLine)
	pflag.Parse()

	err := utils.SetupConfigFile()

	if err != nil {
		panic(fmt.Errorf("fatal error config file: %s", err))
	}

	if err := viper.Unmarshal(&config); err != nil {
		panic(fmt.Errorf("unable to decode config: %s", err))
	}

	protocol = viper.GetString("protocol")
	switch protocol {
	case protocolQWP, protocolILP, protocolILPHTTP:
	default:
		panic(fmt.Errorf("unknown protocol %q, expected %q, %q or %q",
			protocol, protocolQWP, protocolILPHTTP, protocolILP))
	}
	questdbILPBindTo = viper.GetString("ilp-bind-to")
	questdbILPHTTPAddr = viper.GetString("ilp-http-addr")
	questdbQWPAddr = viper.GetString("qwp-addr")
	qwpConfString = viper.GetString("qwp-conf")
	qwpUser = viper.GetString("qwp-user")
	qwpPassword = viper.GetString("qwp-password")
	qwpToken = viper.GetString("qwp-token")
	qwpSFDir = viper.GetString("qwp-sf-dir")
	awaitAck = viper.GetBool("qwp-await-ack")
	nanoTimestamps = viper.GetBool("qwp-nano-timestamps")
	qwpCloseTimeoutMs = viper.GetUint("qwp-close-timeout-ms")
	qwpPreencodeReplay = viper.GetBool("qwp-preencode-replay")
	useTLS = viper.GetBool("tls")
	authTokenId = viper.GetString("auth-id")
	authToken = viper.GetString("auth-token")
	config.HashWorkers = false
	config.NoFlowControl = true
	loader = load.GetBenchmarkRunner(config)
}

type benchmark struct{}

func (b *benchmark) GetDataSource() targets.DataSource {
	if qwpDec != nil {
		return &qwpDataSource{dec: qwpDec}
	}
	return &fileDataSource{scanner: bufio.NewScanner(input)}
}

func (b *benchmark) GetBatchFactory() targets.BatchFactory {
	if qwpDec != nil {
		return &qwpFactory{}
	}
	return &factory{}
}

func (b *benchmark) GetPointIndexer(_ uint) targets.PointIndexer {
	return &targets.ConstantIndexer{}
}

func (b *benchmark) GetProcessor() targets.Processor {
	// Both client-library transports share the row-builder processor;
	// only the legacy raw-socket ILP path has its own.
	if protocol == protocolQWP || protocol == protocolILPHTTP {
		return &qwpProcessor{}
	}
	return &processor{}
}

// senderConf builds the client configuration string for a worker's
// sender. Auto-flush is off: the loader flushes on TSBS batch boundaries,
// which for QWP also keeps every published batch below the server's
// ~2 MiB frame cap at the default --batch-size.
func senderConf(numWorker int) string {
	if protocol == protocolILPHTTP {
		return ilpHTTPConf()
	}
	return qwpConf(numWorker)
}

// ilpHTTPConf configures line protocol over HTTP. Unlike the TCP path it
// is request/response, so a successful Flush means the server processed
// that batch. It is also served by the server's shared thread pools rather
// than the dedicated ILP/TCP pools, whose size varies by build and
// configuration, which makes it the steadier line protocol baseline.
func ilpHTTPConf() string {
	if qwpConfString != "" {
		return qwpConfString
	}

	var sb strings.Builder
	if useTLS {
		sb.WriteString("https::")
	} else {
		sb.WriteString("http::")
	}
	sb.WriteString("addr=")
	sb.WriteString(questdbILPHTTPAddr)
	sb.WriteString(";auto_flush=off;")
	if useTLS {
		sb.WriteString("tls_verify=unsafe_off;")
	}
	if qwpUser != "" {
		sb.WriteString("username=")
		sb.WriteString(qwpUser)
		sb.WriteString(";password=")
		sb.WriteString(qwpPassword)
		sb.WriteString(";")
	}
	if qwpToken != "" {
		sb.WriteString("token=")
		sb.WriteString(qwpToken)
		sb.WriteString(";")
	}
	return sb.String()
}

// qwpConf builds the QWP configuration string.
func qwpConf(numWorker int) string {
	if qwpConfString != "" {
		return qwpConfString
	}

	var sb strings.Builder
	if useTLS {
		sb.WriteString("wss::")
	} else {
		sb.WriteString("ws::")
	}
	sb.WriteString("addr=")
	sb.WriteString(questdbQWPAddr)
	sb.WriteString(";auto_flush=off;")
	// Close drains and waits for outstanding ACKs, and that wait is the
	// loader's ack barrier: the client's 5s default is not enough for the
	// last batches of a large run, and a timeout there means unacked rows.
	sb.WriteString("close_flush_timeout_millis=")
	sb.WriteString(strconv.FormatUint(uint64(qwpCloseTimeoutMs), 10))
	sb.WriteString(";")
	if useTLS {
		// Same posture as the ILP path: the certificate is not checked.
		sb.WriteString("tls_verify=unsafe_off;")
	}
	if qwpUser != "" {
		sb.WriteString("username=")
		sb.WriteString(qwpUser)
		sb.WriteString(";password=")
		sb.WriteString(qwpPassword)
		sb.WriteString(";")
	}
	if qwpToken != "" {
		sb.WriteString("token=")
		sb.WriteString(qwpToken)
		sb.WriteString(";")
	}
	if qwpSFDir != "" {
		// Each sender needs its own slot under the shared directory.
		sb.WriteString("sf_dir=")
		sb.WriteString(qwpSFDir)
		sb.WriteString(";sender_id=tsbs-")
		sb.WriteString(strconv.Itoa(numWorker))
		sb.WriteString(";")
	}
	return sb.String()
}

func (b *benchmark) GetDBCreator() targets.DBCreator {
	return &dbCreator{}
}

func main() {
	bufPool = sync.Pool{
		New: func() interface{} {
			return bytes.NewBuffer(make([]byte, 0, 4*1024*1024))
		},
	}

	input = load.GetBufferedReader(config.FileName)
	binaryInput, err := qwpDetect(input)
	if err != nil {
		fatal("failed to read input: %v", err)
	}
	if binaryInput {
		// Both client-library transports build rows through the same
		// API, so either can send a binary file. Only the legacy
		// raw-socket ILP path needs line protocol text.
		if protocol == protocolILP {
			fatal("input is a binary QWP data file, which --protocol=%s cannot send. Generate with --format questdb for ILP over TCP", protocol)
		}
		qwpDec, err = newQwpDecoder(input)
		if err != nil {
			fatal("failed to read QWP data file: %v", err)
		}
	}

	if qwpPreencodeReplay {
		if protocol != protocolQWP {
			fatal("--qwp-preencode-replay requires --protocol=%s", protocolQWP)
		}
		if qwpDec == nil {
			fatal("--qwp-preencode-replay requires binary QWP input generated with --format=questdb-qwp")
		}
		if qwpConfString != "" {
			fatal("--qwp-preencode-replay does not support --qwp-conf; use the explicit QWP address and authentication flags")
		}
		if awaitAck {
			fatal("--qwp-await-ack paces every generated batch and is incompatible with raw replay; replay validates the final cumulative ACK instead")
		}
		if err := runQwpPreencodedReplay(qwpDec, config); err != nil {
			fatal("QWP pre-encoded replay failed: %v", err)
		}
		return
	}

	loader.RunBenchmark(&benchmark{})
}
