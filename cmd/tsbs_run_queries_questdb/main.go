// tsbs_run_queries_questdb speed tests QuestDB using requests from stdin or file.
//
// It reads encoded Query objects from stdin or file, and makes concurrent requests
// to the provided endpoint. Three transports are supported, selected with
// --query-protocol: PostgreSQL wire (pgx v5, the default), HTTP/JSON on the REST
// endpoint, and QWEP, the QuestDB Wire Execution Protocol that streams results
// back as columnar batches.
package main

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/blagojts/viper"
	"github.com/jackc/pgx/v5"
	"github.com/questdb/tsbs/internal/utils"
	"github.com/questdb/tsbs/pkg/query"
	"github.com/spf13/pflag"
)

// Query transports supported by this runner.
const (
	protocolPGWire       = "pgwire"
	protocolHTTP         = "http"
	protocolQWEP         = "qwep"
	defaultQueryProtocol = protocolPGWire
)

func resolveQueryProtocol(value string, useHTTP bool) (string, error) {
	if useHTTP && value == protocolPGWire {
		return protocolHTTP, nil
	}
	switch value {
	case protocolPGWire, protocolHTTP, protocolQWEP:
		return value, nil
	default:
		return "", fmt.Errorf("unknown query protocol %q, expected %q, %q or %q",
			value, protocolPGWire, protocolHTTP, protocolQWEP)
	}
}

// Program option vars:
var (
	protocol string
	restURL  string
	username string
	password string
	// PostgreSQL mode options (default)
	useHTTP  bool
	pgHost   string
	pgPort   string
	pgUser   string
	pgPass   string
	pgDBName string
	// QWP mode options
	qwpAddr      string
	qwpQueryConf string
	qwpUseTLS    bool
)

// Global vars:
var (
	runner *query.BenchmarkRunner
)

// Parse args:
func init() {
	var config query.BenchmarkRunnerConfig
	config.AddToFlagSet(pflag.CommandLine)

	// PostgreSQL/pgx options (default mode)
	pflag.String("pg-host", "localhost", "PostgreSQL host")
	pflag.String("pg-port", "8812", "PostgreSQL port")
	pflag.String("pg-user", "admin", "PostgreSQL user")
	pflag.String("pg-pass", "quest", "PostgreSQL password")
	pflag.String("pg-db", "qdb", "PostgreSQL database name")

	// HTTP options (legacy mode)
	pflag.Bool("use-http", false, "Use HTTP REST API instead of PostgreSQL wire protocol. Deprecated, same as --query-protocol=http")
	pflag.String("url", "http://localhost:9000/", "Server URL for HTTP mode")
	pflag.String("username", "", "Basic auth username (HTTP and QWP modes)")
	pflag.String("password", "", "Basic auth password (HTTP and QWP modes)")

	// Query transport
	pflag.String("query-protocol", defaultQueryProtocol, "Query transport: 'pgwire' (PostgreSQL wire), 'http' (REST /exec), or 'qwep' (QuestDB Wire Execution Protocol)")

	// QWP options
	pflag.String("qwp-addr", "127.0.0.1:9000", "QuestDB wire protocol WebSocket ip:port. Comma-separated list enables failover")
	pflag.String("qwp-conf", "", "Full QWP query client configuration string. Overrides every other QWP connection flag")
	pflag.Bool("qwp-tls", false, "Use TLS for QWP. The certificate check is disabled, so the client will trust any server")

	pflag.Parse()

	err := utils.SetupConfigFile()

	if err != nil {
		panic(fmt.Errorf("fatal error config file: %s", err))
	}

	if err := viper.Unmarshal(&config); err != nil {
		panic(fmt.Errorf("unable to decode config: %s", err))
	}

	pgHost = viper.GetString("pg-host")
	pgPort = viper.GetString("pg-port")
	pgUser = viper.GetString("pg-user")
	pgPass = viper.GetString("pg-pass")
	pgDBName = viper.GetString("pg-db")

	useHTTP = viper.GetBool("use-http")
	restURL = viper.GetString("url")
	username = viper.GetString("username")
	password = viper.GetString("password")

	qwpAddr = viper.GetString("qwp-addr")
	qwpQueryConf = viper.GetString("qwp-conf")
	qwpUseTLS = viper.GetBool("qwp-tls")

	protocol, err = resolveQueryProtocol(viper.GetString("query-protocol"), useHTTP)
	if err != nil {
		panic(err)
	}

	runner = query.NewBenchmarkRunner(config)
}

func main() {
	runner.Run(&query.HTTPPool, newProcessor)
}

type processor struct {
	// HTTP mode
	httpClient *HTTPClient
	httpOpts   *HTTPClientDoOptions
	// QWP mode
	qwpClient *QwpClient
	// pgx mode
	conn *pgx.Conn
	ctx  context.Context
}

func newProcessor() query.Processor { return &processor{} }

func (p *processor) Init(workerNumber int) {
	switch protocol {
	case protocolHTTP:
		p.httpOpts = &HTTPClientDoOptions{
			Username:             username,
			Password:             password,
			Debug:                runner.DebugLevel(),
			PrettyPrintResponses: runner.DoPrintResponses(),
		}
		p.httpClient = NewHTTPClient(restURL)
	case protocolQWEP:
		// One query client per worker: a client is not safe for
		// concurrent Query calls.
		client, err := NewQwpClient(qwpConf(), &QwpClientDoOptions{
			Debug:                runner.DebugLevel(),
			PrettyPrintResponses: runner.DoPrintResponses(),
		})
		if err != nil {
			panic(fmt.Sprintf("Unable to connect to QuestDB via QWP: %v", err))
		}
		p.qwpClient = client
	default:
		connStr := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
			pgHost, pgPort, pgUser, pgPass, pgDBName)
		p.ctx = context.Background()
		conn, err := pgx.Connect(p.ctx, connStr)
		if err != nil {
			panic(fmt.Sprintf("Unable to connect to QuestDB via pgx: %v", err))
		}
		p.conn = conn
	}
}

func (p *processor) ProcessQuery(q query.Query, _ bool) ([]*query.Stat, error) {
	hq := q.(*query.HTTP)

	var lag float64
	var err error

	switch protocol {
	case protocolHTTP:
		lag, err = p.httpClient.Do(hq, p.httpOpts)
	case protocolQWEP:
		lag, err = p.qwpClient.Do(hq)
	default:
		lag, err = p.processQueryPgx(hq)
	}

	if err != nil {
		return nil, err
	}
	stat := query.GetStat()
	stat.Init(q.HumanLabelName(), lag)
	return []*query.Stat{stat}, nil
}

func (p *processor) Close() error {
	var pgErr, qwpErr error
	if p.conn != nil {
		pgErr = p.conn.Close(p.ctx)
	}
	if p.qwpClient != nil {
		qwpErr = p.qwpClient.Close()
	}
	return errors.Join(pgErr, qwpErr)
}

// processQueryPgx runs a query via native pgx v5, using bind variables
// for the parameters the query carries.
func (p *processor) processQueryPgx(hq *query.HTTP) (float64, error) {
	sqlQuery, params, err := sqlFromQuery(hq)
	if err != nil {
		return 0, err
	}

	start := time.Now()

	// Use native pgx Query (not database/sql) for better performance
	rows, err := p.conn.Query(p.ctx, sqlQuery, params...)
	if err != nil {
		return 0, fmt.Errorf("query failed: %v (sql: %s)", err, sqlQuery)
	}

	// Fetch all rows - same approach as TimescaleDB benchmark
	for rows.Next() {
	}
	rows.Close()

	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("row iteration error: %v", err)
	}

	lag := float64(time.Since(start).Nanoseconds()) / 1e6 // milliseconds
	return lag, nil
}
