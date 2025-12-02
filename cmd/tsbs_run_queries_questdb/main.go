// tsbs_run_queries_questdb speed tests QuestDB using requests from stdin or file.
//
// It reads encoded Query objects from stdin or file, and makes concurrent requests
// to the provided endpoint. Supports both HTTP/JSON and PostgreSQL wire protocol (pgx v5).
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/blagojts/viper"
	"github.com/jackc/pgx/v5"
	"github.com/questdb/tsbs/internal/utils"
	"github.com/questdb/tsbs/pkg/query"
	"github.com/spf13/pflag"
)

// Program option vars:
var (
	restURL  string
	username string
	password string
	// PostgreSQL mode options
	usePgx   bool
	pgHost   string
	pgPort   string
	pgUser   string
	pgPass   string
	pgDBName string
)

// Global vars:
var (
	runner *query.BenchmarkRunner
)

// Parse args:
func init() {
	var config query.BenchmarkRunnerConfig
	config.AddToFlagSet(pflag.CommandLine)

	// HTTP options
	pflag.String("url", "http://localhost:9000/", "Server URL for HTTP mode")
	pflag.String("username", "", "Basic auth username (HTTP mode)")
	pflag.String("password", "", "Basic auth password (HTTP mode)")

	// PostgreSQL/pgx options
	pflag.Bool("use-pgx", false, "Use PostgreSQL wire protocol (pgx v5) instead of HTTP")
	pflag.String("pg-host", "localhost", "PostgreSQL host")
	pflag.String("pg-port", "8812", "PostgreSQL port")
	pflag.String("pg-user", "admin", "PostgreSQL user")
	pflag.String("pg-pass", "quest", "PostgreSQL password")
	pflag.String("pg-db", "qdb", "PostgreSQL database name")

	pflag.Parse()

	err := utils.SetupConfigFile()

	if err != nil {
		panic(fmt.Errorf("fatal error config file: %s", err))
	}

	if err := viper.Unmarshal(&config); err != nil {
		panic(fmt.Errorf("unable to decode config: %s", err))
	}

	restURL = viper.GetString("url")
	username = viper.GetString("username")
	password = viper.GetString("password")

	usePgx = viper.GetBool("use-pgx")
	pgHost = viper.GetString("pg-host")
	pgPort = viper.GetString("pg-port")
	pgUser = viper.GetString("pg-user")
	pgPass = viper.GetString("pg-pass")
	pgDBName = viper.GetString("pg-db")

	runner = query.NewBenchmarkRunner(config)
}

func main() {
	runner.Run(&query.HTTPPool, newProcessor)
}

// preparedStmtCache caches prepared statements by SQL template
var (
	preparedStmtCache     = make(map[string]string) // sqlTemplate -> stmtName
	preparedStmtCacheMu   sync.RWMutex
	preparedStmtCounter   int
	preparedStmtCounterMu sync.Mutex
)

type processor struct {
	// HTTP mode
	httpClient *HTTPClient
	httpOpts   *HTTPClientDoOptions
	// pgx v5 mode - native connection (not database/sql)
	conn *pgx.Conn
	ctx  context.Context
	// Local cache of prepared statement names for this connection
	localPreparedStmts map[string]struct{}
}

func newProcessor() query.Processor { return &processor{} }

func (p *processor) Init(workerNumber int) {
	if usePgx {
		connStr := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
			pgHost, pgPort, pgUser, pgPass, pgDBName)
		p.ctx = context.Background()
		conn, err := pgx.Connect(p.ctx, connStr)
		if err != nil {
			panic(fmt.Sprintf("Unable to connect to QuestDB via pgx v5: %v", err))
		}
		p.conn = conn
		p.localPreparedStmts = make(map[string]struct{})
	} else {
		p.httpOpts = &HTTPClientDoOptions{
			Username:             username,
			Password:             password,
			Debug:                runner.DebugLevel(),
			PrettyPrintResponses: runner.DoPrintResponses(),
		}
		p.httpClient = NewHTTPClient(restURL)
	}
}

func (p *processor) ProcessQuery(q query.Query, _ bool) ([]*query.Stat, error) {
	hq := q.(*query.HTTP)

	var lag float64
	var err error

	if usePgx {
		lag, err = p.processQueryPgx(hq)
	} else {
		lag, err = p.httpClient.Do(hq, p.httpOpts)
	}

	if err != nil {
		return nil, err
	}
	stat := query.GetStat()
	stat.Init(q.HumanLabelName(), lag)
	return []*query.Stat{stat}, nil
}

// processQueryPgx extracts SQL from HTTP query and runs it via native pgx v5
// Supports parameterized queries with bind variables for better performance
func (p *processor) processQueryPgx(hq *query.HTTP) (float64, error) {
	// Check if query has parameters in Body (new parameterized format)
	if len(hq.Body) > 0 && len(hq.RawQuery) > 0 {
		return p.processQueryPgxWithParams(hq)
	}

	// Fall back to legacy non-parameterized path extraction
	pathStr := string(hq.Path)

	// Parse URL to extract query parameter
	// The path looks like: /exec?count=false&query=SELECT...
	if idx := strings.Index(pathStr, "?"); idx != -1 {
		queryStr := pathStr[idx+1:]
		values, err := url.ParseQuery(queryStr)
		if err != nil {
			return 0, fmt.Errorf("failed to parse query params: %v", err)
		}

		sqlQuery := values.Get("query")
		if sqlQuery == "" {
			return 0, fmt.Errorf("no SQL query found in path: %s", pathStr)
		}

		start := time.Now()

		// Use native pgx Query (not database/sql) for better performance
		rows, err := p.conn.Query(p.ctx, sqlQuery)
		if err != nil {
			return 0, fmt.Errorf("query failed: %v", err)
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

	return 0, fmt.Errorf("invalid path format: %s", pathStr)
}

// processQueryPgxWithParams executes parameterized query with bind variables
// Arrays are inlined since QuestDB doesn't support array bind params for IN clause
func (p *processor) processQueryPgxWithParams(hq *query.HTTP) (float64, error) {
	sqlTemplate := string(hq.RawQuery)

	// Parse parameters from JSON in Body
	var rawParams []interface{}
	if err := json.Unmarshal(hq.Body, &rawParams); err != nil {
		return 0, fmt.Errorf("failed to parse query params JSON: %v", err)
	}

	// First pass: inline arrays and track which original indices are kept
	var params []interface{}
	inlinedIndices := make(map[int]bool)
	for i, raw := range rawParams {
		switch v := raw.(type) {
		case []interface{}:
			// Inline array values in SQL (QuestDB doesn't support array bind params)
			placeholder := fmt.Sprintf("$%d", i+1)
			var quoted []string
			for _, item := range v {
				quoted = append(quoted, fmt.Sprintf("'%v'", item))
			}
			inlineList := "(" + strings.Join(quoted, ",") + ")"
			sqlTemplate = strings.Replace(sqlTemplate, placeholder, inlineList, 1)
			inlinedIndices[i+1] = true
		default:
			params = append(params, v)
		}
	}

	// Second pass: renumber remaining placeholders
	// Build mapping from old index to new index
	newIdx := 1
	for origIdx := 1; origIdx <= len(rawParams); origIdx++ {
		if !inlinedIndices[origIdx] {
			if origIdx != newIdx {
				sqlTemplate = strings.ReplaceAll(sqlTemplate, fmt.Sprintf("$%d", origIdx), fmt.Sprintf("$%d", newIdx))
			}
			newIdx++
		}
	}

	start := time.Now()

	// Use Query with prepared statement-style execution
	rows, err := p.conn.Query(p.ctx, sqlTemplate, params...)
	if err != nil {
		return 0, fmt.Errorf("parameterized query failed: %v (sql: %s)", err, sqlTemplate)
	}

	// Fetch all rows
	for rows.Next() {
	}
	rows.Close()

	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("row iteration error: %v", err)
	}

	lag := float64(time.Since(start).Nanoseconds()) / 1e6 // milliseconds
	return lag, nil
}
