package questdb

import (
	"encoding/json"
	"fmt"
	"net/url"
	"time"

	"github.com/questdb/tsbs/cmd/tsbs_generate_queries/uses/devops"
	"github.com/questdb/tsbs/cmd/tsbs_generate_queries/utils"
	"github.com/questdb/tsbs/pkg/query"
)

// BaseGenerator contains settings specific for QuestDB
type BaseGenerator struct {
}

// GenerateEmptyQuery returns an empty query.QuestDB.
func (g *BaseGenerator) GenerateEmptyQuery() query.Query {
	return query.NewHTTP()
}

// fillInQuery fills the query struct with data (legacy non-parameterized).
func (g *BaseGenerator) fillInQuery(qi query.Query, humanLabel, humanDesc, sql string) {
	v := url.Values{}
	v.Set("count", "false")
	v.Set("query", sql)
	q := qi.(*query.HTTP)
	q.HumanLabel = []byte(humanLabel)
	q.RawQuery = []byte(sql)
	q.HumanDescription = []byte(humanDesc)
	q.Method = []byte("GET")
	q.Path = []byte(fmt.Sprintf("/exec?%s", v.Encode()))
	q.Body = nil
}

// fillInQueryWithParams fills the query struct with parameterized SQL and bind values.
// sqlTemplate uses $1, $2, etc. placeholders for PostgreSQL prepared statements.
// params contains the values to bind at runtime.
func (g *BaseGenerator) fillInQueryWithParams(qi query.Query, humanLabel, humanDesc, sqlTemplate string, params []interface{}) {
	q := qi.(*query.HTTP)
	q.HumanLabel = []byte(humanLabel)
	q.HumanDescription = []byte(humanDesc)
	q.Method = []byte("GET")

	// Store parameterized SQL in RawQuery
	q.RawQuery = []byte(sqlTemplate)

	// Store parameters as JSON in Body for pgx mode
	paramsJSON, _ := json.Marshal(params)
	q.Body = paramsJSON

	// For HTTP mode, we still need to generate the full SQL path
	// Substitute parameters to create the HTTP URL
	fullSQL := substituteParams(sqlTemplate, params)
	v := url.Values{}
	v.Set("count", "false")
	v.Set("query", fullSQL)
	q.Path = []byte(fmt.Sprintf("/exec?%s", v.Encode()))
}

// substituteParams replaces $1, $2, etc. with actual values for HTTP mode
func substituteParams(sql string, params []interface{}) string {
	result := sql
	for i, param := range params {
		placeholder := fmt.Sprintf("$%d", i+1)
		var replacement string
		switch v := param.(type) {
		case string:
			replacement = fmt.Sprintf("'%s'", v)
		case []string:
			// Handle string arrays for hostname IN clauses
			quoted := make([]string, len(v))
			for j, s := range v {
				quoted[j] = fmt.Sprintf("'%s'", s)
			}
			replacement = fmt.Sprintf("(%s)", joinStrings(quoted, ", "))
		default:
			replacement = fmt.Sprintf("%v", v)
		}
		result = replaceFirst(result, placeholder, replacement)
	}
	return result
}

func replaceFirst(s, old, new string) string {
	for i := 0; i <= len(s)-len(old); i++ {
		if s[i:i+len(old)] == old {
			return s[:i] + new + s[i+len(old):]
		}
	}
	return s
}

func joinStrings(strs []string, sep string) string {
	if len(strs) == 0 {
		return ""
	}
	result := strs[0]
	for i := 1; i < len(strs); i++ {
		result += sep + strs[i]
	}
	return result
}

// NewDevops creates a new devops use case query generator.
func (g *BaseGenerator) NewDevops(start, end time.Time, scale int) (utils.QueryGenerator, error) {
	core, err := devops.NewCore(start, end, scale)

	if err != nil {
		return nil, err
	}

	devops := &Devops{
		BaseGenerator: g,
		Core:          core,
	}

	return devops, nil
}
