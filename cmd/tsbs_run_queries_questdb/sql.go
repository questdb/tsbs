package main

import (
	"encoding/json"
	"fmt"
	"net/url"
	"strings"

	"github.com/questdb/tsbs/pkg/query"
)

// sqlFromQuery turns a generated query into the SQL text and the scalar
// bind parameters to send with it.
//
// Generated queries carry a parameterized SQL template in RawQuery and a
// JSON array of parameters in Body. Array parameters are inlined into the
// SQL, because QuestDB does not accept an array bind parameter for an IN
// clause, and the remaining placeholders are renumbered so the scalars
// that survive are $1..$n in order. Older query files carry no template,
// only an HTTP path with the SQL in its query string; those return no
// parameters.
//
// Every transport shares this, so the server sees the same statement and
// the same bind values whichever one runs the benchmark.
func sqlFromQuery(hq *query.HTTP) (string, []interface{}, error) {
	if len(hq.Body) > 0 && len(hq.RawQuery) > 0 {
		return inlineArrayParams(string(hq.RawQuery), hq.Body)
	}

	pathStr := string(hq.Path)
	idx := strings.Index(pathStr, "?")
	if idx == -1 {
		return "", nil, fmt.Errorf("invalid path format: %s", pathStr)
	}
	values, err := url.ParseQuery(pathStr[idx+1:])
	if err != nil {
		return "", nil, fmt.Errorf("failed to parse query params: %v", err)
	}
	sqlQuery := values.Get("query")
	if sqlQuery == "" {
		return "", nil, fmt.Errorf("no SQL query found in path: %s", pathStr)
	}
	return sqlQuery, nil, nil
}

// inlineArrayParams implements the array-inlining and renumbering
// described on sqlFromQuery.
func inlineArrayParams(sqlTemplate string, body []byte) (string, []interface{}, error) {
	var rawParams []interface{}
	if err := json.Unmarshal(body, &rawParams); err != nil {
		return "", nil, fmt.Errorf("failed to parse query params JSON: %v", err)
	}

	var params []interface{}
	inlinedIndices := make(map[int]bool)
	for i, raw := range rawParams {
		switch v := raw.(type) {
		case []interface{}:
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

	newIdx := 1
	for origIdx := 1; origIdx <= len(rawParams); origIdx++ {
		if !inlinedIndices[origIdx] {
			if origIdx != newIdx {
				sqlTemplate = strings.ReplaceAll(sqlTemplate, fmt.Sprintf("$%d", origIdx), fmt.Sprintf("$%d", newIdx))
			}
			newIdx++
		}
	}

	return sqlTemplate, params, nil
}
