package main

import (
	"encoding/json"
	"fmt"
	"net/url"
	"strconv"
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
// Pgwire and QWP egress share this scalar-bind path. The legacy HTTP transport
// sends hq.Path, where generated scalar values are literalized instead. The
// generated queries are semantically equivalent across all three transports,
// but their SQL text and bind mechanism are not identical.
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
	replacements := make([]string, len(rawParams)+1)
	for i, raw := range rawParams {
		origIdx := i + 1
		switch v := raw.(type) {
		case []interface{}:
			quoted := make([]string, 0, len(v))
			for _, item := range v {
				value := strings.ReplaceAll(fmt.Sprint(item), "'", "''")
				quoted = append(quoted, "'"+value+"'")
			}
			replacements[origIdx] = "(" + strings.Join(quoted, ",") + ")"
		default:
			params = append(params, v)
			replacements[origIdx] = fmt.Sprintf("$%d", len(params))
		}
	}

	return rewriteSQLPlaceholders(sqlTemplate, replacements), params, nil
}

func rewriteSQLPlaceholders(sqlTemplate string, replacements []string) string {
	var rewritten strings.Builder
	rewritten.Grow(len(sqlTemplate))
	for i := 0; i < len(sqlTemplate); {
		switch {
		case sqlTemplate[i] == '\'' || sqlTemplate[i] == '"':
			i = copySQLQuoted(&rewritten, sqlTemplate, i, sqlTemplate[i])
		case strings.HasPrefix(sqlTemplate[i:], "--"):
			i = copySQLLineComment(&rewritten, sqlTemplate, i)
		case strings.HasPrefix(sqlTemplate[i:], "/*"):
			i = copySQLBlockComment(&rewritten, sqlTemplate, i)
		case sqlTemplate[i] == '$':
			if delimiter := sqlDollarQuoteDelimiter(sqlTemplate, i); delimiter != "" {
				end := strings.Index(sqlTemplate[i+len(delimiter):], delimiter)
				if end < 0 {
					rewritten.WriteString(sqlTemplate[i:])
					return rewritten.String()
				}
				end += i + 2*len(delimiter)
				rewritten.WriteString(sqlTemplate[i:end])
				i = end
				continue
			}
			i = rewriteSQLPlaceholder(&rewritten, sqlTemplate, i, replacements)
		default:
			rewritten.WriteByte(sqlTemplate[i])
			i++
		}
	}
	return rewritten.String()
}

func copySQLQuoted(dst *strings.Builder, sqlTemplate string, start int, quote byte) int {
	dst.WriteByte(quote)
	for i := start + 1; i < len(sqlTemplate); {
		dst.WriteByte(sqlTemplate[i])
		if sqlTemplate[i] == '\\' && i+1 < len(sqlTemplate) {
			dst.WriteByte(sqlTemplate[i+1])
			i += 2
			continue
		}
		if sqlTemplate[i] == quote {
			if i+1 < len(sqlTemplate) && sqlTemplate[i+1] == quote {
				dst.WriteByte(quote)
				i += 2
				continue
			}
			return i + 1
		}
		i++
	}
	return len(sqlTemplate)
}

func copySQLLineComment(dst *strings.Builder, sqlTemplate string, start int) int {
	end := strings.IndexByte(sqlTemplate[start:], '\n')
	if end < 0 {
		dst.WriteString(sqlTemplate[start:])
		return len(sqlTemplate)
	}
	end += start + 1
	dst.WriteString(sqlTemplate[start:end])
	return end
}

func copySQLBlockComment(dst *strings.Builder, sqlTemplate string, start int) int {
	depth := 0
	for i := start; i < len(sqlTemplate); {
		switch {
		case strings.HasPrefix(sqlTemplate[i:], "/*"):
			depth++
			dst.WriteString("/*")
			i += 2
		case strings.HasPrefix(sqlTemplate[i:], "*/"):
			depth--
			dst.WriteString("*/")
			i += 2
			if depth == 0 {
				return i
			}
		default:
			dst.WriteByte(sqlTemplate[i])
			i++
		}
	}
	return len(sqlTemplate)
}

func sqlDollarQuoteDelimiter(sqlTemplate string, start int) string {
	if start+1 >= len(sqlTemplate) {
		return ""
	}
	if sqlTemplate[start+1] == '$' {
		return "$$"
	}
	first := sqlTemplate[start+1]
	if !((first >= 'a' && first <= 'z') || (first >= 'A' && first <= 'Z') || first == '_') {
		return ""
	}
	end := start + 2
	for end < len(sqlTemplate) {
		char := sqlTemplate[end]
		if char == '$' {
			return sqlTemplate[start : end+1]
		}
		if !((char >= 'a' && char <= 'z') || (char >= 'A' && char <= 'Z') ||
			(char >= '0' && char <= '9') || char == '_') {
			return ""
		}
		end++
	}
	return ""
}

func rewriteSQLPlaceholder(dst *strings.Builder, sqlTemplate string, start int, replacements []string) int {
	if start+1 == len(sqlTemplate) || sqlTemplate[start+1] < '0' || sqlTemplate[start+1] > '9' {
		dst.WriteByte('$')
		return start + 1
	}
	end := start + 2
	for end < len(sqlTemplate) && sqlTemplate[end] >= '0' && sqlTemplate[end] <= '9' {
		end++
	}
	origIdx, err := strconv.Atoi(sqlTemplate[start+1 : end])
	if err == nil && origIdx > 0 && origIdx < len(replacements) {
		dst.WriteString(replacements[origIdx])
	} else {
		dst.WriteString(sqlTemplate[start:end])
	}
	return end
}
