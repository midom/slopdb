package mysqlserver

import (
	"fmt"
	"strconv"
	"strings"

	"slopdb/db"
	"slopdb/sql"
)

type mysqlWarning struct {
	code    uint16
	message string
}

func (c *client) addWarning(code uint16, message string) {
	c.warningMu.Lock()
	defer c.warningMu.Unlock()
	c.warningTotal++
	if len(c.warnings) < 1024 {
		c.warnings = append(c.warnings, mysqlWarning{code, message})
	}
}
func (c *client) warningCount() uint16 {
	c.warningMu.Lock()
	defer c.warningMu.Unlock()
	if c.warningTotal > 65535 {
		return 65535
	}
	return uint16(c.warningTotal)
}
func (c *client) beginDiagnostics(query string) {
	t := sql.NewTokenizerWithOptions(query, c.currentDatabase().ParseOptions())
	a, b := t.Next(), t.Next()
	if strings.EqualFold(a.Value, "SHOW") && (strings.EqualFold(b.Value, "WARNINGS") || strings.EqualFold(b.Value, "COUNT")) {
		return
	}
	if strings.EqualFold(a.Value, "SELECT") && strings.Contains(strings.ToLower(query), "warning_count") {
		return
	}
	c.warningMu.Lock()
	c.warningTotal = 0
	c.warnings = nil
	c.warningMu.Unlock()
}
func (c *client) showWarnings(tokens []sql.Token) (*db.ExecResult, bool, error) {
	if !tokenWord(tokens, 0, "SHOW") || !tokenWord(tokens, 1, "WARNINGS") {
		return nil, false, nil
	}
	start, limit := 0, 1024
	if len(tokens) > 2 {
		if !tokenWord(tokens, 2, "LIMIT") {
			return nil, true, fmt.Errorf("expected LIMIT in SHOW WARNINGS")
		}
		if len(tokens) != 4 && len(tokens) != 6 {
			return nil, true, fmt.Errorf("invalid SHOW WARNINGS LIMIT")
		}
		n, err := strconv.Atoi(tokens[3].Value)
		if err != nil || n < 0 {
			return nil, true, fmt.Errorf("invalid warning limit")
		}
		limit = n
		if len(tokens) == 6 {
			if tokens[4].Type != sql.TokComma {
				return nil, true, fmt.Errorf("invalid warning offset")
			}
			m, err := strconv.Atoi(tokens[5].Value)
			if err != nil || m < 0 {
				return nil, true, fmt.Errorf("invalid warning limit")
			}
			start, limit = n, m
		}
	}
	r := &db.ExecResult{Columns: []sql.ColumnDef{{Name: "Level", ColType: sql.ColText}, {Name: "Code", ColType: sql.ColInteger}, {Name: "Message", ColType: sql.ColText}}}
	c.warningMu.Lock()
	defer c.warningMu.Unlock()
	for i := start; i < len(c.warnings) && len(r.Rows) < limit; i++ {
		w := c.warnings[i]
		r.Rows = append(r.Rows, map[string]sql.Value{"Level": sql.NewTextValue("Warning"), "Code": sql.NewIntValue(int64(w.code)), "Message": sql.NewTextValue(w.message)})
	}
	return r, true, nil
}
