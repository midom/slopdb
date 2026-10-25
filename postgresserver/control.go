package postgresserver

import (
	"context"
	"github.com/jackc/pgx/v5/pgproto3"
	"strings"

	"slopdb/db"
	"slopdb/sql"
)

type helper struct {
	kind          string
	fields        []field
	value         func(*client) string
	name, setting string
}

func tokens(query string) []sql.Token {
	t := sql.NewTokenizer(query)
	var out []sql.Token
	for tok := t.Next(); tok.Type != sql.TokEOF; tok = t.Next() {
		out = append(out, tok)
		if len(out) > 16 {
			return nil
		}
	}
	if len(out) > 0 && out[len(out)-1].Type == sql.TokSemicolon {
		out = out[:len(out)-1]
	}
	for _, tok := range out {
		if tok.Type == sql.TokSemicolon {
			return nil
		}
	}
	return out
}
func (c *client) helperMetadata(query string) (helper, bool) {
	ts := tokens(query)
	if len(ts) == 0 {
		return helper{}, false
	}
	equal := func(i int, s string) bool {
		return i < len(ts) && strings.EqualFold(ts[i].Value, s) && ts[i].Type != sql.TokString
	}
	scalar := func(name string, oid uint32, value func(*client) string) (helper, bool) {
		return helper{kind: "SELECT", fields: []field{{name, oid}}, value: value}, true
	}
	if equal(0, "BEGIN") || equal(0, "START") {
		i := 1
		if equal(i, "TRANSACTION") || equal(i, "WORK") {
			i++
		} else if equal(0, "START") {
			return helper{}, false
		}
		if equal(i, "ISOLATION") {
			if !equal(i+1, "LEVEL") || i+3 >= len(ts) {
				return helper{}, false
			}
			if _, err := sql.ParseIsolationLevel(ts[i+2].Value + " " + ts[i+3].Value); err != nil {
				return helper{}, false
			}
			i += 4
		}
		if equal(i, "READ") && equal(i+1, "WRITE") {
			i += 2
		}
		if i == len(ts) {
			return helper{kind: "BEGIN", setting: query}, true
		}
	}
	if equal(0, "SELECT") && len(ts) == 4 && equal(1, "CURRENT_SCHEMA") && ts[2].Type == sql.TokLParen && ts[3].Type == sql.TokRParen {
		return helper{kind: "CURRENT_SCHEMA", fields: []field{{"current_schema", oidText}}}, true
	}
	if equal(0, "SELECT") {
		if len(ts) == 2 && (equal(1, "CURRENT_USER") || equal(1, "SESSION_USER")) {
			return scalar(strings.ToLower(ts[1].Value), oidText, func(c *client) string { return c.server.config.Username })
		}
		if len(ts) == 4 && ts[2].Type == sql.TokLParen && ts[3].Type == sql.TokRParen {
			switch strings.ToUpper(ts[1].Value) {
			case "VERSION":
				return scalar("version", oidText, func(*client) string { return "PostgreSQL 15.0 (SlopDB protocol emulation)" })
			case "CURRENT_DATABASE":
				return scalar("current_database", oidText, func(c *client) string { return c.server.config.Database })
			case "PG_BACKEND_PID":
				return helper{kind: "PID", fields: []field{{"pg_backend_pid", oidInt4}}}, true
			}
		}
	}
	if equal(0, "SHOW") && len(ts) == 2 {
		name := strings.ToLower(ts[1].Value)
		switch name {
		case "server_version", "server_encoding", "client_encoding", "datestyle", "timezone", "standard_conforming_strings", "transaction_isolation", "default_transaction_isolation", "application_name", "search_path":
			return helper{kind: "SHOW", fields: []field{{name, oidText}}, name: name}, true
		}
	}
	if equal(0, "SET") {
		offset := 1
		if equal(offset, "SESSION") {
			offset++
		}
		if len(ts) == offset+5 && equal(offset, "TRANSACTION") && equal(offset+1, "ISOLATION") && equal(offset+2, "LEVEL") {
			if _, err := sql.ParseIsolationLevel(ts[offset+3].Value + " " + ts[offset+4].Value); err != nil {
				return helper{}, false
			}
			return helper{kind: "SET", name: "isolation", setting: query}, true
		}
		if len(ts) == offset+3 && (ts[offset+1].Type == sql.TokEq || equal(offset+1, "TO")) {
			name, value := strings.ToLower(ts[offset].Value), ts[offset+2].Value
			switch name {
			case "transaction_isolation", "default_transaction_isolation":
				if level, err := sql.ParseIsolationLevel(value); err == nil {
					return helper{kind: "SET", name: "isolation_default", setting: string(level)}, true
				}
			case "client_encoding":
				if strings.EqualFold(value, "UTF8") || strings.EqualFold(value, "UTF-8") {
					return helper{kind: "SET"}, true
				}
			case "standard_conforming_strings":
				if strings.EqualFold(value, "on") {
					return helper{kind: "SET"}, true
				}
			case "timezone":
				if strings.EqualFold(value, "UTC") {
					return helper{kind: "SET"}, true
				}
			case "datestyle":
				if strings.EqualFold(value, "ISO, MDY") {
					return helper{kind: "SET"}, true
				}
			case "application_name":
				return helper{kind: "SET", name: name, setting: value}, true
			}
		}
	}
	if equal(0, "DISCARD") && len(ts) == 2 && equal(1, "ALL") {
		return helper{kind: "DISCARD"}, true
	}
	return helper{}, false
}
func (c *client) runHelper(ctx context.Context, h helper) (*db.ExecResult, error) {
	result := &db.ExecResult{}
	if h.kind == "CURRENT_SCHEMA" {
		name, _, err := c.session.NamespaceSettings(ctx)
		if err != nil {
			return nil, err
		}
		if name == "main" {
			name = "public"
		}
		return &db.ExecResult{Columns: []sql.ColumnDef{{Name: "current_schema", ColType: sql.ColText}}, Rows: []map[string]sql.Value{{"current_schema": sql.NewTextValue(name)}}}, nil
	}
	if h.kind == "BEGIN" {
		query := h.setting
		if query == "" {
			query = "BEGIN"
		}
		return c.session.ExecContext(ctx, query)
	}
	if h.kind == "SET" && h.name == "isolation" {
		return c.session.ExecContext(ctx, h.setting)
	}
	if h.kind == "SET" && h.name == "isolation_default" {
		return result, c.session.SetIsolationLevel(sql.IsolationLevel(h.setting))
	}
	if h.kind == "DISCARD" {
		if c.session.InTransaction() {
			return nil, &protocolError{"25001", "DISCARD ALL cannot run inside a transaction"}
		}
		c.clearPortals()
		c.clearStatements()
		c.applicationName = ""
		return result, c.send(&pgproto3.ParameterStatus{Name: "application_name", Value: ""})
	}
	if h.kind == "SET" {
		if h.name == "application_name" {
			c.applicationName = h.setting
			if err := c.send(&pgproto3.ParameterStatus{Name: "application_name", Value: h.setting}); err != nil {
				return nil, err
			}
		}
		return result, nil
	}
	if len(h.fields) == 0 {
		return result, nil
	}
	f := h.fields[0]
	value := sql.NewTextValue("")
	if h.kind == "PID" {
		value = sql.NewIntValue(int64(c.id))
	} else if h.value != nil {
		value = sql.NewTextValue(h.value(c))
	} else {
		text := ""
		switch h.name {
		case "server_version":
			text = "15.0"
		case "server_encoding", "client_encoding":
			text = "UTF8"
		case "datestyle":
			text = "ISO, MDY"
		case "timezone":
			text = "UTC"
		case "standard_conforming_strings":
			text = "on"
		case "transaction_isolation", "default_transaction_isolation":
			text = strings.ToLower(string(c.session.IsolationLevel()))
		case "search_path":
			_, path, err := c.session.NamespaceSettings(ctx)
			if err != nil {
				return nil, err
			}
			for i := range path {
				if path[i] == "main" {
					path[i] = "public"
				}
			}
			text = strings.Join(path, ", ")
		case "application_name":
			text = c.applicationName
		}
		value = sql.NewTextValue(text)
	}
	result.Columns = []sql.ColumnDef{{Name: f.name, ColType: value.Type}}
	result.Rows = []map[string]sql.Value{{f.name: value}}
	return result, nil
}

// splitQueries respects literals, comments and compound native trigger bodies.
func splitQueries(query string) []string {
	t := sql.NewTokenizer(query)
	start := 0
	var out []string
	var stack []string
	trigger, create := false, false
	appendPart := func(end int) {
		part := strings.TrimSpace(query[start:end])
		if sql.NewTokenizer(part).Next().Type != sql.TokEOF {
			out = append(out, part)
		}
	}
	for tok := t.Next(); tok.Type != sql.TokEOF; tok = t.Next() {
		if tok.Type == sql.TokKeyword {
			switch strings.ToUpper(tok.Value) {
			case "CREATE":
				create = true
			case "TRIGGER":
				if create {
					trigger = true
				}
			case "BEGIN", "CASE":
				if trigger {
					stack = append(stack, strings.ToUpper(tok.Value))
				}
			case "END":
				if trigger && len(stack) > 0 {
					stack = stack[:len(stack)-1]
				}
			}
		}
		if tok.Type == sql.TokSemicolon && len(stack) == 0 {
			appendPart(tok.Start)
			start = t.Pos()
			create = false
			trigger = false
		}
	}
	appendPart(len(query))
	return out
}
