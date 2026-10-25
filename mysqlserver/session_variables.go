package mysqlserver

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"slopdb/db"
	"slopdb/sql"
)

type sessionVariables struct {
	groupConcatMaxLen               int64
	netReadTimeout, netWriteTimeout int64
	sqlMode, charset                string
}

func defaultSessionVariables() sessionVariables {
	return sessionVariables{groupConcatMaxLen: 1024, netReadTimeout: 30, netWriteTimeout: 60, sqlMode: "STRICT_TRANS_TABLES", charset: "utf8mb4"}
}

func (c *client) mysqlContext() context.Context {
	ctx := sql.WithMySQLSession(c.ctx, sql.MySQLSessionOptions{Database: c.selectedDatabaseName(), RootDatabase: c.server.config.Database, User: c.username, BackendID: uint64(c.id), GroupConcatMaxLen: c.variables.groupConcatMaxLen, SQLMode: c.variables.sqlMode, Function: c.mysqlFunction, SystemTable: c.accountTable, FoundRows: c.capabilities&clientFoundRows != 0, Warning: c.addWarning})
	if len(c.preparedPath) > 0 {
		ctx = sql.WithNamespaceSearchPath(ctx, c.preparedPath)
	}
	return ctx
}

func sqlTokens(query string, options sql.ParseOptions) []sql.Token {
	t := sql.NewTokenizerWithOptions(query, options)
	var tokens []sql.Token
	for tok := t.Next(); tok.Type != sql.TokEOF; tok = t.Next() {
		tokens = append(tokens, tok)
	}
	if len(tokens) > 0 && tokens[len(tokens)-1].Type == sql.TokSemicolon {
		tokens = tokens[:len(tokens)-1]
	}
	return tokens
}

func (c *client) variable(name string, global bool) (sql.Value, error) {
	switch strings.ToLower(name) {
	case "net_read_timeout":
		if global {
			return sql.NewIntValue(30), nil
		}
		return sql.NewIntValue(c.variables.netReadTimeout), nil
	case "net_write_timeout":
		if global {
			return sql.NewIntValue(60), nil
		}
		return sql.NewIntValue(c.variables.netWriteTimeout), nil
	case "warning_count":
		return sql.NewIntValue(int64(c.warningCount())), nil
	case "log_bin":
		return sql.NewIntValue(1), nil
	case "binlog_format":
		return sql.NewTextValue("ROW"), nil
	case "binlog_row_image":
		return sql.NewTextValue("FULL"), nil
	case "binlog_checksum":
		return sql.NewTextValue("CRC32"), nil
	case "gtid_mode", "enforce_gtid_consistency":
		return sql.NewTextValue("ON"), nil
	case "server_id", "server_uuid", "gtid_executed", "gtid_purged":
		snapshot, err := c.server.binlogSnapshot(c.ctx)
		if err != nil {
			return sql.Value{}, err
		}
		if strings.EqualFold(name, "server_id") {
			return sql.NewIntValue(int64(snapshot.serverID)), nil
		}
		if strings.EqualFold(name, "gtid_purged") {
			return sql.NewTextValue(""), nil
		}
		source := snapshot.source
		uuid := source[:8] + "-" + source[8:12] + "-" + source[12:16] + "-" + source[16:20] + "-" + source[20:]
		if strings.EqualFold(name, "server_uuid") {
			return sql.NewTextValue(uuid), nil
		}
		if len(snapshot.entries) == 0 {
			return sql.NewTextValue(""), nil
		}
		return sql.NewTextValue(uuid + ":1-" + fmt.Sprint(snapshot.entries[len(snapshot.entries)-1].sequence)), nil
	case "autocommit":
		if c.autocommit {
			return sql.NewIntValue(1), nil
		}
		return sql.NewIntValue(0), nil
	case "group_concat_max_len":
		if global {
			return sql.NewIntValue(1024), nil
		}
		return sql.NewIntValue(c.variables.groupConcatMaxLen), nil
	case "sql_mode":
		if global {
			return sql.NewTextValue("STRICT_TRANS_TABLES"), nil
		}
		return sql.NewTextValue(c.variables.sqlMode), nil
	case "read_only", "super_read_only":
		v, err := c.server.database.MySQLReadOnly(c.ctx)
		if err != nil {
			return sql.Value{}, err
		}
		if v {
			return sql.NewIntValue(1), nil
		}
		return sql.NewIntValue(0), nil
	case "character_set_client", "character_set_connection", "character_set_results", "character_set_server", "character_set_database":
		return sql.NewTextValue(c.variables.charset), nil
	case "collation_connection", "collation_server", "collation_database":
		return sql.NewTextValue("utf8mb4_bin"), nil
	case "version":
		return sql.NewTextValue(serverVersion), nil
	case "version_comment":
		return sql.NewTextValue("SlopDB MySQL compatibility server"), nil
	case "max_allowed_packet":
		return sql.NewIntValue(int64(c.server.config.MaxPacketBytes)), nil
	case "ft_min_word_len":
		return sql.NewIntValue(4), nil
	case "transaction_isolation", "tx_isolation":
		if global {
			return sql.NewTextValue("REPEATABLE-READ"), nil
		}
		return sql.NewTextValue(strings.ReplaceAll(string(c.session.IsolationLevel()), " ", "-")), nil
	case "lower_case_table_names":
		return sql.NewIntValue(1), nil
	case "time_zone", "system_time_zone":
		return sql.NewTextValue("UTC"), nil
	default:
		return sql.Value{}, &protocolError{1193, "HY000", fmt.Sprintf("unknown system variable %q", name)}
	}
}

// rewriteVariables replaces only lexical @@ references. Quoted strings,
// identifiers and comments are left untouched, and the ordinary SQL parser
// still rejects multi-statements and validates the resulting expression.
func (c *client) rewriteVariables(query string) (string, bool, error) {
	tokens := sqlTokens(query, c.currentDatabase().ParseOptions())
	var out strings.Builder
	end := 0
	changed := false
	for i := 0; i < len(tokens); i++ {
		if tokens[i].Value != "@" || tokens[i].Type != sql.TokInvalid {
			continue
		}
		if i+2 >= len(tokens) || tokens[i+1].Value != "@" {
			return "", false, fmt.Errorf("user variables are not supported")
		}
		start := tokens[i].Start
		j := i + 2
		global := false
		if j+1 < len(tokens) && tokens[j+1].Type == sql.TokDot {
			scope := strings.ToUpper(tokens[j].Value)
			if scope != "GLOBAL" && scope != "SESSION" && scope != "LOCAL" {
				return "", false, fmt.Errorf("unknown variable scope %s", scope)
			}
			global = scope == "GLOBAL"
			j += 2
		}
		if j >= len(tokens) || tokens[j].Type != sql.TokIdent && tokens[j].Type != sql.TokKeyword {
			return "", false, fmt.Errorf("expected system variable name")
		}
		value, err := c.variable(tokens[j].Value, global)
		if err != nil {
			return "", false, err
		}
		next := len(query)
		if j+1 < len(tokens) {
			next = tokens[j+1].Start
		} else {
			next = len(query)
		}
		out.WriteString(query[end:start])
		if value.Type == sql.ColInteger {
			out.WriteString(strconv.FormatInt(value.Int, 10))
		} else {
			out.WriteByte('\'')
			out.WriteString(strings.ReplaceAll(value.Text, "'", "''"))
			out.WriteByte('\'')
		}
		out.WriteByte(' ')
		end = next
		i = j
		changed = true
	}
	if !changed {
		return query, false, nil
	}
	out.WriteString(query[end:])
	return out.String(), true, nil
}

func (c *client) compatibilityControl(query string) (*db.ExecResult, bool, error) {
	first := sql.NewTokenizerWithOptions(query, c.currentDatabase().ParseOptions()).Next()
	if strings.EqualFold(first.Value, "SELECT") && strings.Contains(query, "@") {
		rewritten, changed, err := c.rewriteVariables(query)
		if err != nil {
			return nil, true, err
		}
		if changed {
			statements, parseErr := sql.ParseWithOptions(rewritten, c.currentDatabase().ParseOptions())
			if parseErr != nil {
				return nil, true, parseErr
			}
			if len(statements) != 1 {
				return nil, true, &protocolError{1064, "42000", "exactly one SQL statement is required"}
			}
			if err := c.authorize(statements[0]); err != nil {
				return nil, true, err
			}
			result, err := c.session.ExecContext(c.mysqlContext(), rewritten)
			return result, true, err
		}
	}
	if !strings.EqualFold(first.Value, "SET") && !strings.EqualFold(first.Value, "SHOW") {
		return nil, false, nil
	}
	tokens := sqlTokens(query, c.currentDatabase().ParseOptions())
	for _, t := range tokens {
		if t.Type == sql.TokSemicolon {
			return nil, false, nil
		}
	}
	if len(tokens) < 2 {
		return nil, false, nil
	}
	if result, handled, err := c.showWarnings(tokens); handled {
		return result, true, err
	}
	if strings.EqualFold(first.Value, "SHOW") {
		i := 1
		global := false
		if strings.EqualFold(tokens[i].Value, "GLOBAL") || strings.EqualFold(tokens[i].Value, "SESSION") {
			global = strings.EqualFold(tokens[i].Value, "GLOBAL")
			i++
		}
		if i >= len(tokens) {
			return nil, false, nil
		}
		if strings.EqualFold(tokens[i].Value, "ENGINES") && i+1 == len(tokens) {
			r := &db.ExecResult{Columns: []sql.ColumnDef{{Name: "Engine", ColType: sql.ColText}, {Name: "Support", ColType: sql.ColText}, {Name: "Comment", ColType: sql.ColText}, {Name: "Transactions", ColType: sql.ColText}, {Name: "XA", ColType: sql.ColText}, {Name: "Savepoints", ColType: sql.ColText}}}
			r.Rows = []map[string]sql.Value{{"Engine": sql.NewTextValue("LSM"), "Support": sql.NewTextValue("DEFAULT"), "Comment": sql.NewTextValue("SlopDB transactional LSM storage"), "Transactions": sql.NewTextValue("YES"), "XA": sql.NewTextValue("NO"), "Savepoints": sql.NewTextValue("YES")}}
			return r, true, nil
		}
		if !strings.EqualFold(tokens[i].Value, "VARIABLES") {
			return nil, false, nil
		}
		i++
		pattern := "%"
		if i < len(tokens) {
			if i+2 != len(tokens) || !strings.EqualFold(tokens[i].Value, "LIKE") || tokens[i+1].Type != sql.TokString {
				return nil, true, fmt.Errorf("SHOW VARIABLES requires LIKE followed by a string")
			}
			pattern = tokens[i+1].Value
		}
		names := []string{"autocommit", "group_concat_max_len", "sql_mode", "read_only", "super_read_only", "character_set_client", "character_set_connection", "character_set_results", "collation_connection", "version", "version_comment", "max_allowed_packet", "ft_min_word_len", "transaction_isolation", "tx_isolation", "lower_case_table_names", "time_zone", "system_time_zone"}
		names = append(names, "net_read_timeout", "net_write_timeout", "log_bin", "binlog_format", "binlog_row_image", "binlog_checksum", "gtid_mode", "enforce_gtid_consistency", "server_id", "server_uuid", "gtid_executed", "gtid_purged")
		sort.Strings(names)
		r := &db.ExecResult{Columns: []sql.ColumnDef{{Name: "Variable_name", ColType: sql.ColText}, {Name: "Value", ColType: sql.ColText}}}
		for _, name := range names {
			if !variableLike(name, pattern) {
				continue
			}
			value, err := c.variable(name, global)
			if err != nil {
				return nil, true, err
			}
			text := value.Text
			if value.Type == sql.ColInteger {
				text = strconv.FormatInt(value.Int, 10)
			}
			r.Rows = append(r.Rows, map[string]sql.Value{"Variable_name": sql.NewTextValue(name), "Value": sql.NewTextValue(text)})
		}
		return r, true, nil
	}
	return c.setSessionVariables(tokens)
}

func variableLike(value, pattern string) bool {
	var visit func(int, int) bool
	visit = func(i, j int) bool {
		for j < len(pattern) {
			switch pattern[j] {
			case '\\':
				j++
				if j == len(pattern) || i == len(value) || value[i] != pattern[j] {
					return false
				}
				i++
				j++
			case '_':
				if i == len(value) {
					return false
				}
				i++
				j++
			case '%':
				for j < len(pattern) && pattern[j] == '%' {
					j++
				}
				if j == len(pattern) {
					return true
				}
				for ; i <= len(value); i++ {
					if visit(i, j) {
						return true
					}
				}
				return false
			default:
				if i == len(value) || strings.ToLower(value[i:i+1]) != strings.ToLower(pattern[j:j+1]) {
					return false
				}
				i++
				j++
			}
		}
		return i == len(value)
	}
	return visit(0, 0)
}

func (c *client) setSessionVariables(tokens []sql.Token) (*db.ExecResult, bool, error) {
	i := 1
	if strings.EqualFold(tokens[i].Value, "READ") || strings.EqualFold(tokens[i].Value, "TRANSACTION") || strings.EqualFold(tokens[i].Value, "NAMES") {
		return nil, false, nil
	}
	if strings.EqualFold(tokens[i].Value, "GLOBAL") {
		return nil, true, fmt.Errorf("global variable changes are not supported")
	}
	if strings.EqualFold(tokens[i].Value, "SESSION") || strings.EqualFold(tokens[i].Value, "LOCAL") {
		i++
	}
	if i < len(tokens) && strings.EqualFold(tokens[i].Value, "TRANSACTION") {
		return nil, false, nil
	}
	next := c.variables
	autocommit := c.autocommit
	var isolation sql.IsolationLevel
	for i < len(tokens) {
		name := strings.ToLower(tokens[i].Value)
		i++
		if i >= len(tokens) || tokens[i].Type != sql.TokEq {
			return nil, true, fmt.Errorf("expected = after system variable")
		}
		i++
		if i >= len(tokens) {
			return nil, true, fmt.Errorf("missing system variable value")
		}
		v := tokens[i]
		i++
		switch name {
		case "transaction_isolation", "tx_isolation":
			var err error
			isolation, err = sql.ParseIsolationLevel(v.Value)
			if err != nil {
				return nil, true, err
			}
		case "net_read_timeout", "net_write_timeout":
			n, err := strconv.ParseInt(v.Value, 10, 64)
			if err != nil || n < 1 || n > 31536000 {
				return nil, true, fmt.Errorf("%s must be between 1 and 31536000 seconds", name)
			}
			if name == "net_read_timeout" {
				next.netReadTimeout = n
			} else {
				next.netWriteTimeout = n
			}
		case "group_concat_max_len":
			n, err := strconv.ParseInt(v.Value, 10, 64)
			if err != nil || n < 4 || n > 1<<32-1 {
				return nil, true, fmt.Errorf("group_concat_max_len must be between 4 and 4294967295")
			}
			next.groupConcatMaxLen = n
		case "sql_mode":
			if v.Type != sql.TokString {
				return nil, true, fmt.Errorf("sql_mode requires a string")
			}
			modes := strings.Split(strings.ToUpper(v.Value), ",")
			for _, mode := range modes {
				switch mode {
				case "", "STRICT_TRANS_TABLES", "STRICT_ALL_TABLES", "NO_AUTO_VALUE_ON_ZERO":
				default:
					return nil, true, fmt.Errorf("unsupported sql_mode %q", mode)
				}
			}
			next.sqlMode = strings.Join(modes, ",")
		case "autocommit":
			switch strings.ToUpper(v.Value) {
			case "0", "OFF":
				autocommit = false
			case "1", "ON":
				autocommit = true
			default:
				return nil, true, fmt.Errorf("autocommit must be 0, 1, ON or OFF")
			}
		case "character_set_client", "character_set_connection", "character_set_results":
			switch strings.ToLower(v.Value) {
			case "utf8", "utf8mb3", "utf8mb4", "binary":
				next.charset = strings.ToLower(v.Value)
			default:
				return nil, true, fmt.Errorf("unsupported client character set")
			}
		default:
			return nil, true, &protocolError{1193, "HY000", fmt.Sprintf("unknown system variable %q", name)}
		}
		if i < len(tokens) {
			if tokens[i].Type != sql.TokComma {
				return nil, true, fmt.Errorf("expected comma after system variable value")
			}
			i++
			if i == len(tokens) {
				return nil, true, fmt.Errorf("missing system variable assignment")
			}
		}
	}
	var result *db.ExecResult
	if autocommit && !c.autocommit && c.session.InTransaction() {
		var err error
		result, err = c.session.ExecContext(c.mysqlContext(), "COMMIT")
		if err != nil {
			return nil, true, err
		}
	}
	if isolation != "" {
		if err := c.session.SetIsolationLevel(isolation); err != nil {
			return nil, true, err
		}
	}
	c.variables = next
	if c.transport != nil {
		c.transport.configure(next)
	}
	c.autocommit = autocommit
	return result, true, nil
}

func (c *client) selectedDatabaseName() string {
	if c.selected != "" {
		return c.selected
	}
	return c.server.config.Database
}
