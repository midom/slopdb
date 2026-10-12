package cli

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"

	"slopdb/db"
)

type cliState struct {
	allowDotCommands bool
	mode             string
	nullStr          string
	output           io.Writer
	onceFile         io.WriteCloser
	echo             bool
	trace            bool
	timer            bool
	bail             bool
	changes          bool
}

var cliCfg = &cliState{
	mode:    "table",
	nullStr: "",
	output:  os.Stdout,
}

func cliPrintf(format string, args ...interface{}) {
	fmt.Fprintf(cliCfg.output, format, args...)
}

func cliPrint(s string) {
	fmt.Fprint(cliCfg.output, s)
}

func formatResultTable(result *db.ExecResult) string {
	if result == nil || len(result.Columns) == 0 || len(result.Rows) == 0 {
		return ""
	}
	var b strings.Builder
	widths := make([]int, len(result.Columns))
	for i, col := range result.Columns {
		widths[i] = len(col.Name)
	}
	for _, row := range result.Rows {
		for i, col := range result.Columns {
			v := row[col.Name]
			s := cliCfg.nullStr
			if !v.Null {
				s = v.String()
			}
			if len(s) > widths[i] {
				widths[i] = len(s)
			}
		}
	}
	for i, col := range result.Columns {
		if i > 0 {
			b.WriteString(" | ")
		}
		fmt.Fprintf(&b, "%-*s", widths[i], col.Name)
	}
	b.WriteString("\n")
	for i, w := range widths {
		if i > 0 {
			b.WriteString("-+-")
		}
		b.WriteString(strings.Repeat("-", w))
	}
	b.WriteString("\n")
	for _, row := range result.Rows {
		for i, col := range result.Columns {
			if i > 0 {
				b.WriteString(" | ")
			}
			v := row[col.Name]
			s := cliCfg.nullStr
			if !v.Null {
				s = v.String()
			}
			fmt.Fprintf(&b, "%-*s", widths[i], s)
		}
		b.WriteString("\n")
	}
	return b.String()
}

func formatResultCSV(result *db.ExecResult) string {
	var b strings.Builder
	for i, col := range result.Columns {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteByte('"')
		b.WriteString(strings.ReplaceAll(col.Name, `"`, `""`))
		b.WriteByte('"')
	}
	b.WriteByte('\n')
	for _, row := range result.Rows {
		for i, col := range result.Columns {
			if i > 0 {
				b.WriteByte(',')
			}
			b.WriteByte('"')
			v := row[col.Name]
			if v.Null {
				b.WriteString(cliCfg.nullStr)
			} else {
				s := v.String()
				b.WriteString(strings.ReplaceAll(s, `"`, `""`))
			}
			b.WriteByte('"')
		}
		b.WriteByte('\n')
	}
	return b.String()
}

func formatResultJSON(result *db.ExecResult) string {
	var b strings.Builder
	b.WriteByte('[')
	for i, row := range result.Rows {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteByte('{')
		for j, col := range result.Columns {
			if j > 0 {
				b.WriteByte(',')
			}
			name, _ := json.Marshal(col.Name)
			b.Write(name)
			b.WriteByte(':')
			value, err := json.Marshal(row[col.Name])
			if err != nil {
				b.WriteString("null")
			} else {
				b.Write(value)
			}
		}
		b.WriteByte('}')
	}
	b.WriteByte(']')
	return b.String() + "\n"
}

func formatResultLine(result *db.ExecResult) string {
	var b strings.Builder
	for _, row := range result.Rows {
		for i, col := range result.Columns {
			if i > 0 {
				b.WriteString("|")
			}
			v := row[col.Name]
			if v.Null {
				b.WriteString(cliCfg.nullStr)
			} else {
				b.WriteString(v.String())
			}
		}
		b.WriteByte('\n')
	}
	return b.String()
}

func formatResultMarkdown(result *db.ExecResult) string {
	var b strings.Builder
	b.WriteByte('|')
	for _, col := range result.Columns {
		fmt.Fprintf(&b, " %s |", col.Name)
	}
	b.WriteByte('\n')
	b.WriteByte('|')
	for range result.Columns {
		b.WriteString(" --- |")
	}
	b.WriteByte('\n')
	for _, row := range result.Rows {
		b.WriteByte('|')
		for _, col := range result.Columns {
			v := row[col.Name]
			if v.Null {
				fmt.Fprintf(&b, " %s |", cliCfg.nullStr)
			} else {
				fmt.Fprintf(&b, " %s |", v.String())
			}
		}
		b.WriteByte('\n')
	}
	return b.String()
}

func formatResultBox(result *db.ExecResult) string {
	if len(result.Columns) == 0 || len(result.Rows) == 0 {
		return ""
	}
	widths := make([]int, len(result.Columns))
	for i, col := range result.Columns {
		widths[i] = len(col.Name)
	}
	for _, row := range result.Rows {
		for i, col := range result.Columns {
			v := row[col.Name]
			s := cliCfg.nullStr
			if !v.Null {
				s = v.String()
			}
			if len(s) > widths[i] {
				widths[i] = len(s)
			}
		}
	}
	var b strings.Builder
	b.WriteString("\u250c")
	for i, w := range widths {
		if i > 0 {
			b.WriteString("\u252c")
		}
		b.WriteString(strings.Repeat("\u2500", w+2))
	}
	b.WriteString("\u2510\n")
	b.WriteString("\u2502")
	for i, col := range result.Columns {
		if i > 0 {
			b.WriteString("\u2502")
		}
		fmt.Fprintf(&b, " %-*s ", widths[i], col.Name)
	}
	b.WriteString("\u2502\n")
	b.WriteString("\u251c")
	for i, w := range widths {
		if i > 0 {
			b.WriteString("\u253c")
		}
		b.WriteString(strings.Repeat("\u2500", w+2))
	}
	b.WriteString("\u2524\n")
	for _, row := range result.Rows {
		b.WriteString("\u2502")
		for i, col := range result.Columns {
			if i > 0 {
				b.WriteString("\u2502")
			}
			v := row[col.Name]
			s := cliCfg.nullStr
			if !v.Null {
				s = v.String()
			}
			fmt.Fprintf(&b, " %-*s ", widths[i], s)
		}
		b.WriteString("\u2502\n")
	}
	b.WriteString("\u2514")
	for i, w := range widths {
		if i > 0 {
			b.WriteString("\u2534")
		}
		b.WriteString(strings.Repeat("\u2500", w+2))
	}
	b.WriteString("\u2518\n")
	return b.String()
}

func formatResultTabs(result *db.ExecResult) string {
	var b strings.Builder
	for i, col := range result.Columns {
		if i > 0 {
			b.WriteByte('\t')
		}
		b.WriteString(col.Name)
	}
	b.WriteByte('\n')
	for _, row := range result.Rows {
		for i, col := range result.Columns {
			if i > 0 {
				b.WriteByte('\t')
			}
			v := row[col.Name]
			if v.Null {
				b.WriteString(cliCfg.nullStr)
			} else {
				b.WriteString(v.String())
			}
		}
		b.WriteByte('\n')
	}
	return b.String()
}

func formatSQLResult(result *db.ExecResult) string {
	if result == nil {
		return ""
	}
	switch cliCfg.mode {
	case "csv":
		return formatResultCSV(result)
	case "json":
		return formatResultJSON(result)
	case "line":
		return formatResultLine(result)
	case "markdown":
		return formatResultMarkdown(result)
	case "box":
		return formatResultBox(result)
	case "tabs":
		return formatResultTabs(result)
	default:
		return formatResultTable(result)
	}
}

func handleMode(cmd string) {
	arg := strings.TrimSpace(strings.TrimPrefix(cmd, ".mode "))
	if arg == "" {
		fmt.Fprintf(os.Stderr, "Current mode: %s\n", cliCfg.mode)
		fmt.Fprintf(os.Stderr, "Available: table, csv, json, line, markdown, box, tabs\n")
		return
	}
	switch arg {
	case "table", "csv", "json", "line", "markdown", "box", "tabs":
		cliCfg.mode = arg
	default:
		fmt.Fprintf(os.Stderr, "Unknown mode: %s\n", arg)
		fmt.Fprintf(os.Stderr, "Available: table, csv, json, line, markdown, box, tabs\n")
	}
}

func handleBoolFlag(cmd, prefix string) {
	arg := strings.TrimSpace(strings.TrimPrefix(cmd, prefix+" "))
	var current *bool
	switch prefix {
	case ".echo":
		current = &cliCfg.echo
	case ".trace":
		current = &cliCfg.trace
	case ".timer":
		current = &cliCfg.timer
	case ".bail":
		current = &cliCfg.bail
	case ".changes":
		current = &cliCfg.changes
	case ".stats":
		current = &cliCfg.timer
	default:
		return
	}
	switch strings.ToLower(arg) {
	case "on", "1", "yes":
		*current = true
	case "off", "0", "no":
		*current = false
	default:
		*current = !*current
	}
	fmt.Fprintf(os.Stderr, "%s: %v\n", prefix, *current)
}

func sanitizeIdent(s string) string {
	var b strings.Builder
	for i, ch := range s {
		if (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || (ch >= '0' && ch <= '9') || ch == '_' {
			if i == 0 && ch >= '0' && ch <= '9' {
				b.WriteByte('_')
			}
			b.WriteRune(ch)
		} else {
			b.WriteByte('_')
		}
	}
	result := b.String()
	if result == "" {
		result = "_"
	}
	if result[0] >= '0' && result[0] <= '9' {
		result = "_" + result
	}
	return result
}

func handleImport(filePath, tableName string) {
	f, err := os.Open(filePath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: cannot open %q: %v\n", filePath, err)
		return
	}
	defer f.Close()

	reader := csv.NewReader(f)
	headers, err := reader.Read()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: cannot read CSV header: %v\n", err)
		return
	}

	if currentDB.GetCreateTableSQL(tableName) == "" {
		var cols []string
		seen := make(map[string]bool)
		for _, h := range headers {
			col := sanitizeIdent(h)
			for seen[col] {
				col += "_"
			}
			seen[col] = true
			cols = append(cols, quoteCLIIdentifier(col)+" TEXT")
		}
		createSQL := "CREATE TABLE " + quoteCLIIdentifier(tableName) + " (" + strings.Join(cols, ", ") + ")"
		_, err := currentDB.ExecContext(context.Background(), createSQL)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error creating table: %v\n", err)
			return
		}
	}

	rows, err := currentDB.Query("SELECT * FROM " + quoteCLIIdentifier(tableName) + " LIMIT 0")
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error reading table columns: %v\n", err)
		return
	}
	columns := rows.Columns()
	_ = rows.Close()
	names := make([]string, len(columns))
	for i, column := range columns {
		names[i] = column.Name
	}
	if len(headers) != len(names) {
		fmt.Fprintf(os.Stderr, "Error: CSV has %d columns, table has %d\n", len(headers), len(names))
		return
	}
	count, err := currentDB.BulkInsert(context.Background(), tableName, names, func(ctx context.Context) ([]any, error) {
		record, readErr := reader.Read()
		if readErr != nil {
			return nil, readErr
		}
		values := make([]any, len(record))
		for i, value := range record {
			if value != "" {
				values[i] = value
			}
		}
		return values, nil
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "Import failed; all rows rolled back: %v\n", err)
		return
	}
	fmt.Fprintf(os.Stderr, "Imported %d rows into %q\n", count, tableName)
}

func handleRead(arg string) {
	var content []byte
	if strings.HasPrefix(arg, "|") {
		cmdStr := strings.TrimPrefix(arg, "|")
		out, err := exec.Command("sh", "-c", cmdStr).Output()
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error running command: %v\n", err)
			return
		}
		content = out
	} else {
		var err error
		content, err = os.ReadFile(arg)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: cannot read %q: %v\n", arg, err)
			return
		}
	}
	processSQLContent(string(content))
}

func processSQLContent(content string) {
	statements := splitSQL(content)
	for _, sql := range statements {
		sql = strings.TrimSpace(sql)
		if strings.HasPrefix(sql, ".") {
			handleDotCommand(sql)
			continue
		}
		if sql == "" {
			continue
		}
		executeSQL(sql, false)
	}
}

func handleOpen(cmd string) {
	currentDBMu.Lock()
	defer currentDBMu.Unlock()
	if shuttingDown.Load() {
		return
	}
	parts := strings.Fields(cmd)
	var filePath string
	newFlag := false
	for _, p := range parts[1:] {
		if p == "--new" {
			newFlag = true
		} else if !strings.HasPrefix(p, "-") {
			filePath = p
		}
	}

	if currentDB != nil {
		if err := currentDB.Close(); err != nil {
			fmt.Fprintf(os.Stderr, "Error closing database: %v\n", err)
			return
		}
	}

	if filePath == "" {
		tmpDir, err := os.MkdirTemp("", "slopdb_mem_*")
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			return
		}
		filePath = tmpDir
	} else if newFlag {
		os.RemoveAll(filePath)
	}

	opt := currentDBOptions
	var err error
	currentDB, err = db.Open(filePath, opt)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error opening %q: %v\n", filePath, err)
		return
	}
	currentDBPath = filePath
	fmt.Fprintf(os.Stderr, "Opened database: %s\n", filePath)
}

func runShell(arg string) {
	cmd := exec.Command("sh", "-c", arg)
	cmd.Stdin = os.Stdin
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
	}
}

func quoteCLIIdentifier(name string) string { return `"` + strings.ReplaceAll(name, `"`, `""`) + `"` }
