package mysqlschema

import (
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"strings"

	"slopdb/changefeed"
	"slopdb/sql"
)

func cdcQuote(name string) string { return "`" + strings.ReplaceAll(name, "`", "``") + "`" }
func cdcTableName(table changefeed.Relation, root string) string {
	schema := table.Schema
	if schema == "main" {
		schema = root
	}
	if schema == "" {
		return cdcQuote(table.Name)
	}
	return cdcQuote(schema) + "." + cdcQuote(table.Name)
}
func cdcColumns(columns []string, prefixes []int64) string {
	parts := make([]string, len(columns))
	for i, name := range columns {
		parts[i] = cdcQuote(name)
		if i < len(prefixes) && prefixes[i] > 0 {
			parts[i] += "(" + strconv.FormatInt(prefixes[i], 10) + ")"
		}
	}
	return strings.Join(parts, ",")
}

// Stored expressions use standard SQL quotes. Re-emit tokens using MySQL's
// identifier and string spelling, leaving keywords/operators untouched.
func cdcExpression(input string) string {
	tokenizer := sql.NewTokenizer(input)
	var tokens []sql.Token
	for token := tokenizer.Next(); token.Type != sql.TokEOF; token = tokenizer.Next() {
		tokens = append(tokens, token)
	}
	var result strings.Builder
	for i, token := range tokens {
		end := len(input)
		if i+1 < len(tokens) {
			end = tokens[i+1].Start
		}
		raw := input[token.Start:end]
		trimmed := strings.TrimRight(raw, " \t\r\n")
		space := raw[len(trimmed):]
		switch {
		case token.Type == sql.TokIdent && len(raw) > 0 && (raw[0] == '"' || raw[0] == '[' || raw[0] == '`'):
			result.WriteString(cdcQuote(token.Value))
			result.WriteString(space)
		case token.Type == sql.TokString:
			value := strings.NewReplacer("\\", "\\\\", "\x00", "\\0", "\n", "\\n", "\r", "\\r", "\x1a", "\\Z", "'", "''").Replace(token.Value)
			result.WriteString("'" + value + "'")
			result.WriteString(space)
		default:
			result.WriteString(raw)
		}
	}
	return result.String()
}
func cdcColumn(column sql.ColumnDef) string {
	typeName := column.DeclaredType()
	if column.ColType == sql.ColInteger && !column.MySQL && (column.Declared == "" || column.Declared == "INTEGER" || column.Declared == "INT") {
		typeName = "BIGINT"
		if column.Unsigned {
			typeName += " UNSIGNED"
		}
	}
	if typeName == "REAL" {
		typeName = "DOUBLE"
	}
	// Native TEXT/BLOB have no 64 KiB limit. Their binlog representation uses
	// a four-byte length, matching MySQL's LONGTEXT/LONGBLOB families.
	if column.Length == 0 && column.Declared == "" {
		if column.ColType == sql.ColText {
			typeName = "LONGTEXT"
		}
		if column.ColType == sql.ColBlob {
			typeName = "LONGBLOB"
		}
	}
	result := cdcQuote(column.Name) + " " + typeName
	if column.Binary && column.ColType == sql.ColText {
		result += " COLLATE utf8mb4_bin"
	} else if column.ColType == sql.ColText && column.Collation != "" {
		result += " COLLATE " + strings.ToLower(string(column.Collation))
	}
	if column.NotNull {
		result += " NOT NULL"
	}
	if column.DefaultSQL != "" {
		expression := cdcExpression(column.DefaultSQL)
		if strings.HasSuffix(typeName, "TEXT") || strings.HasSuffix(typeName, "BLOB") || typeName == "JSON" {
			expression = "(" + expression + ")"
		}
		result += " DEFAULT " + expression
	}
	if column.AutoIncrement {
		result += " AUTO_INCREMENT"
	}
	return result
}
func cdcConstraint(constraint sql.ConstraintDef, root string) string {
	prefix := "CONSTRAINT " + cdcQuote(constraint.Name) + " "
	switch constraint.Kind {
	case "PRIMARY":
		return "PRIMARY KEY (" + cdcColumns(constraint.Columns, nil) + ")"
	case "UNIQUE":
		return "UNIQUE KEY " + cdcQuote(constraint.Name) + " (" + cdcColumns(constraint.Columns, constraint.PrefixLengths) + ")"
	case "CHECK":
		return prefix + "CHECK (" + cdcExpression(constraint.CheckSQL) + ")"
	case "FOREIGN":
		schema, name := sql.SplitSchemaName(constraint.RefTable)
		if schema == "main" {
			schema = root
		}
		ref := cdcQuote(name)
		if schema != "" {
			ref = cdcQuote(schema) + "." + ref
		}
		result := prefix + "FOREIGN KEY (" + cdcColumns(constraint.Columns, nil) + ") REFERENCES " + ref + " (" + cdcColumns(constraint.RefColumns, nil) + ")"
		if constraint.OnDelete != "" {
			result += " ON DELETE " + constraint.OnDelete
		}
		if constraint.OnUpdate != "" {
			result += " ON UPDATE " + constraint.OnUpdate
		}
		return result
	}
	return ""
}
func cdcIndex(index *sql.IndexInfo) string {
	columns := index.Columns
	if len(columns) == 0 {
		columns = []string{index.Column}
	}
	prefix := "KEY "
	if index.IsUnique {
		prefix = "UNIQUE KEY "
	}
	if index.Fulltext {
		prefix = "FULLTEXT KEY "
	}
	return prefix + cdcQuote(index.SQLName()) + " (" + cdcColumns(columns, index.PrefixLengths) + ")"
}
func cdcKeys(table changefeed.Relation, root string) map[string]string {
	keys := make(map[string]string)
	if table.Definition != nil {
		for _, constraint := range table.Definition.Constraints {
			definition := cdcConstraint(constraint, root)
			if definition == "" {
				continue
			}
			name := constraint.Kind + ":" + constraint.Name
			if constraint.Kind == "PRIMARY" {
				name = "PRIMARY"
			}
			keys[name] = definition
		}
		if len(table.Definition.PKColumns) > 0 && keys["PRIMARY"] == "" {
			keys["PRIMARY"] = "PRIMARY KEY (" + cdcColumns(table.Definition.PKColumns, nil) + ")"
		}
	}
	for _, index := range table.Indexes {
		if index.Auto {
			continue
		}
		represented := false
		if table.Definition != nil {
			for _, constraint := range table.Definition.Constraints {
				represented = represented || constraint.IndexID == index.ID
			}
		}
		if represented {
			continue
		}
		keys["INDEX:"+index.SQLName()] = cdcIndex(index)
	}
	return keys
}

// Create preserves declared types, defaults, keys and referential
// actions. An empty root emits an unqualified root table for QueryEvent's DB.
func Create(table changefeed.Relation, root string) string {
	parts := make([]string, 0, len(table.Columns))
	for _, column := range table.Columns {
		parts = append(parts, cdcColumn(column))
	}
	keys := cdcKeys(table, root)
	names := make([]string, 0, len(keys))
	for name := range keys {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		parts = append(parts, keys[name])
	}
	mode := "utf8mb4_bin"
	if table.Definition != nil && table.Definition.DefaultCollation != "" {
		mode = strings.ToLower(string(table.Definition.DefaultCollation))
	}
	return "CREATE TABLE " + cdcTableName(table, root) + " (" + strings.Join(parts, ", ") + ") DEFAULT CHARACTER SET utf8mb4 COLLATE " + mode
}
func cdcSameColumn(a, b sql.ColumnDef) bool {
	a.Name, b.Name = "", ""
	left, _ := json.Marshal(a)
	right, _ := json.Marshal(b)
	return string(left) == string(right)
}
func cdcDropKey(name string, table changefeed.Relation) string {
	if name == "PRIMARY" {
		return "DROP PRIMARY KEY"
	}
	kind, symbol, _ := strings.Cut(name, ":")
	if kind == "FOREIGN" {
		return "DROP FOREIGN KEY " + cdcQuote(symbol)
	}
	if kind == "CHECK" {
		return "DROP CHECK " + cdcQuote(symbol)
	}
	return "DROP INDEX " + cdcQuote(symbol)
}

// Alter emits row-preserving ALTER operations, including column renames
// (CHANGE COLUMN) rather than destructive table replacement. All changes to one
// table share one ALTER so AUTO_INCREMENT/key invariants hold at publication.
func Alter(before, after changefeed.Relation, root string) ([]string, error) {
	if before.ID != after.ID {
		return nil, fmt.Errorf("cannot ALTER unrelated CDC tables")
	}
	var queries []string
	if before.Schema != after.Schema || before.Name != after.Name {
		queries = append(queries, "RENAME TABLE "+cdcTableName(before, root)+" TO "+cdcTableName(after, root))
	}
	oldKeys, newKeys := cdcKeys(before, root), cdcKeys(after, root)
	var drops, adds []string
	for name, definition := range oldKeys {
		if newKeys[name] != definition {
			drops = append(drops, cdcDropKey(name, before))
		}
	}
	for name, definition := range newKeys {
		if oldKeys[name] != definition {
			adds = append(adds, "ADD "+definition)
		}
	}
	sort.Strings(drops)
	sort.Strings(adds)
	oldColumns := make(map[string]sql.ColumnDef)
	newNames := make(map[string]bool)
	for _, column := range before.Columns {
		oldColumns[column.Name] = column
	}
	for _, column := range after.Columns {
		newNames[column.Name] = true
	}
	renamed := make(map[string]string)
	used := make(map[string]bool)
	for _, column := range after.Columns {
		if _, exists := oldColumns[column.Name]; exists {
			continue
		}
		var candidates []string
		for name, old := range oldColumns {
			if !newNames[name] && !used[name] && cdcSameColumn(old, column) {
				candidates = append(candidates, name)
			}
		}
		if len(candidates) == 1 {
			renamed[column.Name] = candidates[0]
			used[candidates[0]] = true
		}
	}
	var changes []string
	var removed, added int
	for _, column := range before.Columns {
		if !newNames[column.Name] && !used[column.Name] {
			removed++
		}
	}
	for _, column := range after.Columns {
		if _, exists := oldColumns[column.Name]; !exists && renamed[column.Name] == "" {
			added++
		}
	}
	if removed > 0 && added > 0 {
		return nil, fmt.Errorf("CDC cannot distinguish a combined column rename/type change from DROP/ADD in table %s", after.Name)
	}
	for _, column := range before.Columns {
		if !newNames[column.Name] && !used[column.Name] {
			changes = append(changes, "DROP COLUMN "+cdcQuote(column.Name))
		}
	}
	for i, column := range after.Columns {
		position := " FIRST"
		if i > 0 {
			position = " AFTER " + cdcQuote(after.Columns[i-1].Name)
		}
		if oldName := renamed[column.Name]; oldName != "" {
			changes = append(changes, "CHANGE COLUMN "+cdcQuote(oldName)+" "+cdcColumn(column)+position)
			continue
		}
		old, exists := oldColumns[column.Name]
		if !exists {
			changes = append(changes, "ADD COLUMN "+cdcColumn(column)+position)
			continue
		}
		oldPosition := -1
		for j, previous := range before.Columns {
			if previous.Name == column.Name {
				oldPosition = j
				break
			}
		}
		if !reflect.DeepEqual(old, column) || oldPosition != i {
			changes = append(changes, "MODIFY COLUMN "+cdcColumn(column)+position)
		}
	}
	clauses := append(drops, changes...)
	clauses = append(clauses, adds...)
	if len(clauses) > 0 {
		queries = append(queries, "ALTER TABLE "+cdcTableName(after, root)+" "+strings.Join(clauses, ", "))
	}
	return queries, nil
}
