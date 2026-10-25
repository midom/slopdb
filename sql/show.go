package sql

import (
	"fmt"
	"sort"
	"strings"
)

func showResult(names ...string) *ResultSet {
	rs := &ResultSet{Rows: make([]map[string]Value, 0)}
	for _, name := range names {
		rs.Columns = append(rs.Columns, ColumnDef{Name: name, ColType: ColText})
	}
	return rs
}

// ExecuteShow returns ordinary query results without starting a write transaction.
// Virtual namespace names resolve through the same catalog as ordinary queries.
func (s *Schema) ExecuteShow(stmt *ShowStmt) (*ResultSet, error) {
	if err := s.checkInterrupt(); err != nil {
		return nil, err
	}
	oldView := s.catalogView
	if s.catalogView == nil {
		s.catalogView = s.CatalogSnapshot()
	}
	defer func() { s.catalogView = oldView }()
	namespace := s.SearchPath()[0]
	if stmt.Database != "" {
		namespace = s.namespaceAlias(stmt.Database)
	}
	if !s.namespaces[namespace] {
		return nil, fmt.Errorf("unknown database %q", namespace)
	}
	var rs *ResultSet
	var err error
	likeColumn := ""
	switch stmt.Kind {
	case "TABLE STATUS":
		rs, err = s.showTableStatus(stmt)
		likeColumn = "Name"
	case "DATABASES":
		data, e := s.informationSchemaTable("schemata", catalogFilter{})
		if e != nil {
			return nil, e
		}
		rs = showResult("Database")
		likeColumn = "Database"
		for _, row := range data.Rows {
			name := row["schema_name"].Text
			if name == "information_schema" || name == "mysql" || name == "pg_catalog" {
				continue
			}
			rs.Rows = append(rs.Rows, map[string]Value{"Database": row["schema_name"]})
		}
	case "TABLES":
		display := s.catalogNamespace(namespace)
		likeColumn = "Tables_in_" + display
		rs = showResult(likeColumn)
		if stmt.Full {
			rs.Columns = append(rs.Columns, ColumnDef{Name: "Table_type", ColType: ColText})
		}
		data, e := s.informationSchemaTable("tables", catalogFilter{Schema: display})
		if e != nil {
			return nil, e
		}
		for _, row := range data.Rows {
			projected := map[string]Value{likeColumn: row["table_name"]}
			if stmt.Full {
				kind := row["table_type"]
				if kind.Text == "LOCAL TEMPORARY" {
					kind = NewTextValue("BASE TABLE")
				}
				projected["Table_type"] = kind
			}
			rs.Rows = append(rs.Rows, projected)
		}
	case "COLUMNS":
		rs, err = s.showColumns(stmt)
		likeColumn = "Field"
	case "INDEX":
		rs, err = s.showIndexes(stmt.Object)
	case "CREATE TABLE", "CREATE VIEW", "CREATE TRIGGER":
		return s.showCreate(stmt)
	case "TRIGGERS":
		rs, err = s.showTriggers(namespace)
		likeColumn = "Table" // MySQL matches the table name, not the trigger name.
	default:
		return nil, fmt.Errorf("unsupported SHOW command: %s", stmt.Kind)
	}
	if err != nil {
		return nil, err
	}
	if stmt.Where != nil {
		if err := s.validateQuery(&SelectStmt{Where: stmt.Where}, &TableInfo{Columns: rs.Columns}); err != nil {
			return nil, err
		}
	}
	filtered := make([]map[string]Value, 0, len(rs.Rows))
	for _, row := range rs.Rows {
		if err := s.checkInterrupt(); err != nil {
			return nil, err
		}
		if stmt.Like != nil && !likeMatchEsc(row[likeColumn].String(), *stmt.Like, '\\') {
			continue
		}
		if stmt.Column != "" && !strings.EqualFold(row["Field"].String(), stmt.Column) {
			continue
		}
		if stmt.Where != nil {
			match, err := s.evalCondition(stmt.Where, row)
			if err != nil {
				return nil, err
			}
			if !match {
				continue
			}
		}
		filtered = append(filtered, row)
	}
	rs.Rows = filtered
	return rs, nil
}

type showKey struct {
	name    string
	columns []string
	unique  bool
	method  string
}

// Include logical PRIMARY/UNIQUE keys even when constraints are enforced by a
// row key or a scan rather than a separate secondary index.
func (s *Schema) showKeys(info *TableInfo) []showKey { return s.catalogFacts().keys(info) }
func (view *CatalogSnapshot) keys(info *TableInfo) []showKey {
	var keys []showKey
	if len(info.PKColumns) > 0 {
		keys = append(keys, showKey{"PRIMARY", info.PKColumns, true, "LSM"})
	}
	for _, idx := range view.indexes {
		if idx.Table != info.Name {
			continue
		}
		if len(info.PKColumns) > 0 && idx.Auto && colsEqual(idx.columnNames(), info.PKColumns) {
			continue // the auto-created primary-key index is represented above
		}
		method := "LSM"
		if idx.Fulltext {
			method = "FULLTEXT"
		}
		if idx.Auto {
			method = "CONSTRAINT"
		}
		keys = append(keys, showKey{idx.SQLName(), idx.columnNames(), idx.IsUnique, method})
	}
	for _, col := range info.Columns {
		if !col.Unique {
			continue
		}
		covered := false
		for _, key := range keys {
			if key.unique && len(key.columns) == 1 && key.columns[0] == col.Name {
				covered = true
				break
			}
		}
		if !covered {
			keys = append(keys, showKey{info.Name + "_" + col.Name + "_unique", []string{col.Name}, true, "CONSTRAINT"})
		}
	}
	for i, cols := range info.TableUnique {
		if colsEqual(cols, info.PKColumns) {
			continue
		}
		covered := false
		for _, key := range keys {
			covered = covered || key.unique && candidateColumns(key.columns, cols)
		}
		if !covered {
			keys = append(keys, showKey{fmt.Sprintf("%s_unique_%d", info.Name, i+1), cols, true, "CONSTRAINT"})
		}
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].name == "PRIMARY" || keys[j].name == "PRIMARY" {
			return keys[i].name == "PRIMARY" && keys[j].name != "PRIMARY"
		}
		return keys[i].name < keys[j].name
	})
	return keys
}

func (s *Schema) showColumns(stmt *ShowStmt) (*ResultSet, error) {
	if _, ok := s.catalogFacts().relation(stmt.Object); !ok {
		return nil, fmt.Errorf("table or view %q does not exist", stmt.Object)
	}
	ns, name := SplitSchemaName(stmt.Object)
	data, err := s.informationSchemaTable("columns", catalogFilter{Schema: s.catalogNamespace(ns), Table: name})
	if err != nil {
		return nil, err
	}
	rs := showResult("Field", "Type", "Null", "Key", "Default", "Extra")
	if stmt.Full {
		rs = showResult("Field", "Type", "Collation", "Null", "Key", "Default", "Extra", "Privileges", "Comment")
	}
	for _, row := range data.Rows {
		projected := map[string]Value{"Field": row["column_name"], "Type": row["slopdb_declared_type"], "Null": row["is_nullable"], "Key": row["column_key"], "Default": row["column_default"], "Extra": row["extra"]}
		if s.PostgresMode() { // SHOW preserves its native literal-default presentation.
			if fact, ok := s.catalogFacts().relation(stmt.Object); ok {
				for _, col := range fact.Columns {
					if col.DeclaredName() == row["column_name"].Text {
						projected["Default"] = catalogDefault(col, false)
					}
				}
			}
		}
		if stmt.Full {
			projected["Collation"] = row["collation_name"]
			projected["Privileges"] = NewNullValue()
			projected["Comment"] = row["column_comment"]
		}
		rs.Rows = append(rs.Rows, projected)
	}
	return rs, nil
}
func (s *Schema) showIndexes(name string) (*ResultSet, error) {
	fact, ok := s.catalogFacts().relation(name)
	if !ok || fact.Kind != "table" {
		return nil, fmt.Errorf("table %q does not exist", name)
	}
	data, err := s.informationSchemaTable("statistics", catalogFilter{Schema: s.catalogNamespace(fact.Schema), Table: fact.Name})
	if err != nil {
		return nil, err
	}
	rs := showResult("Table", "Non_unique", "Key_name", "Seq_in_index", "Column_name", "Collation", "Cardinality", "Sub_part", "Packed", "Null", "Index_type", "Comment", "Index_comment", "Visible", "Expression")
	fields := map[string]string{"Non_unique": "non_unique", "Key_name": "index_name", "Seq_in_index": "seq_in_index", "Column_name": "column_name", "Collation": "collation", "Cardinality": "cardinality", "Sub_part": "sub_part", "Packed": "packed", "Null": "nullable", "Index_type": "index_type", "Comment": "comment", "Index_comment": "index_comment", "Visible": "is_visible", "Expression": "expression"}
	for i := range rs.Columns {
		switch rs.Columns[i].Name {
		case "Non_unique", "Seq_in_index", "Cardinality", "Sub_part":
			rs.Columns[i].ColType = ColInteger
		}
	}
	for _, row := range data.Rows {
		projected := map[string]Value{"Table": NewTextValue(name)}
		for label, source := range fields {
			projected[label] = row[source]
		}
		rs.Rows = append(rs.Rows, projected)
	}
	return rs, nil
}

func (s *Schema) showCreate(stmt *ShowStmt) (*ResultSet, error) {
	objectType := strings.TrimPrefix(stmt.Kind, "CREATE ")
	text := ""
	switch objectType {
	case "TABLE":
		text = s.getCreateTableSQL(stmt.Object, true)
		if text != "" && s.MySQLMode() {
			text = strings.TrimSuffix(text, ";") + " ENGINE=LSM;"
		}
		if text == "" && s.views[stmt.Object] != nil {
			objectType = "VIEW"
			text = s.GetCreateViewSQL(stmt.Object)
		}
	case "VIEW":
		text = s.GetCreateViewSQL(stmt.Object)
	case "TRIGGER":
		text = s.GetTriggerSQL(stmt.Object)
	}
	if text == "" {
		return nil, fmt.Errorf("%s %q does not exist", strings.ToLower(objectType), stmt.Object)
	}
	nameColumn := strings.ToUpper(objectType[:1]) + strings.ToLower(objectType[1:])
	ddlColumn := "Create " + strings.ToLower(objectType)
	if objectType == "TRIGGER" {
		ddlColumn = "SQL Original Statement"
	}
	rs := showResult(nameColumn, ddlColumn)
	if objectType == "VIEW" {
		rs = showResult(nameColumn, ddlColumn, "character_set_client", "collation_connection")
	} else if objectType == "TRIGGER" {
		rs = showResult(nameColumn, "sql_mode", ddlColumn, "character_set_client", "collation_connection", "Database Collation", "Created")
	}
	row := make(map[string]Value, len(rs.Columns))
	for _, col := range rs.Columns {
		row[col.Name] = NewNullValue()
	}
	row[nameColumn], row[ddlColumn] = NewTextValue(stmt.Object), NewTextValue(text)
	rs.Rows = append(rs.Rows, row)
	return rs, nil
}

func (s *Schema) showTriggers(namespace string) (*ResultSet, error) {
	data, err := s.informationSchemaTable("triggers", catalogFilter{Schema: s.catalogNamespace(namespace)})
	if err != nil {
		return nil, err
	}
	rs := showResult("Trigger", "Event", "Table", "Statement", "Timing", "Created", "sql_mode", "Definer", "character_set_client", "collation_connection", "Database Collation")
	fields := map[string]string{"Trigger": "trigger_name", "Event": "event_manipulation", "Table": "event_object_table", "Statement": "action_statement", "Timing": "action_timing", "Created": "created", "sql_mode": "sql_mode", "Definer": "definer", "character_set_client": "character_set_client", "collation_connection": "collation_connection", "Database Collation": "database_collation"}
	for _, row := range data.Rows {
		projected := make(map[string]Value, len(fields))
		for label, source := range fields {
			projected[label] = row[source]
		}
		rs.Rows = append(rs.Rows, projected)
	}
	return rs, nil
}

// Derive view metadata without executing its SELECT or reading table rows.
func (s *Schema) showViewColumns(view *ViewInfo, depth int) ([]ColumnDef, error) {
	if depth >= 100 {
		return nil, fmt.Errorf("view nesting limit exceeded")
	}
	query := view.Parsed
	if query == nil {
		return nil, fmt.Errorf("view %q has no query", view.Name)
	}
	sources := make(map[string]ColumnDef)
	var base []ColumnDef
	addSource := func(name, alias string) error {
		var cols []ColumnDef
		if table := s.tables[name]; table != nil {
			cols = table.Columns
		} else if nested := s.views[name]; nested != nil {
			var err error
			cols, err = s.showViewColumns(nested, depth+1)
			if err != nil {
				return err
			}
		} else if name != "" {
			return fmt.Errorf("table or view %q does not exist", name)
		}
		if alias == "" {
			alias = name
		}
		for _, col := range cols {
			sources[col.Name] = col
			sources[alias+"."+col.Name] = col
			base = append(base, col)
		}
		return nil
	}
	if err := addSource(query.From, query.FromAlias); err != nil {
		return nil, err
	}
	for _, join := range query.Joins {
		if err := addSource(join.Table, join.Alias); err != nil {
			return nil, err
		}
	}
	var cols []ColumnDef
	for i, selected := range query.Columns {
		if selected.Star {
			cols = append(cols, base...)
			continue
		}
		col := ColumnDef{Name: selected.Alias, ColType: ColInteger}
		switch expr := selected.Expr.(type) {
		case *ColumnRefExpr:
			if source, ok := sources[expr.Name]; ok {
				col.ColType = source.ColType
			}
			if col.Name == "" {
				col.Name = expr.Name
			}
		case *LiteralExpr:
			col.ColType = expr.Value.Type
		case *FuncCallExpr:
			if col.Name == "" {
				col.Name = expr.Name
			}
			switch expr.Name {
			case "UPPER", "LOWER", "SUBSTR", "REPLACE", "TRIM", "LTRIM", "RTRIM", "GROUP_CONCAT", "DATE", "TIME", "DATETIME", "STRFTIME", "HEX", "TYPEOF", "QUOTE":
				col.ColType = ColText
			case "AVG", "TOTAL", "JULIANDAY":
				col.ColType = ColReal
			case "CAST":
				if len(expr.Args) == 2 {
					if typ, ok := expr.Args[1].(*LiteralExpr); ok {
						switch typ.Value.Text {
						case "TEXT":
							col.ColType = ColText
						case "REAL":
							col.ColType = ColReal
						case "BLOB":
							col.ColType = ColBlob
						}
					}
				}
			}
		case *BinaryExpr:
			if expr.Op == "||" {
				col.ColType = ColText
			}
		}
		if col.Name == "" {
			col.Name = fmt.Sprintf("expr%d", i)
		}
		cols = append(cols, col)
	}
	if query.SelectAll && len(cols) == 0 {
		cols = append(cols, base...)
	}
	if len(view.Columns) > 0 {
		if len(view.Columns) != len(cols) {
			return nil, fmt.Errorf("view %q column count does not match its query", view.Name)
		}
		for i, name := range view.Columns {
			cols[i].Name = name
		}
	}
	// View columns do not inherit constraints or defaults from their sources.
	for i, col := range cols {
		cols[i] = ColumnDef{Name: col.Name, ColType: col.ColType}
	}
	return cols, nil
}
