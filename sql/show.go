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
// SlopDB has one database, named main, as in PRAGMA database_list.
func (s *Schema) ExecuteShow(stmt *ShowStmt) (*ResultSet, error) {
	if err := s.checkInterrupt(); err != nil {
		return nil, err
	}
	if stmt.Database != "" && !strings.EqualFold(stmt.Database, "main") {
		return nil, fmt.Errorf("unknown database %q", stmt.Database)
	}
	var rs *ResultSet
	var err error
	likeColumn := ""
	switch stmt.Kind {
	case "DATABASES":
		rs = showResult("Database")
		rs.Rows = append(rs.Rows, map[string]Value{"Database": NewTextValue("main")})
		likeColumn = "Database"
	case "TABLES":
		likeColumn = "Tables_in_main"
		rs = showResult(likeColumn)
		if stmt.Full {
			rs.Columns = append(rs.Columns, ColumnDef{Name: "Table_type", ColType: ColText})
		}
		names := append(s.GetTableList(), s.GetViewList()...)
		sort.Strings(names)
		for _, name := range names {
			row := map[string]Value{likeColumn: NewTextValue(name)}
			if stmt.Full {
				kind := "BASE TABLE"
				if _, ok := s.views[name]; ok {
					kind = "VIEW"
				}
				row["Table_type"] = NewTextValue(kind)
			}
			rs.Rows = append(rs.Rows, row)
		}
	case "COLUMNS":
		rs, err = s.showColumns(stmt)
		likeColumn = "Field"
	case "INDEX":
		rs, err = s.showIndexes(stmt.Object)
	case "CREATE TABLE", "CREATE VIEW", "CREATE TRIGGER":
		return s.showCreate(stmt)
	case "TRIGGERS":
		rs = s.showTriggers()
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
func (s *Schema) showKeys(info *TableInfo) []showKey {
	var keys []showKey
	if len(info.PKColumns) > 0 {
		keys = append(keys, showKey{"PRIMARY", info.PKColumns, true, "LSM"})
	}
	for _, idx := range s.indexes {
		if idx.Table != info.Name {
			continue
		}
		if len(info.PKColumns) == 1 && s.isAutoIndex(idx) && len(idx.columnNames()) == 1 && idx.Column == info.PKColumns[0] {
			continue // the auto-created primary-key index is represented above
		}
		method := "LSM"
		if idx.Auto {
			method = "CONSTRAINT"
		}
		keys = append(keys, showKey{idx.Name, idx.columnNames(), idx.IsUnique, method})
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
	info := s.tables[stmt.Object]
	if info == nil {
		if view := s.views[stmt.Object]; view != nil {
			cols, err := s.showViewColumns(view, 0)
			if err != nil {
				return nil, err
			}
			info = &TableInfo{Name: view.Name, Columns: cols}
		} else {
			return nil, fmt.Errorf("table or view %q does not exist", stmt.Object)
		}
	}
	rs := showResult("Field", "Type", "Null", "Key", "Default", "Extra")
	if stmt.Full {
		rs = showResult("Field", "Type", "Collation", "Null", "Key", "Default", "Extra", "Privileges", "Comment")
	}
	keys := s.showKeys(info)
	for _, col := range info.Columns {
		key := ""
		for _, k := range keys {
			for i, name := range k.columns {
				if name != col.Name {
					continue
				}
				if k.name == "PRIMARY" {
					key = "PRI"
				} else if i == 0 && key != "PRI" {
					if k.unique && len(k.columns) == 1 {
						key = "UNI"
					} else if key == "" {
						key = "MUL"
					}
				}
			}
		}
		nullable := "YES"
		if col.NotNull || (key == "PRI" && col.ColType == ColInteger && len(info.PKColumns) == 1) {
			nullable = "NO"
		}
		dflt := NewNullValue()
		if lit, ok := col.Default.(*LiteralExpr); ok {
			if !lit.Value.Null {
				dflt = NewTextValue(lit.Value.String())
			}
		} else if col.Default != nil {
			dflt = NewTextValue(formatExpr(col.Default))
			if unary, ok := col.Default.(*UnaryExpr); ok && (unary.Op == "-" || unary.Op == "+") {
				if lit, ok := unary.Right.(*LiteralExpr); ok && (lit.Value.Type == ColInteger || lit.Value.Type == ColReal) {
					value, err := s.evalExpr(unary, nil)
					if err != nil {
						return nil, err
					}
					dflt = NewTextValue(value.String())
				}
			}
		}
		extra := ""
		if col.AutoIncrement {
			extra = "auto_increment"
		}
		row := map[string]Value{
			"Field": NewTextValue(col.Name), "Type": NewTextValue(col.ColType.String()),
			"Null": NewTextValue(nullable), "Key": NewTextValue(key),
			"Default": dflt, "Extra": NewTextValue(extra),
		}
		if stmt.Full {
			row["Collation"] = NewNullValue()
			row["Privileges"] = NewTextValue("select,insert,update,references")
			row["Comment"] = NewTextValue("")
		}
		rs.Rows = append(rs.Rows, row)
	}
	return rs, nil
}

func (s *Schema) showIndexes(name string) (*ResultSet, error) {
	info := s.tables[name]
	if info == nil {
		return nil, fmt.Errorf("table %q does not exist", name)
	}
	rs := showResult("Table", "Non_unique", "Key_name", "Seq_in_index", "Column_name", "Collation", "Cardinality", "Sub_part", "Packed", "Null", "Index_type", "Comment", "Index_comment", "Visible", "Expression")
	for i := range rs.Columns {
		switch rs.Columns[i].Name {
		case "Non_unique", "Seq_in_index", "Cardinality", "Sub_part":
			rs.Columns[i].ColType = ColInteger
		}
	}
	for _, key := range s.showKeys(info) {
		for i, name := range key.columns {
			nullable := "YES"
			for _, col := range info.Columns {
				if col.Name == name && (col.NotNull || (key.name == "PRIMARY" && len(info.PKColumns) == 1 && col.ColType == ColInteger)) {
					nullable = ""
				}
			}
			nonUnique := int64(1)
			if key.unique {
				nonUnique = 0
			}
			rs.Rows = append(rs.Rows, map[string]Value{
				"Table": NewTextValue(info.Name), "Non_unique": NewIntValue(nonUnique),
				"Key_name": NewTextValue(key.name), "Seq_in_index": NewIntValue(int64(i + 1)),
				"Column_name": NewTextValue(name), "Collation": NewTextValue("A"),
				"Cardinality": NewNullValue(), "Sub_part": NewNullValue(), "Packed": NewNullValue(),
				"Null": NewTextValue(nullable), "Index_type": NewTextValue(key.method),
				"Comment": NewTextValue(""), "Index_comment": NewTextValue(""),
				"Visible": NewTextValue("YES"), "Expression": NewNullValue(),
			})
		}
	}
	return rs, nil
}

func (s *Schema) showCreate(stmt *ShowStmt) (*ResultSet, error) {
	objectType := strings.TrimPrefix(stmt.Kind, "CREATE ")
	text := ""
	switch objectType {
	case "TABLE":
		text = s.getCreateTableSQL(stmt.Object, true)
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

func (s *Schema) showTriggers() *ResultSet {
	rs := showResult("Trigger", "Event", "Table", "Statement", "Timing", "Created", "sql_mode", "Definer", "character_set_client", "collation_connection", "Database Collation")
	for _, name := range s.GetTriggerList() {
		t := s.triggers[name]
		target := t.Table
		if target == "" {
			target = t.View
		}
		timing := t.Timing
		if t.InsteadOf {
			timing = "INSTEAD OF"
		} else if timing == "" {
			timing = "AFTER"
		}
		row := make(map[string]Value, len(rs.Columns))
		for _, col := range rs.Columns {
			row[col.Name] = NewNullValue()
		}
		row["Trigger"], row["Event"], row["Table"] = NewTextValue(name), NewTextValue(t.Event), NewTextValue(target)
		row["Statement"], row["Timing"] = NewTextValue(strings.Join(t.Body, "; ")), NewTextValue(timing)
		rs.Rows = append(rs.Rows, row)
	}
	return rs
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
