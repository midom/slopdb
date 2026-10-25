package sql

import (
	"fmt"
	"maps"
	"slices"
	"strings"
)

// Describe returns catalog-derived result columns without evaluating any user
// expression, acquiring row locks, or running a statement. Callers serialize the
// session and hold a catalog execution view as for ordinary metadata reads.
func (s *Schema) Describe(stmt Statement) ([]ColumnDef, error) {
	cols, err := s.describeStatement(stmt, nil, 0)
	for i := range cols {
		c := &cols[i]
		c.EnumValues = slices.Clone(c.EnumValues)
		c.RefColumns = slices.Clone(c.RefColumns)
		c.ConstraintNames = maps.Clone(c.ConstraintNames)
		c.Default = nil
		c.Check = nil
	}
	return cols, err
}
func describeType(name string) ColumnDef {
	p := &Parser{tok: NewTokenizer(name)}
	p.advance()
	var c ColumnDef
	if err := p.parseColumnType(&c); err != nil || p.cur.Type != TokEOF {
		return ColumnDef{ColType: ColText}
	}
	return c
}
func (s *Schema) describeTable(name string, scope map[string][]ColumnDef, depth int) ([]ColumnDef, error) {
	if name == "" {
		return nil, nil
	}
	name = strings.ToLower(name)
	if name == "sqlite_master" {
		return describeColumns("type", "name", "tbl_name", "rootpage", "sql"), nil
	}
	if name == "sqlite_stat1" {
		return describeColumns("tbl", "idx", "stat"), nil
	}
	if c, ok := scope[name]; ok {
		return append([]ColumnDef(nil), c...), nil
	}
	if t := s.tables[name]; t != nil {
		return append([]ColumnDef(nil), t.Columns...), nil
	}
	if v := s.views[name]; v != nil {
		cols, err := s.describeStatement(v.Parsed, scope, depth+1)
		if err != nil {
			return nil, err
		}
		if len(v.Columns) > 0 {
			if len(v.Columns) != len(cols) {
				return nil, fmt.Errorf("view column count mismatch")
			}
			for i := range cols {
				cols[i].Name = v.Columns[i]
			}
		}
		return cols, nil
	}
	if s.postgresMode {
		if data, err := s.PostgresCatalogTable(name); data != nil || err != nil {
			if err != nil {
				return nil, err
			}
			return append([]ColumnDef(nil), data.Columns...), nil
		}
	}
	if options, ok := MySQLSession(s.ctx); ok && (strings.HasPrefix(name, "information_schema.") || name == "mysql.user") {
		data, err := s.mysqlCatalogTable(name, options)
		if err != nil {
			return nil, err
		}
		return append([]ColumnDef(nil), data.Columns...), nil
	}
	return nil, fmt.Errorf("table %q does not exist", name)
}
func describeEnv(cols []ColumnDef, table, alias string, env map[string]ColumnDef) {
	for _, c := range cols {
		if _, exists := env[c.Name]; exists {
			env[c.Name] = ColumnDef{ColType: ColumnType(-1)}
		} else {
			env[c.Name] = c
		}
		if table != "" {
			env[table+"."+c.Name] = c
		}
		if alias != "" {
			env[alias+"."+c.Name] = c
		}
	}
}
func (s *Schema) describeStatement(stmt Statement, outer map[string][]ColumnDef, depth int) ([]ColumnDef, error) {
	if depth > 64 {
		return nil, fmt.Errorf("metadata nesting limit exceeded")
	}
	scope := make(map[string][]ColumnDef, len(outer))
	for n, c := range outer {
		scope[n] = c
	}
	var with *WithClause
	switch st := stmt.(type) {
	case *SelectStmt:
		with = st.With
	case *UnionStmt:
		with = st.With
	case *InsertStmt:
		with = st.With
	case *UpdateStmt:
		with = st.With
	case *DeleteStmt:
		with = st.With
	}
	if with != nil {
		for _, cte := range with.Entries {
			if with.Recursive && len(cte.Columns) > 0 {
				for _, name := range cte.Columns {
					scope[cte.Name] = append(scope[cte.Name], ColumnDef{Name: name, ColType: ColText})
				}
			}
			cols, err := s.describeStatement(cte.Query, scope, depth+1)
			if err != nil {
				return nil, err
			}
			if len(cte.Columns) > 0 {
				if len(cte.Columns) != len(cols) {
					return nil, fmt.Errorf("CTE column count mismatch")
				}
				for i := range cols {
					cols[i].Name = cte.Columns[i]
				}
			}
			scope[cte.Name] = cols
		}
	}
	switch st := stmt.(type) {
	case *SelectStmt:
		if st.Compound != nil {
			return s.describeStatement(st.Compound, scope, depth+1)
		}
		if st.materialized != nil {
			return append([]ColumnDef(nil), st.materialized.Columns...), nil
		}
		var base []ColumnDef
		var err error
		if sub, ok := st.FromExpr.(*SubqueryExpr); ok {
			base, err = s.describeStatement(sub.Stmt, scope, depth+1)
		} else {
			base, err = s.describeTable(st.From, scope, depth+1)
		}
		if err != nil {
			return nil, err
		}
		all := append([]ColumnDef(nil), base...)
		env := make(map[string]ColumnDef)
		describeEnv(base, st.From, st.FromAlias, env)
		for _, j := range st.Joins {
			var cols []ColumnDef
			if j.Subquery != nil {
				cols, err = s.describeStatement(j.Subquery, scope, depth+1)
			} else {
				cols, err = s.describeTable(j.Table, scope, depth+1)
			}
			if err != nil {
				return nil, err
			}
			if j.Type == "LEFT" || j.Type == "RIGHT" {
				for i := range cols {
					cols[i].NotNull = false
				}
			}
			describeEnv(cols, j.Table, j.Alias, env)
			all = append(all, cols...)
		}
		for _, c := range scope["\x00outer"] {
			if _, exists := env[c.Name]; !exists {
				env[c.Name] = c
			}
		}
		if st.JoinGroup {
			return all, nil
		}
		// Resolve every referenced expression statically. Volatile functions, locking
		// reads, recursive rows and user data never execute during this traversal.
		var exprs []Expression
		exprs = append(exprs, st.Where, st.Having)
		exprs = append(exprs, st.GroupBy...)
		for _, j := range st.Joins {
			exprs = append(exprs, j.On)
		}
		for _, o := range st.OrderBy {
			exprs = append(exprs, o.Expr)
		}
		aliases := make(map[string]ColumnDef, len(env))
		for n, c := range env {
			aliases[n] = c
		}
		result, err := s.describeProjection(st.Columns, all, env, scope, depth, false)
		if err != nil {
			return nil, err
		}
		for _, c := range result {
			aliases[c.Name] = c
		}
		for _, e := range exprs {
			if _, err = s.describeExpression(e, aliases, scope, depth+1); err != nil {
				return nil, err
			}
		}
		if st.SelectAll {
			return all, nil
		}
		return result, nil
	case *UnionStmt:
		a, err := s.describeStatement(st.Left, scope, depth+1)
		if err != nil {
			return nil, err
		}
		b, err := s.describeStatement(st.Right, scope, depth+1)
		if err != nil {
			return nil, err
		}
		if len(a) != len(b) {
			return nil, fmt.Errorf("compound column count mismatch")
		}
		for i := range a {
			a[i] = describeCommon(a[i], b[i])
		}
		return a, nil
	case *InsertStmt:
		cols, err := s.describeTable(st.Table, scope, depth+1)
		if err != nil {
			return nil, err
		}
		env := make(map[string]ColumnDef)
		describeEnv(cols, st.Table, "", env)
		for _, name := range st.ColumnNames {
			if _, ok := env[name]; !ok {
				return nil, fmt.Errorf("no such column %s", name)
			}
		}
		if st.Select != nil {
			if _, err = s.describeStatement(st.Select, scope, depth+1); err != nil {
				return nil, err
			}
		}
		return s.describeProjection(st.Returning, cols, env, scope, depth, true)
	case *UpdateStmt:
		cols, err := s.describeTable(st.Table, scope, depth+1)
		if err != nil {
			return nil, err
		}
		env := make(map[string]ColumnDef)
		describeEnv(cols, st.Table, st.Alias, env)
		for _, j := range st.Joins {
			other, e := s.describeTable(j.Table, scope, depth+1)
			if e != nil {
				return nil, e
			}
			describeEnv(other, j.Table, j.Alias, env)
		}
		for _, set := range st.Set {
			if _, ok := env[set.Column]; !ok {
				return nil, fmt.Errorf("no such column %s", set.Column)
			}
			if _, err = s.describeExpression(set.Value, env, scope, depth+1); err != nil {
				return nil, err
			}
		}
		return s.describeProjection(st.Returning, cols, env, scope, depth, true)
	case *DeleteStmt:
		cols, err := s.describeTable(st.From, scope, depth+1)
		if err != nil {
			return nil, err
		}
		env := make(map[string]ColumnDef)
		describeEnv(cols, st.From, st.Alias, env)
		return s.describeProjection(st.Returning, cols, env, scope, depth, true)
	case *ExplainStmt:
		if _, err := s.describeStatement(st.Stmt, scope, depth+1); err != nil {
			return nil, err
		}
		if s.mysqlMode {
			return describeColumns("id", "select_type", "table", "partitions", "type", "possible_keys", "key", "key_len", "ref", "rows", "filtered", "Extra"), nil
		}
		return describeColumns("plan"), nil
	case *ShowStmt:
		switch st.Kind {
		case "COLUMNS":
			return describeColumns("Field", "Type", "Null", "Key", "Default", "Extra"), nil
		case "INDEX":
			return describeColumns("Table", "Non_unique", "Key_name", "Seq_in_index", "Column_name", "Collation", "Cardinality", "Sub_part", "Packed", "Null", "Index_type", "Comment", "Index_comment", "Visible", "Expression"), nil
		case "CREATE TABLE":
			return describeColumns("Table", "Create table"), nil
		case "CREATE VIEW":
			return describeColumns("View", "Create view", "character_set_client", "collation_connection"), nil
		case "DATABASES":
			return describeColumns("Database"), nil
		case "TABLES":
			return describeColumns("Tables_in_main"), nil
		}
		return nil, fmt.Errorf("metadata unsupported for SHOW %s", st.Kind)
	case *PragmaStmt:
		switch st.Name {
		case "table_info":
			return describeColumns("cid", "name", "type", "notnull", "dflt_value", "pk"), nil
		case "page_size":
			return []ColumnDef{{Name: "page_size", ColType: ColInteger}}, nil
		}
		return nil, nil
	}
	return nil, nil
}
func describeColumns(names ...string) []ColumnDef {
	out := make([]ColumnDef, len(names))
	for i, n := range names {
		out[i] = ColumnDef{Name: n, ColType: ColText}
	}
	return out
}
func describeCommon(a, b ColumnDef) ColumnDef {
	if a.ColType != b.ColType {
		numeric := func(t ColumnType) bool { return t == ColInteger || t == ColUnsigned || t == ColDecimal || t == ColReal }
		if a.ColType == ColReal || b.ColType == ColReal {
			a = ColumnDef{Name: a.Name, ColType: ColReal}
		} else if numeric(a.ColType) && numeric(b.ColType) && (a.ColType == ColDecimal || b.ColType == ColDecimal) {
			a.ColType = ColDecimal
			a.Declared = "DECIMAL"
			a.Unsigned = false
		} else if numeric(a.ColType) && numeric(b.ColType) && (a.ColType == ColUnsigned || b.ColType == ColUnsigned) {
			a.ColType = ColUnsigned
			a.Unsigned = true
		} else {
			a = ColumnDef{Name: a.Name, ColType: ColText}
		}
	}
	a.NotNull = a.NotNull && b.NotNull
	return a
}
func (s *Schema) describeProjection(columns []SelectColumn, all []ColumnDef, env map[string]ColumnDef, scope map[string][]ColumnDef, depth int, returning bool) ([]ColumnDef, error) {
	var out []ColumnDef
	exprIndex := 0
	for i, sc := range columns {
		if sc.Star {
			out = append(out, all...)
			continue
		}
		c, err := s.describeExpression(sc.Expr, env, scope, depth+1)
		if err != nil {
			return nil, err
		}
		name := sc.Alias
		if name == "" {
			switch e := sc.Expr.(type) {
			case *ColumnRefExpr:
				name = e.Name
			case *FuncCallExpr:
				if !returning {
					name = e.Name
				}
			}
		}
		if name == "" {
			index := exprIndex
			if returning {
				index = i
			}
			name = fmt.Sprintf("expr%d", index)
		}
		c.Name = name
		out = append(out, c)
		exprIndex++
	}
	return out, nil
}
func (s *Schema) describeExpression(expr Expression, env map[string]ColumnDef, scope map[string][]ColumnDef, depth int) (ColumnDef, error) {
	if depth > 128 {
		return ColumnDef{}, fmt.Errorf("expression metadata nesting limit exceeded")
	}
	text := ColumnDef{ColType: ColText}
	integer := ColumnDef{ColType: ColInteger}
	real := ColumnDef{ColType: ColReal}
	switch e := expr.(type) {
	case nil:
		return text, nil
	case *LiteralExpr:
		if e.BoundParameter || e.Value.Null {
			return text, nil
		}
		if s.mysqlMode && (e.Value.NumericText != "" && !strings.ContainsAny(e.Value.NumericText, "eE") || e.Value.Type == ColDecimal) {
			value := e.Value
			if value.NumericText != "" {
				var err error
				value, err = NewDecimalValue(value.NumericText)
				if err != nil {
					return text, err
				}
			}
			n, scale, err := valueDecimal(value)
			if err != nil {
				return text, err
			}
			digits := len(n.String())
			if n.Sign() < 0 {
				digits--
			}
			return ColumnDef{ColType: ColDecimal, Precision: max(digits, scale), Scale: scale, NotNull: true}, nil
		}
		return ColumnDef{ColType: e.Value.Type, NotNull: true, Declared: e.Value.PGType, Postgres: s.postgresMode && e.Value.PGType != ""}, nil
	case *ParameterExpr:
		return text, nil
	case *ColumnRefExpr:
		if e.Name == "*" {
			return integer, nil
		}
		c, ok := env[e.Name]
		if !ok {
			return text, fmt.Errorf("no such column %s", e.Name)
		}
		if c.ColType < 0 {
			return text, fmt.Errorf("ambiguous column %s", e.Name)
		}
		return c, nil
	case *SubqueryExpr:
		nested := make(map[string][]ColumnDef, len(scope)+1)
		for n, c := range scope {
			nested[n] = c
		}
		for n, c := range env {
			c.Name = n
			nested["\x00outer"] = append(nested["\x00outer"], c)
		}
		cols, err := s.describeStatement(e.Stmt, nested, depth+1)
		if err != nil {
			return text, err
		}
		if len(cols) != 1 {
			return text, fmt.Errorf("scalar subquery requires one column")
		}
		cols[0].NotNull = false
		return cols[0], nil
	case *ExistsExpr:
		return integer, nil
	case *InListExpr, *BetweenExpr:
		return integer, nil
	case *UnaryExpr:
		c, err := s.describeExpression(e.Right, env, scope, depth+1)
		if strings.EqualFold(e.Op, "NOT") {
			c = integer
		}
		return c, err
	case *BinaryExpr:
		a, err := s.describeExpression(e.Left, env, scope, depth+1)
		if err != nil {
			return text, err
		}
		b, err := s.describeExpression(e.Right, env, scope, depth+1)
		if err != nil {
			return text, err
		}
		if s.mysqlMode {
			if mysqlNumericHexExpression(e.Left) {
				a = ColumnDef{ColType: ColUnsigned, Unsigned: true}
			}
			if mysqlNumericHexExpression(e.Right) {
				b = ColumnDef{ColType: ColUnsigned, Unsigned: true}
			}
		}
		switch strings.ToUpper(e.Op) {
		case "=", "!=", "<>", "<", ">", "<=", ">=", "AND", "OR", "IS", "IS NOT", "LIKE", "NOT LIKE", "REGEXP", "GLOB":
			if s.postgresMode {
				return ColumnDef{ColType: ColInteger, Declared: "BOOLEAN", Postgres: true}, nil
			}
			return integer, nil
		case "||", "->>":
			return text, nil
		case "->":
			return describeType("JSON"), nil
		}
		if a.ColType == ColReal || b.ColType == ColReal {
			return real, nil
		}
		if e.Op == "/" && (s.mysqlMode || a.ColType == ColDecimal || b.ColType == ColDecimal) {
			return ColumnDef{ColType: ColDecimal, Declared: "DECIMAL", Precision: 65, Scale: min(30, a.Scale+4)}, nil
		}
		if a.ColType == ColText || b.ColType == ColText {
			return integer, nil
		}
		return describeCommon(a, b), nil
	case *CaseExpr:
		var result ColumnDef
		set := false
		for _, w := range e.Whens {
			c, err := s.describeExpression(w.Then, env, scope, depth+1)
			if err != nil {
				return text, err
			}
			if !set {
				result = c
				set = true
			} else {
				result = describeCommon(result, c)
			}
		}
		if e.ElseExpr != nil {
			c, err := s.describeExpression(e.ElseExpr, env, scope, depth+1)
			if err != nil {
				return text, err
			}
			if !set {
				return c, nil
			}
			result = describeCommon(result, c)
		}
		return result, nil
	case *FuncCallExpr:
		if s.postgresMode {
			if column, ok, err := s.describePostgresFunction(e, env, scope, depth); ok || err != nil {
				return column, err
			}
		}
		if err := validateScalarFunction(e); err != nil && !isAggregate(e) && e.Window == nil {
			return text, err
		}
		name := strings.ToUpper(e.Name)
		if name == "CAST" && len(e.Args) == 2 {
			if target, ok := e.Args[1].(*LiteralExpr); ok {
				name := strings.ToUpper(mysqlCastTarget(target.Value.Text))
				if !s.mysqlMode && (name == "DECIMAL" || name == "NUMERIC") {
					return ColumnDef{ColType: ColDecimal, Declared: name}, nil
				}
				c := describeType(name)
				if s.postgresMode {
					c = postgresDescribeType(name)
				}
				return c, nil
			}
		}
		var args []ColumnDef
		for _, a := range e.Args {
			c, err := s.describeExpression(a, env, scope, depth+1)
			if err != nil {
				return text, err
			}
			args = append(args, c)
			if s.mysqlMode && mysqlNumericMetadataFunction(name) {
				if mysqlNumericHexExpression(a) {
					args[len(args)-1] = ColumnDef{ColType: ColUnsigned, Unsigned: true}
				}
			}
		}
		if column, ok := s.describeRegisteredMySQLFunction(name, args); ok {
			return column, nil
		}
		common := text
		if len(args) > 0 {
			common = args[0]
			for _, a := range args[1:] {
				common = describeCommon(common, a)
			}
		}
		switch name {
		case "COUNT", "LENGTH", "OCTET_LENGTH", "CHAR_LENGTH", "INSTR", "CHANGES", "LAST_INSERT_ROWID", "ROW_NUMBER", "RANK", "DENSE_RANK", "NTILE", "CONNECTION_ID", "GET_LOCK", "RELEASE_LOCK", "IS_FREE_LOCK", "UNIX_TIMESTAMP":
			return integer, nil
		case "AVG":
			if common.ColType == ColDecimal || common.ColType == ColUnsigned {
				return ColumnDef{ColType: ColDecimal, Declared: "DECIMAL"}, nil
			}
			return real, nil
		case "TOTAL", "PERCENT_RANK", "CUME_DIST", fulltextFunction:
			return real, nil
		case "MIN", "MAX", "ABS", "SUM", "COALESCE", "IFNULL", "NULLIF", "LAG", "LEAD", "FIRST_VALUE", "LAST_VALUE", "NTH_VALUE", "GREATEST", "LEAST":
			return common, nil
		case "IF":
			if len(args) == 3 {
				return describeCommon(args[1], args[2]), nil
			}
		case "GROUP_CONCAT", "CONCAT", "SUBSTRING", "SUBSTR":
			for _, a := range args {
				if a.ColType == ColBlob {
					return ColumnDef{ColType: ColBlob}, nil
				}
			}
			return text, nil
		}
		if strings.HasPrefix(name, "JSON_") || name == "JSON" || name == "JSONB" {
			switch name {
			case "JSON_UNQUOTE", "JSON_TYPE", "JSON_VALUE":
				return text, nil
			case "JSON_VALID", "JSON_LENGTH", "JSON_DEPTH", "JSON_CONTAINS", "JSON_CONTAINS_PATH", "JSON_OVERLAPS":
				return integer, nil
			}
			return describeType("JSON"), nil
		}
		return text, nil
	}
	return text, fmt.Errorf("unsupported metadata expression %T", expr)
}
