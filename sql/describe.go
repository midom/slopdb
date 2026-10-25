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
	uniqueResultColumns(cols, nil)
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
		defer s.enterDefinitionDialect(v.DefinitionDialect)()
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
	if columns, known, err := s.virtualRelationColumns(name); known || err != nil {
		return append([]ColumnDef(nil), columns...), err
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
	if st, ok := stmt.(*SelectStmt); ok && st.definitionDialect != 0 {
		defer s.enterDefinitionDialect(st.definitionDialect)()
	}
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
		var expandErr error
		st, expandErr = s.expandQualifiedStars(st, scope, depth+1)
		if expandErr != nil {
			return nil, expandErr
		}
		s.describeLimitParameters(st.LimitParam, st.OffsetParam)
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
		s.constrainDescription(st.Where, s.booleanColumn(), aliases)
		s.constrainDescription(st.Having, s.booleanColumn(), aliases)
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
		s.describeLimitParameters(st.LimitParam, st.OffsetParam)
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
		names := st.ColumnNames
		if len(names) == 0 {
			for _, c := range cols {
				names = append(names, c.Name)
			}
		}
		groups := st.ValueGroups
		if len(groups) == 0 {
			groups = [][]Expression{st.Values}
		}
		for _, row := range groups {
			for i, e := range row {
				if i < len(names) {
					s.constrainDescription(e, env[names[i]], env)
				}
				if _, err = s.describeExpression(e, env, scope, depth+1); err != nil {
					return nil, err
				}
			}
		}
		if st.Select != nil {
			if _, err = s.describeStatement(st.Select, scope, depth+1); err != nil {
				return nil, err
			}
		}
		return s.describeProjection(st.Returning, cols, env, scope, depth, true)
	case *UpdateStmt:
		s.describeLimitParameters(st.LimitParam, st.OffsetParam)
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
			s.constrainDescription(set.Value, env[set.Column], env)
			if _, err = s.describeExpression(set.Value, env, scope, depth+1); err != nil {
				return nil, err
			}
		}
		if _, err = s.describeExpression(st.Where, env, scope, depth+1); err != nil {
			return nil, err
		}
		return s.describeProjection(st.Returning, cols, env, scope, depth, true)
	case *DeleteStmt:
		s.describeLimitParameters(st.LimitParam, st.OffsetParam)
		cols, err := s.describeTable(st.From, scope, depth+1)
		if err != nil {
			return nil, err
		}
		env := make(map[string]ColumnDef)
		describeEnv(cols, st.From, st.Alias, env)
		if _, err = s.describeExpression(st.Where, env, scope, depth+1); err != nil {
			return nil, err
		}
		return s.describeProjection(st.Returning, cols, env, scope, depth, true)
	case *ReplicationPositionStmt:
		if st.Operation == "wait" {
			text := LogicalType{Kind: TypeText}.Column()
			s.constrainDescription(st.Position, text, nil)
			s.constrainDescription(st.Timeout, text, nil)
			for _, expr := range []Expression{st.Position, st.Timeout} {
				if expr != nil {
					if _, err := s.describeExpression(expr, nil, scope, depth+1); err != nil {
						return nil, err
					}
				}
			}
		}
		return st.ResultColumns(), nil
	case *SessionVariableStmt:
		for _, e := range st.Assignments {
			if _, err := s.describeExpression(e, nil, scope, depth+1); err != nil {
				return nil, err
			}
		}
		return nil, nil
	case *ExplainStmt:
		if _, err := s.describeStatement(st.Stmt, scope, depth+1); err != nil {
			return nil, err
		}
		if s.MySQLMode() {
			return describeColumns("id", "select_type", "table", "partitions", "type", "possible_keys", "key", "key_len", "ref", "rows", "filtered", "Extra"), nil
		}
		return describeColumns("plan"), nil
	case *ShowStmt:
		switch st.Kind {
		case "LSM STATUS", "RAFT STATUS", "GLOBAL VARIABLES":
			return describeColumns("Variable_name", "Value"), nil
		default:
			return s.showMetadata(st)
		}

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
	if a.ColType == ColUnknown {
		b.Name = a.Name
		b.NotNull = false
		return b
	}
	if b.ColType == ColUnknown {
		a.NotNull = false
		return a
	}

	// Equal storage tags can still carry different SQL semantics (ENUM/TEXT,
	// UUID/BYTEA, timestamp/text or float4/float8). Do not retain one branch's
	// narrower declaration as the result descriptor for all branches.
	if a.ColType == b.ColType && a.LogicalType().Kind != b.LogicalType().Kind {
		nullable := a.NotNull && b.NotNull
		name := a.Name
		switch a.ColType {
		case ColInteger:
			a = ColumnDef{Name: name, ColType: ColInteger}
		case ColReal:
			a = ColumnDef{Name: name, ColType: ColReal}
		case ColText:
			a = ColumnDef{Name: name, ColType: ColText}
		case ColBlob:
			a = ColumnDef{Name: name, ColType: ColBlob}
		case ColJSON:
			a = ColumnDef{Name: name, ColType: ColJSON, Declared: "JSON"}
		}
		a.NotNull = nullable
		return a
	}
	if a.ColType != b.ColType {
		numeric := func(t ColumnType) bool { return t == ColInteger || t == ColUnsigned || t == ColDecimal || t == ColReal }
		if numeric(a.ColType) && numeric(b.ColType) && (a.ColType == ColReal || b.ColType == ColReal) {
			a = ColumnDef{Name: a.Name, ColType: ColReal}
		} else if numeric(a.ColType) && numeric(b.ColType) && (a.ColType == ColDecimal || b.ColType == ColDecimal) {
			a.ColType = ColDecimal
			a.Declared = "DECIMAL"
			a.Unsigned = false
		} else if numeric(a.ColType) && numeric(b.ColType) && (a.ColType == ColUnsigned || b.ColType == ColUnsigned) {
			a.ColType = ColDecimal
			a.Declared = "NUMERIC"
			a.SemanticType = TypeDecimal
			a.Unsigned = false
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
			if sc.StarQualifier != "" {
				qualifier := sc.StarQualifier
				if len(all) > 0 {
					if _, ok := env[qualifier+"."+all[0].Name]; !ok {
						qualifier = s.canonicalStarQualifier(qualifier)
					}
				}
				valid := len(all) > 0
				for _, col := range all {
					if _, ok := env[qualifier+"."+col.Name]; !ok {
						valid = false
						break
					}
				}
				if !valid {
					return nil, fmt.Errorf("unknown star qualifier %q", qualifier)
				}
			}
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
					name = e.ResultName()
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
		if sc.Alias == "" {
			if ref, ok := sc.Expr.(*ColumnRefExpr); ok {
				label := c.DeclaredName()
				if label == "" {
					label = ref.Name
				}
				if dot := strings.LastIndexByte(label, '.'); dot >= 0 {
					label = label[dot+1:]
				}
				if label != name {
					c.DisplayName = label
				} else {
					c.DisplayName = ""
				}
			}
		}
		c.Name = name
		if sc.Alias != "" {
			c.OriginalName = ""
			c.DisplayName = ""
		}
		if sc.starLabel != "" {
			c.DisplayName = sc.starLabel
		}
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
		if e.BoundParameter && s.describeTypes != nil {
			return s.describeParameter(int(e.Value.Int) - 1), nil
		}
		if e.Value.Null {
			if typ := e.Value.LogicalType(); typ.Known() {
				return typ.Column(), nil
			}
			return ColumnDef{ColType: ColUnknown}, nil
		}
		if (s.expressionMySQL() || s.expressionPostgres()) && (e.Value.NumericText != "" && !strings.ContainsAny(e.Value.NumericText, "eE") || e.Value.Type == ColDecimal) {
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
		col := ColumnDef{ColType: e.Value.Type, NotNull: true}
		if s.expressionPostgres() {
			if typ := e.Value.LogicalType(); typ.Known() {
				col.SemanticType = typ.Kind
				col.Declared = typ.Declaration()
			}
		}
		return col, nil
	case *ParameterExpr:
		return s.describeParameter(e.Index), nil
	case *ColumnRefExpr:
		if e.Name == "*" {
			return integer, nil
		}
		c, ok := env[e.Name]
		if !ok {
			return text, fmt.Errorf("no such column %s", e.Name)
		}
		if c.ColType == -1 {
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
		return s.booleanColumn(), nil
	case *InListExpr:
		c, err := s.describeExpression(e.Expr, env, scope, depth+1)
		if err != nil {
			return text, err
		}
		for _, v := range e.List {
			other, err := s.describeExpression(v, env, scope, depth+1)
			if err != nil {
				return text, err
			}
			s.constrainDescription(v, c, env)
			s.constrainDescription(e.Expr, other, env)
		}
		return s.booleanColumn(), nil
	case *BetweenExpr:
		c, err := s.describeExpression(e.Expr, env, scope, depth+1)
		if err != nil {
			return text, err
		}
		for _, v := range []Expression{e.Low, e.High} {
			other, err := s.describeExpression(v, env, scope, depth+1)
			if err != nil {
				return text, err
			}
			s.constrainDescription(v, c, env)
			s.constrainDescription(e.Expr, other, env)
		}
		return s.booleanColumn(), nil
	case *UnaryExpr:
		if strings.EqualFold(e.Op, "NOT") {
			s.constrainDescription(e.Right, s.booleanColumn(), env)
		}
		c, err := s.describeExpression(e.Right, env, scope, depth+1)
		if strings.EqualFold(e.Op, "NOT") {
			c = s.booleanColumn()
		}
		return c, err
	case *BinaryExpr:
		switch strings.ToUpper(e.Op) {
		case "AND", "OR":
			s.constrainDescription(e.Left, s.booleanColumn(), env)
			s.constrainDescription(e.Right, s.booleanColumn(), env)
		case "LIKE", "NOT LIKE", "GLOB", "NOT GLOB", "REGEXP", "NOT REGEXP":
			s.constrainDescription(e.Left, ColumnDef{ColType: ColText}, env)
			s.constrainDescription(e.Right, ColumnDef{ColType: ColText}, env)
		}
		a, err := s.describeExpression(e.Left, env, scope, depth+1)
		if err != nil {
			return text, err
		}
		b, err := s.describeExpression(e.Right, env, scope, depth+1)
		if err != nil {
			return text, err
		}
		s.constrainDescription(e.Left, b, env)
		s.constrainDescription(e.Right, a, env)
		if s.describeTypes != nil {
			if p, ok := e.Left.(*ParameterExpr); ok {
				a = s.describeParameter(p.Index)
			}
			if p, ok := e.Right.(*ParameterExpr); ok {
				b = s.describeParameter(p.Index)
			}
		}
		if s.expressionMySQL() {
			if mysqlNumericHexExpression(e.Left) {
				a = ColumnDef{ColType: ColUnsigned, Unsigned: true}
			}
			if mysqlNumericHexExpression(e.Right) {
				b = ColumnDef{ColType: ColUnsigned, Unsigned: true}
			}
		}
		switch strings.ToUpper(e.Op) {
		case "=", "!=", "<>", "<", ">", "<=", ">=", "AND", "OR", "IS", "IS NOT", "LIKE", "NOT LIKE", "REGEXP", "GLOB":
			if s.expressionPostgres() {
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
		if e.Op == "/" && (s.expressionMySQL() || a.ColType == ColDecimal || b.ColType == ColDecimal) {
			return ColumnDef{ColType: ColDecimal, Declared: "DECIMAL", Precision: 65, Scale: min(30, a.Scale+4)}, nil
		}
		if a.ColType == ColText || b.ColType == ColText {
			return integer, nil
		}
		return describeCommon(a, b), nil
	case *CaseExpr:
		var caseType ColumnDef
		if e.Expr != nil {
			var err error
			caseType, err = s.describeExpression(e.Expr, env, scope, depth+1)
			if err != nil {
				return text, err
			}
		}
		for _, w := range e.Whens {
			if e.Expr == nil {
				s.constrainDescription(w.Cond, s.booleanColumn(), env)
			} else {
				s.constrainDescription(w.Cond, caseType, env)
			}
			c, err := s.describeExpression(w.Cond, env, scope, depth+1)
			if err != nil {
				return text, err
			}
			if e.Expr != nil {
				s.constrainDescription(e.Expr, c, env)
			}
		}
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
		name := strings.ToUpper(e.Name)
		if name == "CAST" && len(e.Args) == 2 {
			if target, ok := e.Args[1].(*LiteralExpr); ok {
				if _, err := s.describeExpression(e.Args[0], env, scope, depth+1); err != nil {
					return text, err
				}
				name := strings.ToUpper(mysqlCastTarget(target.Value.Text))
				c := describeType(name)
				if s.expressionPostgres() {
					c = postgresDescribeType(name)
				} else if !s.expressionMySQL() && (name == "DECIMAL" || name == "NUMERIC") {
					c = ColumnDef{ColType: ColDecimal, Declared: name}
				}
				s.constrainDescription(e.Args[0], c, env)
				return c, nil
			}
		}

		if err := validateResolvedFunction(s.ExpressionDialect(), e); err != nil && !isAggregate(e) && e.Window == nil {

			return text, err
		}

		var args []ColumnDef
		for _, a := range e.Args {
			c, err := s.describeExpression(a, env, scope, depth+1)
			if err != nil {
				return text, err
			}
			args = append(args, c)
			if s.expressionMySQL() && mysqlNumericMetadataFunction(name) {
				if mysqlNumericHexExpression(a) {
					args[len(args)-1] = ColumnDef{ColType: ColUnsigned, Unsigned: true}
				}
			}
		}
		if expected, known, err := FunctionArgumentTypes(s.ExpressionDialect(), name, args); known || err != nil {
			if err != nil {
				return text, err
			}
			for i, want := range expected {
				if i < len(e.Args) {
					s.constrainDescription(e.Args[i], want, env)
				}
			}
			if s.describeTypes != nil {
				for i, arg := range e.Args {
					c, err := s.describeExpression(arg, env, scope, depth+1)
					if err != nil {
						return text, err
					}
					args[i] = c
				}
			}
		}
		if column, known, err := FunctionMetadata(s.ExpressionDialect(), name, args); known || err != nil {
			return column, err
		}
		return ColumnDef{ColType: ColUnknown}, nil
	}
	return text, fmt.Errorf("unsupported metadata expression %T", expr)
}
