package postgresserver

import (
	"context"
	"fmt"
	"strings"

	"slopdb/sql"
)

type metadata struct {
	client   *client
	ctx      context.Context
	prepared *prepared
	tables   map[string][]field
}

func quote(s string) string { return "'" + strings.ReplaceAll(s, "'", "''") + "'" }
func (m *metadata) table(name string, scope map[string][]field, depth int) ([]field, error) {
	if name == "" {
		return nil, nil
	}
	name = strings.ToLower(name)
	if fields, ok := scope[name]; ok {
		return fields, nil
	}
	if fields, ok := m.tables[name]; ok {
		return fields, nil
	}
	if depth > 64 {
		return nil, &protocolError{"54001", "metadata nesting limit exceeded"}
	}
	if data, err := m.client.session.PostgresCatalog(sql.WithPostgresSession(m.ctx, sql.PostgresSessionOptions{Database: m.client.server.config.Database, User: m.client.server.config.Username}), name); data != nil || err != nil {
		if err != nil {
			return nil, err
		}
		var fields []field
		for _, col := range data.Columns {
			oid := sql.PostgresColumnOID(col)
			if oid == 0 {
				oid = nativeOID(col.ColType)
			}
			fields = append(fields, field{col.Name, oid})
		}
		m.tables[name] = fields
		return fields, nil
	}
	// This reads catalog metadata only; user SELECTs and DML never run at Parse,
	// Bind or Describe, including volatile expressions and locking reads.
	result, err := m.client.session.ExecContext(m.ctx, "PRAGMA table_info("+quote(name)+")")
	if err != nil {
		// SHOW CREATE VIEW exposes canonical catalog SQL. Resolve projections
		// statically, including this session's uncommitted catalog changes.
		views, e := m.client.session.ExecContext(m.ctx, "SHOW CREATE VIEW \""+strings.ReplaceAll(name, "\"", "\"\"")+"\"")
		if e != nil {
			return nil, err
		}
		if len(views.Rows) == 0 {
			return nil, err
		}
		stmts, e := sql.Parse(views.Rows[0]["Create view"].Text)
		if e != nil {
			return nil, e
		}
		if len(stmts) != 1 {
			return nil, fmt.Errorf("invalid view metadata")
		}
		view, ok := stmts[0].(*sql.CreateViewStmt)
		if !ok {
			return nil, fmt.Errorf("invalid view metadata")
		}
		fields, e := m.statement(view.Select, scope, depth+1)
		if e != nil {
			return nil, e
		}
		if len(view.Columns) > 0 {
			if len(view.Columns) != len(fields) {
				return nil, fmt.Errorf("invalid view columns")
			}
			for i, name := range view.Columns {
				fields[i].name = name
			}
		}
		m.tables[name] = fields
		return fields, nil
	}
	var fields []field
	for _, row := range result.Rows {
		typ := strings.ToUpper(row["type"].Text)
		oid := oidText
		switch {
		case strings.Contains(typ, "UNSIGNED") && strings.HasPrefix(typ, "BIGINT"):
			oid = oidNumeric
		case strings.HasPrefix(typ, "DECIMAL") || strings.HasPrefix(typ, "NUMERIC"):
			oid = oidNumeric
		case typ == "JSON" || typ == "JSONB":
			oid = oidJSON
		case strings.HasPrefix(typ, "INT") || strings.HasPrefix(typ, "BIGINT") || strings.HasPrefix(typ, "SMALLINT") || strings.HasPrefix(typ, "TINYINT") || strings.HasPrefix(typ, "MEDIUMINT"):
			oid = oidInt8
		case typ == "REAL":
			oid = oidFloat8
		case typ == "BLOB" || strings.HasPrefix(typ, "VARBINARY"):
			oid = oidBytea
		}
		fields = append(fields, field{row["name"].Text, oid})
	}
	m.tables[name] = fields
	return fields, nil
}
func (m *metadata) statement(stmt sql.Statement, outer map[string][]field, depth int) ([]field, error) {
	if depth > 64 {
		return nil, &protocolError{"54001", "metadata nesting limit exceeded"}
	}
	scope := make(map[string][]field, len(outer))
	for k, v := range outer {
		scope[k] = v
	}
	var with *sql.WithClause
	switch s := stmt.(type) {
	case *sql.ReplicationPositionStmt:
		if s.Operation == "wait" {
			m.hint(s.Position, oidText)
			m.hint(s.Timeout, oidText)
			return nil, nil
		}
		return []field{{"Position", oidText}}, nil
	case *sql.SelectStmt:
		with = s.With
	case *sql.UnionStmt:
		with = s.With
	case *sql.InsertStmt:
		with = s.With
	case *sql.UpdateStmt:
		with = s.With
	case *sql.DeleteStmt:
		with = s.With
	}
	if with != nil {
		if with.Recursive {
			return nil, &protocolError{"0A000", "recursive queries require the simple query protocol"}
		}
		for _, cte := range with.Entries {
			fields, err := m.statement(cte.Query, scope, depth+1)
			if err != nil {
				return nil, err
			}
			if len(cte.Columns) > 0 {
				if len(cte.Columns) != len(fields) {
					return nil, fmt.Errorf("CTE column count mismatch")
				}
				for i, name := range cte.Columns {
					fields[i].name = name
				}
			}
			scope[strings.ToLower(cte.Name)] = fields
		}
	}
	switch s := stmt.(type) {
	case *sql.SelectStmt:
		if s.Compound != nil {
			return m.statement(s.Compound, scope, depth+1)
		}
		base, err := m.table(s.From, scope, depth+1)
		if s.FromExpr != nil {
			if sub, ok := s.FromExpr.(*sql.SubqueryExpr); ok {
				base, err = m.statement(sub.Stmt, scope, depth+1)
			} else {
				return nil, &protocolError{"0A000", "unsupported extended query source"}
			}
		}
		if err != nil {
			return nil, err
		}
		env := make(map[string]uint32)
		addEnv(env, base, s.From, s.FromAlias)
		all := append([]field(nil), base...)
		for _, j := range s.Joins {
			var fields []field
			if j.Subquery != nil {
				fields, err = m.statement(j.Subquery, scope, depth+1)
			} else {
				fields, err = m.table(j.Table, scope, depth+1)
			}
			if err != nil {
				return nil, err
			}
			addEnv(env, fields, j.Table, j.Alias)
			for _, f := range fields {
				for _, existing := range all {
					if f.name == existing.name {
						f.name = j.Alias + "." + f.name
						break
					}
				}
				all = append(all, f)
			}
			m.expression(j.On, env)
		}
		m.expression(s.Where, env)
		m.expression(s.Having, env)
		for _, o := range s.OrderBy {
			m.expression(o.Expr, env)
		}
		if s.LimitParam > 0 {
			m.hintOccurrence(s.LimitParam-1, oidInt8)
		}
		if s.OffsetParam > 0 {
			m.hintOccurrence(s.OffsetParam-1, oidInt8)
		}
		if s.SelectAll {
			return all, nil
		}
		return m.project(s.Columns, all, env, false), nil
	case *sql.UnionStmt:
		left, err := m.statement(s.Left, scope, depth+1)
		if err != nil {
			return nil, err
		}
		right, err := m.statement(s.Right, scope, depth+1)
		if err != nil {
			return nil, err
		}
		if len(left) != len(right) {
			return nil, fmt.Errorf("compound column count mismatch")
		}
		for i := range left {
			if left[i].oid != right[i].oid {
				left[i].oid = oidText
			}
		}
		return left, nil
	case *sql.InsertStmt:
		fields, err := m.table(s.Table, scope, depth+1)
		if err != nil {
			return nil, err
		}
		names := s.ColumnNames
		if len(names) == 0 {
			for _, f := range fields {
				names = append(names, f.name)
			}
		}
		env := make(map[string]uint32)
		addEnv(env, fields, s.Table, "")
		groups := s.ValueGroups
		if len(groups) == 0 {
			groups = [][]sql.Expression{s.Values}
		}
		for _, row := range groups {
			for i, e := range row {
				if i < len(names) {
					m.hint(e, env[names[i]])
				}
				m.expression(e, env)
			}
		}
		if s.Select != nil {
			if _, err := m.statement(s.Select, scope, depth+1); err != nil {
				return nil, err
			}
		}
		return m.project(s.Returning, fields, env, true), nil
	case *sql.UpdateStmt:
		fields, err := m.table(s.Table, scope, depth+1)
		if err != nil {
			return nil, err
		}
		env := make(map[string]uint32)
		addEnv(env, fields, s.Table, "")
		for _, set := range s.Set {
			m.hint(set.Value, env[set.Column])
			m.expression(set.Value, env)
		}
		m.expression(s.Where, env)
		if s.LimitParam > 0 {
			m.hintOccurrence(s.LimitParam-1, oidInt8)
		}
		if s.OffsetParam > 0 {
			m.hintOccurrence(s.OffsetParam-1, oidInt8)
		}
		return m.project(s.Returning, fields, env, true), nil
	case *sql.DeleteStmt:
		fields, err := m.table(s.From, scope, depth+1)
		if err != nil {
			return nil, err
		}
		env := make(map[string]uint32)
		addEnv(env, fields, s.From, "")
		m.expression(s.Where, env)
		if s.LimitParam > 0 {
			m.hintOccurrence(s.LimitParam-1, oidInt8)
		}
		if s.OffsetParam > 0 {
			m.hintOccurrence(s.OffsetParam-1, oidInt8)
		}
		return m.project(s.Returning, fields, env, true), nil
	case *sql.ExplainStmt, *sql.ShowStmt, *sql.PragmaStmt:
		return nil, &protocolError{"0A000", "this introspection statement requires the simple query protocol"}
	default:
		return nil, nil
	}
}
func addEnv(env map[string]uint32, fields []field, table, alias string) {
	for _, f := range fields {
		if _, ok := env[f.name]; !ok {
			env[f.name] = f.oid
		}
		if table != "" {
			env[table+"."+f.name] = f.oid
		}
		if alias != "" {
			env[alias+"."+f.name] = f.oid
		}
	}
}
func (m *metadata) project(columns []sql.SelectColumn, all []field, env map[string]uint32, returning bool) []field {
	var out []field
	exprIndex := 0
	for i, col := range columns {
		if col.Star {
			out = append(out, all...)
			continue
		}
		name := col.Alias
		if name == "" {
			switch e := col.Expr.(type) {
			case *sql.ColumnRefExpr:
				name = e.Name
			case *sql.FuncCallExpr:
				if !returning {
					name = e.Name
				}
			}
			if name == "" {
				index := exprIndex
				if returning {
					index = i
				}
				name = fmt.Sprintf("expr%d", index)
			}
		}
		out = append(out, field{name, m.expression(col.Expr, env)})
		exprIndex++
	}
	return out
}
func (m *metadata) hintOccurrence(index int, oid uint32) {
	if oid == 0 || index < 0 || index >= len(m.prepared.positions) {
		return
	}
	param := m.prepared.positions[index]
	if m.prepared.params[param] == 0 {
		m.prepared.params[param] = oid
	}
}
func (m *metadata) hint(e sql.Expression, oid uint32) {
	if lit, ok := e.(*sql.LiteralExpr); ok && lit.BoundParameter {
		m.hintOccurrence(int(lit.Value.Int)-1, oid)
	}
}
func (m *metadata) expression(expr sql.Expression, env map[string]uint32) uint32 {
	switch e := expr.(type) {
	case *sql.LiteralExpr:
		if e.BoundParameter {
			index := int(e.Value.Int) - 1
			if index >= 0 && index < len(m.prepared.positions) {
				oid := m.prepared.params[m.prepared.positions[index]]
				if oid != 0 {
					return oid
				}
			}
			return oidText
		}
		if e.Value.Null {
			return oidText
		}
		return nativeOID(e.Value.Type)
	case *sql.ColumnRefExpr:
		if oid := env[e.Name]; oid != 0 {
			return oid
		}
		return oidText
	case *sql.UnaryExpr:
		return m.expression(e.Right, env)
	case *sql.BinaryExpr:
		left, right := m.expression(e.Left, env), m.expression(e.Right, env)
		if _, ok := e.Left.(*sql.ColumnRefExpr); ok {
			m.hint(e.Right, left)
		}
		if _, ok := e.Right.(*sql.ColumnRefExpr); ok {
			m.hint(e.Left, right)
		}
		if left != oidText {
			m.hint(e.Right, left)
		}
		if right != oidText {
			m.hint(e.Left, right)
		}
		left = m.currentType(e.Left, left)
		right = m.currentType(e.Right, right)
		switch strings.ToUpper(e.Op) {
		case "=", "!=", "<>", "<", ">", "<=", ">=", "IS", "IS NOT", "AND", "OR", "LIKE", "GLOB", "REGEXP":
			return oidInt8
		case "||", "->>":
			return oidText
		case "->":
			return oidJSON
		}
		if left == oidFloat8 || right == oidFloat8 || left == oidFloat4 || right == oidFloat4 {
			return oidFloat8
		}
		if left == oidNumeric || right == oidNumeric {
			return oidNumeric
		}
		if left == oidText || right == oidText {
			return oidText
		}
		return oidInt8
	case *sql.FuncCallExpr:
		name := strings.ToUpper(e.Name)
		if name == "CAST" && len(e.Args) == 2 {
			if typ, ok := e.Args[1].(*sql.LiteralExpr); ok {
				_, oid := typeName(typ.Value.Text)
				if oid != 0 {
					m.hint(e.Args[0], oid)
					return nativeOIDForCast(oid)
				}
			}
		}
		var types []uint32
		for _, arg := range e.Args {
			types = append(types, m.expression(arg, env))
		}

		args := make([]sql.ColumnDef, len(types))
		for i, oid := range types {
			args[i] = sql.PostgresTypeDefinition(oid)
		}
		if col, ok := sql.PostgresFunctionMetadata(name, args); ok {
			if oid := sql.PostgresColumnOID(col); oid != 0 {
				return oid
			}
			return nativeOID(col.ColType)
		}
		switch name {
		case "COUNT", "LENGTH", "INSTR", "CHANGES", "LAST_INSERT_ROWID", "ROW_NUMBER", "RANK", "DENSE_RANK", "NTILE":
			return oidInt8
		case "AVG":
			if len(types) > 0 && types[0] == oidNumeric {
				return oidNumeric
			}
			return oidFloat8
		case "TOTAL", "PERCENT_RANK", "CUME_DIST":
			return oidFloat8
		case "SUM", "MIN", "MAX", "ABS", "COALESCE", "IFNULL", "NULLIF", "LAG", "LEAD", "FIRST_VALUE", "LAST_VALUE", "NTH_VALUE":
			return commonType(types)
		}
		if strings.HasPrefix(name, "JSON_") || name == "JSON" || name == "JSONB" {
			switch name {
			case "JSON_UNQUOTE", "JSON_TYPE", "JSON_VALUE":
				return oidText
			case "JSON_VALID", "JSON_LENGTH", "JSON_DEPTH", "JSON_CONTAINS", "JSON_CONTAINS_PATH", "JSON_OVERLAPS":
				return oidInt8
			}
			return oidJSON
		}
	case *sql.InListExpr:
		oid := m.expression(e.Expr, env)
		for _, v := range e.List {
			m.hint(v, oid)
			m.expression(v, env)
		}
		return oidInt8
	case *sql.BetweenExpr:
		oid := m.expression(e.Expr, env)
		m.hint(e.Low, oid)
		m.hint(e.High, oid)
		return oidInt8
	case *sql.CaseExpr:
		var types []uint32
		for _, when := range e.Whens {
			m.expression(when.Cond, env)
			types = append(types, m.expression(when.Then, env))
		}
		if e.ElseExpr != nil {
			types = append(types, m.expression(e.ElseExpr, env))
		}
		return commonType(types)
	}
	return oidText
}
func nativeOIDForCast(oid uint32) uint32 {
	switch oid {
	case oidInt2, oidInt4:
		return oidInt8
	case oidFloat4:
		return oidFloat8
	}
	return oid
}
func commonType(types []uint32) uint32 {
	if len(types) == 0 {
		return oidText
	}
	oid := types[0]
	for _, other := range types {
		if other != oid {
			return oidText
		}
	}
	return oid
}

func (m *metadata) currentType(e sql.Expression, previous uint32) uint32 {
	if lit, ok := e.(*sql.LiteralExpr); ok && lit.BoundParameter {
		index := int(lit.Value.Int) - 1
		if index >= 0 && index < len(m.prepared.positions) {
			if oid := m.prepared.params[m.prepared.positions[index]]; oid != 0 {
				return oid
			}
		}
	}
	return previous
}
