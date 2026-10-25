package sql

import (
	"fmt"
	"strings"
)

// Configurable int64 guardrails bound runaway UNION ALL recursion. Context
// cancellation is checked in every iteration and emitted-row deduplication.

func copyCTEResult(rs *ResultSet) *ResultSet {
	out := &ResultSet{Columns: append([]ColumnDef(nil), rs.Columns...), Rows: make([]map[string]Value, len(rs.Rows))}
	for i, row := range rs.Rows {
		out.Rows[i] = copyRow(row)
	}
	return out
}

func (s *Schema) resolveCTETables(stmt *SelectStmt) (*SelectStmt, error) {
	if len(s.cteScope) == 0 {
		return stmt, nil
	}
	cp := *stmt
	resolve := func(name string) (*SelectStmt, error) {
		rs, exists := s.cteScope[strings.ToLower(name)]
		if !exists {
			return nil, nil
		}
		if rs == nil {
			return nil, fmt.Errorf("CTE %q has a forward or unsupported recursive reference", name)
		}
		return &SelectStmt{materialized: rs, Limit: -1}, nil
	}
	if cp.FromExpr == nil && cp.From != "" {
		sub, err := resolve(cp.From)
		if err != nil {
			return nil, err
		}
		if sub != nil {
			if cp.Lock != nil {
				return nil, fmt.Errorf("locking reads of CTEs are not supported")
			}
			if cp.FromAlias == "" {
				cp.FromAlias = cp.From
			}
			cp.From = ""
			cp.FromExpr = &SubqueryExpr{Stmt: sub}
		}
	}
	cp.Joins = append([]JoinClause(nil), stmt.Joins...)
	for i := range cp.Joins {
		jc := &cp.Joins[i]
		if jc.Subquery != nil {
			continue
		}
		sub, err := resolve(jc.Table)
		if err != nil {
			return nil, err
		}
		if sub != nil {
			if cp.Lock != nil {
				return nil, fmt.Errorf("locking reads of CTEs are not supported")
			}
			if jc.Alias == "" {
				jc.Alias = jc.Table
			}
			jc.Table = ""
			jc.Subquery = sub
		}
	}
	return &cp, nil
}

func (s *Schema) enterCTEs(w *WithClause) (func(), error) {
	previous := s.cteScope
	scope := make(map[string]*ResultSet, len(previous)+len(w.Entries))
	for name, rs := range previous {
		scope[name] = rs
	}
	for _, c := range w.Entries {
		scope[c.Name] = nil
	}
	s.cteScope = scope
	restore := func() { s.cteScope = previous }
	for _, c := range w.Entries {
		if err := s.checkInterrupt(); err != nil {
			restore()
			return nil, err
		}
		var rs *ResultSet
		var err error
		refs := cteReferences(c.Query, c.Name)
		if refs > 0 {
			if !w.Recursive {
				restore()
				return nil, fmt.Errorf("recursive CTE %q requires WITH RECURSIVE", c.Name)
			}
			rs, err = s.recursiveCTE(c)
		} else {
			rs, err = s.execUnionRight(c.Query)
			if err == nil {
				rs, err = renameCTEColumns(rs, c.Columns)
			}
		}
		if err != nil {
			restore()
			return nil, fmt.Errorf("CTE %q: %w", c.Name, err)
		}
		scope[c.Name] = rs
	}
	return restore, nil
}

func renameCTEColumns(rs *ResultSet, names []string) (*ResultSet, error) {
	if len(names) != 0 && len(names) != len(rs.Columns) {
		return nil, fmt.Errorf("column count mismatch: declared %d, query returned %d", len(names), len(rs.Columns))
	}
	out := &ResultSet{Columns: append([]ColumnDef(nil), rs.Columns...), Rows: make([]map[string]Value, len(rs.Rows))}
	seen := make(map[string]bool)
	for i := range out.Columns {
		if len(names) > 0 {
			out.Columns[i].Name = names[i]
		}
		name := strings.ToLower(out.Columns[i].Name)
		if seen[name] {
			return nil, fmt.Errorf("ambiguous CTE output column %q; provide explicit column names", name)
		}
		seen[name] = true
		out.Columns[i].Name = name
	}
	for i, row := range rs.Rows {
		mapped := make(map[string]Value, len(out.Columns))
		for j, col := range rs.Columns {
			mapped[out.Columns[j].Name] = row[col.Name]
		}
		out.Rows[i] = mapped
	}
	return out, nil
}

func validateRecursiveCTE(c CommonTableExpr) (*UnionStmt, *SelectStmt, error) {
	compound, ok := c.Query.(*UnionStmt)
	if !ok || compound.Op != "UNION" || compound.With != nil {
		return nil, nil, fmt.Errorf("recursive CTE requires one anchor UNION [ALL] one recursive SELECT")
	}
	step, ok := compound.Right.(*SelectStmt)
	if !ok || step.With != nil || step.Compound != nil || cteReferences(compound.Left, c.Name) != 0 || cteReferences(step, c.Name) != 1 || directCTEReferences(step, c.Name) != 1 {
		return nil, nil, fmt.Errorf("recursive CTE requires exactly one direct recursive table reference and a nonrecursive anchor")
	}
	if len(compound.OrderBy) > 0 || compound.Limit >= 0 || compound.Offset != 0 || step.Distinct || hasWindows(step) || hasAggregates(step) || len(step.GroupBy) > 0 || step.Having != nil || len(step.OrderBy) > 0 || step.Limit >= 0 || step.Offset != 0 || step.Lock != nil {
		return nil, nil, fmt.Errorf("recursive CTE aggregation, windows, ordering, DISTINCT, locking and inner LIMIT/OFFSET are not supported")
	}
	for _, j := range step.Joins {
		if j.Type != "INNER" && j.Type != "CROSS" {
			return nil, nil, fmt.Errorf("recursive outer joins are not supported")
		}
	}
	return compound, step, nil
}

func (s *Schema) recursiveCTE(c CommonTableExpr) (*ResultSet, error) {
	compound, step, err := validateRecursiveCTE(c)
	if err != nil {
		return nil, err
	}
	anchor, err := s.Select(compound.Left)
	if err != nil {
		return nil, err
	}
	result, err := renameCTEColumns(anchor, c.Columns)
	if err != nil {
		return nil, err
	}
	seen := make(map[string]bool)
	filter := func(rows []map[string]Value) ([]map[string]Value, error) {
		out := make([]map[string]Value, 0, len(rows))
		for _, row := range rows {
			if err := s.checkInterrupt(); err != nil {
				return nil, err
			}
			if !compound.All {
				key := s.rowKey(row)
				if seen[key] {
					continue
				}
				seen[key] = true
			}
			out = append(out, row)
		}
		return out, nil
	}
	result.Rows, err = filter(result.Rows)
	if err != nil {
		return nil, err
	}
	delta := &ResultSet{Columns: result.Columns, Rows: result.Rows}
	for iteration := int64(0); len(delta.Rows) > 0; iteration++ {
		if err := s.checkInterrupt(); err != nil {
			return nil, err
		}
		if s.maxCTEIterations > 0 && iteration >= s.maxCTEIterations {
			return nil, fmt.Errorf("recursive CTE iteration limit (%d) exceeded", s.maxCTEIterations)
		}
		if s.maxCTERows > 0 && int64(len(result.Rows)) > s.maxCTERows {
			return nil, fmt.Errorf("recursive CTE row limit (%d) exceeded", s.maxCTERows)
		}
		s.cteScope[c.Name] = delta
		// A recursive term must observe this iteration's frontier, never a memo from
		// the preceding iteration (including correlated subquery membership caches).
		s.clearSelectCaches()
		next, err := s.Select(step)
		if err != nil {
			return nil, err
		}
		if len(next.Columns) != len(result.Columns) {
			return nil, fmt.Errorf("recursive term column count mismatch")
		}
		next, err = renameCTEColumns(next, cteColumnNames(result.Columns))
		if err != nil {
			return nil, err
		}
		next.Rows, err = filter(next.Rows)
		if err != nil {
			return nil, err
		}
		if s.maxCTERows > 0 && int64(len(next.Rows)) > s.maxCTERows-int64(len(result.Rows)) {
			return nil, fmt.Errorf("recursive CTE row limit (%d) exceeded", s.maxCTERows)
		}
		result.Rows = append(result.Rows, next.Rows...)
		delta = next
	}
	return result, nil
}
func cteColumnNames(cols []ColumnDef) []string {
	out := make([]string, len(cols))
	for i, c := range cols {
		out[i] = c.Name
	}
	return out
}
func directCTEReferences(sel *SelectStmt, name string) int {
	n := 0
	if strings.EqualFold(sel.From, name) {
		n++
	}
	for _, j := range sel.Joins {
		if strings.EqualFold(j.Table, name) {
			n++
		}
	}
	return n
}

// References respect lexical WITH shadowing. References in nested queries are
// counted so unsupported recursive subquery forms fail before execution.
func cteReferences(query Statement, name string) int {
	var countQuery func(Statement) int
	countQuery = func(query Statement) int {
		var with *WithClause
		switch q := query.(type) {
		case *SelectStmt:
			with = q.With
		case *UnionStmt:
			with = q.With
		}
		if with != nil {
			for _, c := range with.Entries {
				if strings.EqualFold(c.Name, name) {
					return 0
				}
			}
		}
		count := 0
		if with != nil {
			for _, c := range with.Entries {
				count += countQuery(c.Query)
			}
		}
		expr := func(e Expression) {
			_ = walkExpression(e, func(e Expression) (bool, error) {
				switch x := e.(type) {
				case *SubqueryExpr:
					count += countQuery(x.Stmt)
					return true, nil
				case *ExistsExpr:
					count += countQuery(x.Stmt)
					return true, nil
				}
				return false, nil
			})
		}
		switch q := query.(type) {
		case *SelectStmt:
			count += directCTEReferences(q, name)
			if q.Compound != nil {
				count += countQuery(q.Compound)
			}
			expr(q.FromExpr)
			expr(q.Where)
			expr(q.Having)
			for _, c := range q.Columns {
				expr(c.Expr)
			}
			for _, e := range q.GroupBy {
				expr(e)
			}
			for _, o := range q.OrderBy {
				expr(o.Expr)
			}
			for _, j := range q.Joins {
				if j.Subquery != nil {
					count += countQuery(j.Subquery)
				}
				expr(j.On)
			}
		case *UnionStmt:
			count += countQuery(q.Left) + countQuery(q.Right)
			for _, o := range q.OrderBy {
				expr(o.Expr)
			}
		}
		return count
	}
	return countQuery(query)
}

// Infer view metadata without executing CTE expressions or touching storage.
// Recursive outputs take their names from the anchor, as compound SELECT does.
func (s *Schema) cteQueryColumnDefs(query Statement, outer map[string][]ColumnDef) []ColumnDef {
	var with *WithClause
	switch q := query.(type) {
	case *SelectStmt:
		with = q.With
	case *UnionStmt:
		with = q.With
	}
	names := make(map[string][]ColumnDef, len(outer))
	for name, cols := range outer {
		names[name] = cols
	}
	if with != nil {
		for _, c := range with.Entries {
			cols := s.cteQueryColumnDefs(c.Query, names)
			if len(c.Columns) > 0 {
				cols = make([]ColumnDef, len(c.Columns))
				for i, name := range c.Columns {
					cols[i] = ColumnDef{Name: name}
				}
			}
			names[c.Name] = cols
		}
	}
	switch q := query.(type) {
	case *UnionStmt:
		return s.cteQueryColumnDefs(q.Left, names)
	case *SelectStmt:
		if q.JoinGroup {
			cols, _ := s.joinGroupColumns(q)
			return cols
		}
		if q.materialized != nil {
			return q.materialized.Columns
		}
		if q.Compound != nil {
			return s.cteQueryColumnDefs(q.Compound, names)
		}
		tableCols := func(name string) []ColumnDef {
			if cols, ok := names[strings.ToLower(name)]; ok {
				return cols
			}
			if t := s.tables[strings.ToLower(name)]; t != nil {
				return t.Columns
			}
			if v := s.views[strings.ToLower(name)]; v != nil {
				return s.viewColumnDefs(v)
			}
			return nil
		}
		var out []ColumnDef
		base := tableCols(q.From)
		if sub, ok := q.FromExpr.(*SubqueryExpr); ok {
			base = s.cteQueryColumnDefs(sub.Stmt, names)
		}
		for i, c := range q.Columns {
			if c.Star {
				out = append(out, base...)
				for _, j := range q.Joins {
					cols := tableCols(j.Table)
					if j.Subquery != nil {
						cols = s.cteQueryColumnDefs(j.Subquery, names)
					}
					out = append(out, cols...)
				}
				continue
			}
			name := c.Alias
			if name == "" {
				switch e := c.Expr.(type) {
				case *ColumnRefExpr:
					name = e.Name
				case *FuncCallExpr:
					name = e.Name
				default:
					name = fmt.Sprintf("expr%d", i)
				}
			}
			out = append(out, ColumnDef{Name: name})
		}
		return out
	}
	return nil
}

// EXPLAIN resolves names and output metadata without evaluating expressions or
// running recursive iteration. EXPLAIN ANALYZE uses the actual materializer.
func (s *Schema) enterCTEMetadata(w *WithClause) (func(), error) {
	previous := s.cteScope
	scope := make(map[string]*ResultSet, len(previous)+len(w.Entries))
	columns := make(map[string][]ColumnDef, len(previous)+len(w.Entries))
	for name, rs := range previous {
		scope[name] = rs
		if rs != nil {
			columns[name] = rs.Columns
		}
	}
	for _, c := range w.Entries {
		scope[c.Name] = nil
	}
	s.cteScope = scope
	restore := func() { s.cteScope = previous }
	for _, c := range w.Entries {
		if cteReferences(c.Query, c.Name) > 0 {
			if !w.Recursive {
				restore()
				return nil, fmt.Errorf("recursive CTE %q requires WITH RECURSIVE", c.Name)
			}
			if _, _, err := validateRecursiveCTE(c); err != nil {
				restore()
				return nil, err
			}
		}
		cols := s.cteQueryColumnDefs(c.Query, columns)
		result, err := renameCTEColumns(&ResultSet{Columns: cols}, c.Columns)
		if err != nil {
			restore()
			return nil, err
		}
		scope[c.Name] = result
		columns[c.Name] = result.Columns
	}
	return restore, nil
}

func renameCTEQuery(query Statement, table, newTable, oldCol, newCol, operation string) bool {
	switch q := query.(type) {
	case *SelectStmt:
		return renameViewSelect(q, table, newTable, oldCol, newCol, operation)
	case *UnionStmt:
		if q.With != nil {
			wrapper := selectQuery(q)
			changed := renameViewSelect(wrapper, table, newTable, oldCol, newCol, operation)
			*q = *wrapper.Compound
			q.With = wrapper.With
			return changed
		}
		changed := renameViewSelect(q.Left, table, newTable, oldCol, newCol, operation)
		if renameCTEQuery(q.Right, table, newTable, oldCol, newCol, operation) {
			changed = true
		}
		return changed
	}
	return false
}

// Preserve the lexical output names consumed by dependent CTEs when ALTER
// renames underlying catalog columns. The query expressions are rewritten, but
// explicit CTE output names keep references in outer query blocks valid.
func (s *Schema) preserveCTEViewOutputs(query Statement, table, newTable, oldCol, newCol, operation string) {
	original := &Schema{tables: make(map[string]*TableInfo, len(s.tables)), views: s.views}
	for name, info := range s.tables {
		original.tables[name] = info
	}
	info := s.tables[table]
	if operation == "RENAME_TO" {
		info = s.tables[newTable]
	}
	if info != nil {
		cp := *info
		cp.Columns = append([]ColumnDef(nil), info.Columns...)
		if operation == "RENAME_COLUMN" {
			for i := range cp.Columns {
				if cp.Columns[i].Name == newCol {
					cp.Columns[i].Name = oldCol
				}
			}
		}
		original.tables[table] = &cp
	}
	var visit func(Statement, map[string][]ColumnDef)
	visit = func(query Statement, outer map[string][]ColumnDef) {
		var with *WithClause
		switch q := query.(type) {
		case *SelectStmt:
			with = q.With
		case *UnionStmt:
			with = q.With
		}
		scope := make(map[string][]ColumnDef, len(outer))
		for n, cols := range outer {
			scope[n] = cols
		}
		if with != nil {
			for i := range with.Entries {
				c := &with.Entries[i]
				visit(c.Query, scope)
				cols := original.cteQueryColumnDefs(c.Query, scope)
				if len(c.Columns) == 0 && len(cols) > 0 {
					c.Columns = cteColumnNames(cols)
				}
				if len(c.Columns) > 0 {
					cols = make([]ColumnDef, len(c.Columns))
					for i, n := range c.Columns {
						cols[i] = ColumnDef{Name: n}
					}
				}
				scope[c.Name] = cols
			}
		}
		expr := func(e Expression) {
			_ = walkExpression(e, func(e Expression) (bool, error) {
				switch x := e.(type) {
				case *SubqueryExpr:
					visit(x.Stmt, scope)
					return true, nil
				case *ExistsExpr:
					visit(x.Stmt, scope)
					return true, nil
				}
				return false, nil
			})
		}
		switch q := query.(type) {
		case *UnionStmt:
			visit(q.Left, scope)
			visit(q.Right, scope)
		case *SelectStmt:
			if q.Compound != nil {
				visit(q.Compound, scope)
			}
			if operation == "RENAME_COLUMN" {
				if sub, ok := q.FromExpr.(*SubqueryExpr); ok {
					original.preserveDerivedOutputNames(sub.Stmt, scope)
				}
				for _, j := range q.Joins {
					if j.Subquery != nil {
						original.preserveDerivedOutputNames(j.Subquery, scope)
					}
				}
			}
			expr(q.FromExpr)
			expr(q.Where)
			expr(q.Having)
			for _, c := range q.Columns {
				expr(c.Expr)
			}
			for _, o := range q.OrderBy {
				expr(o.Expr)
			}
			for _, g := range q.GroupBy {
				expr(g)
			}
			for _, j := range q.Joins {
				if j.Subquery != nil {
					visit(j.Subquery, scope)
				}
				expr(j.On)
			}
		}
	}
	visit(query, nil)
}

func cteScopeColumns(scope map[string]*ResultSet) map[string][]ColumnDef {
	cols := make(map[string][]ColumnDef, len(scope))
	for name, rs := range scope {
		if rs != nil {
			cols[name] = rs.Columns
		}
	}
	return cols
}

// Derived-table outputs are lexical bindings just like named CTE columns. Keep
// these names stable while rewriting the underlying catalog column references.
func (s *Schema) preserveDerivedOutputNames(q *SelectStmt, scope map[string][]ColumnDef) {
	if q == nil {
		return
	}
	if q.Compound != nil {
		s.preserveDerivedOutputNames(q.Compound.Left, scope)
		return
	}
	defs := s.cteQueryColumnDefs(q, scope)
	var columns []SelectColumn
	position := 0
	seenNames := make(map[string]bool)
	appendSource := func(name, alias string, sub *SelectStmt, using []string, natural bool) {
		var source []ColumnDef
		if sub != nil {
			source = s.cteQueryColumnDefs(sub, scope)
		} else if cols, ok := scope[name]; ok {
			source = cols
		} else if info := s.tables[name]; info != nil {
			source = info.Columns
		} else if view := s.views[name]; view != nil {
			source = s.viewColumnDefs(view)
		}
		if alias == "" {
			alias = name
		}
		for _, col := range source {
			if containsColumn(using, col.Name) || natural && seenNames[col.Name] {
				continue
			}
			seenNames[col.Name] = true
			ref := col.Name
			if alias != "" {
				ref = alias + "." + col.Name
			}
			columns = append(columns, SelectColumn{Expr: &ColumnRefExpr{Name: ref}, Alias: col.Name})
			position++
		}
	}
	for _, col := range q.Columns {
		if col.Star {
			var sub *SelectStmt
			if e, ok := q.FromExpr.(*SubqueryExpr); ok {
				sub = e.Stmt
			}
			appendSource(q.From, q.FromAlias, sub, nil, false)
			for _, j := range q.Joins {
				appendSource(j.Table, j.Alias, j.Subquery, j.Using, j.Type == "NATURAL")
			}
			continue
		}
		if col.Alias == "" && position < len(defs) {
			col.Alias = defs[position].Name
		}
		position++
		columns = append(columns, col)
	}
	q.Columns = columns
	q.SelectAll = false
}
