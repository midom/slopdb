package sql

import (
	"errors"
	"fmt"
	"sort"
)

type subqueryMemo struct {
	dependencies []string
	safe         bool
	results      map[string]*ResultSet
	resultRows   int
}

type subqueryMembership struct {
	values  map[string]bool
	hasNull bool
}

// Resolve names lexically, including references inside nested query blocks.
// Unknown catalogs and derived tables conservatively disable memoization.
func (s *Schema) subqueryDependencies(stmt *SelectStmt) ([]string, bool) {
	// Catalog-only lexical analysis cannot distinguish a persistent table
	// from a CTE shadowing it. Such scopes also change between recursion steps.
	if s.cteScope != nil {
		return nil, false
	}
	free := make(map[string]bool)
	safe := true
	depth := 0
	var query func(*SelectStmt, map[string]bool)
	query = func(q *SelectStmt, enclosing map[string]bool) {
		depth++
		defer func() { depth-- }()
		if depth > 100 {
			safe = false
			return
		}
		if q.With != nil || q.Compound != nil || q.materialized != nil || q.Lock != nil || q.trackRows || q.Virtual != nil {
			safe = false
		}
		names := make(map[string]bool)
		for n := range enclosing {
			names[n] = true
		}
		addTable := func(table, alias string) {
			if table == "" {
				return
			}
			info := s.tables[table]
			if info == nil {
				if view := s.views[table]; view != nil {
					query(view.Parsed, nil)
					info = &TableInfo{Columns: s.viewColumnDefs(view)}
				} else {
					safe = false
					return
				}
			}
			if alias == "" {
				alias = table
			}
			for _, c := range info.Columns {
				names[c.Name] = true
				names[alias+"."+c.Name] = true
			}
		}
		addTable(q.From, q.FromAlias)
		if q.FromExpr != nil {
			safe = false
		}
		for _, j := range q.Joins {
			addTable(j.Table, j.Alias)
			if j.Subquery != nil {
				safe = false
			}
		}
		// Output aliases are local in grouping, ordering, and HAVING.
		aliases := make(map[string]bool)
		for _, c := range q.Columns {
			if c.Alias != "" {
				aliases[c.Alias] = true
			}
		}
		visit := func(expr Expression, allowAlias bool) {
			_ = walkExpression(expr, func(e Expression) (bool, error) {
				switch e := e.(type) {
				case *ColumnRefExpr:
					if e.Name != "*" && !names[e.Name] && !(allowAlias && aliases[e.Name]) {
						free[e.Name] = true
					}
				case *FuncCallExpr:
					if !deterministicFunction(e.Name) {
						safe = false
					}
				case *SubqueryExpr:
					query(e.Stmt, names)
					return true, nil
				case *ExistsExpr:
					query(e.Stmt, names)
					return true, nil
				}
				return false, nil
			})
		}
		for _, c := range q.Columns {
			visit(c.Expr, false)
		}
		visit(q.Where, false)
		for _, j := range q.Joins {
			visit(j.On, false)
		}
		for _, g := range q.GroupBy {
			visit(g, true)
		}
		for _, o := range q.OrderBy {
			visit(o.Expr, true)
		}
		visit(q.Having, true)
	}
	query(stmt, nil)
	names := make([]string, 0, len(free))
	for name := range free {
		names = append(names, name)
	}
	sort.Strings(names)
	return names, safe
}

// Bind exact typed outer comparisons for the existing physical index plans.
// Mixed-type comparisons keep their column semantics and use the fallback.
func (s *Schema) bindCorrelatedWhere(q *SelectStmt, outer map[string]Value) *SelectStmt {
	if hasWindows(q) || s.cteScope != nil || q.With != nil || q.Compound != nil || q.materialized != nil || q.FromExpr != nil {
		return q
	}
	info := s.tables[q.From]
	if info == nil {
		return q
	}
	local := make(map[string]ColumnType)
	alias := q.FromAlias
	if alias == "" {
		alias = q.From
	}
	for _, c := range info.Columns {
		local[c.Name] = c.ColType
		local[alias+"."+c.Name] = c.ColType
	}
	for _, j := range q.Joins {
		info := s.tables[j.Table]
		if info == nil || j.Subquery != nil {
			return q
		}
		alias := j.Alias
		if alias == "" {
			alias = j.Table
		}
		for _, c := range info.Columns {
			local[c.Name] = c.ColType
			local[alias+"."+c.Name] = c.ColType
		}
	}
	changed := false
	where, _ := mapExpression(q.Where, func(e Expression) (Expression, bool, error) {
		b, ok := e.(*BinaryExpr)
		if !ok {
			return e, false, nil
		}
		switch b.Op {
		case "=", "<>", "<", ">", "<=", ">=":
		default:
			return e, false, nil
		}
		a, aOK := b.Left.(*ColumnRefExpr)
		c, cOK := b.Right.(*ColumnRefExpr)
		if !aOK || !cOK {
			return e, false, nil
		}
		copy := *b
		if typ, ok := local[a.Name]; ok {
			if _, localRef := local[c.Name]; !localRef {
				if v, ok := outer[c.Name]; ok && !v.Null && v.Type == typ {
					copy.Right = &LiteralExpr{Value: v, BoundParameter: true}
					changed = true
					return &copy, true, nil
				}
			}
		}
		if typ, ok := local[c.Name]; ok {
			if _, localRef := local[a.Name]; !localRef {
				if v, ok := outer[a.Name]; ok && !v.Null && v.Type == typ {
					copy.Left = &LiteralExpr{Value: v, BoundParameter: true}
					changed = true
					return &copy, true, nil
				}
			}
		}
		return e, false, nil
	})
	if !changed {
		return q
	}
	copy := *q
	copy.Where = where
	return &copy
}

func (s *Schema) executeSubquery(sub *SelectStmt, outer map[string]Value) (*ResultSet, error) {
	return s.executeSubqueryMode(sub, outer, false)
}

func (s *Schema) executeSubqueryMode(sub *SelectStmt, outer map[string]Value, exists bool) (*ResultSet, error) {
	if s.selectDepth == 0 {
		defer s.clearSelectCaches()
	}
	prev := s.outerRow
	if len(prev) > 0 {
		merged := make(map[string]Value, len(prev)+len(outer))
		for k, v := range prev {
			merged[k] = v
		}
		for k, v := range outer {
			merged[k] = v
		}
		outer = merged
	}
	var memo *subqueryMemo
	var key string
	if s.selectDepth > 0 {
		memos := &s.subqueryMemos
		if exists {
			memos = &s.existsMemos
		}
		if *memos == nil {
			*memos = make(map[*SelectStmt]*subqueryMemo)
		}
		memo = (*memos)[sub]
		if memo == nil {
			deps, safe := s.subqueryDependencies(sub)
			memo = &subqueryMemo{dependencies: deps, safe: safe, results: make(map[string]*ResultSet)}
			(*memos)[sub] = memo
		}
		if memo.safe {
			var bytes []byte
			for _, name := range memo.dependencies {
				v, ok := outer[name]
				if !ok {
					memo = nil
					break
				}
				// Preserve type: correlated literals can apply column affinity differently.
				bytes = appendValue(bytes, v)
			}
			key = string(bytes)
			if memo != nil {
				if rs := memo.results[key]; rs != nil {
					return rs, nil
				}
			}
		} else {
			memo = nil
		}
	}
	s.outerRow = outer
	defer func() { s.outerRow = prev }()
	bound := s.bindCorrelatedWhere(sub, outer)
	var rs *ResultSet
	var err error
	if exists {
		var used bool
		rs, used, err = s.readExists(bound)
		if !used && err == nil {
			rs, err = s.Select(bound)
		}
	} else {
		rs, err = s.Select(bound)
	}
	if err == nil && memo != nil && len(memo.results) < 4096 && (len(memo.dependencies) == 0 || memo.resultRows+len(rs.Rows) <= 8192) {
		memo.results[key] = rs
		memo.resultRows += len(rs.Rows)
	}
	return rs, err
}

func (s *Schema) clearSelectCaches() {
	// Detach ownership before close, which otherwise prunes this slice while
	// ranging it and could skip stores still open after errors/cancellation.
	disks := s.queryDisks
	s.queryDisks = nil
	for _, disk := range disks {
		disk.close()
	}
	clear(disks)
	s.subqueryMemos = nil
	s.existsMemos = nil
	s.subquerySets = nil
	s.selectValues = nil
	s.selectValueBytes = 0
	s.likePatterns = nil
	s.fulltextQueries = nil
	s.fulltextBindings = nil
	s.fulltextQueryBytes = 0
}

// EXISTS ignores projection values and stops after the first qualifying row,
// while preserving offsets, empty inputs, aggregate semantics, and errors.
func (s *Schema) readExists(q *SelectStmt) (*ResultSet, bool, error) {
	if hasWindows(q) || s.cteScope != nil || q.With != nil || q.Compound != nil || q.materialized != nil || q.FromExpr != nil || q.Virtual != nil || q.Lock != nil || q.trackRows || q.Distinct || hasAggregates(q) || len(q.GroupBy) > 0 || q.Having != nil || len(q.OrderBy) > 0 {
		return nil, false, nil
	}
	info := s.tables[q.From]
	if info == nil {
		return nil, false, nil
	}
	if _, safe := s.expressionDependencies(q.Where); !safe {
		return nil, false, nil
	}
	if err := s.validateQuery(q, info); err != nil {
		return nil, true, err
	}
	// Projection values are unused, but their names/functions still resolve.
	validation := *q
	if err := validateGrouping(&validation, info, nil); err != nil {
		return nil, true, err
	}
	if s.selectDepth >= 100 {
		return nil, true, fmt.Errorf("query nesting limit exceeded")
	}
	s.selectDepth++
	defer func() { s.selectDepth-- }()
	rs := &ResultSet{Columns: []ColumnDef{{Name: "exists"}}}
	if q.Limit == 0 {
		return rs, true, nil
	}
	var found bool
	var matched int64
	accept := func(row map[string]Value) error {
		matched++
		if matched <= q.Offset {
			return nil
		}
		found = true
		return errAccessStop
	}
	var err error
	if len(q.Joins) > 0 {
		plan := s.makeJoinPlan(q)
		if plan == nil {
			return nil, false, nil
		}
		err = s.walkJoinPlan(plan, accept)
	} else {
		needed := make(map[string]bool)
		for _, name := range expressionNames(q.Where) {
			if col := coveringColumn(name, q.From, q.FromAlias); col != "" {
				needed[col] = true
			}
		}
		where, _ := mapExpression(q.Where, func(e Expression) (Expression, bool, error) {
			switch e := e.(type) {
			case *ColumnRefExpr:
				if name := coveringColumn(e.Name, q.From, q.FromAlias); name != "" {
					return &ColumnRefExpr{Name: name}, true, nil
				}
			case *SubqueryExpr, *ExistsExpr:
				return e, true, nil
			}
			return e, false, nil
		})
		err = s.walkAccess(info, needed, s.accessPaths(where, info), func(row map[string]Value) error {
			qualifyAccessRow(row, q.From, q.FromAlias)
			if q.Where != nil {
				ok, err := s.evalCondition(q.Where, row)
				if err != nil {
					return err
				}
				if !ok {
					return nil
				}
			}
			return accept(row)
		})
	}
	if errors.Is(err, errAccessStop) {
		err = nil
	}
	if found {
		rs.Rows = []map[string]Value{{"exists": NewIntValue(1)}}
	}
	return rs, true, err
}

func (s *Schema) membership(rs *ResultSet) *subqueryMembership {
	if m := s.subquerySets[rs]; m != nil {
		return m
	}
	m := &subqueryMembership{values: make(map[string]bool, len(rs.Rows))}
	for _, row := range rs.Rows {
		v := row[rs.Columns[0].Name]
		if v.Null {
			m.hasNull = true
		} else {
			m.values[string(s.appendEqualityKey(nil, v))] = true
		}
	}
	if s.selectDepth > 0 && len(s.subquerySets) < 4096 {
		if s.subquerySets == nil {
			s.subquerySets = make(map[*ResultSet]*subqueryMembership)
		}
		s.subquerySets[rs] = m
	}
	return m
}

func typedIndexEquality(expr Expression, info *TableInfo) bool {
	if info == nil {
		return false
	}
	b, ok := expr.(*BinaryExpr)
	if !ok || b.Op != "=" {
		return false
	}
	c, cOK := b.Left.(*ColumnRefExpr)
	l, lOK := b.Right.(*LiteralExpr)
	if !cOK || !lOK {
		c, cOK = b.Right.(*ColumnRefExpr)
		l, lOK = b.Left.(*LiteralExpr)
	}
	if !cOK || !lOK || l.Value.Null {
		return false
	}
	for _, def := range info.Columns {
		if c.Name == def.Name {
			return l.Value.Type == def.ColType
		}
	}
	return false
}

func (s *Schema) hasSimpleIndexEquality(stmt *SelectStmt, info *TableInfo) bool {
	for _, term := range andTerms(stmt.Where) {
		if !typedIndexEquality(term, info) {
			continue
		}
		b := term.(*BinaryExpr)
		c, ok := b.Left.(*ColumnRefExpr)
		if !ok {
			c = b.Right.(*ColumnRefExpr)
		}
		for _, idx := range s.tableIndexes(stmt.From) {
			if idx.Column == c.Name && len(idx.Columns) <= 1 {
				return true
			}
		}
	}
	return false
}
