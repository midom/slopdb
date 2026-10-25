package sql

import (
	"fmt"
	"strings"
)

// Rewrite only this query's expression tree; subqueries have their own scope.
func mapExpression(expr Expression, visit func(Expression) (Expression, bool, error)) (Expression, error) {
	if expr == nil {
		return nil, nil
	}
	result, stop, err := visit(expr)
	if err != nil || stop {
		return result, err
	}
	rewrite := func(e Expression) (Expression, error) { return mapExpression(e, visit) }
	switch e := expr.(type) {
	case *BinaryExpr:
		c := *e
		c.Left, err = rewrite(e.Left)
		if err == nil {
			c.Right, err = rewrite(e.Right)
		}
		return &c, err
	case *UnaryExpr:
		c := *e
		c.Right, err = rewrite(e.Right)
		return &c, err
	case *FuncCallExpr:
		c := *e
		c.Args = append([]Expression(nil), e.Args...)
		for i, a := range c.Args {
			c.Args[i], err = rewrite(a)
			if err != nil {
				return nil, err
			}
		}
		c.GroupConcatSeparator, err = rewrite(e.GroupConcatSeparator)
		if err != nil {
			return nil, err
		}
		c.GroupConcatOrder = append([]OrderByClause(nil), e.GroupConcatOrder...)
		for i, order := range c.GroupConcatOrder {
			c.GroupConcatOrder[i].Expr, err = rewrite(order.Expr)
			if err != nil {
				return nil, err
			}
		}
		c.Window, err = mapWindowSpec(e.Window, rewrite)
		return &c, err
	case *InListExpr:
		c := *e
		c.Expr, err = rewrite(e.Expr)
		if err != nil {
			return nil, err
		}
		c.List = append([]Expression(nil), e.List...)
		for i, a := range c.List {
			c.List[i], err = rewrite(a)
			if err != nil {
				return nil, err
			}
		}
		return &c, nil
	case *BetweenExpr:
		c := *e
		c.Expr, err = rewrite(e.Expr)
		if err == nil {
			c.Low, err = rewrite(e.Low)
		}
		if err == nil {
			c.High, err = rewrite(e.High)
		}
		return &c, err
	case *CaseExpr:
		c := *e
		c.Expr, err = rewrite(e.Expr)
		if err != nil {
			return nil, err
		}
		c.Whens = append([]CaseWhen(nil), e.Whens...)
		for i, w := range c.Whens {
			c.Whens[i].Cond, err = rewrite(w.Cond)
			if err == nil {
				c.Whens[i].Then, err = rewrite(w.Then)
			}
			if err != nil {
				return nil, err
			}
		}
		c.ElseExpr, err = rewrite(e.ElseExpr)
		return &c, err
	}
	return expr, nil
}

func validateAggregateExpr(expr Expression, allow bool) error {
	return walkExpression(expr, func(e Expression) (bool, error) {
		if col, ok := e.(*ColumnRefExpr); ok && col.Name == "*" {
			return true, fmt.Errorf("star is only allowed in COUNT(*)")
		}
		if f, ok := e.(*FuncCallExpr); ok {
			if f.Window != nil {
				if !allow {
					return true, fmt.Errorf("window function is not allowed in this context")
				}
				return true, validateWindowCall(f)
			}
			if !isAggregate(f) {
				if err := validateScalarFunction(f); err != nil {
					return true, err
				}
			}
			if f.Distinct && (!isAggregate(f) || len(f.Args) != 1 && !f.MySQLGroupConcat) {
				return true, fmt.Errorf("DISTINCT requires a single aggregate argument")
			}
			if isAggregate(f) {
				if !allow {
					return true, fmt.Errorf("aggregate %s is not allowed in this context", f.Name)
				}
				if len(f.Args) == 0 || !f.MySQLGroupConcat && len(f.Args) > 1 && (!strings.EqualFold(f.Name, "GROUP_CONCAT") || len(f.Args) > 2) {
					return true, fmt.Errorf("wrong number of arguments to %s", f.Name)
				}
				for _, order := range f.GroupConcatOrder {
					if err := validateAggregateExpr(order.Expr, false); err != nil {
						return true, err
					}
				}
				if err := validateAggregateExpr(f.GroupConcatSeparator, false); err != nil {
					return true, err
				}
				for _, arg := range f.Args {
					if c, ok := arg.(*ColumnRefExpr); ok && c.Name == "*" {
						if !strings.EqualFold(f.Name, "COUNT") || f.Distinct {
							return true, fmt.Errorf("invalid aggregate star argument")
						}
						continue
					}
					if err := validateAggregateExpr(arg, false); err != nil {
						return true, err
					}
				}
				return true, nil
			}
		}
		return false, nil
	})
}

func (s *Schema) validateGrouping(stmt *SelectStmt, info *TableInfo, columns []ColumnDef) error {
	stmt.GroupBy = append([]Expression(nil), stmt.GroupBy...)
	for i, g := range stmt.GroupBy {
		if literal, ok := g.(*LiteralExpr); ok && !literal.BoundParameter && !literal.Value.Null && literal.Value.Type == ColInteger {
			ordinal := literal.Value.Int
			if ordinal < 1 || ordinal > int64(len(columns)) {
				return fmt.Errorf("GROUP BY position out of range")
			}
			if !stmt.SelectAll && len(stmt.Columns) == len(columns) {
				stmt.GroupBy[i] = stmt.Columns[ordinal-1].Expr
			} else {
				stmt.GroupBy[i] = &ColumnRefExpr{Name: columns[ordinal-1].Name}
			}
		}
		if col, ok := stmt.GroupBy[i].(*ColumnRefExpr); ok {
			input := false
			if info != nil {
				for _, def := range info.Columns {
					input = input || def.Name == col.Name
				}
			}
			if input {
				continue
			}
			for _, selected := range stmt.Columns {
				if selected.Alias != "" && selected.Alias == col.Name {
					stmt.GroupBy[i] = selected.Expr
					break
				}
			}
		}
	}

	if err := validateAggregateExpr(stmt.Where, false); err != nil {
		return err
	}
	for _, j := range stmt.Joins {
		if err := validateAggregateExpr(j.On, false); err != nil {
			return err
		}
	}
	for _, g := range stmt.GroupBy {
		if err := validateAggregateExpr(g, false); err != nil {
			return err
		}
	}
	for _, c := range stmt.Columns {
		if err := validateAggregateExpr(c.Expr, true); err != nil {
			return err
		}
	}
	if err := validateAggregateExpr(stmt.Having, true); err != nil {
		return err
	}
	for _, o := range stmt.OrderBy {
		if err := validateAggregateExpr(o.Expr, true); err != nil {
			return err
		}
	}
	if !hasAggregates(stmt) && len(stmt.GroupBy) == 0 && stmt.Having == nil {
		return nil
	}
	aliases := make(map[string]Expression)
	for _, c := range stmt.Columns {
		if c.Alias != "" {
			aliases[c.Alias] = c.Expr
		}
	}
	normalize := func(e Expression) string {
		mapped, _ := mapExpression(e, func(a Expression) (Expression, bool, error) {
			if c, ok := a.(*ColumnRefExpr); ok {
				n := coveringColumn(c.Name, stmt.From, stmt.FromAlias)
				if n != "" {
					return &ColumnRefExpr{Name: n}, true, nil
				}
			}
			return a, false, nil
		})
		return formatExpr(mapped)
	}
	grouped := make(map[string]bool)
	for _, g := range stmt.GroupBy {
		grouped[normalize(g)] = true
	}
	dependent := info != nil && len(info.PKColumns) > 0
	if dependent {
		for _, pk := range info.PKColumns {
			dependent = dependent && grouped[normalize(&ColumnRefExpr{Name: pk})]
		}
	}
	var check func(Expression, bool) error
	check = func(e Expression, allowAlias bool) error {
		_, err := mapExpression(e, func(a Expression) (Expression, bool, error) {
			if grouped[normalize(a)] {
				return a, true, nil
			}
			if f, ok := a.(*FuncCallExpr); ok && isAggregate(f) {
				return a, true, nil
			}
			if c, ok := a.(*ColumnRefExpr); ok {
				if allowAlias {
					if alias := aliases[c.Name]; alias != nil {
						return a, true, check(alias, false)
					}
				}
				baseColumn := coveringColumn(c.Name, stmt.From, stmt.FromAlias)
				baseQualified := len(stmt.Joins) == 0 || strings.Contains(c.Name, ".") || s.unambiguousGroupedBaseColumn(stmt, c.Name)
				baseExists := false
				if info != nil {
					for _, column := range info.Columns {
						baseExists = baseExists || column.Name == baseColumn
					}
				}
				if dependent && baseQualified && baseExists {
					return a, true, nil
				}
				return nil, true, fmt.Errorf("column %s must appear in GROUP BY or an aggregate", c.Name)
			}
			return a, false, nil
		})
		return err
	}
	for _, c := range stmt.Columns {
		if c.Star {
			for _, col := range columns {
				if err := check(&ColumnRefExpr{Name: col.Name}, false); err != nil {
					return err
				}
			}
		} else if err := check(c.Expr, false); err != nil {
			return err
		}
	}
	if stmt.SelectAll {
		for _, col := range columns {
			if err := check(&ColumnRefExpr{Name: col.Name}, false); err != nil {
				return err
			}
		}
	}
	if err := check(stmt.Having, true); err != nil {
		return err
	}
	for _, o := range stmt.OrderBy {
		if err := check(o.Expr, true); err != nil {
			return err
		}
	}
	return nil
}

// Validation and aggregate discovery must not allocate copies of a hot query's
// AST. Rewriting is reserved for expressions that actually need replacement.
func walkExpression(expr Expression, visit func(Expression) (bool, error)) error {
	if expr == nil {
		return nil
	}
	stop, err := visit(expr)
	if stop || err != nil {
		return err
	}
	walk := func(e Expression) error { return walkExpression(e, visit) }
	switch e := expr.(type) {
	case *BinaryExpr:
		if err := walk(e.Left); err != nil {
			return err
		}
		return walk(e.Right)
	case *UnaryExpr:
		return walk(e.Right)
	case *FuncCallExpr:
		for _, a := range e.Args {
			if err := walk(a); err != nil {
				return err
			}
		}
		if err := walk(e.GroupConcatSeparator); err != nil {
			return err
		}
		for _, order := range e.GroupConcatOrder {
			if err := walk(order.Expr); err != nil {
				return err
			}
		}
		for _, a := range windowExpressions(e.Window) {
			if err := walk(a); err != nil {
				return err
			}
		}
	case *InListExpr:
		if err := walk(e.Expr); err != nil {
			return err
		}
		for _, a := range e.List {
			if err := walk(a); err != nil {
				return err
			}
		}
	case *BetweenExpr:
		for _, a := range []Expression{e.Expr, e.Low, e.High} {
			if err := walk(a); err != nil {
				return err
			}
		}
	case *CaseExpr:
		if err := walk(e.Expr); err != nil {
			return err
		}
		for _, w := range e.Whens {
			if err := walk(w.Cond); err != nil {
				return err
			}
			if err := walk(w.Then); err != nil {
				return err
			}
		}
		return walk(e.ElseExpr)
	}
	return nil
}

// An unqualified dependent column must belong solely to the base relation.
// Qualified references remain available for derived or unresolved join inputs.
func (s *Schema) unambiguousGroupedBaseColumn(stmt *SelectStmt, name string) bool {
	for _, join := range stmt.Joins {
		if join.Subquery != nil {
			return false
		}
		info := s.tables[join.Table]
		if info == nil {
			return false
		}
		for _, column := range info.Columns {
			if column.Name == name {
				return false
			}
		}
	}
	return true
}
