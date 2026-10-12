package sql

import "strings"

// A dependency follows physical relations through nested expressions, compound
// SELECTs and lexical CTE scopes. An empty column means any use of the relation.
func queryDependsOnColumn(query Statement, table, column string, outer map[string]bool, inherited map[string]bool) bool {
	if query == nil {
		return false
	}
	hidden := make(map[string]bool, len(outer))
	for name, v := range outer {
		hidden[name] = v
	}
	var with *WithClause
	switch q := query.(type) {
	case *SelectStmt:
		if q == nil {
			return false
		}
		with = q.With
	case *UnionStmt:
		if q == nil {
			return false
		}
		with = q.With
	}
	if with != nil {
		for _, cte := range with.Entries {
			if with.Recursive {
				hidden[cte.Name] = true
			}
			if queryDependsOnColumn(cte.Query, table, column, hidden, inherited) {
				return true
			}
			hidden[cte.Name] = true
		}
	}
	if q, ok := query.(*UnionStmt); ok {
		return queryDependsOnColumn(q.Left, table, column, hidden, inherited) || queryDependsOnColumn(q.Right, table, column, hidden, inherited)
	}
	q, ok := query.(*SelectStmt)
	if !ok {
		return false
	}
	if q.Compound != nil && queryDependsOnColumn(q.Compound, table, column, hidden, inherited) {
		return true
	}
	aliases := make(map[string]bool, len(inherited)+len(q.Joins)+1)
	for name, v := range inherited {
		aliases[name] = v
	}
	local := false
	add := func(name, alias string) {
		if name == "" {
			return
		}
		if alias == "" {
			alias = name
		}
		match := name == table && !hidden[name]
		aliases[alias] = match
		local = local || match
	}
	add(q.From, q.FromAlias)
	for _, j := range q.Joins {
		add(j.Table, j.Alias)
	}
	if column == "" && local {
		return true
	}
	expr := func(e Expression) bool {
		found := false
		_ = walkExpression(e, func(e Expression) (bool, error) {
			switch x := e.(type) {
			case *SubqueryExpr:
				found = found || queryDependsOnColumn(x.Stmt, table, column, hidden, aliases)
			case *ExistsExpr:
				found = found || queryDependsOnColumn(x.Stmt, table, column, hidden, aliases)
			case *ColumnRefExpr:
				parts := strings.Split(x.Name, ".")
				if len(parts) == 1 {
					found = found || (local && (parts[0] == column || parts[0] == "*"))
				} else {
					found = found || (aliases[parts[len(parts)-2]] && (parts[len(parts)-1] == column || parts[len(parts)-1] == "*"))
				}
			}
			return found, nil
		})
		return found
	}
	if expr(q.FromExpr) || expr(q.Where) || expr(q.Having) {
		return true
	}
	for _, c := range q.Columns {
		if (c.Star && local) || expr(c.Expr) {
			return true
		}
	}
	for _, e := range q.GroupBy {
		if expr(e) {
			return true
		}
	}
	for _, e := range q.OrderBy {
		if expr(e.Expr) {
			return true
		}
	}
	for _, w := range q.Windows {
		for _, e := range windowExpressions(w.Spec) {
			if expr(e) {
				return true
			}
		}
	}
	for _, j := range q.Joins {
		if queryDependsOnColumn(j.Subquery, table, column, hidden, nil) || expr(j.On) {
			return true
		}
		for _, c := range j.Using {
			if local && c == column {
				return true
			}
		}
	}
	return false
}

func triggerExprDepends(expr Expression, table, column string, local, context bool) bool {
	found := false
	_ = walkExpression(expr, func(e Expression) (bool, error) {
		switch x := e.(type) {
		case *ColumnRefExpr:
			parts := strings.Split(x.Name, ".")
			if parts[len(parts)-1] == column {
				found = found || (len(parts) == 1 && local) || (len(parts) > 1 && (parts[len(parts)-2] == table || (context && (parts[len(parts)-2] == "old" || parts[len(parts)-2] == "new"))))
			}
		case *SubqueryExpr:
			found = found || queryDependsOnColumn(x.Stmt, table, column, nil, nil)
		case *ExistsExpr:
			found = found || queryDependsOnColumn(x.Stmt, table, column, nil, nil)
		}
		return found, nil
	})
	return found
}

func triggerStatementDepends(stmt Statement, table, column string, context bool) bool {
	var with *WithClause
	var expressions []Expression
	var returning []SelectColumn
	local := false
	switch q := stmt.(type) {
	case *SelectStmt:
		if q == nil {
			return false
		}
		if queryDependsOnColumn(q, table, column, nil, nil) {
			return true
		}
		expressions = append(expressions, q.Where, q.Having)
		for _, c := range q.Columns {
			expressions = append(expressions, c.Expr)
		}
		for _, j := range q.Joins {
			expressions = append(expressions, j.On)
			if triggerStatementDepends(j.Subquery, table, column, context) {
				return true
			}
		}
		for _, e := range q.OrderBy {
			expressions = append(expressions, e.Expr)
		}
		expressions = append(expressions, q.GroupBy...)
		with = q.With
		if q.Compound != nil && triggerStatementDepends(q.Compound, table, column, context) {
			return true
		}
	case *UnionStmt:
		if q == nil {
			return false
		}
		return triggerStatementDepends(q.Left, table, column, context) || triggerStatementDepends(q.Right, table, column, context)
	case *InsertStmt:
		local = q.Table == table
		if local && (len(q.ColumnNames) == 0 || containsColumn(q.ColumnNames, column)) {
			return true
		}
		with, returning = q.With, q.Returning
		expressions = append(expressions, q.Values...)
		for _, g := range q.ValueGroups {
			expressions = append(expressions, g...)
		}
		if q.Select != nil && triggerStatementDepends(q.Select, table, column, context) {
			return true
		}
		if q.OnConflict != nil {
			if local && containsColumn(q.OnConflict.Target, column) {
				return true
			}
			for _, set := range q.OnConflict.Set {
				if local && set.Column == column {
					return true
				}
				expressions = append(expressions, set.Value)
			}
		}
	case *UpdateStmt:
		local = q.Table == table
		with, returning = q.With, q.Returning
		expressions = append(expressions, q.Where)
		for _, set := range q.Set {
			if local && set.Column == column {
				return true
			}
			expressions = append(expressions, set.Value)
		}
	case *DeleteStmt:
		local = q.From == table
		with, returning = q.With, q.Returning
		expressions = append(expressions, q.Where)
	default:
		return false
	}
	if with != nil {
		for _, cte := range with.Entries {
			if triggerStatementDepends(cte.Query, table, column, context) {
				return true
			}
		}
	}
	for _, c := range returning {
		if c.Star && local {
			return true
		}
		expressions = append(expressions, c.Expr)
	}
	for _, e := range expressions {
		if triggerExprDepends(e, table, column, local, context) {
			return true
		}
	}
	return false
}

func containsColumn(columns []string, column string) bool {
	for _, c := range columns {
		if c == column {
			return true
		}
	}
	return false
}
