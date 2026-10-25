package sql

import (
	"fmt"
	"strings"
)

func (s *Schema) renameTable(table, name string) error {
	if table == name {
		return nil
	}
	if s.isTemporaryTable(table) {
		return s.renameTemporaryTable(table, name)
	}
	info := s.tables[table]
	if s.tables[name] != nil || s.views[name] != nil {
		return fmt.Errorf("object %q already exists", name)
	}
	info.Name = name
	s.tables[name] = info
	delete(s.tables, table)
	for key, idx := range s.indexes {
		if idx.Table == table {
			if strings.HasPrefix(idx.Name, "\x00mysql-index:") {
				idx.Name = tableLocalIndexIdentity(name, idx.SQLName())
				delete(s.indexes, key)
				s.indexes[idx.Name] = idx
			}
			idx.Table = name
		}
	}
	if v, ok := s.stats[table]; ok {
		s.stats[name] = v
		delete(s.stats, table)
	}
	if v, ok := s.distributions[table]; ok {
		s.distributions[name] = v
		delete(s.distributions, table)
	}
	if v, ok := s.autoincMax[table]; ok {
		s.autoincMax[name] = v
		delete(s.autoincMax, table)
	}
	s.renameForeignKeys(table, name, "", "")
	if err := s.renameDependencies(table, name, "", ""); err != nil {
		return err
	}
	s.catalogLookup = nil
	if err := s.saveIndexes(); err != nil {
		return err
	}
	if err := s.saveViews(); err != nil {
		return err
	}
	if err := s.saveTriggers(); err != nil {
		return err
	}
	if err := s.saveAutoincMax(); err != nil {
		return err
	}
	if err := s.saveStats(); err != nil {
		return err
	}
	if err := s.saveDistributions(); err != nil {
		return err
	}
	return s.saveSchema()
}

func (s *Schema) renameDependencies(table, newTable, oldCol, newCol string) error {
	if s.isTemporaryTable(table) {
		return nil
	}
	op := "RENAME_TO"
	if oldCol != "" {
		op = "RENAME_COLUMN"
	}
	for _, view := range s.views {
		parsed, err := Parse(view.SQL)
		if err != nil {
			return err
		}
		if len(parsed) != 1 {
			return fmt.Errorf("invalid view definition")
		}
		sel := selectQuery(parsed[0])
		if sel == nil {
			return fmt.Errorf("invalid view definition")
		}
		s.preserveCTEViewOutputs(sel, table, newTable, oldCol, newCol, op)
		if renameViewSelect(sel, table, newTable, oldCol, newCol, op) {
			view.Parsed = sel
			view.SQL = formatSelectSQL(sel)
		}
	}
	for _, tr := range s.triggers {
		owner := tr.Table == table || tr.View == table
		if oldCol == "" {
			if tr.Table == table {
				tr.Table = newTable
			}
			if tr.View == table {
				tr.View = newTable
			}
		}
		if tr.When != "" {
			e, err := parseExpr(tr.When)
			if err != nil {
				return err
			}
			tr.When = formatExpr(renameDependencyExpr(e, table, newTable, oldCol, newCol, false, owner))
		}
		body := append([]string(nil), tr.Body...)
		for i, query := range body {
			parsed, err := Parse(query)
			if err != nil {
				return err
			}
			if len(parsed) != 1 {
				return fmt.Errorf("invalid trigger body")
			}
			if err = renameDependencyStatement(parsed[0], table, newTable, oldCol, newCol, owner); err != nil {
				return err
			}
			body[i], err = formatDMLStatement(parsed[0])
			if err != nil {
				return err
			}
		}
		tr.Body = body
	}
	return nil
}

func renameDependencyExpr(expr Expression, table, newTable, oldCol, newCol string, local, trigger bool) Expression {
	result, _ := mapExpression(expr, func(e Expression) (Expression, bool, error) {
		switch x := e.(type) {
		case *ColumnRefExpr:
			c := *x
			parts := strings.Split(c.Name, ".")
			if oldCol == "" {
				if len(parts) == 2 && parts[0] == table && (!trigger || parts[0] != "new" && parts[0] != "old") {
					c.Name = newTable + "." + parts[1]
				}
			} else if len(parts) == 1 && local && parts[0] == oldCol {
				c.Name = newCol
			} else if len(parts) == 2 && parts[1] == oldCol && (parts[0] == table || local && parts[0] == "excluded" || trigger && (parts[0] == "new" || parts[0] == "old")) {
				c.Name = parts[0] + "." + newCol
			}
			return &c, true, nil
		case *SubqueryExpr:
			renameViewSelect(x.Stmt, table, newTable, oldCol, newCol, renameOperation(oldCol))
			return x, true, nil
		case *ExistsExpr:
			renameViewSelect(x.Stmt, table, newTable, oldCol, newCol, renameOperation(oldCol))
			return x, true, nil
		}
		return e, false, nil
	})
	return result
}
func renameOperation(column string) string {
	if column != "" {
		return "RENAME_COLUMN"
	}
	return "RENAME_TO"
}

func renameDependencyStatement(stmt Statement, table, newTable, oldCol, newCol string, trigger bool) error {
	if renameJoinedMutation(stmt, table, newTable, oldCol, newCol, trigger) {
		return nil
	}
	local := false
	var with *WithClause
	switch x := stmt.(type) {
	case *SelectStmt:
		renameViewSelect(x, table, newTable, oldCol, newCol, renameOperation(oldCol))
		if trigger {
			rewriteSelectTriggerRefs(x, table, newTable, oldCol, newCol)
		}
		return nil
	case *UnionStmt:
		renameCTEQuery(x, table, newTable, oldCol, newCol, renameOperation(oldCol))
		return nil
	case *InsertStmt:
		local = x.Table == table
		with = x.With
	case *UpdateStmt:
		local = x.Table == table
		with = x.With
	case *DeleteStmt:
		local = x.From == table
		with = x.With
	default:
		return fmt.Errorf("unsupported stored statement %T", stmt)
	}
	if with != nil {
		for _, cte := range with.Entries {
			if cte.Name == table {
				return fmt.Errorf("rename of trigger DML with shadowing CTE is ambiguous")
			}
			renameCTEQuery(cte.Query, table, newTable, oldCol, newCol, renameOperation(oldCol))
		}
	}
	rewrite := func(e Expression) Expression {
		return renameDependencyExpr(e, table, newTable, oldCol, newCol, local, trigger)
	}
	returning := func(cols []SelectColumn) {
		for i := range cols {
			cols[i].Expr = rewrite(cols[i].Expr)
		}
	}
	switch x := stmt.(type) {
	case *InsertStmt:
		if local {
			if oldCol == "" {
				x.Table = newTable
			} else {
				for i, c := range x.ColumnNames {
					if c == oldCol {
						x.ColumnNames[i] = newCol
					}
				}
			}
		}
		for i, e := range x.Values {
			x.Values[i] = rewrite(e)
		}
		for _, group := range x.ValueGroups {
			for i, e := range group {
				group[i] = rewrite(e)
			}
		}
		if x.Select != nil {
			renameViewSelect(x.Select, table, newTable, oldCol, newCol, renameOperation(oldCol))
			if trigger {
				rewriteSelectTriggerRefs(x.Select, table, newTable, oldCol, newCol)
			}
		}
		if c := x.OnConflict; c != nil {
			if local && oldCol != "" {
				for i, n := range c.Target {
					if n == oldCol {
						c.Target[i] = newCol
					}
				}
			}
			for i := range c.Set {
				if local && c.Set[i].Column == oldCol {
					c.Set[i].Column = newCol
				}
				c.Set[i].Value = rewrite(c.Set[i].Value)
			}
			c.Where = rewrite(c.Where)
		}
		returning(x.Returning)
	case *UpdateStmt:
		if local && oldCol == "" {
			x.Table = newTable
		}
		for i := range x.Set {
			if local && x.Set[i].Column == oldCol {
				x.Set[i].Column = newCol
			}
			x.Set[i].Value = rewrite(x.Set[i].Value)
		}
		x.Where = rewrite(x.Where)
		returning(x.Returning)
	case *DeleteStmt:
		if local && oldCol == "" {
			x.From = newTable
		}
		x.Where = rewrite(x.Where)
		returning(x.Returning)
	}
	return nil
}

func rewriteSelectTriggerRefs(q *SelectStmt, table, newTable, oldCol, newCol string) {
	rewrite := func(e Expression) Expression {
		return renameDependencyExpr(e, table, newTable, oldCol, newCol, false, true)
	}
	for i := range q.Columns {
		q.Columns[i].Expr = rewrite(q.Columns[i].Expr)
	}
	q.Where = rewrite(q.Where)
	q.Having = rewrite(q.Having)
	for i := range q.GroupBy {
		q.GroupBy[i] = rewrite(q.GroupBy[i])
	}
	for i := range q.OrderBy {
		q.OrderBy[i].Expr = rewrite(q.OrderBy[i].Expr)
	}
	for i := range q.Joins {
		q.Joins[i].On = rewrite(q.Joins[i].On)
		if q.Joins[i].Subquery != nil {
			rewriteSelectTriggerRefs(q.Joins[i].Subquery, table, newTable, oldCol, newCol)
		}
	}
}

func formatDMLStatement(stmt Statement) (string, error) {
	var b strings.Builder
	exprs := func(values []Expression) string {
		a := make([]string, len(values))
		for i, e := range values {
			a[i] = formatExpr(e)
		}
		return strings.Join(a, ", ")
	}
	sets := func(values []UpdateSet) string {
		a := make([]string, len(values))
		for i, e := range values {
			name := quoteIdent(e.Column)
			if qualifier, col, ok := strings.Cut(e.Column, "."); ok {
				name = quoteIdent(qualifier) + "." + quoteIdent(col)
			}
			a[i] = name + " = " + formatExpr(e.Value)
		}
		return strings.Join(a, ", ")
	}
	finish := func(where Expression, limit, offset int64, cols []SelectColumn) {
		if where != nil {
			b.WriteString(" WHERE " + formatExpr(where))
		}
		if limit >= 0 || offset > 0 {
			fmt.Fprintf(&b, " LIMIT %d", limit)
			if offset > 0 {
				fmt.Fprintf(&b, " OFFSET %d", offset)
			}
		}
		if len(cols) > 0 {
			a := make([]string, len(cols))
			for i, c := range cols {
				if c.Star {
					a[i] = "*"
				} else {
					a[i] = formatExpr(c.Expr)
				}
				if c.Alias != "" {
					a[i] += " AS " + quoteIdent(c.Alias)
				}
			}
			b.WriteString(" RETURNING " + strings.Join(a, ", "))
		}
	}
	switch x := stmt.(type) {
	case *SelectStmt:
		return formatSelectSQL(x), nil
	case *UnionStmt:
		return formatCTEQuery(x), nil
	case *InsertStmt:
		b.WriteString(formatWithSQL(x.With))
		if x.Replace {
			b.WriteString("REPLACE INTO ")
		} else {
			b.WriteString("INSERT INTO ")
		}
		b.WriteString(quoteIdent(x.Table))
		if len(x.ColumnNames) > 0 {
			a := make([]string, len(x.ColumnNames))
			for i, n := range x.ColumnNames {
				a[i] = quoteIdent(n)
			}
			b.WriteString(" (" + strings.Join(a, ", ") + ")")
		}
		if x.DefaultValues {
			b.WriteString(" DEFAULT VALUES")
		} else if x.Select != nil {
			b.WriteString(" " + formatSelectSQL(x.Select))
		} else {
			groups := x.ValueGroups
			if len(groups) == 0 {
				groups = [][]Expression{x.Values}
			}
			a := make([]string, len(groups))
			for i, g := range groups {
				a[i] = "(" + exprs(g) + ")"
			}
			b.WriteString(" VALUES " + strings.Join(a, ", "))
		}
		if c := x.OnConflict; c != nil {
			b.WriteString(" ON CONFLICT")
			if len(c.Target) > 0 {
				a := make([]string, len(c.Target))
				for i, n := range c.Target {
					a[i] = quoteIdent(n)
				}
				b.WriteString(" (" + strings.Join(a, ", ") + ")")
			}
			if c.DoNothing {
				b.WriteString(" DO NOTHING")
			} else {
				b.WriteString(" DO UPDATE SET " + sets(c.Set))
				if c.Where != nil {
					b.WriteString(" WHERE " + formatExpr(c.Where))
				}
			}
		}
		finish(nil, -1, 0, x.Returning)
	case *UpdateStmt:
		b.WriteString(formatWithSQL(x.With) + "UPDATE " + mysqlMutationFromSQL(x.Table, x.Alias, x.Joins) + " SET " + sets(x.Set))
		finish(x.Where, x.Limit, x.Offset, x.Returning)
	case *DeleteStmt:
		b.WriteString(formatWithSQL(x.With) + "DELETE ")
		if len(x.Targets) > 0 {
			for i, target := range x.Targets {
				if i > 0 {
					b.WriteString(", ")
				}
				b.WriteString(quoteIdent(target))
			}
			b.WriteString(" ")
		}
		b.WriteString("FROM " + mysqlMutationFromSQL(x.From, x.Alias, x.Joins))
		finish(x.Where, x.Limit, x.Offset, x.Returning)
	default:
		return "", fmt.Errorf("cannot format stored statement %T", stmt)
	}
	return b.String(), nil
}
