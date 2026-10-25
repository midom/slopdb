package sql

import (
	"fmt"
	"strings"
)

// A grouped joined table is materialized as one join input. Its inner joins
// must complete before an outer LEFT JOIN decides whether to extend with NULL.
// Keep qualified names rather than turning it into a derived SELECT *, which
// would otherwise hide the original tables and collapse duplicate names.
func (s *Schema) joinGroupColumns(stmt *SelectStmt) ([]ColumnDef, error) {
	var result []ColumnDef
	add := func(table, alias string, sub *SelectStmt) error {
		var defs []ColumnDef
		if sub != nil {
			if sub.JoinGroup {
				var err error
				defs, err = s.joinGroupColumns(sub)
				if err != nil {
					return err
				}
			} else {
				defs = s.cteQueryColumnDefs(sub, cteScopeColumns(s.cteScope))
			}
		} else if info := s.tables[table]; info != nil {
			defs = info.Columns
		} else if view := s.views[table]; view != nil {
			defs = s.viewColumnDefs(view)
		} else {
			return fmt.Errorf("table %q does not exist", table)
		}
		if alias == "" {
			alias = table
		}
		for _, col := range defs {
			if !strings.Contains(col.Name, ".") && alias != "" {
				col.Name = alias + "." + col.Name
			}
			result = append(result, col)
		}
		return nil
	}
	var sub *SelectStmt
	if expr, ok := stmt.FromExpr.(*SubqueryExpr); ok {
		sub = expr.Stmt
	}
	if err := add(stmt.From, stmt.FromAlias, sub); err != nil {
		return nil, err
	}
	for _, join := range stmt.Joins {
		if err := add(join.Table, join.Alias, join.Subquery); err != nil {
			return nil, err
		}
	}
	return result, nil
}

func (s *Schema) selectJoinGroup(stmt *SelectStmt) (*ResultSet, error) {
	columns, err := s.joinGroupColumns(stmt)
	if err != nil {
		return nil, err
	}
	base := JoinClause{Table: stmt.From, Alias: stmt.FromAlias}
	if expr, ok := stmt.FromExpr.(*SubqueryExpr); ok {
		base.Subquery = expr.Stmt
	}
	rows, _, keys, err := s.getJoinRows(base, stmt.trackRows, nil)
	if err != nil {
		return nil, err
	}
	for _, row := range rows {
		qualifyRow(row, stmt.From)
		qualifyRow(row, stmt.FromAlias)
	}
	// SelectAll makes pruning retain every column, including every source's
	// qualified name. The enclosing query applies its own WHERE afterward.
	rows, keys, err = s.executeJoins(stmt, rows, keys)
	if err != nil {
		return nil, err
	}
	return &ResultSet{Columns: columns, Rows: rows, rowKeys: keys}, nil
}
