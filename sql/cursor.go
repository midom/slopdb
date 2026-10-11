package sql

import (
	"fmt"
	"strings"

	"slopdb/storage"
)

// RowCursor incrementally evaluates a simple SELECT at the caller's statement
// snapshot. The caller must keep the statement alive until Close. It is not
// safe for concurrent calls.
type RowCursor struct {
	schema           *Schema
	stmt             *SelectStmt
	info             *TableInfo
	columns          []ColumnDef
	expressions      []selectExprCol
	iterator         storage.Iterator
	reverse          bool
	skipped, emitted int64
	closed           bool
}

// OpenRowCursor returns handled=false for operators which need the ordinary
// executor (joins, grouping, sorting, DISTINCT, views or subqueries). No rows
// are fetched while opening an eligible cursor.
func (s *Schema) OpenRowCursor(stmt *SelectStmt) (*RowCursor, bool, error) {
	if s.cteScope != nil || stmt.With != nil || stmt.Compound != nil || stmt.materialized != nil {
		return nil, false, nil
	}
	info := s.tables[strings.ToLower(stmt.From)]
	if info == nil || stmt.FromExpr != nil || stmt.Virtual != nil || len(stmt.Joins) != 0 || stmt.Lock != nil || stmt.Distinct || len(stmt.GroupBy) != 0 || stmt.Having != nil || hasAggregates(stmt) {
		return nil, false, nil
	}
	if !cursorExpressionSafe(s, stmt.Where) {
		return nil, false, nil
	}
	for _, col := range stmt.Columns {
		if !cursorExpressionSafe(s, col.Expr) {
			return nil, false, nil
		}
	}
	reverse := false
	if len(stmt.OrderBy) > 0 {
		if len(stmt.OrderBy) > len(info.PKColumns) {
			return nil, false, nil
		}
		reverse = stmt.OrderBy[0].Desc
		for i, order := range stmt.OrderBy {
			ref, ok := order.Expr.(*ColumnRefExpr)
			if !ok || coveringColumn(ref.Name, stmt.From, stmt.FromAlias) != info.PKColumns[i] || order.Desc != reverse {
				return nil, false, nil
			}
		}
	}
	if stmt.prepared != nil {
		defer s.restorePreparedPlan(s.usePreparedPlan(stmt.prepared, info))
	}
	if err := s.validateQuery(stmt, info); err != nil {
		return nil, true, err
	}
	c := &RowCursor{schema: s, stmt: stmt, info: info, reverse: reverse}
	if stmt.SelectAll {
		c.columns = append([]ColumnDef(nil), info.Columns...)
	} else {
		expressionIndex := 0
		for _, col := range stmt.Columns {
			if col.Star {
				c.columns = append(c.columns, info.Columns...)
				continue
			}
			name := col.Alias
			if name == "" {
				switch e := col.Expr.(type) {
				case *ColumnRefExpr:
					name = e.Name
				case *FuncCallExpr:
					name = e.Name
				default:
					name = fmt.Sprintf("expr%d", expressionIndex)
				}
			}
			expressionIndex++
			c.columns = append(c.columns, ColumnDef{Name: name})
			c.expressions = append(c.expressions, selectExprCol{name: name, expr: col.Expr})
		}
	}
	prefix := []byte(tableDataPrefix(info.Name))
	lower, upper := prefix, prefixSuccessor(prefix)
	// Reuse exact clustered tuple bounds without allowing a secondary index to
	// change the requested order. Multiple intervals remain a streaming scan.
	if key, found := s.primaryKeyLookup(stmt.Where, info.Name, info); found {
		lower = key
		upper = append(append([]byte{}, key...), 0)
	} else {
		constraints := tupleConstraints(stmt.Where, info.Name, stmt.FromAlias, info)
		if plan := buildTuplePlan(stmt, info, nil, constraints); plan != nil && len(plan.intervals) == 1 {
			lower, upper = plan.intervals[0].lower, plan.intervals[0].upper
		}
	}
	c.iterator = s.newRangeIterator(lower, upper, reverse)
	return c, true, nil
}
func (c *RowCursor) Columns() []ColumnDef {
	columns := append([]ColumnDef(nil), c.columns...)
	for i := range columns {
		columns[i].Default = nil
		columns[i].Check = nil
		columns[i].RefColumns = append([]string(nil), columns[i].RefColumns...)
	}
	return columns
}
func (c *RowCursor) Next() (map[string]Value, bool, error) {
	if c.closed {
		return nil, false, nil
	}
	if err := c.schema.checkInterrupt(); err != nil {
		c.Close()
		return nil, false, err
	}
	if c.stmt.Limit >= 0 && c.emitted >= c.stmt.Limit {
		c.Close()
		return nil, false, nil
	}
	for c.iterator.Valid() {
		if err := c.schema.checkInterrupt(); err != nil {
			c.Close()
			return nil, false, err
		}
		row := decodeRow(c.iterator.Value(), c.info)
		if c.reverse {
			c.iterator.Prev()
		} else {
			c.iterator.Next()
		}
		if needsQualifiedRows(c.stmt) {
			qualifyAccessRow(row, c.stmt.From, c.stmt.FromAlias)
		}
		if c.stmt.Where != nil {
			matches, err := c.schema.evalCondition(c.stmt.Where, row)
			if err != nil {
				c.Close()
				return nil, false, err
			}
			if !matches {
				continue
			}
		}
		if c.skipped < c.stmt.Offset {
			c.skipped++
			continue
		}
		out := make(map[string]Value, len(c.columns))
		for _, col := range c.columns {
			out[col.Name] = row[col.Name]
		}
		for _, expr := range c.expressions {
			value, err := c.schema.evalExpr(expr.expr, row)
			if err != nil {
				c.Close()
				return nil, false, err
			}
			out[expr.name] = value
		}
		c.emitted++
		return out, true, nil
	}
	err := storage.IteratorError(c.iterator)
	c.Close()
	return nil, false, err
}
func (c *RowCursor) Close() {
	if !c.closed {
		c.closed = true
		if c.iterator != nil {
			c.iterator.Close()
		}
		c.schema.clearSelectCaches()
	}
}

func cursorExpressionSafe(s *Schema, expression Expression) bool {
	_, safe := s.expressionDependencies(expression)
	if !safe {
		return false
	}
	_ = walkExpression(expression, func(e Expression) (bool, error) {
		switch e.(type) {
		case *SubqueryExpr, *ExistsExpr:
			safe = false
			return true, nil
		}
		return false, nil
	})
	return safe
}
