package sql

import (
	"bytes"
	"sort"
	"strings"

	"slopdb/storage"
)

// RowCursor incrementally evaluates a simple SELECT at the caller's statement
// snapshot. The caller must keep the statement alive until Close. It is not
// safe for concurrent calls.
type RowCursor struct {
	fulltext         bool
	fulltextOpened   bool
	schema           *Schema
	stmt             *SelectStmt
	info             *TableInfo
	columns          []ColumnDef
	expressions      []selectExprCol
	iterator         storage.Iterator
	intervals        []coveringInterval
	position         int
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
	if hasWindows(stmt) {
		return nil, false, nil
	}

	info := s.tables[strings.ToLower(stmt.From)]
	if info == nil || stmt.FromExpr != nil || stmt.Virtual != nil || len(stmt.Joins) != 0 || stmt.Lock != nil || stmt.Distinct || len(stmt.GroupBy) != 0 || stmt.Having != nil || hasAggregates(stmt) {
		return nil, false, nil
	}
	expandedStatement, err := s.expandQualifiedStars(stmt, nil, 0)
	if err != nil {
		return nil, true, err
	}
	stmt = expandedStatement
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
			if !ok || cursorOrderColumn(stmt, ref.Name) != info.PKColumns[i] || order.Desc != reverse {
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
	c.columns, c.expressions, _ = s.selectProjection(stmt, info)
	if hasFulltextCondition(stmt.Where) {
		c.fulltext = true
		return c, true, nil
	}
	prefix := []byte(info.storagePrefix(info.Name))
	// Merge clustered intervals so OR/IN reads remain bounded, ordered and
	// duplicate-free. Any branch that cannot constrain the key keeps the scan.
	branches, expanded := accessBranches(stmt.Where)
	if !expanded {
		branches = []Expression{stmt.Where}
	}
	for _, branch := range branches {
		if key, found := s.primaryKeyLookup(branch, info.Name, info); found {
			c.intervals = append(c.intervals, coveringInterval{key, append(copyBytes(key), 0)})
			continue
		}
		copy := *stmt
		copy.Where = branch
		plan := buildTuplePlan(&copy, info, nil, tupleConstraints(branch, info.Name, stmt.FromAlias, info))
		if plan == nil {
			c.intervals = []coveringInterval{{prefix, prefixSuccessor(prefix)}}
			break
		}
		c.intervals = append(c.intervals, plan.intervals...)
	}
	sort.Slice(c.intervals, func(i, j int) bool { return bytes.Compare(c.intervals[i].lower, c.intervals[j].lower) < 0 })
	merged := c.intervals[:0]
	for _, interval := range c.intervals {
		if len(merged) > 0 && bytes.Compare(interval.lower, merged[len(merged)-1].upper) <= 0 {
			if bytes.Compare(interval.upper, merged[len(merged)-1].upper) > 0 {
				merged[len(merged)-1].upper = interval.upper
			}
		} else {
			merged = append(merged, interval)
		}
	}
	c.intervals = merged
	return c, true, nil
}

func cursorOrderColumn(stmt *SelectStmt, name string) string {
	for _, selected := range stmt.Columns {
		if selected.Alias != "" && strings.EqualFold(selected.Alias, name) {
			ref, ok := selected.Expr.(*ColumnRefExpr)
			if !ok {
				return ""
			}
			return coveringColumn(ref.Name, stmt.From, stmt.FromAlias)
		}
	}
	return coveringColumn(name, stmt.From, stmt.FromAlias)
}

func (c *RowCursor) nextSourceRow() (map[string]Value, bool, error) {
	if c.fulltext && !c.fulltextOpened {
		c.fulltextOpened = true
		keys, used, err := c.schema.fulltextCandidates(c.stmt.Where, c.info)
		if err != nil {
			return nil, false, err
		}
		if used {
			for key := range keys {
				k := []byte(key)
				c.intervals = append(c.intervals, coveringInterval{k, append(copyBytes(k), 0)})
			}
			sort.Slice(c.intervals, func(i, j int) bool { return bytes.Compare(c.intervals[i].lower, c.intervals[j].lower) < 0 })
		} else {
			prefix := []byte(c.info.storagePrefix(c.info.Name))
			c.intervals = []coveringInterval{{prefix, prefixSuccessor(prefix)}}
		}
	}
	for {
		if c.iterator == nil {
			if c.position >= len(c.intervals) {
				return nil, false, nil
			}
			position := c.position
			if c.reverse {
				position = len(c.intervals) - 1 - position
			}
			interval := c.intervals[position]
			c.position++
			c.iterator = c.schema.newRangeIterator(interval.lower, interval.upper, c.reverse)
		}
		if c.iterator.Valid() {
			row := decodeRow(c.iterator.Value(), c.info)
			if c.reverse {
				c.iterator.Prev()
			} else {
				c.iterator.Next()
			}
			return row, true, nil
		}
		err := storage.IteratorError(c.iterator)
		c.iterator.Close()
		c.iterator = nil
		if err != nil {
			return nil, false, err
		}
	}
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
	for {
		if err := c.schema.checkInterrupt(); err != nil {
			c.Close()
			return nil, false, err
		}
		row, found, err := c.nextSourceRow()
		if err != nil || !found {
			c.Close()
			return nil, false, err
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
