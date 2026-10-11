package sql

import (
	"container/heap"
	"errors"
	"fmt"
)

type topHeap struct {
	rows  []spillRecord
	less  func(spillRecord, spillRecord) bool
	bytes int64
}

func (h topHeap) Len() int           { return len(h.rows) }
func (h topHeap) Less(i, j int) bool { return h.less(h.rows[j], h.rows[i]) }
func (h topHeap) Swap(i, j int)      { h.rows[i], h.rows[j] = h.rows[j], h.rows[i] }
func (h *topHeap) Push(v any) {
	r := v.(spillRecord)
	r.Row = copyRow(r.Row)
	h.rows = append(h.rows, r)
	h.bytes += rowMemory(r.Row)
}
func (h *topHeap) Pop() any {
	n := len(h.rows) - 1
	r := h.rows[n]
	h.rows[n] = spillRecord{}
	h.rows = h.rows[:n]
	h.bytes -= rowMemory(r.Row)
	return r
}

// Consume the snapshot's input incrementally, retaining only LIMIT+OFFSET rows
// until that window exceeds the working budget, then switch to external sort.
func (s *Schema) streamOrderedSelect(stmt *SelectStmt, info *TableInfo, exprCols []selectExprCol, columns []ColumnDef) (result []map[string]Value, used bool, err error) {
	if info == nil || stmt.FromExpr != nil || stmt.Virtual != nil || len(stmt.Joins) > 0 || len(stmt.OrderBy) == 0 || stmt.Lock != nil || stmt.trackRows || stmt.Distinct || hasAggregates(stmt) || len(stmt.GroupBy) > 0 || stmt.Having != nil {
		return nil, false, nil
	}
	// Preserve volatile expression semantics on the established path.
	for _, e := range append([]Expression{stmt.Where}, func() []Expression {
		var x []Expression
		for _, c := range exprCols {
			x = append(x, c.expr)
		}
		for _, o := range stmt.OrderBy {
			x = append(x, o.Expr)
		}
		return x
	}()...) {
		if _, ok := s.expressionDependencies(e); !ok {
			return nil, false, nil
		}
	}
	for i, o := range stmt.OrderBy {
		if lit, ok := o.Expr.(*LiteralExpr); ok && !lit.BoundParameter && !lit.Value.Null && lit.Value.Type == ColInteger {
			if lit.Value.Int < 1 || lit.Value.Int > int64(len(columns)) {
				return nil, true, fmt.Errorf("ORDER BY position out of range")
			}
			stmt.OrderBy[i].Expr = &ColumnRefExpr{Name: columns[lit.Value.Int-1].Name}
		}
	}
	// Already ordered access keeps its existing early-stop optimization.
	if p := s.tuplePlan(stmt, info); p != nil && p.ordered {
		return nil, false, nil
	}
	used = true
	if stmt.Limit == 0 {
		return nil, true, nil
	}
	operator := "TopK"
	if stmt.Limit < 0 {
		operator = "ExternalSort"
	}
	finish := s.ProfileOperator(operator, stmt.From)
	var inputs int64
	defer func() {
		if finish != nil {
			finish(inputs, int64(len(result)), err)
		}
	}()
	less := stableRecordLess(s, stmt.OrderBy)
	h := &topHeap{less: less}
	sorter := newSpillSorter(s, less)
	defer sorter.close()
	spilling := stmt.Limit < 0
	window := limitWindow(stmt.Limit, stmt.Offset)
	needed := joinRequiredNames(stmt)
	bare := map[string]bool{}
	for _, c := range info.Columns {
		if needed == nil || needed[c.Name] || needed[stmt.From+"."+c.Name] || needed[stmt.FromAlias+"."+c.Name] {
			bare[c.Name] = true
		}
	}
	original := make(map[string]Value)
	where := localAccessPredicate(stmt.Where, stmt.From, stmt.FromAlias)
	err = s.walkAccess(info, bare, s.accessPaths(where, info), func(row map[string]Value) error {
		if needsQualifiedRows(stmt) {
			qualifyAccessRow(row, stmt.From, stmt.FromAlias)
		}
		if stmt.Where != nil {
			ok, e := s.evalCondition(stmt.Where, row)
			if e != nil {
				return e
			}
			if !ok {
				return nil
			}
		}
		clear(original)
		for name, v := range row {
			original[name] = v
		}
		for _, c := range exprCols {
			v, e := s.evalExpr(c.expr, original)
			if e != nil {
				return e
			}
			row[c.name] = v
		}
		for _, o := range stmt.OrderBy {
			if _, e := s.evalExpr(o.Expr, row); e != nil {
				return e
			}
		}
		rec := spillRecord{Row: row, Seq: inputs}
		inputs++
		if !spilling {
			if int64(h.Len()) < window {
				heap.Push(h, rec)
			} else if less(rec, h.rows[0]) {
				heap.Pop(h)
				heap.Push(h, rec)
			}
			if h.bytes <= s.memoryBudget() {
				return nil
			}
			spilling = true
			for _, r := range h.rows {
				if e := sorter.addRecord(r); e != nil {
					return e
				}
			}
			h.rows = nil
			return nil
		}
		return sorter.addRecord(rec)
	})
	if err != nil {
		return nil, true, err
	}
	var out []map[string]Value
	var position int64
	accept := func(r spillRecord) error {
		if position >= stmt.Offset {
			p := map[string]Value{}
			for _, c := range columns {
				v, ok := r.Row[c.Name]
				if !ok {
					v = NewNullValue()
				}
				p[c.Name] = v
			}
			out = append(out, p)
		}
		position++
		if stmt.Limit >= 0 && int64(len(out)) >= stmt.Limit {
			return errAccessStop
		}
		return nil
	}
	if spilling {
		err = sorter.walk(accept)
	} else {
		for h.Len() > 0 {
			rec := heap.Pop(h).(spillRecord)
			sorter.rows = append(sorter.rows, rec)
		}
		err = sorter.walk(accept)
	}
	if errors.Is(err, errAccessStop) {
		err = nil
	}
	return out, true, err
}
