package sql

import (
	"fmt"
	"math/big"
	"sort"
)

func windowExpressions(w *WindowSpec) []Expression {
	if w == nil {
		return nil
	}
	a := append([]Expression(nil), w.PartitionBy...)
	for _, o := range w.OrderBy {
		a = append(a, o.Expr)
	}
	if w.Frame != nil {
		a = append(a, w.Frame.Start.Offset, w.Frame.End.Offset)
	}
	return a
}
func mapWindowSpec(w *WindowSpec, rewrite func(Expression) (Expression, error)) (*WindowSpec, error) {
	if w == nil {
		return nil, nil
	}
	c := *w
	c.PartitionBy = append([]Expression(nil), w.PartitionBy...)
	c.OrderBy = append([]OrderByClause(nil), w.OrderBy...)
	for i, e := range c.PartitionBy {
		v, err := rewrite(e)
		if err != nil {
			return nil, err
		}
		c.PartitionBy[i] = v
	}
	for i, o := range c.OrderBy {
		v, err := rewrite(o.Expr)
		if err != nil {
			return nil, err
		}
		c.OrderBy[i].Expr = v
	}
	if w.Frame != nil {
		f := *w.Frame
		c.Frame = &f
		var err error
		f.Start.Offset, err = rewrite(f.Start.Offset)
		if err != nil {
			return nil, err
		}
		f.End.Offset, err = rewrite(f.End.Offset)
		if err != nil {
			return nil, err
		}
	}
	return &c, nil
}
func containsWindow(e Expression) bool {
	found := false
	_ = walkExpression(e, func(e Expression) (bool, error) {
		if f, ok := e.(*FuncCallExpr); ok && f.Window != nil {
			found = true
			return true, nil
		}
		return false, nil
	})
	return found
}
func hasWindows(q *SelectStmt) bool {
	for _, c := range q.Columns {
		if containsWindow(c.Expr) {
			return true
		}
	}
	for _, o := range q.OrderBy {
		if containsWindow(o.Expr) {
			return true
		}
	}
	return false
}
func windowSlot(f *FuncCallExpr) string {
	if f.windowKey != "" {
		return f.windowKey
	}
	return "\x00window:" + formatExpr(f)
}
func aggregateSlot(f *FuncCallExpr) string { return "\x00aggregate:" + formatExpr(f) }

func validateWindowCall(f *FuncCallExpr) error {
	if f.Distinct {
		return fmt.Errorf("DISTINCT window functions are not supported")
	}
	n := len(f.Args)
	switch f.Name {
	case "ROW_NUMBER", "RANK", "DENSE_RANK":
		if n != 0 {
			return fmt.Errorf("%s expects no arguments", f.Name)
		}
	case "LAG", "LEAD":
		if n < 1 || n > 3 {
			return fmt.Errorf("%s expects one to three arguments", f.Name)
		}
	case "FIRST_VALUE", "LAST_VALUE", "COUNT", "SUM", "AVG", "MIN", "MAX":
		if n != 1 {
			return fmt.Errorf("%s expects one argument", f.Name)
		}
	case "NTH_VALUE":
		if n != 2 {
			return fmt.Errorf("NTH_VALUE expects two arguments")
		}
	default:
		return fmt.Errorf("unsupported window function: %s", f.Name)
	}
	for i, e := range append(append([]Expression(nil), f.Args...), windowExpressions(f.Window)...) {
		if containsWindow(e) {
			return fmt.Errorf("nested window functions are not allowed")
		}
		if c, ok := e.(*ColumnRefExpr); ok && c.Name == "*" && f.Name == "COUNT" && i < len(f.Args) {
			continue
		}
		if err := validateAggregateExpr(e, true); err != nil {
			return err
		}
	}
	if f.Window.Frame != nil {
		for _, b := range []WindowBound{f.Window.Frame.Start, f.Window.Frame.End} {
			if b.Offset == nil {
				continue
			}
			bad := false
			_ = walkExpression(b.Offset, func(e Expression) (bool, error) {
				switch x := e.(type) {
				case *ColumnRefExpr, *SubqueryExpr, *ExistsExpr:
					bad = true
				case *FuncCallExpr:
					if !deterministicFunction(x.Name) || isAggregate(x) {
						bad = true
					}
				}
				return false, nil
			})
			if bad {
				return fmt.Errorf("window frame offset must be constant")
			}
		}
		if f.Window.Frame.Unit == "RANGE" && (f.Window.Frame.Start.Offset != nil || f.Window.Frame.End.Offset != nil) && len(f.Window.OrderBy) != 1 {
			return fmt.Errorf("RANGE offsets require exactly one ORDER BY expression")
		}
	}
	return nil
}
func resolveWindows(q *SelectStmt) error {
	for _, e := range []Expression{q.Where, q.Having} {
		if containsWindow(e) {
			return fmt.Errorf("window functions are allowed only in SELECT and ORDER BY")
		}
	}
	for _, e := range q.GroupBy {
		if containsWindow(e) {
			return fmt.Errorf("window functions are allowed only in SELECT and ORDER BY")
		}
	}
	for _, j := range q.Joins {
		if containsWindow(j.On) {
			return fmt.Errorf("window functions are allowed only in SELECT and ORDER BY")
		}
	}
	if len(q.Windows) == 0 && !hasWindows(q) {
		return nil
	}
	defs := map[string]*WindowSpec{}
	merge := func(w *WindowSpec) (*WindowSpec, error) {
		c := *w
		if w.Name == "" {
			return &c, nil
		}
		base := defs[w.Name]
		if base == nil {
			return nil, fmt.Errorf("no such window: %s", w.Name)
		}
		if len(w.PartitionBy) > 0 {
			return nil, fmt.Errorf("cannot override window PARTITION BY")
		}
		if len(w.OrderBy) > 0 && len(base.OrderBy) > 0 {
			return nil, fmt.Errorf("cannot override window ORDER BY")
		}
		if base.Frame != nil && !w.DirectName {
			return nil, fmt.Errorf("cannot override framed window")
		}
		c.Name = ""
		c.DirectName = false
		c.PartitionBy = base.PartitionBy
		if len(c.OrderBy) == 0 {
			c.OrderBy = base.OrderBy
		}
		if c.Frame == nil {
			c.Frame = base.Frame
		}
		return &c, nil
	}
	for _, d := range q.Windows {
		if defs[d.Name] != nil {
			return fmt.Errorf("duplicate window: %s", d.Name)
		}
		w, err := merge(d.Spec)
		if err != nil {
			return err
		}
		defs[d.Name] = w
	}
	windowNumber := 0
	rewrite := func(e Expression) (Expression, error) {
		return mapExpression(e, func(e Expression) (Expression, bool, error) {
			f, ok := e.(*FuncCallExpr)
			if !ok || f.Window == nil {
				return e, false, nil
			}
			c := *f
			w, err := merge(f.Window)
			if err != nil {
				return nil, true, err
			}
			c.Window = w
			c.windowKey = "\x00window:" + formatExpr(&c)
			windowNumber++
			if windowVolatile(&c) {
				c.windowKey += fmt.Sprintf(":volatile:%d", windowNumber)
			}
			return &c, true, validateWindowCall(&c)
		})
	}
	q.Columns = append([]SelectColumn(nil), q.Columns...)
	q.OrderBy = append([]OrderByClause(nil), q.OrderBy...)
	for i, c := range q.Columns {
		e, err := rewrite(c.Expr)
		if err != nil {
			return err
		}
		q.Columns[i].Expr = e
	}
	for i, o := range q.OrderBy {
		e, err := rewrite(o.Expr)
		if err != nil {
			return err
		}
		q.OrderBy[i].Expr = e
	}
	return nil
}

// Group aggregation occurs before window evaluation. Retain aggregate arguments
// of windows under internal names without trying to evaluate the window early.
func (s *Schema) preserveWindowAggregates(e Expression, group []map[string]Value, base map[string]Value) error {
	return walkExpression(e, func(e Expression) (bool, error) {
		if f, ok := e.(*FuncCallExpr); ok && isAggregate(f) {
			v, err := s.evalAggregate(f, group)
			base[aggregateSlot(f)] = v
			return true, err
		}
		return false, nil
	})
}

type windowRow struct {
	row  map[string]Value
	keys []Value
}

func windowCompare(a, b Value, o OrderByClause) int {
	if a.Null || b.Null {
		if a.Null && b.Null {
			return 0
		}
		first := !o.Desc
		if o.NullsFirst {
			first = true
		}
		if o.NullsLast {
			first = false
		}
		if a.Null == first {
			return -1
		}
		return 1
	}
	c := ValueCompare(a, b)
	if o.Desc {
		return -c
	}
	return c
}
func windowPeers(a, b windowRow, orders []OrderByClause) bool {
	for i, o := range orders {
		if windowCompare(a.keys[i], b.keys[i], o) != 0 {
			return false
		}
	}
	return true
}
func (s *Schema) processWindows(q *SelectStmt, rows []map[string]Value, cols []selectExprCol) (err error) {
	if finish := s.ProfileOperator("Window", q.From); finish != nil {
		defer func() { finish(int64(len(rows)), int64(len(rows)), err) }()
	}
	var calls []*FuncCallExpr
	seen := map[string]bool{}
	visit := func(e Expression) (bool, error) {
		if f, ok := e.(*FuncCallExpr); ok && f.Window != nil {
			k := windowSlot(f)
			if !seen[k] {
				calls = append(calls, f)
				seen[k] = true
			}
			return true, nil
		}
		return false, nil
	}
	for _, c := range cols {
		_ = walkExpression(c.expr, visit)
	}
	for _, o := range q.OrderBy {
		_ = walkExpression(o.Expr, visit)
	}
	used := int64(0)
	for _, row := range rows {
		used += 2*rowMemory(row) + 256 + int64(len(calls))*64
		if used > s.memoryBudget() {
			return fmt.Errorf("query memory budget exceeded by window input")
		}
	}
	for _, f := range calls {
		if err := s.checkInterrupt(); err != nil {
			return err
		}
		if err := s.evaluateWindow(f, rows); err != nil {
			return err
		}
	}
	for _, row := range rows {
		if err := s.checkInterrupt(); err != nil {
			return err
		}
		original := make(map[string]Value, len(row))
		for k, v := range row {
			original[k] = v
		}
		for _, c := range cols {
			if v, ok := original["\x00selected:"+c.name]; ok && !containsWindow(c.expr) {
				row[c.name] = v
				continue
			}
			v, err := s.evalExpr(c.expr, original)
			if err != nil {
				return err
			}
			row[c.name] = v
		}
	}
	return nil
}
func (s *Schema) evaluateWindow(f *FuncCallExpr, rows []map[string]Value) error {
	w := f.Window
	if w.Frame != nil {
		for _, b := range []WindowBound{w.Frame.Start, w.Frame.End} {
			if b.Offset == nil {
				continue
			}
			if w.Frame.Unit == "RANGE" {
				v, err := s.evalExpr(b.Offset, nil)
				if err != nil {
					return err
				}
				if v.Null || (v.Type != ColInteger && v.Type != ColReal) || toFloat(v) < 0 {
					return fmt.Errorf("RANGE offset must be nonnegative numeric")
				}
			} else {
				if _, err := s.windowInt(b.Offset, nil, false); err != nil {
					return err
				}
			}
		}
	}
	partitions := map[string][]windowRow{}
	var order []string
	for _, row := range rows {
		if err := s.checkInterrupt(); err != nil {
			return err
		}
		var key []byte
		for _, e := range w.PartitionBy {
			v, err := s.evalExpr(e, row)
			if err != nil {
				return err
			}
			key = append(key, valueIdentity(v)...)
		}
		k := string(key)
		if _, ok := partitions[k]; !ok {
			order = append(order, k)
		}
		r := windowRow{row: row}
		for _, o := range w.OrderBy {
			v, err := s.evalExpr(o.Expr, row)
			if err != nil {
				return err
			}
			r.keys = append(r.keys, v)
		}
		partitions[k] = append(partitions[k], r)
	}
	for _, k := range order {
		part := partitions[k]
		sortChecks := 0
		var sortErr error
		sort.SliceStable(part, func(i, j int) bool {
			sortChecks++
			if sortChecks%1024 == 0 {
				sortErr = s.checkInterrupt()
			}
			if sortErr != nil {
				return false
			}
			for n, o := range w.OrderBy {
				c := windowCompare(part[i].keys[n], part[j].keys[n], o)
				if c != 0 {
					return c < 0
				}
			}
			return false
		})
		if sortErr != nil {
			return sortErr
		}
		starts := make([]int, len(part))
		ends := make([]int, len(part))
		groups := make([]int, len(part))
		var gs, ge []int
		for start := 0; start < len(part); {
			end := start + 1
			for end < len(part) && windowPeers(part[start], part[end], w.OrderBy) {
				end++
			}
			g := len(gs)
			gs = append(gs, start)
			ge = append(ge, end)
			for i := start; i < end; i++ {
				starts[i] = start
				ends[i] = end
				groups[i] = g
			}
			start = end
		}
		aggregate, err := s.newWindowAggregate(f, part)
		if err != nil {
			return err
		}
		for i, r := range part {
			if err := s.checkInterrupt(); err != nil {
				return err
			}
			v := NewNullValue()
			switch f.Name {
			case "ROW_NUMBER":
				v = NewIntValue(int64(i) + 1)
			case "RANK":
				v = NewIntValue(int64(starts[i]) + 1)
			case "DENSE_RANK":
				v = NewIntValue(int64(groups[i]) + 1)
			case "LAG", "LEAD":
				off := int64(1)
				var err error
				if len(f.Args) > 1 {
					off, err = s.windowInt(f.Args[1], r.row, false)
					if err != nil {
						return err
					}
				}
				target := windowShift(int64(i), off, f.Name == "LEAD", int64(len(part)))
				if target >= 0 && target < int64(len(part)) {
					v, err = s.evalExpr(f.Args[0], part[int(target)].row)
				} else if len(f.Args) > 2 {
					v, err = s.evalExpr(f.Args[2], r.row)
				}
				if err != nil {
					return err
				}
			default:
				lo, hi, err := s.windowFrame(w, part, i, starts, ends, groups, gs, ge)
				if err != nil {
					return err
				}
				switch f.Name {
				case "FIRST_VALUE", "LAST_VALUE", "NTH_VALUE":
					target := int64(lo)
					if f.Name == "LAST_VALUE" {
						target = int64(hi) - 1
					}
					if f.Name == "NTH_VALUE" {
						n, err := s.windowInt(f.Args[1], r.row, true)
						if err != nil {
							return err
						}
						target = windowShift(int64(lo), n-1, true, int64(len(part)))
					}
					if target >= int64(lo) && target < int64(hi) {
						v, err = s.evalExpr(f.Args[0], part[int(target)].row)
					}
				default:
					v, err = aggregate.frame(lo, hi)
				}
				if err != nil {
					return err
				}
			}
			r.row[windowSlot(f)] = v
		}
	}
	return nil
}
func (s *Schema) windowInt(e Expression, row map[string]Value, positive bool) (int64, error) {
	v, err := s.evalExpr(e, row)
	if err != nil {
		return 0, err
	}
	if v.Null || v.Type != ColInteger || v.Int < 0 || positive && v.Int == 0 {
		return 0, fmt.Errorf("window offset must be a %s integer", map[bool]string{true: "positive", false: "nonnegative"}[positive])
	}
	return v.Int, nil
}

// Clamp before adding/subtracting to avoid overflow with offsets up to MaxInt64.
func windowShift(i, n int64, forward bool, size int64) int64 {
	if forward {
		if n > size-i {
			return size
		}
		return i + n
	}
	if n > i {
		return -1
	}
	return i - n
}
func (s *Schema) windowFrame(w *WindowSpec, p []windowRow, i int, starts, ends, groups, gs, ge []int) (int, int, error) {
	if w.Frame == nil {
		if len(w.OrderBy) == 0 {
			return 0, len(p), nil
		}
		return 0, ends[i], nil
	}
	f := w.Frame
	bound := func(b WindowBound, end bool) (int, error) {
		switch b.Kind {
		case "UNBOUNDED PRECEDING":
			return 0, nil
		case "UNBOUNDED FOLLOWING":
			return len(p), nil
		case "CURRENT ROW":
			if f.Unit == "ROWS" {
				if end {
					return i + 1, nil
				}
				return i, nil
			}
			if end {
				return ends[i], nil
			}
			return starts[i], nil
		}
		forward := b.Kind == "FOLLOWING"
		if f.Unit == "RANGE" {
			off, err := s.evalExpr(b.Offset, nil)
			if err != nil {
				return 0, err
			}
			if off.Null || (off.Type != ColInteger && off.Type != ColReal) || toFloat(off) < 0 {
				return 0, fmt.Errorf("RANGE offset must be nonnegative numeric")
			}
			cur := p[i].keys[0]
			if cur.Null || cur.Type != ColInteger && cur.Type != ColReal {
				if end {
					return ends[i], nil
				}
				return starts[i], nil
			}
			threshold := windowRat(cur)
			delta := windowRat(off)
			if forward == w.OrderBy[0].Desc {
				delta.Neg(delta)
			}
			threshold.Add(threshold, delta)
			pos := sort.Search(len(p), func(j int) bool {
				v := p[j].keys[0]
				c := 0
				if !v.Null && (v.Type == ColInteger || v.Type == ColReal) {
					c = windowRat(v).Cmp(threshold)
					if w.OrderBy[0].Desc {
						c = -c
					}
				} else {
					c = windowCompare(v, NewIntValue(0), w.OrderBy[0])
				}
				if end {
					return c > 0
				}
				return c >= 0
			})
			return pos, nil
		}
		n, err := s.windowInt(b.Offset, nil, false)
		if err != nil {
			return 0, err
		}
		if f.Unit == "GROUPS" {
			g := windowShift(int64(groups[i]), n, forward, int64(len(gs)))
			if g < 0 {
				return 0, nil
			}
			if g >= int64(len(gs)) {
				return len(p), nil
			}
			if end {
				return ge[int(g)], nil
			}
			return gs[int(g)], nil
		}
		pos := windowShift(int64(i), n, forward, int64(len(p)))
		if end {
			pos++
		}
		if pos < 0 {
			pos = 0
		}
		if pos > int64(len(p)) {
			pos = int64(len(p))
		}
		return int(pos), nil
	}
	lo, err := bound(f.Start, false)
	if err != nil {
		return 0, 0, err
	}
	hi, err := bound(f.End, true)
	if err != nil {
		return 0, 0, err
	}
	if hi < lo {
		hi = lo
	}
	return lo, hi, nil
}

func (s *Schema) makeJoinPlanIfNoWindows(q *SelectStmt) *joinPlan {
	if hasWindows(q) {
		return nil
	}
	return s.makeJoinPlan(q)
}
func (s *Schema) streamOrderedSelectIfNoWindows(q *SelectStmt, info *TableInfo, cols []selectExprCol, defs []ColumnDef) ([]map[string]Value, bool, error) {
	if hasWindows(q) {
		return nil, false, nil
	}
	return s.streamOrderedSelect(q, info, cols, defs)
}

func windowRat(v Value) *big.Rat {
	if v.Type == ColInteger {
		return new(big.Rat).SetInt64(v.Int)
	}
	return new(big.Rat).SetFloat64(v.Real)
}

func windowVolatile(f *FuncCallExpr) bool {
	volatile := false
	for _, e := range append(append([]Expression(nil), f.Args...), windowExpressions(f.Window)...) {
		_ = walkExpression(e, func(e Expression) (bool, error) {
			switch x := e.(type) {
			case *FuncCallExpr:
				if !isAggregate(x) && !deterministicFunction(x.Name) {
					volatile = true
				}
			case *SubqueryExpr, *ExistsExpr:
				volatile = true
			}
			return false, nil
		})
	}
	return volatile
}
