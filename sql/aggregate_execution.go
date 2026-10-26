package sql

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"math/big"
	"sort"
	"strings"
)

type aggregateState struct {
	exact        *big.Rat
	integerWide  *big.Int
	exactScale   int
	hasReal      bool
	owner        *Schema
	distinctDisk *diskHash
	seenBytes    int64
	partsBytes   int64
	count        int64
	integer      int64
	total        float64
	allInt       bool
	overflow     bool
	best         Value
	seen         map[string]bool
	parts        []string
	separator    string
}

func (s *Schema) addAggregate(a *aggregateState, f *FuncCallExpr, row map[string]Value) error {
	if strings.EqualFold(f.Name, "COUNT") {
		if c, ok := f.Args[0].(*ColumnRefExpr); ok && c.Name == "*" {
			a.count++
			return nil
		}
	}
	v, err := s.evalExpr(f.Args[0], row)
	if err != nil {
		return err
	}
	return s.addAggregateValue(a, f, v)
}

// Typed scans bind/evaluate arguments before this common state transition.
func addAggregateValue(a *aggregateState, f *FuncCallExpr, v Value) error {
	name := strings.ToUpper(f.Name)
	if v.Null {
		return nil
	}
	if f.Distinct && a.owner != nil {
		ok, err := a.owner.aggregateDistinct(a, v)
		if err != nil {
			return err
		}
		if !ok {
			return nil
		}
	} else if f.Distinct {
		if a.seen == nil {
			a.seen = make(map[string]bool)
		}
		// Conversion in the lookup borrows scratch only for the lookup. The
		// insertion below makes an owned string; no stored key aliases scratch.
		var scratch [128]byte
		key := appendValueIdentity(scratch[:0], v)
		if a.seen[string(key)] {
			return nil
		}
		a.seen[string(key)] = true
	}
	if a.count == 0 {
		a.allInt = true
		if name == "MIN" || name == "MAX" {
			a.best = ownAggregateBest(v, a.best.Blob)
		}
	}
	a.count++
	switch name {
	case "COUNT":
	case "MIN", "MAX":
		cmp := ValueCompare(v, a.best)
		if a.owner != nil {
			cmp = a.owner.valueCompare(v, a.best)
		}
		if name == "MIN" && cmp < 0 || name == "MAX" && cmp > 0 {
			a.best = ownAggregateBest(v, a.best.Blob)
		}
	case "GROUP_CONCAT":
		part := v.String()
		a.parts = append(a.parts, part)
		a.partsBytes += int64(len(part) + 16)
	case "SUM", "AVG", "TOTAL":
		if v.EnumValue {
			v = NewIntValue(v.EnumOrdinal)
		}
		if !isNumeric(v) {
			return fmt.Errorf("%s requires numeric arguments", name)
		}
		if v.Type == ColReal {
			a.hasReal = true
		}
		if v.Type == ColUnsigned || v.Type == ColDecimal {
			if a.exact == nil {
				a.exact = new(big.Rat)
				if a.integerWide != nil {
					a.exact.SetInt(a.integerWide)
				} else {
					a.exact.SetInt64(a.integer)
				}
			}
			_, scale, _ := valueDecimal(v)
			a.exactScale = max(a.exactScale, scale)
		}
		if a.exact != nil {
			a.exact.Add(a.exact, valueRational(v))
		}
		a.total += toFloat(v)
		if v.Type != ColInteger {
			a.allInt = false
		} else {
			next := a.integer + v.Int
			overflow := v.Int > 0 && next < a.integer || v.Int < 0 && next > a.integer
			if overflow && a.integerWide == nil {
				a.integerWide = big.NewInt(a.integer)
			}
			if a.integerWide != nil {
				a.integerWide.Add(a.integerWide, big.NewInt(v.Int))
			}
			a.overflow = a.overflow || overflow
			a.integer = next
		}
	}
	return nil
}

func aggregateValue(a *aggregateState, f *FuncCallExpr) (Value, error) {
	name := strings.ToUpper(f.Name)
	if name == "COUNT" {
		return NewIntValue(a.count), nil
	}
	if a.count == 0 {
		if name == "TOTAL" {
			return NewRealValue(0), nil
		}
		return NewNullValue(), nil
	}
	if a.exact != nil && !a.hasReal && (name == "SUM" || name == "AVG") {
		sum := new(big.Rat).Set(a.exact)
		scale := a.exactScale
		if name == "AVG" {
			sum.Quo(sum, new(big.Rat).SetInt64(a.count))
			scale = min(scale+4, 30)
		}
		return rationalDecimal(sum, scale)
	}
	switch name {
	case "MIN", "MAX":
		return a.best, nil
	case "GROUP_CONCAT":
		return NewTextValue(strings.Join(a.parts, a.separator)), nil
	case "SUM":
		if a.allInt {
			if a.overflow {
				return NewNullValue(), fmt.Errorf("integer overflow")
			}
			return NewIntValue(a.integer), nil
		}
	}
	total := a.total
	if name == "AVG" {
		total /= float64(a.count)
	}
	if math.IsNaN(total) || math.IsInf(total, 0) {
		return NewNullValue(), fmt.Errorf("numeric value out of range")
	}
	return NewRealValue(total), nil
}

type aggregateGroup struct {
	inputs     int64
	base       map[string]Value
	states     []aggregateState
	baseValues []Value
}

// Decode only referenced columns, into one map reused for the entire scan.
// Skip unused variable-width values without allocating strings or blobs.
type scanColumn struct {
	name, qualified string
	needed          bool
	definition      ColumnDef
}

func decodeScanRow(data []byte, columns []scanColumn, row map[string]Value) {
	decodeScanRowMode(data, columns, row, false)
}

// Explicit reuse callers relinquish the previous row's values. Aggregate
// retained winners/representatives own copies before the next decode.
func decodeScanRowMode(data []byte, columns []scanColumn, row map[string]Value, reusable bool) {
	offset := 0
	for _, c := range columns {
		if offset >= len(data) {
			if c.needed {
				row[c.name] = NewNullValue()
				if c.qualified != "" {
					row[c.qualified] = NewNullValue()
				}
			}
			continue
		}
		if c.needed {
			var v Value
			var n int
			if reusable {
				previous := row[c.name]
				if cap(previous.Blob) > borrowedAccessScratchLimit {
					previous.Blob = nil
				}
				v, n = decodePointValueInto(data[offset:], previous)
			} else {
				v, n = decodeValue(data[offset:])
			}
			v = decorateColumnValue(v, c.definition)
			offset += n
			row[c.name] = v
			if c.qualified != "" {
				row[c.qualified] = v
			}
			continue
		}
		switch data[offset] {
		case binTypeInt64, binTypeFloat64, binTypeUint64:
			if len(data)-offset >= 9 {
				offset += 9
			} else {
				offset++
			}
		case binTypeText, binTypeBlob, binTypeDecimal, binTypeJSON:
			length, n := binary.Uvarint(data[offset+1:])
			if n > 0 && length <= uint64(len(data)-offset-1-n) {
				offset += 1 + n + int(length)
			} else {
				offset++
				if n > 0 {
					offset += n
				}
			}
		default:
			offset++
		}
	}
}

func (s *Schema) streamAggregateScan(stmt *SelectStmt, info *TableInfo, exprCols []selectExprCol) ([]map[string]Value, bool, error) {
	if hasWindows(stmt) {
		return nil, false, nil
	}

	if info == nil || stmt.FromExpr != nil || stmt.Lock != nil || stmt.trackRows || stmt.SelectAll || stmt.Virtual != nil || (!hasAggregates(stmt) && len(stmt.GroupBy) == 0 && stmt.Having == nil) {
		return nil, false, nil
	}
	var plan *joinPlan
	if len(stmt.Joins) > 0 {
		plan = s.makeJoinPlan(stmt)
		if plan == nil {
			return nil, false, nil
		}
	}
	var functions []*FuncCallExpr
	positions := make(map[*FuncCallExpr]int)
	needed := make(map[string]bool)
	expressions := []Expression{stmt.Where, stmt.Having}
	for _, ec := range exprCols {
		expressions = append(expressions, ec.expr)
	}
	expressions = append(expressions, stmt.GroupBy...)
	for _, o := range stmt.OrderBy {
		expressions = append(expressions, o.Expr)
	}
	safe := true
	reusableInputs := true
	for _, expr := range expressions {
		_ = walkExpression(expr, func(e Expression) (bool, error) {
			switch e := e.(type) {
			case *SubqueryExpr, *ExistsExpr:
				// Memoized subqueries can retain outer-row values. Keep owned
				// decoder values unless every consumer is local to this aggregate.
				reusableInputs = false
				deps, ok := s.expressionDependencies(e)
				safe = safe && ok
				for _, name := range deps {
					needed[name] = true
				}
				return true, nil
			case *ColumnRefExpr:
				needed[e.Name] = true
			case *FuncCallExpr:
				if e.MySQLGroupConcat {
					safe = false
				}
				if !s.deterministicFunction(e.Name) {
					safe = false
				}
				if isAggregate(e) {
					if _, ok := positions[e]; !ok {
						positions[e] = len(functions)
						functions = append(functions, e)
					}
				}
			}
			return false, nil
		})
	}
	if !safe {
		return nil, false, nil
	}
	for _, c := range stmt.Columns {
		if c.Star {
			return nil, false, nil
		}
	}
	alias := stmt.FromAlias
	if alias == "" {
		alias = stmt.From
	}
	bareNeeded := make(map[string]bool)
	for _, c := range info.Columns {
		if needed[c.Name] || needed[alias+"."+c.Name] || needed[stmt.From+"."+c.Name] {
			bareNeeded[c.Name] = true
		}
	}
	row := make(map[string]Value, len(needed))
	groupExprs := append([]Expression(nil), stmt.GroupBy...)
	for i, expr := range groupExprs {
		if lit, ok := expr.(*LiteralExpr); ok && !lit.BoundParameter && !lit.Value.Null && lit.Value.Type == ColInteger && lit.Value.Int > 0 && lit.Value.Int <= int64(len(exprCols)) {
			groupExprs[i] = exprCols[lit.Value.Int-1].expr
			continue
		}
		if ref, ok := expr.(*ColumnRefExpr); ok {
			for _, ec := range exprCols {
				if ref.Name == ec.name {
					groupExprs[i] = ec.expr
					break
				}
			}
		}
	}
	groups := make(map[string]*aggregateGroup)
	var order []*aggregateGroup
	newGroup := func() *aggregateGroup {
		g := &aggregateGroup{states: make([]aggregateState, len(functions))}
		order = append(order, g)
		return g
	}
	if len(stmt.GroupBy) == 0 {
		groups[""] = newGroup()
	}
	var keyBuffer [128]byte
	keyScratch := keyBuffer[:0]
	// A query-local key buffer covers duplicate wide groups without retaining
	// unbounded inputs. Charge its heap capacity to the hash-group budget below.
	keyScratchLimit := min(int64(64<<10), s.memoryBudget()/4)
	encodeGroupKey := func(input map[string]Value) ([]byte, error) {
		key := keyScratch[:0]
		for _, expr := range groupExprs {
			v, err := s.evalExpr(expr, input)
			if err != nil {
				return nil, err
			}
			key = s.appendValueIdentity(key, v)
		}
		if int64(cap(key)) <= keyScratchLimit {
			keyScratch = key[:0]
		}
		return key, nil
	}
	qualified := needsQualifiedRows(stmt)
	representative := make(map[string]bool)
	collect := func(expr Expression) {
		_ = walkExpression(expr, func(e Expression) (bool, error) {
			switch e := e.(type) {
			case *FuncCallExpr:
				if isAggregate(e) {
					return true, nil
				}
			case *ColumnRefExpr:
				representative[e.Name] = true
			case *SubqueryExpr, *ExistsExpr:
				deps, _ := s.expressionDependencies(e)
				for _, n := range deps {
					representative[n] = true
				}
				return true, nil
			}
			return false, nil
		})
	}
	for _, ec := range exprCols {
		collect(ec.expr)
	}
	collect(stmt.Having)
	for _, o := range stmt.OrderBy {
		collect(o.Expr)
	}
	for _, g := range groupExprs {
		collect(g)
	}
	for _, f := range functions {
		if strings.EqualFold(f.Name, "GROUP_CONCAT") && len(f.Args) > 1 {
			collect(f.Args[1])
		}
	}
	var baseNames []string
	namesReady := false
	var result []map[string]Value
	scratch := make(map[string]Value, len(baseNames)+len(exprCols))
	emit := func(g *aggregateGroup) error {
		defer closeAggregateDisks(g)
		clear(scratch)
		g.base = scratch
		for i, name := range baseNames {
			if i < len(g.baseValues) {
				g.base[name] = g.baseValues[i]
			}
		}
		values := make([]Value, len(functions))
		for i, f := range functions {
			if strings.EqualFold(f.Name, "GROUP_CONCAT") && g.states[i].count > 0 {
				g.states[i].separator = ","
				if len(f.Args) > 1 {
					v, err := s.evalExpr(f.Args[1], g.base)
					if err != nil {
						return err
					}
					g.states[i].separator = ""
					if !v.Null {
						g.states[i].separator = v.String()
					}
				}
			}
			v, err := aggregateValue(&g.states[i], f)
			if err != nil {
				return err
			}
			values[i] = v
		}
		replace := func(e Expression) (Expression, error) {
			return mapExpression(e, func(e Expression) (Expression, bool, error) {
				if f, ok := e.(*FuncCallExpr); ok && isAggregate(f) {
					return &LiteralExpr{Value: values[positions[f]]}, true, nil
				}
				return e, false, nil
			})
		}
		for _, ec := range exprCols {
			expr, err := replace(ec.expr)
			if err != nil {
				return err
			}
			v, err := s.evalExpr(expr, g.base)
			if err != nil {
				return err
			}
			g.base[ec.name] = v
		}
		for i, o := range stmt.OrderBy {
			if containsAggregate(o.Expr) {
				expr, err := replace(o.Expr)
				if err != nil {
					return err
				}
				v, err := s.evalExpr(expr, g.base)
				if err != nil {
					return err
				}
				g.base[fmt.Sprintf("\x00order%d", i)] = v
			}
		}
		if stmt.Having != nil {
			expr, err := replace(stmt.Having)
			if err != nil {
				return err
			}
			match, err := s.evalCondition(expr, g.base)
			if err != nil {
				return err
			}
			if !match {
				return nil
			}
		}
		result = append(result, copyRow(g.base))
		return nil
	}
	ordered := false
	var orderedKey string
	var orderedGroup *aggregateGroup
	var orderedInputs int64
	orderedEmit := func(g *aggregateGroup) error { orderedInputs += g.inputs; return emit(g) }
	var groupMemory int64
	for _, g := range groups {
		groupMemory += aggregateGroupMemory(g)
	}
	accept := func(input map[string]Value) error {
		row = input
		if plan == nil && qualified {
			qualifyAccessRow(row, stmt.From, alias)
		}
		if stmt.Where != nil && plan == nil {
			match, err := s.evalCondition(stmt.Where, row)
			if err != nil {
				return err
			}
			if !match {
				return nil
			}
		}
		key, err := encodeGroupKey(row)
		if err != nil {
			return err
		}
		var g *aggregateGroup
		var before int64
		if ordered {
			if orderedGroup == nil {
				orderedGroup = &aggregateGroup{states: make([]aggregateState, len(functions))}
				orderedKey = string(key)
			} else if orderedKey != string(key) {
				if err := orderedEmit(orderedGroup); err != nil {
					return err
				}
				resetOrderedAggregate(orderedGroup)
				orderedKey = string(key)
			}
			g = orderedGroup
		} else {
			g = groups[string(key)]
			if g != nil {
				before = aggregateGroupMemory(g)
			}
			if g == nil {
				g = newGroup()
				groups[string(key)] = g
			}
		}
		first := g.inputs == 0
		if first {
			if !namesReady {
				for _, name := range sortedAccessNames(representative) {
					if _, ok := row[name]; ok {
						baseNames = append(baseNames, name)
					}
				}
				namesReady = true
			}
			if cap(g.baseValues) < len(baseNames) {
				g.baseValues = make([]Value, len(baseNames))
			} else {
				g.baseValues = g.baseValues[:len(baseNames)]
			}
			for i, name := range baseNames {
				g.baseValues[i] = ownAggregateBest(row[name], nil)
			}
		}
		g.inputs++
		for i, f := range functions {
			if err := s.addAggregate(&g.states[i], f, row); err != nil {
				return err
			}
		}
		if ordered {
			return nil
		}
		groupMemory += aggregateGroupMemory(g) - before
		if groupMemory+int64(max(0, cap(keyScratch)-len(keyBuffer))) > s.memoryBudget() {
			return errAggregateSpill
		}
		return nil
	}
	var readErr error
	if plan != nil {
		readErr = s.walkJoinPlan(plan, accept)
	} else {
		where, _ := mapExpression(stmt.Where, func(e Expression) (Expression, bool, error) {
			switch e := e.(type) {
			case *ColumnRefExpr:
				if name := coveringColumn(e.Name, stmt.From, alias); name != "" {
					return &ColumnRefExpr{Name: name}, true, nil
				}
			case *SubqueryExpr, *ExistsExpr:
				return e, true, nil
			}
			return e, false, nil
		})
		paths := s.accessPaths(where, info)
		// A one-sided secondary range often reads most of a table. Sequential
		// decoding avoids fetching the same wide input through a broad index.
		if len(paths) == 1 && paths[0].index != nil && !paths[0].complete {
			constraints := s.accessConstraints(where, info)
			c := constraints[paths[0].index.columnNames()[0]]
			if c != nil && (c.lower == nil || c.upper == nil) && c.equal == nil && c.listTerm == nil {
				paths = nil
			}
		}
		ordered = orderedAggregateInput(stmt, info, groupExprs, paths)
		if s.expressionMySQL() || s.nonbinaryDefaultCollation() || tableHasExplicitCollation(info) {
			ordered = false
		}
		finish := (func(int64, int64, error))(nil)
		if ordered {
			finish = s.ProfileOperator("OrderedAggregate", stmt.From)
		}
		var typed *typedAggregate
		var ok bool
		if s.nonbinaryDefaultCollation() || tableHasExplicitCollation(info) {
			// The typed program uses bytewise comparison and grouping keys.
			// Unicode modes use the expression evaluator's collation rules.
		} else if p := stmt.prepared; p != nil {
			if !p.aggregateReady {
				p.aggregate, _ = compileTypedAggregate(info, stmt.From, alias, stmt.Where, groupExprs, functions, representative)
				p.aggregateReady = true
			}
			typed, ok = p.aggregate, p.aggregate != nil
		} else {
			typed, ok = compileTypedAggregate(info, stmt.From, alias, stmt.Where, groupExprs, functions, representative)
		}
		if ok {
			baseNames = typed.baseNames
			namesReady = true
			if ordered {
				readErr = s.walkTypedOrderedAggregate(typed, info, bareNeeded, paths, orderedEmit)
			} else {
				readErr = s.walkTypedAggregate(typed, info, bareNeeded, paths, groups, newGroup)
			}
		} else {
			readErr = s.walkAggregateAccess(info, bareNeeded, where, paths, accept, reusableInputs)
			if ordered && readErr == nil && orderedGroup != nil {
				readErr = orderedEmit(orderedGroup)
			}
		}
		if finish != nil {
			finish(orderedInputs, int64(len(result)), readErr)
		}
	}
	if readErr != nil && !errors.Is(readErr, errAggregateSpill) {
		return nil, true, readErr
	}

	if ordered {
		// Groups were finalized while reading the clustered prefix.
	} else if errors.Is(readErr, errAggregateSpill) {
		for _, g := range groups {
			closeAggregateDisks(g)
		}
		groups = make(map[string]*aggregateGroup)
		order = nil
		sorter := newSpillSorter(s, func(a, b spillRecord) bool {
			if a.Key != b.Key {
				return a.Key < b.Key
			}
			return a.Seq < b.Seq
		})
		defer sorter.close()
		finish := s.ProfileOperator("SpillAggregate", stmt.From)
		if plan != nil {
			plan = s.makeJoinPlan(stmt)
		}
		var inputs int64
		capture := func(input map[string]Value) error {
			if plan == nil && qualified {
				qualifyAccessRow(input, stmt.From, alias)
			}
			if plan == nil && stmt.Where != nil {
				ok, e := s.evalCondition(stmt.Where, input)
				if e != nil {
					return e
				}
				if !ok {
					return nil
				}
			}
			key, e := encodeGroupKey(input)
			if e != nil {
				return e
			}
			inputs++
			return sorter.add(input, nil, string(key))
		}
		if plan != nil {
			readErr = s.walkJoinPlan(plan, capture)
		} else {
			readErr = s.walkFilteredAccess(info, bareNeeded, localAccessPredicate(stmt.Where, stmt.From, stmt.FromAlias), nil, capture)
		}
		var last string
		var current *aggregateGroup
		var firstSeq int64
		if readErr == nil {
			readErr = sorter.walk(func(rec spillRecord) error {
				if current == nil || rec.Key != last {
					if current != nil {
						before := len(result)
						if e := emit(current); e != nil {
							return e
						}
						if len(result) > before {
							result[len(result)-1]["\x00groupseq"] = NewIntValue(firstSeq)
						}
					}
					clear(groups)
					order = nil
					current = &aggregateGroup{states: make([]aggregateState, len(functions))}
					groups[rec.Key] = current
					last = rec.Key
					firstSeq = rec.Seq
				}
				// Input predicates were checked before sorting; the accumulator only
				// sees qualified accepted rows from the same statement snapshot.
				current.inputs++
				if current.inputs == 1 {
					if !namesReady {
						for _, name := range sortedAccessNames(representative) {
							if _, ok := rec.Row[name]; ok {
								baseNames = append(baseNames, name)
							}
						}
						namesReady = true
					}
					current.baseValues = make([]Value, len(baseNames))
					for i, name := range baseNames {
						current.baseValues[i] = rec.Row[name]
					}
				}
				for i, f := range functions {
					if e := s.addAggregate(&current.states[i], f, rec.Row); e != nil {
						return e
					}
				}
				return nil
			})
		}
		if readErr == nil && current != nil {
			before := len(result)
			readErr = emit(current)
			if len(result) > before {
				result[len(result)-1]["\x00groupseq"] = NewIntValue(firstSeq)
			}
		}
		if finish != nil {
			finish(inputs, int64(len(result)), readErr)
		}
		if readErr != nil {
			return nil, true, readErr
		}
		sort.SliceStable(result, func(i, j int) bool { return result[i]["\x00groupseq"].Int < result[j]["\x00groupseq"].Int })
		for _, row := range result {
			delete(row, "\x00groupseq")
		}
	} else {
		for _, g := range order {
			if err := s.checkInterrupt(); err != nil {
				return nil, true, err
			}
			if err := emit(g); err != nil {
				return nil, true, err
			}
		}
	}

	for i, o := range stmt.OrderBy {
		if containsAggregate(o.Expr) {
			stmt.OrderBy[i].Expr = &ColumnRefExpr{Name: fmt.Sprintf("\x00order%d", i)}
		}
	}
	return result, true, nil
}

// Input decoders may reuse their BLOB buffers after this transition. MIN/MAX
// must own a retained winner, unlike COUNT/SUM/AVG/TOTAL which retain no input.
// Reuse applies only within a group: resetOrderedAggregate drops the state
// after emission, so later groups cannot overwrite emitted BLOB results.
func ownAggregateBest(value Value, buffer []byte) Value {
	if value.Type == ColBlob && !value.Null {
		// A large old winner must not hide oversized capacity behind a much
		// smaller replacement. Keep retained bytes proportional to the live
		// winner, with a small-buffer allowance for normal reuse.
		if cap(buffer) > 64<<10 && cap(buffer)/2 > len(value.Blob) {
			buffer = nil
		}
		if buffer == nil {
			buffer = []byte{}
		}
		value.Blob = append(buffer[:0], value.Blob...)
	}
	return value
}

// The aggregate transition owns retained BLOB winners and representative
// values. This consumer alone may reuse decoder BLOB/text input buffers;
// ordinary walkAccess and spill visitors retain their owned-value contracts.
func (s *Schema) walkAggregateAccess(info *TableInfo, needed map[string]bool, where Expression, paths []*accessPath, visit func(map[string]Value) error, reusableInputs bool) error {
	if !reusableInputs || hasFulltextCondition(where) {
		return s.walkFilteredAccess(info, needed, where, paths, visit)
	}
	return s.walkAccessDecoded(info, needed, paths, visit, true)
}
