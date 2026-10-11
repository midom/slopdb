package sql

import (
	"bytes"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"

	"slopdb/storage"
)

// Tuple access uses equality prefixes, followed by an IN list or integer range.
// Every extracted condition is type exact; the remaining predicate still runs
// with ordinary SQL semantics. No format changes or extra indexes are required.
type tupleConstraint struct {
	equal, lower, upper             *Value
	equalTerm, lowerTerm, upperTerm Expression
	list                            []Value
	listTerm                        Expression
	lowerStrict, upperStrict        bool
}

type tupleReadPlan struct {
	index                     *IndexInfo // nil means the clustered primary key
	info                      *TableInfo
	intervals                 []coveringInterval
	points                    [][]byte
	residual                  Expression
	ordered, reverse, covered bool
	score                     int
}

func tupleConstraints(where Expression, table, alias string, info *TableInfo) map[string]*tupleConstraint {
	types := make(map[string]ColumnType, len(info.Columns))
	for _, col := range info.Columns {
		types[col.Name] = col.ColType
	}
	result := make(map[string]*tupleConstraint)
	valid := func(name string, value Value) bool {
		typ, ok := types[name]
		return ok && !value.Null && value.Type == typ && (typ != ColReal || (value.Real != 0 && !math.IsNaN(value.Real)))
	}
	constraint := func(name string) *tupleConstraint {
		if result[name] == nil {
			result[name] = &tupleConstraint{}
		}
		return result[name]
	}
	var visit func(Expression)
	visit = func(expr Expression) {
		if b, ok := expr.(*BinaryExpr); ok {
			if b.Op == "AND" {
				visit(b.Left)
				visit(b.Right)
				return
			}
			col, colOK := b.Left.(*ColumnRefExpr)
			lit, litOK := b.Right.(*LiteralExpr)
			op := b.Op
			if !colOK || !litOK {
				col, colOK = b.Right.(*ColumnRefExpr)
				lit, litOK = b.Left.(*LiteralExpr)
				switch op {
				case "<":
					op = ">"
				case "<=":
					op = ">="
				case ">":
					op = "<"
				case ">=":
					op = "<="
				}
			}
			if !colOK || !litOK {
				return
			}
			name := coveringColumn(col.Name, table, alias)
			if !valid(name, lit.Value) {
				return
			}
			v := lit.Value
			c := constraint(name)
			switch op {
			case "=":
				if c.equal == nil {
					c.equal, c.equalTerm = &v, expr
				}
			case ">", ">=":
				if v.Type != ColInteger {
					return
				}
				strict := op == ">"
				if c.lower == nil || ValueCompare(v, *c.lower) > 0 || (ValueCompare(v, *c.lower) == 0 && strict) {
					c.lower, c.lowerStrict, c.lowerTerm = &v, strict, expr
				}
			case "<", "<=":
				if v.Type != ColInteger {
					return
				}
				strict := op == "<"
				if c.upper == nil || ValueCompare(v, *c.upper) < 0 || (ValueCompare(v, *c.upper) == 0 && strict) {
					c.upper, c.upperStrict, c.upperTerm = &v, strict, expr
				}
			}
		}
		if in, ok := expr.(*InListExpr); ok && !in.Not {
			col, ok := in.Expr.(*ColumnRefExpr)
			if !ok {
				return
			}
			name := coveringColumn(col.Name, table, alias)
			var list []Value
			seen := make(map[string]bool)
			for _, item := range in.List {
				lit, ok := item.(*LiteralExpr)
				if !ok {
					return
				}
				if lit.Value.Null {
					continue
				}
				if !valid(name, lit.Value) {
					return
				}
				key := string(valueIdentity(lit.Value))
				if !seen[key] {
					list = append(list, lit.Value)
					seen[key] = true
				}
			}
			if len(list) > 1024 {
				return
			}
			c := constraint(name)
			if c.listTerm == nil {
				c.list, c.listTerm = list, expr
			}
		}
	}
	visit(where)
	return result
}

func tupleResidual(expr Expression, enforced map[Expression]bool) Expression {
	if expr == nil || enforced[expr] {
		return nil
	}
	if b, ok := expr.(*BinaryExpr); ok && b.Op == "AND" {
		left, right := tupleResidual(b.Left, enforced), tupleResidual(b.Right, enforced)
		if left == nil {
			return right
		}
		if right == nil {
			return left
		}
		return &BinaryExpr{Op: "AND", Left: left, Right: right}
	}
	return expr
}

func tupleOrdered(stmt *SelectStmt, columns []string, constraints map[string]*tupleConstraint, info *TableInfo) (bool, bool) {
	if len(stmt.OrderBy) == 0 {
		return true, false
	}
	position := 0
	for position < len(columns) {
		c := constraints[columns[position]]
		if c == nil || c.equal == nil {
			break
		}
		position++
	}
	var reverse bool
	directionSet := false
	for _, order := range stmt.OrderBy {
		ref, ok := order.Expr.(*ColumnRefExpr)
		if !ok {
			return false, false
		}
		name := coveringColumn(ref.Name, info.Name, stmt.FromAlias)
		for _, selected := range stmt.Columns {
			if selected.Alias != "" && strings.EqualFold(selected.Alias, ref.Name) {
				return false, false
			}
		}
		if c := constraints[name]; c != nil && c.equal != nil {
			continue
		}
		if position >= len(columns) || columns[position] != name {
			return false, false
		}
		var col ColumnDef
		for _, def := range info.Columns {
			if def.Name == name {
				col = def
				break
			}
		}
		// Integer encodings match SQL ordering, including the NULL band. Other
		// types retain the general sorter rather than assume collation semantics.
		if col.ColType != ColInteger {
			return false, false
		}
		nonNull := col.NotNull
		if c := constraints[name]; c != nil {
			nonNull = nonNull || c.equal != nil || c.listTerm != nil || c.lower != nil || c.upper != nil
		}
		if !nonNull && ((!order.Desc && order.NullsLast) || (order.Desc && order.NullsFirst)) {
			return false, false
		}
		if directionSet && reverse != order.Desc {
			return false, false
		}
		reverse = order.Desc
		directionSet = true
		position++
	}
	return true, reverse
}

func buildTuplePlan(stmt *SelectStmt, info *TableInfo, idx *IndexInfo, constraints map[string]*tupleConstraint) *tupleReadPlan {
	columns := info.PKColumns
	prefix := []byte(tableDataPrefix(info.Name))
	if idx != nil {
		columns = idx.columnNames()
		prefix = []byte(indexDataPrefix(idx.Name))
	}
	if len(columns) < 2 {
		return nil
	}
	appendValue := func(prefix []byte, value Value) []byte {
		if idx == nil {
			return appendClusteredValue(copyBytes(prefix), value)
		}
		key := append(copyBytes(prefix), hex.EncodeToString([]byte(valueToSortableKey(value)))...)
		return append(key, ':')
	}
	plan := &tupleReadPlan{index: idx, info: info}
	plan.ordered, plan.reverse = tupleOrdered(stmt, columns, constraints, info)
	starts := [][]byte{prefix}
	enforced := make(map[Expression]bool)
	position := 0
	for position < len(columns) {
		c := constraints[columns[position]]
		if c == nil {
			break
		}
		var values []Value
		var term Expression
		if c.equal != nil {
			values = []Value{*c.equal}
			term = c.equalTerm
		} else if c.listTerm != nil {
			values = c.list
			term = c.listTerm
		} else {
			break
		}
		if len(values) > 0 && len(starts) > 1024/len(values) {
			break
		}
		var next [][]byte
		for _, start := range starts {
			for _, value := range values {
				next = append(next, appendValue(start, value))
			}
		}
		starts = next
		enforced[term] = true
		position++
		plan.score += 16
	}
	if position == len(columns) && idx == nil {
		plan.points = starts
	}
	bounded := position > 0
	for _, start := range starts {
		low, high := start, prefixSuccessor(start)
		if position < len(columns) {
			c := constraints[columns[position]]
			if c != nil && (c.lower != nil || c.upper != nil) {
				// Integer comparisons exclude NULL and all other type bands.
				low = appendValue(start, NewIntValue(math.MinInt64))
				if c.lower != nil {
					low = appendValue(start, *c.lower)
					if c.lowerStrict {
						low = prefixSuccessor(low)
					}
					enforced[c.lowerTerm] = true
				}
				if c.upper != nil {
					high = appendValue(start, *c.upper)
					if !c.upperStrict {
						high = prefixSuccessor(high)
					}
					enforced[c.upperTerm] = true
				}
				bounded = true
			}
		}
		if bytes.Compare(low, high) < 0 {
			plan.intervals = append(plan.intervals, coveringInterval{low, high})
		}
	}
	if !bounded && (!plan.ordered || len(stmt.OrderBy) == 0 || stmt.Limit < 0) {
		return nil
	}
	sort.Slice(plan.intervals, func(i, j int) bool { return bytes.Compare(plan.intervals[i].lower, plan.intervals[j].lower) < 0 })
	if plan.points != nil {
		sort.Slice(plan.points, func(i, j int) bool { return bytes.Compare(plan.points[i], plan.points[j]) < 0 })
		plan.score += 64
	}
	if position < len(columns) {
		if c := constraints[columns[position]]; c != nil && (c.lower != nil || c.upper != nil) {
			plan.score += 4
		}
	}
	if plan.ordered && len(stmt.OrderBy) > 0 {
		plan.score += 8
	}
	plan.residual = tupleResidual(stmt.Where, enforced)
	return plan
}

func (s *Schema) tuplePlan(stmt *SelectStmt, info *TableInfo) *tupleReadPlan {
	if info == nil || stmt.FromExpr != nil || len(stmt.Joins) != 0 {
		return nil
	}
	// Complete equalities already have a minimal point lookup path.
	if _, ok := s.primaryKeyLookup(stmt.Where, info.Name, info); ok {
		return nil
	}
	constraints := tupleConstraints(stmt.Where, info.Name, stmt.FromAlias, info)
	best := buildTuplePlan(stmt, info, nil, constraints)
	for _, idx := range s.tableIndexes(info.Name) {
		if len(idx.columnNames()) < 2 {
			continue
		}
		plan := buildTuplePlan(stmt, info, idx, constraints)
		if plan == nil {
			continue
		}
		if payload := s.lookups().covering[idx]; payload != nil && queryCovered(stmt, info.Name, stmt.FromAlias, info, payload) {
			plan.covered = true
			plan.score += 2
		}
		if best == nil || plan.score > best.score || (plan.score == best.score && best.index != nil && idx.Name < best.index.Name) {
			best = plan
		}
	}
	return best
}

func tupleSafePredicate(expr Expression) bool {
	if rangePredicate(expr) {
		return true
	}
	switch e := expr.(type) {
	case *BinaryExpr:
		return (e.Op == "AND" || e.Op == "OR") && tupleSafePredicate(e.Left) && tupleSafePredicate(e.Right)
	case *InListExpr:
		if _, ok := e.Expr.(*ColumnRefExpr); !ok {
			return false
		}
		for _, item := range e.List {
			if _, ok := item.(*LiteralExpr); !ok {
				return false
			}
		}
		return true
	case *BetweenExpr:
		return tupleSafePredicate(e.Expr) && tupleSafePredicate(e.Low) && tupleSafePredicate(e.High)
	}
	return false
}

func compositeIndexRowKey(idx *IndexInfo, key []byte) ([]byte, error) {
	remaining := key[len(indexDataPrefix(idx.Name)):]
	for range idx.columnNames() {
		n := bytes.IndexByte(remaining, ':')
		if n < 0 {
			return nil, fmt.Errorf("invalid key for index %s", idx.Name)
		}
		remaining = remaining[n+1:]
	}
	if len(remaining) == 0 {
		return nil, fmt.Errorf("missing row key for index %s", idx.Name)
	}
	return remaining, nil
}

func (s *Schema) tupleRows(stmt *SelectStmt, plan *tupleReadPlan, trackKeys bool) ([]map[string]Value, [][]string, bool, error) {
	var rows []map[string]Value
	var keys [][]string
	simple := !hasAggregates(stmt) && len(stmt.GroupBy) == 0 && stmt.Having == nil && !stmt.Distinct && !stmt.trackRows
	ordered := plan.ordered && simple
	limit := int64(-1)
	if ordered && tupleSafePredicate(stmt.Where) && stmt.Limit >= 0 && stmt.Offset >= 0 {
		limit = limitWindow(stmt.Limit, stmt.Offset)
	}
	if stmt.Limit == 0 && simple {
		return nil, nil, ordered, nil
	}
	qualified := needsQualifiedRows(stmt)
	accept := func(data, key []byte, info *TableInfo) error {
		if err := s.checkInterrupt(); err != nil {
			return err
		}
		row := decodeRow(data, info)
		if qualified {
			qualifyRow(row, plan.info.Name)
			qualifyRow(row, stmt.FromAlias)
		}
		if plan.residual != nil {
			match, err := s.evalCondition(plan.residual, row)
			if err != nil {
				return err
			}
			if !match {
				return nil
			}
		}
		if stmt.Lock != nil && ordered && limit >= 0 {
			if err := s.requestRowLocks([]string{string(key)}, stmt.Lock); err != nil {
				if stmt.Lock.Wait == LockSkipLocked && errors.Is(err, ErrLockNotAvailable) {
					return nil
				}
				return err
			}
		}
		rows = append(rows, row)
		if trackKeys {
			keys = append(keys, []string{string(key)})
		}
		return nil
	}
	if plan.points != nil {
		values, err := s.schemaMultiGet(plan.points)
		if err != nil {
			return nil, nil, false, err
		}
		for position := range plan.points {
			i := position
			if plan.reverse {
				i = len(plan.points) - 1 - position
			}
			if values[i].Found {
				if err := accept(values[i].Value, plan.points[i], plan.info); err != nil {
					return nil, nil, false, err
				}
			}
			if limit >= 0 && int64(len(rows)) >= limit {
				break
			}
		}
		return rows, keys, ordered, nil
	}
	for position := range plan.intervals {
		i := position
		if plan.reverse {
			i = len(plan.intervals) - 1 - position
		}
		interval := plan.intervals[i]
		it := s.newRangeIterator(interval.lower, interval.upper, plan.reverse)
		err := func() error {
			defer it.Close()
			for it.Valid() {
				data, key := it.Value(), it.Key().UserKey
				payloadInfo := plan.info
				if plan.index != nil {
					var err error
					key, err = compositeIndexRowKey(plan.index, key)
					if err != nil {
						return err
					}
					if plan.covered {
						if len(data) == 0 || data[0] != 1 {
							return fmt.Errorf("invalid covering entry for %s", plan.index.Name)
						}
						data = data[1:]
						payloadInfo = s.lookups().covering[plan.index]
					} else {
						var found bool
						data, found, err = s.schemaGet(key)
						if err != nil {
							return err
						}
						if !found {
							return fmt.Errorf("missing row for index %s", plan.index.Name)
						}
					}
				}
				if err := accept(data, key, payloadInfo); err != nil {
					return err
				}
				if limit >= 0 && int64(len(rows)) >= limit {
					break
				}
				if plan.reverse {
					it.Prev()
				} else {
					it.Next()
				}
			}
			return storage.IteratorError(it)
		}()
		if err != nil {
			return nil, nil, false, err
		}
		if limit >= 0 && int64(len(rows)) >= limit {
			break
		}
	}
	return rows, keys, ordered, nil
}

func (p *tupleReadPlan) description() string {
	name := "PRIMARY"
	if p.index != nil {
		name = p.index.Name
	}
	if p.points != nil {
		return fmt.Sprintf("%s: %d point lookups", name, len(p.points))
	}
	direction := "forward"
	if p.reverse {
		direction = "backward"
	}
	return fmt.Sprintf("%s: %d bounded ranges, %s", name, len(p.intervals), direction)
}
