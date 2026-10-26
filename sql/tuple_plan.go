package sql

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"

	"slopdb/storage"
)

// Tuple access uses equality prefixes, followed by an IN list or typed range.
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

// SQL parses signed numeric literals as unary expressions. Recognize their
// exact values without folding row-dependent expressions or hiding overflow.
func tupleLiteral(expr Expression) (*LiteralExpr, bool) {
	if literal, ok := expr.(*LiteralExpr); ok {
		return literal, true
	}
	if binary, ok := expr.(*BinaryExpr); ok {
		switch binary.Op {
		case "+", "-", "*", "%", "||", "&", "|", "<<", ">>":
			if _, ok := tupleLiteral(binary.Left); !ok {
				return nil, false
			}
			if _, ok := tupleLiteral(binary.Right); !ok {
				return nil, false
			}
			if value, err := (&Schema{}).evalExpr(binary, nil); err == nil {
				return &LiteralExpr{Value: value}, true
			}
		}
		// Division depends on the execution dialect (MySQL 5/2 is decimal).
		// Leave it to ordinary expression evaluation rather than proving a
		// key bound with the native dialect's integer quotient.
		return nil, false
	}
	unary, ok := expr.(*UnaryExpr)
	if !ok || (unary.Op != "+" && unary.Op != "-") {
		return nil, false
	}
	literal, ok := tupleLiteral(unary.Right)
	if !ok || (!literal.Value.Null && literal.Value.Type != ColInteger && literal.Value.Type != ColReal) {
		return nil, false
	}
	v := literal.Value
	if unary.Op == "-" && !v.Null {
		if v.Type == ColInteger {
			if v.Int == math.MinInt64 {
				return nil, false
			}
			v.Int = -v.Int
		} else {
			v.Real = -v.Real
		}
	}
	return &LiteralExpr{Value: v}, true
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
		if between, ok := expr.(*BetweenExpr); ok && !between.Not {
			// Extract each inclusive bound using the same type checks and
			// intersection rules as comparisons. Keep the original BETWEEN
			// as a residual: a bound may be unusable or superseded by another
			// conjunct, so neither comparison alone enforces the whole term.
			visit(&BinaryExpr{Op: ">=", Left: between.Expr, Right: between.Low})
			visit(&BinaryExpr{Op: "<=", Left: between.Expr, Right: between.High})
			return
		}
		if b, ok := expr.(*BinaryExpr); ok {
			if b.Op == "AND" {
				visit(b.Left)
				visit(b.Right)
				return
			}
			col, colOK := b.Left.(*ColumnRefExpr)
			lit, litOK := tupleLiteral(b.Right)
			op := b.Op
			if !colOK || !litOK {
				col, colOK = b.Right.(*ColumnRefExpr)
				lit, litOK = tupleLiteral(b.Left)
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
				strict := op == ">"
				if c.lower == nil || ValueCompare(v, *c.lower) > 0 || (ValueCompare(v, *c.lower) == 0 && strict) {
					c.lower, c.lowerStrict, c.lowerTerm = &v, strict, expr
				}
			case "<", "<=":
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
				lit, ok := tupleLiteral(item)
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
		// Clustered sortable values preserve the default binary collation.
		// Secondary variable encodings are rejected by buildTuplePlan.
		if len(col.EnumValues) > 0 {
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
	prefix := []byte(info.storagePrefix(info.Name))
	if idx != nil {
		columns = idx.columnNames()
		prefix = []byte(idx.storagePrefix())
	}
	if len(columns) == 0 {
		return nil
	}
	appendValue := func(prefix []byte, value Value, column string) []byte {
		return appendAccessValueColumn(prefix, value, idx, info, column)
	}
	plan := &tupleReadPlan{index: idx, info: info}
	plan.ordered, plan.reverse = tupleOrdered(stmt, columns, constraints, info)
	if idx != nil {
		for _, name := range columns {
			for _, column := range info.Columns {
				if column.Name == name && column.ColType == ColReal {
					// Legacy secondary encodings distinguish the sign of zero.
					plan.ordered, plan.reverse = false, false
				}
			}
		}
	}
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
				next = append(next, appendValue(start, value, columns[position]))
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
			if c != nil && (c.lower != nil || c.upper != nil) && !collatedTextRange(info, idx, columns[position]) {
				intervals, exact := accessRangeIntervals(start, columns[position], info, idx, c)
				// Legacy secondary TEXT/BLOB components have no terminator.
				// Their bounds are conservative and require SQL rechecking.
				if exact {
					enforced[c.lowerTerm] = true
					enforced[c.upperTerm] = true
				}
				plan.intervals = append(plan.intervals, intervals...)
				bounded = true
				continue
			}
		}
		if bytes.Compare(low, high) < 0 {
			plan.intervals = append(plan.intervals, coveringInterval{low, high})
		}
	}
	if !bounded && (!plan.ordered || len(stmt.OrderBy) == 0 || stmt.Limit < 0) {
		return nil
	}
	plan.intervals = normalizeAccessIntervals(plan.intervals)
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
	if variableIndexColumns(info, idx) {
		// A delimiter following an unterminated variable value can also be
		// a prefix of a longer value. Do not elide equality predicates or
		// claim SQL ordering from that legacy secondary layout.
		plan.residual = stmt.Where
		plan.ordered, plan.reverse = false, false
	}
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
	var constraints map[string]*tupleConstraint
	p := stmt.prepared
	if p != nil {
		constraints = p.constraints(stmt)
	} else {
		constraints = tupleConstraints(stmt.Where, info.Name, stmt.FromAlias, info)
	}
	best := buildTuplePlan(stmt, info, nil, constraints)
	costFor := func(plan *tupleReadPlan) float64 {
		if plan == nil {
			return math.Inf(1)
		}
		rows, known := s.accessEstimate(info, plan.index, constraints)
		if !known {
			return math.Inf(1)
		}
		if plan.ordered && stmt.Limit >= 0 && tupleSafePredicate(stmt.Where) {
			rows = math.Min(rows, float64(limitWindow(stmt.Limit, stmt.Offset)))
		}
		cost := accessCost(rows, plan.index != nil, plan.covered)
		if !plan.ordered && len(stmt.OrderBy) > 0 {
			cost += rows * math.Log2(math.Max(2, rows))
		}
		return cost
	}
	bestCost := costFor(best)
	for _, idx := range s.tableIndexes(info.Name) {
		if s.isClusteredIndex(idx) || idx.Fulltext {
			continue
		}
		plan := buildTuplePlan(stmt, info, idx, constraints)
		if plan == nil {
			continue
		}
		covered := false
		if p != nil {
			covered = p.tupleCoverage(s, stmt, idx)
		} else if payload := s.lookups().covering[idx]; payload != nil {
			covered = queryCovered(stmt, info.Name, stmt.FromAlias, info, payload)
		}
		if covered {
			plan.covered = true
			plan.score += 2
		}
		cost := costFor(plan)
		if cost < bestCost || cost == bestCost && (best == nil || plan.score > best.score || (plan.score == best.score && best.index != nil && idx.Name < best.index.Name)) {
			best = plan
			bestCost = cost
		}
	}
	// A non-covering index with large fanout costs more than one row scan.
	// Preserve ordered LIMIT plans, which can stop without materializing a sort.
	if stats := s.distributions[info.Name]; stats != nil && best != nil && !(best.ordered && stmt.Limit >= 0) && len(stmt.OrderBy) == 0 && bestCost > float64(stats.Rows) && bestCost < math.Inf(1) {
		return nil
	}
	return best
}

func tupleSafePredicate(expr Expression) bool {
	if rangePredicate(expr) {
		return true
	}
	switch e := expr.(type) {
	case *UnaryExpr:
		_, ok := tupleLiteral(e)
		return ok
	case *BinaryExpr:
		switch e.Op {
		case "AND", "OR", "=", "!=", "<>", "<", ">", "<=", ">=":
			return tupleSafePredicate(e.Left) && tupleSafePredicate(e.Right)
		}
	case *InListExpr:
		if _, ok := e.Expr.(*ColumnRefExpr); !ok {
			return false
		}
		for _, item := range e.List {
			if _, ok := tupleLiteral(item); !ok {
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
	remaining := key[len(idx.storagePrefix()):]
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
	if !stmt.CalcFoundRows && ordered && tupleSafePredicate(stmt.Where) && stmt.Limit >= 0 && stmt.Offset >= 0 {
		limit = limitWindow(stmt.Limit, stmt.Offset)
	}
	if !stmt.CalcFoundRows && stmt.Limit == 0 && simple {
		return nil, nil, ordered, nil
	}
	qualified := needsQualifiedRows(stmt)
	needed, partial := s.tupleProjection(stmt, plan)
	var projectedColumns, coveredColumns []pointDecodeMetadata
	if partial {
		projectedColumns = tupleProjectionColumns(plan.info, needed)
		if plan.covered && plan.index != nil {
			coveredColumns = tupleProjectionColumns(s.lookups().covering[plan.index], needed)
		}
	}
	var seen map[string]bool
	if variableIndexColumns(plan.info, plan.index) && len(plan.intervals) > 1 {
		seen = make(map[string]bool)
	}
	accept := func(data, key []byte, info *TableInfo) error {
		if err := s.checkInterrupt(); err != nil {
			return err
		}
		if seen != nil {
			identity := string(key)
			if seen[identity] {
				return nil
			}
			seen[identity] = true
		}
		var row map[string]Value
		if partial {
			columns := projectedColumns
			if info != plan.info {
				columns = coveredColumns
			}
			row = decodeTupleProjection(data, columns, len(needed))
		} else {
			row = decodeRow(data, info)
		}
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
		it := s.newCursorRangeIterator(interval.lower, interval.upper, plan.reverse)
		err := func() error {
			defer it.Close()
			for it.Valid() {
				data, key := it.Value(), it.Key().UserKey
				payloadInfo := plan.info
				if plan.index != nil {
					var err error
					key, err = s.accessIndexRowKey(&accessPath{info: plan.info, index: plan.index}, interval, key, data)
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
