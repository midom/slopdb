package sql

import (
	"bytes"
	"fmt"
	"math"
	"sort"
	"strings"

	"slopdb/storage"
)

// A secondary entry contains only its declared columns and the primary key,
// never an implicit copy of the entire row. The binary payload also preserves
// embedded separators, NULLs and exact REAL values without parsing index keys.
func coveringTableInfo(idx *IndexInfo, table *TableInfo) *TableInfo {
	if table == nil {
		return nil
	}
	info := &TableInfo{}
	seen := make(map[string]bool)
	add := func(names []string) {
		for _, name := range names {
			if seen[name] {
				continue
			}
			for _, col := range table.Columns {
				if col.Name == name {
					info.Columns = append(info.Columns, col)
					seen[name] = true
					break
				}
			}
		}
	}
	for _, name := range idx.columnNames() {
		if idx.prefixLength(name) == 0 {
			add([]string{name})
		}
	}
	add(table.PKColumns)
	return info
}

func (s *Schema) encodeIndexValue(idx *IndexInfo, row map[string]Value) []byte {
	if s.isAutoIndex(idx) {
		return nil
	}
	info := s.lookups().covering[idx]
	if info == nil { // CREATE INDEX has not published its catalog entry yet.
		info = coveringTableInfo(idx, s.tables[idx.Table])
	}
	data := make([]byte, 1, 1+len(info.Columns)*9)
	data[0] = 1
	for _, col := range info.Columns {
		data = appendValue(data, row[col.Name])
	}
	return data
}

type coveringInterval struct{ lower, upper []byte }
type coveringReadPlan struct {
	index     *IndexInfo
	info      *TableInfo
	intervals []coveringInterval
	ordered   bool
	reverse   bool
	score     int
}

// An exact projection of bare index columns already has the final, caller-owned
// row map. Avoid copying it to an expression map and then another result map.
func exactCoveringProjection(stmt *SelectStmt, info *TableInfo) bool {
	if stmt.SelectAll || needsQualifiedRows(stmt) || hasAggregates(stmt) || len(stmt.GroupBy) != 0 || len(stmt.Columns) != len(info.Columns) {
		return false
	}
	for _, column := range info.Columns {
		matches := 0
		for _, selected := range stmt.Columns {
			ref, ok := selected.Expr.(*ColumnRefExpr)
			if ok && ref.Name == column.Name && (selected.Alias == "" || selected.Alias == ref.Name) {
				matches++
			}
		}
		if matches != 1 {
			return false
		}
	}
	return true
}

func coveringColumn(name, table, alias string) string {
	name = strings.ToLower(name)
	if qualifier, column, qualified := strings.Cut(name, "."); qualified {
		if qualifier != table && (alias == "" || qualifier != alias) {
			return ""
		}
		return column
	}
	return name
}

func queryCovered(stmt *SelectStmt, table, alias string, tableInfo, payload *TableInfo) bool {
	available := make(map[string]bool, len(payload.Columns))
	for _, col := range payload.Columns {
		available[col.Name] = true
	}
	all := func() bool {
		for _, col := range tableInfo.Columns {
			if !available[col.Name] {
				return false
			}
		}
		return true
	}
	aliases := make(map[string]Expression)
	for _, col := range stmt.Columns {
		if col.Alias != "" {
			aliases[strings.ToLower(col.Alias)] = col.Expr
		}
	}
	var covered func(Expression, bool) bool
	covered = func(expr Expression, allowAlias bool) bool {
		switch e := expr.(type) {
		case nil, *LiteralExpr:
			return true
		case *ColumnRefExpr:
			name := coveringColumn(e.Name, table, alias)
			if allowAlias {
				if value, ok := aliases[strings.ToLower(e.Name)]; ok {
					return covered(value, false)
				}
			}
			return available[name]
		case *BinaryExpr:
			return covered(e.Left, allowAlias) && covered(e.Right, allowAlias)
		case *UnaryExpr:
			return covered(e.Right, allowAlias)
		case *FuncCallExpr:
			for _, arg := range e.Args {
				if col, ok := arg.(*ColumnRefExpr); ok && col.Name == "*" && strings.EqualFold(e.Name, "COUNT") {
					continue
				}
				if !covered(arg, allowAlias) {
					return false
				}
			}
			return true
		case *InListExpr:
			if !covered(e.Expr, allowAlias) {
				return false
			}
			for _, arg := range e.List {
				if !covered(arg, allowAlias) {
					return false
				}
			}
			return true
		case *BetweenExpr:
			return covered(e.Expr, allowAlias) && covered(e.Low, allowAlias) && covered(e.High, allowAlias)
		case *CaseExpr:
			if !covered(e.Expr, allowAlias) || !covered(e.ElseExpr, allowAlias) {
				return false
			}
			for _, when := range e.Whens {
				if !covered(when.Cond, allowAlias) || !covered(when.Then, allowAlias) {
					return false
				}
			}
			return true
		default: // Subqueries can refer to arbitrary outer columns.
			return false
		}
	}
	if stmt.SelectAll && !all() {
		return false
	}
	for _, col := range stmt.Columns {
		if (col.Star && !all()) || !covered(col.Expr, false) {
			return false
		}
	}
	if !covered(stmt.Where, true) || !covered(stmt.Having, true) {
		return false
	}
	for _, group := range stmt.GroupBy {
		if !covered(group, true) {
			return false
		}
	}
	for _, order := range stmt.OrderBy {
		if !covered(order.Expr, true) {
			return false
		}
	}
	return true
}

// Existing secondary keys provide reliable range order for integer columns.
// Variable-size components retain general sorting, including prefix strings.
func coveringIndexOrder(stmt *SelectStmt, idx *IndexInfo, info *TableInfo) (bool, bool) {
	columns := idx.columnNames()
	if len(stmt.OrderBy) == 0 || len(stmt.OrderBy) > len(columns) {
		return false, false
	}
	reverse := stmt.OrderBy[0].Desc
	for i, order := range stmt.OrderBy {
		ref, ok := order.Expr.(*ColumnRefExpr)
		if !ok || order.Desc != reverse || coveringColumn(ref.Name, idx.Table, stmt.FromAlias) != columns[i] {
			return false, false
		}
		for _, col := range stmt.Columns {
			if strings.EqualFold(col.Alias, columns[i]) {
				return false, false
			}
		}
		for _, col := range info.Columns {
			if col.Name == columns[i] && col.ColType != ColInteger {
				return false, false
			}
		}
	}
	return true, reverse
}

func coveringBounds(stmt *SelectStmt, idx *IndexInfo, info *TableInfo) ([]coveringInterval, bool) {
	prefix := []byte(idx.storagePrefix())
	low, high := prefix, prefixSuccessor(prefix)
	var typ ColumnType
	for _, col := range info.Columns {
		if col.Name == idx.Column {
			typ = col.ColType
		}
	}
	validValue := func(v Value) bool {
		return !v.Null && v.Type == typ && (typ != ColReal || (v.Real != 0 && !math.IsNaN(v.Real)))
	}
	bounded := false
	var points []coveringInterval
	var bounds func(Expression)
	bounds = func(expr Expression) {
		if b, ok := expr.(*BinaryExpr); ok {
			if b.Op == "AND" {
				bounds(b.Left)
				bounds(b.Right)
				return
			}
			ref, ok := b.Left.(*ColumnRefExpr)
			lit, litOK := b.Right.(*LiteralExpr)
			op := b.Op
			if !ok || !litOK {
				ref, ok = b.Right.(*ColumnRefExpr)
				lit, litOK = b.Left.(*LiteralExpr)
				switch op {
				case ">":
					op = "<"
				case ">=":
					op = "<="
				case "<":
					op = ">"
				case "<=":
					op = ">="
				}
			}
			if !ok || !litOK || !validValue(lit.Value) || coveringColumn(ref.Name, idx.Table, stmt.FromAlias) != idx.Column {
				return
			}
			value := []byte(indexValuePrefix(idx, lit.Value))
			var lower, upper []byte
			switch op {
			case "=":
				lower, upper = value, prefixSuccessor(value)
			case ">", ">=", "<", "<=":
				if typ != ColInteger {
					return
				}
				switch op {
				case ">":
					lower = prefixSuccessor(value)
				case ">=":
					lower = value
				case "<":
					upper = value
				case "<=":
					upper = prefixSuccessor(value)
				}
			default:
				return
			}
			if lower != nil && bytes.Compare(lower, low) > 0 {
				low = lower
			}
			if upper != nil && bytes.Compare(upper, high) < 0 {
				high = upper
			}
			bounded = true
		}
		if in, ok := expr.(*InListExpr); ok && !in.Not && points == nil {
			ref, ok := in.Expr.(*ColumnRefExpr)
			if !ok || coveringColumn(ref.Name, idx.Table, stmt.FromAlias) != idx.Column {
				return
			}
			candidate := make([]coveringInterval, 0, len(in.List))
			for _, arg := range in.List {
				lit, ok := arg.(*LiteralExpr)
				if !ok {
					return
				}
				if lit.Value.Null {
					continue
				}
				if !validValue(lit.Value) {
					return
				}
				key := []byte(indexValuePrefix(idx, lit.Value))
				candidate = append(candidate, coveringInterval{key, prefixSuccessor(key)})
			}
			points, bounded = candidate, true
		}
	}
	bounds(stmt.Where)
	if bounded && typ == ColInteger {
		// Comparisons with non-NULL literals never match NULL. An upper-only
		// range must exclude that band before its predicate can be skipped.
		minimum := []byte(indexValuePrefix(idx, NewIntValue(math.MinInt64)))
		if bytes.Compare(low, minimum) < 0 {
			low = minimum
		}
	}
	if points == nil {
		points = []coveringInterval{{low, high}}
	} else {
		for i := range points {
			if bytes.Compare(points[i].lower, low) < 0 {
				points[i].lower = low
			}
			if bytes.Compare(points[i].upper, high) > 0 {
				points[i].upper = high
			}
		}
	}
	sort.Slice(points, func(i, j int) bool { return bytes.Compare(points[i].lower, points[j].lower) < 0 })
	merged := points[:0]
	for _, point := range points {
		if bytes.Compare(point.lower, point.upper) >= 0 {
			continue
		}
		if len(merged) > 0 && bytes.Compare(point.lower, merged[len(merged)-1].upper) <= 0 {
			if bytes.Compare(point.upper, merged[len(merged)-1].upper) > 0 {
				merged[len(merged)-1].upper = point.upper
			}
		} else {
			merged = append(merged, point)
		}
	}
	return merged, bounded
}

func (s *Schema) coveringPlan(stmt *SelectStmt, table string, info *TableInfo) *coveringReadPlan {
	// A complete primary key needs one row read. A covering index with a
	// missing middle component may instead scan an unbounded equality prefix.
	if _, exact := s.primaryKeyLookup(stmt.Where, table, info); exact {
		return nil
	}
	if info == nil || stmt.Lock != nil || stmt.trackRows || len(stmt.Joins) != 0 || stmt.FromExpr != nil {
		return nil
	}
	var best *coveringReadPlan
	for _, idx := range s.tableIndexes(table) {
		payload := s.lookups().covering[idx]
		if payload == nil || (stmt.SelectAll && len(payload.Columns) < len(info.Columns)) || !queryCovered(stmt, table, stmt.FromAlias, info, payload) {
			continue
		}
		intervals, bounded := coveringBounds(stmt, idx, info)
		ordered, reverse := coveringIndexOrder(stmt, idx, info)
		// A direct PK lookup is cheaper than scanning an unrelated covering index.
		if !bounded {
			if _, ok := s.primaryKeyLookup(stmt.Where, table, info); ok {
				continue
			}
			if _, ok := s.primaryKeyList(stmt.Where, table, info); ok {
				continue
			}
		}
		score := 0
		if bounded {
			score += 4
		}
		if ordered {
			score += 2
		}
		if best == nil || score > best.score || (score == best.score && (len(payload.Columns) < len(best.info.Columns) || (len(payload.Columns) == len(best.info.Columns) && idx.Name < best.index.Name))) {
			best = &coveringReadPlan{index: idx, info: payload, intervals: intervals, ordered: ordered, reverse: reverse, score: score}
		}
	}
	return best
}

func (s *Schema) coveringRows(stmt *SelectStmt, plan *coveringReadPlan) ([]map[string]Value, error) {
	rows := make([]map[string]Value, 0)
	limit := int64(-1)
	if plan.ordered && !stmt.Distinct && len(stmt.GroupBy) == 0 && stmt.Having == nil && !hasAggregates(stmt) && rangePredicate(stmt.Where) && stmt.Limit >= 0 && stmt.Offset >= 0 {
		limit = limitWindow(stmt.Limit, stmt.Offset)
		if stmt.Limit == 0 {
			return rows, nil
		}
	}
	needQualified := needsQualifiedRows(stmt)
	conditionSatisfied := stmt.Where == nil
	for _, col := range plan.info.Columns {
		if col.Name == plan.index.Column && col.ColType == ColInteger {
			conditionSatisfied = coveredRangePredicate(stmt.Where, plan.index.Column, plan.index.Table, stmt.FromAlias)
			break
		}
	}
	for pos := range plan.intervals {
		interval := plan.intervals[pos]
		if plan.reverse {
			interval = plan.intervals[len(plan.intervals)-1-pos]
		}
		iter := s.newRangeIterator(interval.lower, interval.upper, plan.reverse)
		for iter.Valid() {
			if err := s.checkInterrupt(); err != nil {
				iter.Close()
				return nil, err
			}
			data := iter.Value()
			if len(data) == 0 || data[0] != 1 {
				iter.Close()
				return nil, fmt.Errorf("invalid covering entry for index %s", plan.index.Name)
			}
			row := decodeRow(data[1:], plan.info)
			if needQualified {
				qualifyRow(row, plan.index.Table)
				qualifyRow(row, stmt.FromAlias)
			}
			matches := true
			if stmt.Where != nil && !conditionSatisfied {
				var err error
				matches, err = s.evalCondition(stmt.Where, row)
				if err != nil {
					iter.Close()
					return nil, err
				}
			}
			if matches {
				rows = append(rows, row)
			}
			if limit >= 0 && int64(len(rows)) >= limit {
				break
			}
			if plan.reverse {
				iter.Prev()
			} else {
				iter.Next()
			}
		}
		err := storage.IteratorError(iter)
		iter.Close()
		if err != nil {
			return nil, err
		}
		if limit >= 0 && int64(len(rows)) >= limit {
			break
		}
	}
	return rows, nil
}
