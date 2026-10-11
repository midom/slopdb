package sql

import (
	"bytes"
	"encoding/binary"
	"math"
	"strings"
	"time"

	"slopdb/storage"
)

// A positive literal IN list on a single, exactly typed primary key is a set
// of point reads. Other predicates retain normal SQL comparison semantics.
func (s *Schema) primaryKeyList(where Expression, table string, info *TableInfo) ([][]byte, bool) {
	if info == nil || len(info.PKColumns) != 1 {
		return nil, false
	}
	in, ok := where.(*InListExpr)
	if !ok {
		if b, ok := where.(*BinaryExpr); ok && b.Op == "AND" {
			if keys, ok := s.primaryKeyList(b.Left, table, info); ok {
				return keys, true
			}
			return s.primaryKeyList(b.Right, table, info)
		}
		return nil, false
	}
	if in.Not {
		return nil, false
	}
	col, ok := in.Expr.(*ColumnRefExpr)
	if !ok {
		return nil, false
	}
	name := strings.ToLower(col.Name)
	name = strings.TrimPrefix(name, table+".")
	if name != info.PKColumns[0] {
		return nil, false
	}
	var typ ColumnType
	for _, def := range info.Columns {
		if def.Name == name {
			typ = def.ColType
		}
	}
	seen := make(map[string]bool, len(in.List))
	keys := make([][]byte, 0, len(in.List))
	for _, expr := range in.List {
		lit, ok := expr.(*LiteralExpr)
		if !ok {
			return nil, false
		}
		if lit.Value.Null {
			continue
		}
		if lit.Value.Type != typ || (typ == ColReal && lit.Value.Real == 0) {
			return nil, false
		}
		var key []byte
		if typ == ColInteger {
			key = append([]byte(tableDataPrefix(table)), 1)
			key = binary.BigEndian.AppendUint64(key, uint64(lit.Value.Int)^(1<<63))
		} else {
			if typ == ColReal && math.IsNaN(lit.Value.Real) {
				return nil, false
			}
			key = info.encodePK(table, map[string]Value{name: lit.Value})
		}
		if !seen[string(key)] {
			seen[string(key)] = true
			keys = append(keys, key)
		}
	}
	return keys, true
}

// indexRangeRows pushes the limit through a type-strict index scan. Clustered
// scalar primary keys support every declared type and read row values directly;
// secondary integer ranges batch their point lookups.
// Joins, grouping, DISTINCT and row locking keep the general executor path.
func (s *Schema) indexRangeRows(stmt *SelectStmt, table string, info *TableInfo) ([]map[string]Value, bool, error) {
	if stmt.Lock != nil || stmt.trackRows || len(stmt.Joins) != 0 || stmt.Distinct || len(stmt.GroupBy) != 0 || stmt.Having != nil || hasAggregates(stmt) || stmt.Limit < 0 || stmt.Offset < 0 {
		return nil, false, nil
	}
	idx := s.findOrderIndex(stmt, table)
	if idx == nil {
		return nil, false, nil
	}
	for _, col := range stmt.Columns {
		if col.Alias == idx.Column {
			return nil, false, nil
		}
	}
	clustered := len(info.PKColumns) == 1 && idx.Column == info.PKColumns[0]
	var column ColumnDef
	for _, col := range info.Columns {
		if col.Name == idx.Column {
			column = col
		}
	}
	if column.ColType != ColInteger && !clustered {
		return nil, false, nil
	}
	var lower, upper *Value
	lowerStrict, upperStrict := false, false
	var bounds func(Expression)
	bounds = func(e Expression) {
		b, ok := e.(*BinaryExpr)
		if !ok {
			return
		}
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
		if !ok || !litOK || lit.Value.Null || lit.Value.Type != column.ColType {
			return
		}
		name := strings.ToLower(ref.Name)
		name = strings.TrimPrefix(name, table+".")
		if stmt.FromAlias != "" {
			name = strings.TrimPrefix(name, stmt.FromAlias+".")
		}
		if name != idx.Column {
			return
		}
		v := lit.Value
		switch op {
		case ">", ">=":
			strict := op == ">"
			if lower == nil || ValueCompare(v, *lower) > 0 || (ValueCompare(v, *lower) == 0 && strict) {
				lower = &v
				lowerStrict = strict
			}
		case "<", "<=":
			strict := op == "<"
			if upper == nil || ValueCompare(v, *upper) < 0 || (ValueCompare(v, *upper) == 0 && strict) {
				upper = &v
				upperStrict = strict
			}
		case "=":
			if lower == nil || ValueCompare(v, *lower) > 0 {
				lower = &v
				lowerStrict = false
			}
			if upper == nil || ValueCompare(v, *upper) < 0 {
				upper = &v
				upperStrict = false
			}
		}
	}
	bounds(stmt.Where)
	// Arbitrary residual predicates could have side effects/subqueries. Only
	// column/literal comparisons and boolean combinations are limit-safe here.
	if !rangePredicate(stmt.Where) {
		return nil, false, nil
	}
	prefix := []byte(indexDataPrefix(idx.Name))
	if clustered {
		prefix = []byte(tableDataPrefix(table))
	}
	tag := byte(1)
	if clustered {
		switch column.ColType {
		case ColReal:
			tag = 2
		case ColText:
			tag = 3
		case ColBlob:
			tag = 4
		}
		if column.ColType != ColInteger && !column.NotNull && lower == nil && upper == nil {
			// SQL semantics allow nullable noninteger primary keys.
			return nil, false, nil
		}
	}
	low := append(append([]byte(nil), prefix...), tag)
	high := append(append([]byte(nil), prefix...), tag+1)
	if lower != nil {
		if clustered {
			low = appendClusteredValue(append([]byte(nil), prefix...), *lower)
		} else {
			low = []byte(indexValuePrefix(idx, *lower))
		}
		if lowerStrict {
			low = prefixSuccessor(low)
		}
	}
	if upper != nil {
		if clustered {
			high = appendClusteredValue(append([]byte(nil), prefix...), *upper)
		} else {
			high = []byte(indexValuePrefix(idx, *upper))
		}
		if !upperStrict {
			high = prefixSuccessor(high)
		}
	}
	reverse := stmt.OrderBy[0].Desc
	start := low
	if reverse {
		start = high
	}
	// Indexed columns are type strict. Bounded ranges can seek directly to
	// their starting key without first probing other type bands.
	unbounded := lower == nil && upper == nil
	if unbounded {
		start = prefix
	}
	var iter storage.Iterator
	if unbounded && !clustered {
		iter = s.newIteratorSeek(start)
	} else {
		iter = s.newRangeIterator(low, high, reverse)
	}
	defer iter.Close()
	if err := storage.IteratorError(iter); err != nil {
		return nil, true, err
	}
	if unbounded && !clustered {
		// Nullable indexes still need the general path when ordering NULLs.
		if iter.Valid() && bytes.HasPrefix(iter.Key().UserKey, prefix) && iter.Key().UserKey[len(prefix)] != 1 {
			return nil, false, nil
		}
		if reverse {
			iter.Seek(high)
		}
	}
	if reverse && unbounded && !clustered {
		iter.Prev()
	}
	rows := make([]map[string]Value, 0, min(limitWindow(stmt.Limit, stmt.Offset), 1024))
	if stmt.Limit == 0 {
		return rows, true, nil
	}
	needQualified := needsQualifiedRows(stmt)
	if clustered {
		covered := coveredRangePredicate(stmt.Where, idx.Column, table, stmt.FromAlias)
		for iter.Valid() {
			if err := s.checkInterrupt(); err != nil {
				return nil, true, err
			}
			row := decodeRow(iter.Value(), info)
			if needQualified {
				qualifyRow(row, table)
				qualifyRow(row, stmt.FromAlias)
			}
			matches := covered
			if !covered {
				var err error
				matches, err = s.evalCondition(stmt.Where, row)
				if err != nil {
					return nil, true, err
				}
			}
			if matches {
				rows = append(rows, row)
				if int64(len(rows)) >= limitWindow(stmt.Limit, stmt.Offset) {
					break
				}
			}
			if reverse {
				iter.Prev()
			} else {
				iter.Next()
			}
		}
		return rows, true, storage.IteratorError(iter)
	}
	if coveredRangePredicate(stmt.Where, idx.Column, table, stmt.FromAlias) {
		for iter.Valid() && int64(len(rows)) < limitWindow(stmt.Limit, stmt.Offset) {
			keys := make([][]byte, 0, min(limitWindow(stmt.Limit, stmt.Offset)-int64(len(rows)), 128))
			for iter.Valid() && len(keys) < cap(keys) {
				if err := s.checkInterrupt(); err != nil {
					return nil, true, err
				}
				key := iter.Key().UserKey
				if !bytes.HasPrefix(key, prefix) || bytes.Compare(key, low) < 0 || bytes.Compare(key, high) >= 0 {
					break
				}
				if len(key) < len(prefix)+10 || key[len(prefix)] != 1 || key[len(prefix)+9] != ':' {
					return nil, false, nil
				}
				keys = append(keys, key[len(prefix)+10:])
				if reverse {
					iter.Prev()
				} else {
					iter.Next()
				}
			}
			if len(keys) == 0 {
				break
			}
			if err := storage.IteratorError(iter); err != nil {
				return nil, true, err
			}
			values, err := s.schemaMultiGet(keys)
			if err != nil {
				return nil, true, err
			}
			for _, v := range values {
				if v.Found {
					row := decodeRow(v.Value, info)
					if needQualified {
						qualifyRow(row, table)
						qualifyRow(row, stmt.FromAlias)
					}
					rows = append(rows, row)
				}
			}
		}
		return rows, true, storage.IteratorError(iter)
	}
	for iter.Valid() {
		if err := s.checkInterrupt(); err != nil {
			return nil, true, err
		}
		key := iter.Key().UserKey
		if !bytes.HasPrefix(key, prefix) || bytes.Compare(key, low) < 0 || bytes.Compare(key, high) >= 0 {
			break
		}
		if len(key) < len(prefix)+10 || key[len(prefix)] != 1 || key[len(prefix)+9] != ':' {
			return nil, false, nil
		}
		// Fixed-width sortable integers make the row-key boundary unambiguous.
		rk := key[len(prefix)+10:]
		data, found, err := s.schemaGet(rk)
		if err != nil {
			return nil, true, err
		}
		if found {
			row := decodeRow(data, info)
			if needQualified {
				qualifyRow(row, table)
				qualifyRow(row, stmt.FromAlias)
			}
			matches := true
			if stmt.Where != nil {
				matches, err = s.evalCondition(stmt.Where, row)
				if err != nil {
					return nil, true, err
				}
			}
			if matches {
				rows = append(rows, row)
				if int64(len(rows)) >= limitWindow(stmt.Limit, stmt.Offset) {
					break
				}
			}
		}
		if reverse {
			iter.Prev()
		} else {
			iter.Next()
		}
	}
	return rows, true, storage.IteratorError(iter)
}
func rangePredicate(e Expression) bool {
	switch x := e.(type) {
	case nil, *ColumnRefExpr, *LiteralExpr:
		return true
	case *BinaryExpr:
		switch x.Op {
		case "AND", "OR", "=", "!=", "<>", "<", ">", "<=", ">=":
			return rangePredicate(x.Left) && rangePredicate(x.Right)
		}
	}
	return false
}
func prefixSuccessor(p []byte) []byte {
	p = append([]byte(nil), p...)
	for i := len(p) - 1; i >= 0; i-- {
		if p[i] != 255 {
			p[i]++
			return p[:i+1]
		}
	}
	return nil
}

// Qualification is needed only when a query refers to qualified names or can
// expose this row to another query block. Avoid doubling every result map for
// the common unqualified, single-table case.
func needsQualifiedRows(stmt *SelectStmt) bool {
	if len(stmt.Joins) != 0 || stmt.FromExpr != nil {
		return true
	}
	var needs func(Expression) bool
	needs = func(e Expression) bool {
		switch x := e.(type) {
		case nil, *LiteralExpr:
			return false
		case *ColumnRefExpr:
			return strings.Contains(x.Name, ".")
		case *BinaryExpr:
			return needs(x.Left) || needs(x.Right)
		case *UnaryExpr:
			return needs(x.Right)
		case *FuncCallExpr:
			for _, a := range x.Args {
				if needs(a) {
					return true
				}
			}
			return false
		case *InListExpr:
			if needs(x.Expr) {
				return true
			}
			for _, a := range x.List {
				if needs(a) {
					return true
				}
			}
			return false
		case *BetweenExpr:
			return needs(x.Expr) || needs(x.Low) || needs(x.High)
		case *CaseExpr:
			if needs(x.Expr) || needs(x.ElseExpr) {
				return true
			}
			for _, w := range x.Whens {
				if needs(w.Cond) || needs(w.Then) {
					return true
				}
			}
			return false
		}
		return true
	}
	for _, c := range stmt.Columns {
		if needs(c.Expr) {
			return true
		}
	}
	for _, o := range stmt.OrderBy {
		if needs(o.Expr) {
			return true
		}
	}
	for _, g := range stmt.GroupBy {
		if needs(g) {
			return true
		}
	}
	return needs(stmt.Where) || needs(stmt.Having)
}

// Reuse one immutable storage view for a SQL key batch, then apply the private
// transaction overlay. Shadowed keys do not read committed data at all.
func (s *Schema) schemaMultiGet(keys [][]byte) (results []storage.PointValue, err error) {
	if len(keys) == 1 {
		v, ok, e := s.schemaGet(keys[0])
		return []storage.PointValue{{Value: v, Found: ok}}, e
	}
	if s.profile != nil && len(keys) > 0 {
		started := time.Now()
		defer func() {
			elapsed := time.Since(started) / time.Duration(len(keys))
			for i, key := range keys {
				var value storage.PointValue
				if i < len(results) {
					value = results[i]
				}
				s.profilePoint(key, value.Value, value.Found, elapsed)
			}
		}()
	}
	read := func(ks [][]byte) ([]storage.PointValue, error) {
		if s.statementActive {
			return s.lsm.GetMultiAt(ks, s.statementSnapshot)
		}
		return s.lsm.GetMulti(ks)
	}
	if !s.inTx || len(s.txLatest) == 0 {
		return read(keys)
	}
	values := make([]storage.PointValue, len(keys))
	pending := make([][]byte, 0, len(keys))
	positions := make([]int, 0, len(keys))
	for i, key := range keys {
		if pos, ok := s.txLatest[string(key)]; ok {
			w := s.txWrites[pos]
			values[i] = storage.PointValue{Value: w.value, Found: !w.del}
		} else {
			pending = append(pending, key)
			positions = append(positions, i)
		}
	}
	if len(pending) > 0 {
		v, e := read(pending)
		if e != nil {
			return nil, e
		}
		for i, pos := range positions {
			values[pos] = v[i]
		}
	}
	return values, nil
}

func (s *Schema) newIteratorSeek(key []byte) storage.Iterator {
	if s.inTx && len(s.txWrites) > 0 {
		it := s.txNewIterator()
		it.Seek(key)
		return it
	}
	if s.statementActive {
		return s.profileReader(s.lsm.NewIteratorAtSeek(s.statementSnapshot, key), "Scan")
	}
	return s.profileReader(s.lsm.NewIteratorSeek(key), "Scan")
}

func (s *Schema) newRangeIterator(lower, upper []byte, reverse bool) storage.Iterator {
	var base storage.Iterator
	overlay := s.inTx && len(s.txWrites) > 0
	// Bounded read views exclude unrelated SSTables and initialize at the
	// requested range, even when private transaction writes are present.
	if s.statementActive {
		base = s.lsm.NewIteratorAtRange(s.statementSnapshot, lower, upper, reverse && !overlay)
	} else {
		base = s.lsm.NewIteratorRange(lower, upper, reverse && !overlay)
	}
	if !overlay {
		return s.profileReader(base, "RangeScan")
	}
	it := s.txIterator(base, lower, upper)
	if reverse {
		it.Seek(upper)
		it.Prev()
	}
	return s.profileReader(storage.BoundIterator(it, lower, upper), "RangeScan")
}

func coveredRangePredicate(e Expression, column, table, alias string) bool {
	if e == nil {
		return true
	}
	b, ok := e.(*BinaryExpr)
	if !ok {
		return false
	}
	if b.Op == "AND" {
		return coveredRangePredicate(b.Left, column, table, alias) && coveredRangePredicate(b.Right, column, table, alias)
	}
	switch b.Op {
	case "=", "<", ">", "<=", ">=":
	default:
		return false
	}
	col, ok := b.Left.(*ColumnRefExpr)
	lit, litOK := b.Right.(*LiteralExpr)
	if !ok || !litOK {
		col, ok = b.Right.(*ColumnRefExpr)
		lit, litOK = b.Left.(*LiteralExpr)
	}
	if !ok || !litOK || lit.Value.Null || lit.Value.Type != ColInteger {
		return false
	}
	name := strings.ToLower(col.Name)
	name = strings.TrimPrefix(name, table+".")
	if alias != "" {
		name = strings.TrimPrefix(name, alias+".")
	}
	return name == column
}
