package sql

import (
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
			key = append([]byte(s.tableDataPrefix(table)), 1)
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

// indexRangeRows remains a compatibility adapter for callers that require an
// ordered read. Scalar and tuple keys share the same constraints, type bands,
// residual predicates, transactional views and LIMIT checks.
func (s *Schema) indexRangeRows(stmt *SelectStmt, table string, info *TableInfo) ([]map[string]Value, bool, error) {
	if stmt.Lock != nil || stmt.trackRows || len(stmt.Joins) != 0 || stmt.Distinct || len(stmt.GroupBy) != 0 || stmt.Having != nil || hasAggregates(stmt) {
		return nil, false, nil
	}
	plan := s.tuplePlan(stmt, info)
	if plan == nil || !plan.ordered {
		return nil, false, nil
	}
	rows, _, ordered, err := s.tupleRows(stmt, plan, false)
	return rows, ordered, err
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
			for _, a := range windowExpressions(x.Window) {
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

// schemaMultiGetBounded refuses an oversized result before retaining a batch of
// large values. Overlay/temporary reads keep their existing visibility rules.
func (s *Schema) schemaMultiGetBounded(keys [][]byte, maxBytes int) ([]storage.PointValue, bool, error) {
	if !s.hasTemporaryTables() && (!s.inTx || len(s.txLatest) == 0) {
		snapshot := ^uint64(0)
		if s.statementActive {
			snapshot = s.statementSnapshot
		}
		return s.lsm.GetMultiAtForReaderBounded(keys, snapshot, s.snapshotReader, maxBytes)
	}
	values := make([]storage.PointValue, len(keys))
	for i, key := range keys {
		value, found, err := s.schemaGet(key)
		if err != nil {
			return nil, false, err
		}
		if len(value) > maxBytes {
			return nil, false, nil
		}
		maxBytes -= len(value)
		values[i] = storage.PointValue{Value: value, Found: found}
	}
	return values, true, nil
}

// Reuse one immutable storage view for a SQL key batch, then apply the private
// transaction overlay. Shadowed keys do not read committed data at all.
func (s *Schema) schemaMultiGet(keys [][]byte) (results []storage.PointValue, err error) {
	if s.hasTemporaryTables() {
		results = make([]storage.PointValue, len(keys))
		for i, key := range keys {
			value, found, e := s.schemaGet(key)
			if e != nil {
				return nil, e
			}
			results[i] = storage.PointValue{Value: value, Found: found}
		}
		return results, nil
	}
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
			return s.lsm.GetMultiAtForReader(ks, s.statementSnapshot, s.snapshotReader)
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
	if s.temporaryKey(key) {
		base := s.lsm.NewIteratorSeek(key)
		if s.inTx && len(s.txWrites) > 0 {
			base = s.txIterator(base, nil, nil)
			base.Seek(key)
		}
		return s.profileReader(base, "Scan")
	}
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
	if s.statementActive && !s.temporaryKey(lower) {
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

// Cursor consumers decode the current value before advancing, so they can use
// borrowed storage buffers. The generic borrowed reader also preserves
// transactional overlay and profiling ownership/measurement contracts.
func (s *Schema) newCursorRangeIterator(lower, upper []byte, reverse bool) storage.Iterator {
	return s.newBorrowedRangeIterator(lower, upper, reverse)
}

// newBorrowedRangeIterator is for synchronous readers that decode or copy every
// key/value before advancing. Profiling visits consume bytes synchronously;
// private overlays copy their current row before advancing the borrowed base.
func (s *Schema) newBorrowedRangeIterator(lower, upper []byte, reverse bool) storage.Iterator {
	return s.newBorrowedRangeReader(lower, upper, reverse, "RangeScan")
}

func (s *Schema) newBorrowedRangeReader(lower, upper []byte, reverse bool, operator string) storage.Iterator {
	overlay := s.inTx && len(s.txWrites) > 0
	var base storage.Iterator
	if s.statementActive && !s.temporaryKey(lower) {
		base = s.lsm.NewIteratorAtRangeReusable(s.statementSnapshot, lower, upper, reverse && !overlay)
	} else {
		base = s.lsm.NewIteratorRangeReusable(lower, upper, reverse && !overlay)
	}
	if overlay {
		base = s.txIteratorMode(base, lower, upper, true)
		if reverse {
			base.Seek(upper)
			base.Prev()
		}
		base = storage.BoundIterator(base, lower, upper)
	}
	return s.profileReader(base, operator)
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

// accessMultiGetInto serves synchronous physical-read callbacks. Encoded values
// are exclusively owned scratch until the next batch; no result enters the
// statement's retained queryMultiGet cache. Temporary keys read the latest view,
// regular keys read the statement snapshot, and private writes shadow storage.
func (s *Schema) accessMultiGetInto(keys [][]byte, dst []storage.PointValue) (results []storage.PointValue, err error) {
	temporary := s.hasTemporaryTables()
	profileBatch := s.profile != nil && !temporary
	if profileBatch && len(keys) > 0 {
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
	snapshot := ^uint64(0)
	if s.statementActive {
		snapshot = s.statementSnapshot
	}
	if !temporary && (!s.inTx || len(s.txLatest) == 0) {
		return s.lsm.GetMultiAtForReaderInto(keys, snapshot, s.snapshotReader, dst)
	}
	if len(dst) > len(keys) {
		clear(dst[len(keys):])
	}
	if cap(dst) < len(keys) {
		grown := make([]storage.PointValue, len(keys))
		copy(grown, dst)
		dst = grown
	} else {
		dst = dst[:len(keys)]
	}
	for i, key := range keys {
		started := time.Time{}
		if s.profile != nil && !profileBatch {
			started = time.Now()
		}
		value, found, readErr := s.accessPointInto(key, snapshot, dst[i].Value)
		if !started.IsZero() {
			s.profilePoint(key, value, found, time.Since(started))
		}
		if readErr != nil {
			return nil, readErr
		}
		dst[i] = storage.PointValue{Value: value, Found: found}
	}
	return dst, nil
}

func (s *Schema) accessPointInto(key []byte, snapshot uint64, dst []byte) ([]byte, bool, error) {
	if s.inTx {
		if position, ok := s.txLatest[string(key)]; ok {
			write := s.txWrites[position]
			if write.del {
				return dst[:0], false, nil
			}
			return append(dst[:0], write.value...), true, nil
		}
	}
	if s.temporaryKey(key) {
		snapshot = ^uint64(0)
	}
	return s.lsm.GetAtForReaderInto(key, snapshot, s.snapshotReader, dst)
}
