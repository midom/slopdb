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

var errAccessStop = errors.New("physical reader stopped")

// Access paths never change comparison semantics: only exactly typed bounds
// become physical seeks, and callers still evaluate the complete predicate.
type accessPath struct {
	info      *TableInfo
	index     *IndexInfo
	points    [][]byte
	intervals []coveringInterval
	score     int
	complete  bool
}

func booleanAnd(a, b Expression) Expression {
	if a == nil {
		return b
	}
	if b == nil {
		return a
	}
	return &BinaryExpr{Op: "AND", Left: a, Right: b}
}

// Distribute a bounded number of OR branches. Overlapping index intervals are
// deduplicated by physical row key, never by projected value.
func accessBranches(e Expression) ([]Expression, bool) {
	if b, ok := e.(*BinaryExpr); ok && (b.Op == "AND" || b.Op == "OR") {
		a, ok := accessBranches(b.Left)
		if !ok {
			return nil, false
		}
		c, ok := accessBranches(b.Right)
		if !ok {
			return nil, false
		}
		if b.Op == "OR" {
			if len(a)+len(c) > 64 {
				return nil, false
			}
			return append(a, c...), true
		}
		if len(a)*len(c) > 64 {
			return nil, false
		}
		var out []Expression
		for _, x := range a {
			for _, y := range c {
				out = append(out, booleanAnd(x, y))
			}
		}
		return out, true
	}
	return []Expression{e}, true
}

func (s *Schema) accessExpression(e Expression) Expression {
	out, _ := mapExpression(e, func(e Expression) (Expression, bool, error) {
		if b, ok := e.(*BinaryExpr); ok && b.Op == "IN" {
			if _, ok := b.Right.(*SubqueryExpr); ok {
				return s.accessExpression(&InListExpr{Expr: b.Left, List: []Expression{b.Right}}), true, nil
			}
		}
		if in, ok := e.(*InListExpr); ok && !in.Not && len(in.List) == 1 && s.selectDepth > 0 {
			if sub, ok := in.List[0].(*SubqueryExpr); ok {
				deps, safe := s.subqueryDependencies(sub.Stmt)
				if safe && len(deps) == 0 {
					if rs, err := s.executeSubquery(sub.Stmt, nil); err == nil && len(rs.Columns) == 1 && len(rs.Rows) <= 1024 {
						copy := *in
						copy.List = make([]Expression, 0, len(rs.Rows))
						for _, row := range rs.Rows {
							copy.List = append(copy.List, &LiteralExpr{Value: row[rs.Columns[0].Name], BoundParameter: true})
						}
						return &copy, true, nil
					}
				}
			}
		}
		switch e.(type) {
		case *SubqueryExpr, *ExistsExpr:
			return e, true, nil
		}
		if b, ok := e.(*BetweenExpr); ok && !b.Not {
			return s.accessExpression(booleanAnd(&BinaryExpr{Op: ">=", Left: b.Expr, Right: b.Low}, &BinaryExpr{Op: "<=", Left: b.Expr, Right: b.High})), true, nil
		}
		switch e.(type) {
		case *BinaryExpr, *UnaryExpr:
			constant := true
			_ = walkExpression(e, func(x Expression) (bool, error) {
				switch x.(type) {
				case *LiteralExpr, *BinaryExpr, *UnaryExpr:
				default:
					constant = false
				}
				return false, nil
			})
			if constant {
				if v, err := s.evalExpr(e, nil); err == nil {
					return &LiteralExpr{Value: v, BoundParameter: true}, true, nil
				}
			}
		}
		return e, false, nil
	})
	return out
}

func (s *Schema) accessConstraints(e Expression, info *TableInfo) map[string]*tupleConstraint {
	e = s.accessExpression(e)
	c := tupleConstraints(e, info.Name, "", info)
	// Tuple access historically limited ranges to integers. This reader supports
	// all sortable type bands, including dates stored as TEXT.
	for _, term := range andTerms(e) {
		b, ok := term.(*BinaryExpr)
		if !ok {
			continue
		}
		ref, rok := b.Left.(*ColumnRefExpr)
		lit, lok := b.Right.(*LiteralExpr)
		op := b.Op
		if !rok || !lok {
			ref, rok = b.Right.(*ColumnRefExpr)
			lit, lok = b.Left.(*LiteralExpr)
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
		if !rok || !lok || lit.Value.Null {
			continue
		}
		name := coveringColumn(ref.Name, info.Name, "")
		valid := false
		for _, def := range info.Columns {
			if def.Name == name && def.ColType == lit.Value.Type {
				valid = true
			}
		}
		v := lit.Value
		if !valid || v.Type == ColReal && (v.Real == 0 || math.IsNaN(v.Real)) {
			continue
		}
		if c[name] == nil {
			c[name] = &tupleConstraint{}
		}
		x := c[name]
		switch op {
		case ">", ">=":
			strict := op == ">"
			if x.lower == nil || ValueCompare(v, *x.lower) > 0 || ValueCompare(v, *x.lower) == 0 && strict {
				x.lower = &v
				x.lowerStrict = strict
			}
		case "<", "<=":
			strict := op == "<"
			if x.upper == nil || ValueCompare(v, *x.upper) < 0 || ValueCompare(v, *x.upper) == 0 && strict {
				x.upper = &v
				x.upperStrict = strict
			}
		}
	}
	return c
}

func appendAccessValue(prefix []byte, v Value, idx *IndexInfo) []byte {
	if idx == nil {
		return appendClusteredValue(copyBytes(prefix), v)
	}
	encoded := valueToSortableKey(v)
	if len(idx.columnNames()) > 1 {
		encoded = hex.EncodeToString([]byte(encoded))
	}
	return append(append(copyBytes(prefix), encoded...), ':')
}

func buildAccessPath(info *TableInfo, idx *IndexInfo, constraints map[string]*tupleConstraint) *accessPath {
	columns := info.PKColumns
	prefix := []byte(tableDataPrefix(info.Name))
	if idx != nil {
		columns = idx.columnNames()
		prefix = []byte(indexDataPrefix(idx.Name))
	}
	if len(columns) == 0 {
		return nil
	}
	p := &accessPath{info: info, index: idx}
	starts := [][]byte{prefix}
	pos := 0
	for pos < len(columns) {
		c := constraints[columns[pos]]
		if c == nil {
			break
		}
		var values []Value
		if c.equal != nil {
			values = []Value{*c.equal}
		} else if c.listTerm != nil {
			values = c.list
		} else {
			break
		}
		if len(values) > 0 && len(starts) > 1024/len(values) {
			break
		}
		var next [][]byte
		for _, a := range starts {
			for _, v := range values {
				next = append(next, appendAccessValue(a, v, idx))
			}
		}
		starts = next
		pos++
		p.score += 16
	}
	if pos == len(columns) && idx == nil {
		p.points = starts
		p.score += 64
		return p
	}
	p.complete = pos == len(columns)
	bounded := pos > 0
	for _, start := range starts {
		low, high := start, prefixSuccessor(start)
		if pos < len(columns) {
			if c := constraints[columns[pos]]; c != nil && (c.lower != nil || c.upper != nil) {
				var typ ColumnType
				for _, def := range info.Columns {
					if def.Name == columns[pos] {
						typ = def.ColType
					}
				}
				minimum := NewIntValue(math.MinInt64)
				switch typ {
				case ColReal:
					minimum = NewRealValue(-math.MaxFloat64)
				case ColText:
					minimum = NewTextValue("")
				case ColBlob:
					minimum = NewBlobValue(nil)
				}
				low = appendAccessValue(start, minimum, idx)
				variableSecondary := idx != nil && (typ == ColText || typ == ColBlob)
				if variableSecondary {
					low = low[:len(low)-1]
				}
				if c.lower != nil {
					low = appendAccessValue(start, *c.lower, idx)
					if variableSecondary {
						low = low[:len(low)-1]
					}
					if c.lowerStrict && !variableSecondary {
						low = prefixSuccessor(low)
					}
				}
				if c.upper != nil {
					high = appendAccessValue(start, *c.upper, idx)
					if !c.upperStrict {
						high = prefixSuccessor(high)
					}
					if variableSecondary {
						high = appendAccessValue(start, *c.upper, idx)
						high = prefixSuccessor(high[:len(high)-1])
					}
				}
				bounded = true
				p.score += 4
			}
		}
		if bytes.Compare(low, high) < 0 {
			p.intervals = append(p.intervals, coveringInterval{low, high})
		}
	}
	if !bounded {
		return nil
	}
	return p
}

func (s *Schema) accessPaths(where Expression, info *TableInfo) []*accessPath {
	branches, ok := accessBranches(where)
	if !ok {
		return nil
	}
	var paths []*accessPath
	for _, branch := range branches {
		constraints := s.accessConstraints(branch, info)
		best := buildAccessPath(info, nil, constraints)
		for _, idx := range s.tableIndexes(info.Name) {
			if s.isClusteredIndex(idx) {
				continue
			}
			p := buildAccessPath(info, idx, constraints)
			if p != nil && !p.complete && len(idx.columnNames()) == 1 && s.isAutoIndex(idx) {
				for _, c := range info.Columns {
					if c.Name == idx.Column && (c.ColType == ColText || c.ColType == ColBlob) {
						p = nil
					}
				}
			}
			if p != nil && (best == nil || p.score > best.score || p.score == best.score && best.index != nil && idx.Name < best.index.Name) {
				best = p
			}
		}
		if best == nil {
			return nil
		}
		paths = append(paths, best)
	}
	return paths
}

// One scratch row per reader. Visitors must copy values they retain.
func (s *Schema) walkAccess(info *TableInfo, needed map[string]bool, paths []*accessPath, visit func(map[string]Value) error) error {
	columnsFor := func(info *TableInfo) []scanColumn {
		cols := make([]scanColumn, len(info.Columns))
		for i, c := range info.Columns {
			cols[i] = scanColumn{name: c.Name, needed: needed == nil || needed[c.Name]}
		}
		return cols
	}
	columns := columnsFor(info)
	columnCount := 0
	for _, column := range columns {
		if column.needed {
			columnCount++
		}
	}
	row := make(map[string]Value, columnCount)
	var seen map[string]bool
	if len(paths) > 1 {
		seen = make(map[string]bool)
	}
	checks := 0
	accept := func(data, key []byte, cols []scanColumn) error {
		checks++
		if checks%128 == 1 {
			if err := s.checkInterrupt(); err != nil {
				return err
			}
		}
		if seen != nil {
			k := string(key)
			if seen[k] {
				return nil
			}
			seen[k] = true
		}
		decodeScanRow(data, cols, row)
		return visit(row)
	}
	if paths == nil {
		prefix := []byte(tableDataPrefix(info.Name))
		it := s.newIteratorSeek(prefix)
		defer it.Close()
		for it.Valid() {
			key := it.Key().UserKey
			if !bytes.HasPrefix(key, prefix) {
				break
			}
			if err := accept(it.Value(), key, columns); err != nil {
				return err
			}
			it.Next()
		}
		return storage.IteratorError(it)
	}
	for _, p := range paths {
		if p.points != nil {
			values, err := s.queryMultiGet(p.points)
			if err != nil {
				return err
			}
			for i, v := range values {
				if v.Found {
					if err := accept(v.Value, p.points[i], columns); err != nil {
						return err
					}
				}
			}
			continue
		}
		payload := s.lookups().covering[p.index]
		covered := payload != nil && needed != nil
		if covered {
			for name, use := range needed {
				if !use {
					continue
				}
				found := false
				for _, c := range payload.Columns {
					if c.Name == name {
						found = true
						break
					}
				}
				if !found {
					covered = false
					break
				}
			}
		}
		payloadColumns := columns
		if covered {
			payloadColumns = columnsFor(payload)
		}
		for _, interval := range p.intervals {
			it := s.newRangeIterator(interval.lower, interval.upper, false)
			err := func() error {
				defer it.Close()
				for it.Valid() {
					if p.index == nil {
						if err := accept(it.Value(), it.Key().UserKey, columns); err != nil {
							return err
						}
						it.Next()
						continue
					}
					if covered {
						key, err := s.accessIndexRowKey(p, interval, it.Key().UserKey, it.Value())
						if err != nil {
							return err
						}
						data := it.Value()
						if len(data) == 0 || data[0] != 1 {
							return fmt.Errorf("invalid covering entry for %s", p.index.Name)
						}
						if err := accept(data[1:], key, payloadColumns); err != nil {
							return err
						}
						it.Next()
						continue
					}
					keys := make([][]byte, 0, 128)
					for it.Valid() && len(keys) < 128 {
						key, err := s.accessIndexRowKey(p, interval, it.Key().UserKey, it.Value())
						if err != nil {
							return err
						}
						keys = append(keys, copyBytes(key))
						it.Next()
					}
					if err := storage.IteratorError(it); err != nil {
						return err
					}
					values, err := s.queryMultiGet(keys)
					if err != nil {
						return err
					}
					for i, v := range values {
						if !v.Found {
							return fmt.Errorf("missing row for index %s", p.index.Name)
						}
						if err := accept(v.Value, keys[i], columns); err != nil {
							return err
						}
					}
				}
				return storage.IteratorError(it)
			}()
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *Schema) countAccessPaths(paths []*accessPath, limit int64) (int64, error) {
	var count int64
	for _, p := range paths {
		if p.points != nil {
			count += int64(len(p.points))
			continue
		}
		for _, interval := range p.intervals {
			it := s.newRangeIterator(interval.lower, interval.upper, false)
			err := func() error {
				defer it.Close()
				for it.Valid() {
					if count%128 == 0 {
						if err := s.checkInterrupt(); err != nil {
							return err
						}
					}
					count++
					if count >= limit {
						break
					}
					it.Next()
				}
				return storage.IteratorError(it)
			}()
			if err != nil {
				return 0, err
			}
			if count >= limit {
				return count, nil
			}
		}
	}
	return count, nil
}

func (s *Schema) queryMultiGet(keys [][]byte) ([]storage.PointValue, error) {
	if s.selectDepth == 0 {
		return s.schemaMultiGet(keys)
	}
	if s.selectValues == nil {
		s.selectValues = make(map[string]storage.PointValue)
	}
	values := make([]storage.PointValue, len(keys))
	var pending [][]byte
	var positions []int
	for i, k := range keys {
		if v, ok := s.selectValues[string(k)]; ok {
			values[i] = v
		} else {
			pending = append(pending, k)
			positions = append(positions, i)
		}
	}
	if len(pending) > 0 {
		vs, err := s.schemaMultiGet(pending)
		if err != nil {
			return nil, err
		}
		for i, v := range vs {
			values[positions[i]] = v
			if len(s.selectValues) < 4096 && s.selectValueBytes+len(v.Value)+len(pending[i]) <= 16<<20 {
				s.selectValues[string(pending[i])] = v
				s.selectValueBytes += len(v.Value) + len(pending[i])
			}
		}
	}
	return values, nil
}

func (s *Schema) accessIndexRowKey(p *accessPath, interval coveringInterval, key, data []byte) ([]byte, error) {
	idx := p.index
	if len(idx.columnNames()) > 1 {
		return compositeIndexRowKey(idx, key)
	}
	// Binary integer/REAL encodings can themselves contain ':'. Text and blob
	// encodings use hex and therefore have an unambiguous delimiter.
	prefix := len(indexDataPrefix(idx.Name))
	rest := key[prefix:]
	if len(rest) == 0 {
		return nil, fmt.Errorf("invalid index key")
	}
	n := 0
	if rest[0] == 1 || rest[0] == 2 {
		n = 9
	} else if len(data) > 1 && data[0] == 1 {
		v, _ := decodeValue(data[1:])
		n = len(valueToSortableKey(v))
	} else {
		// Legacy single-column variable values are raw and can contain ':'.
		// Auto indexes have no payload; recover the authoritative row suffix,
		// rather than interpreting a separator inside the indexed value.
		rowPrefix := []byte(tableDataPrefix(p.info.Name))
		size := len(rowPrefix)
		fixed := true
		if len(p.info.PKColumns) == 0 {
			size += 20
		} else {
			for _, name := range p.info.PKColumns {
				for _, c := range p.info.Columns {
					if c.Name == name {
						if c.ColType == ColInteger || c.ColType == ColReal {
							size += 9
						} else {
							fixed = false
						}
					}
				}
			}
		}
		if fixed && len(key) >= size && bytes.HasPrefix(key[len(key)-size:], rowPrefix) {
			return key[len(key)-size:], nil
		}
		for offset := prefix; offset < len(key); {
			i := bytes.Index(key[offset:], rowPrefix)
			if i < 0 {
				break
			}
			i += offset
			rk := key[i:]
			value, found, err := s.schemaGet(rk)
			if err != nil {
				return nil, err
			}
			if found && bytes.Equal(encodeRowIndexKey(idx, decodeRow(value, p.info), rk), key) {
				return rk, nil
			}
			offset = i + 1
		}
		return nil, fmt.Errorf("invalid row suffix for index %s", idx.Name)
	}
	if n < 0 || len(rest) <= n+1 || rest[n] != ':' {
		return nil, fmt.Errorf("invalid key for index %s", idx.Name)
	}
	return rest[n+1:], nil
}

func copyRow(row map[string]Value) map[string]Value {
	out := make(map[string]Value, len(row))
	for k, v := range row {
		out[k] = v
	}
	return out
}

func sortedAccessNames(names map[string]bool) []string {
	out := make([]string, 0, len(names))
	for n := range names {
		out = append(out, n)
	}
	sort.Strings(out)
	return out
}

func qualifyAccessRow(row map[string]Value, table, alias string) {
	if alias == "" {
		alias = table
	}
	for k, v := range row {
		if !strings.Contains(k, ".") {
			row[alias+"."+k] = v
			if table != alias {
				row[table+"."+k] = v
			}
		}
	}
}

func (s *Schema) streamSelectScan(stmt *SelectStmt, info *TableInfo) ([]map[string]Value, bool, error) {
	if info == nil || stmt.FromExpr != nil || len(stmt.Joins) > 0 || stmt.Virtual != nil || stmt.Lock != nil || stmt.trackRows || hasAggregates(stmt) || len(stmt.GroupBy) > 0 || stmt.Having != nil || stmt.Distinct || len(stmt.OrderBy) > 0 {
		return nil, false, nil
	}
	if _, safe := s.expressionDependencies(stmt.Where); !safe {
		return nil, false, nil
	}
	for _, c := range stmt.Columns {
		if _, safe := s.expressionDependencies(c.Expr); !safe {
			return nil, false, nil
		}
	}
	needed := joinRequiredNames(stmt)
	bareNeeded := make(map[string]bool)
	for _, c := range info.Columns {
		if needed == nil || needed[c.Name] || needed[stmt.From+"."+c.Name] || needed[stmt.FromAlias+"."+c.Name] {
			bareNeeded[c.Name] = true
		}
	}
	where, _ := mapExpression(stmt.Where, func(e Expression) (Expression, bool, error) {
		switch e := e.(type) {
		case *ColumnRefExpr:
			if name := coveringColumn(e.Name, stmt.From, stmt.FromAlias); name != "" {
				return &ColumnRefExpr{Name: name}, true, nil
			}
		case *SubqueryExpr, *ExistsExpr:
			return e, true, nil
		}
		return e, false, nil
	})
	paths := s.accessPaths(where, info)
	// Existing point/covering/ordered paths remain minimal for transactional
	// calls. Use this reader for nested scans and otherwise unsupported OR access.
	if s.selectDepth <= 1 {
		if len(paths) <= 1 {
			return nil, false, nil
		}
	}
	var rows []map[string]Value
	if stmt.Limit == 0 {
		return rows, true, nil
	}
	err := s.walkAccess(info, bareNeeded, paths, func(row map[string]Value) error {
		if needsQualifiedRows(stmt) {
			qualifyAccessRow(row, stmt.From, stmt.FromAlias)
		}
		if stmt.Where != nil {
			ok, err := s.evalCondition(stmt.Where, row)
			if err != nil {
				return err
			}
			if !ok {
				return nil
			}
		}
		rows = append(rows, copyRow(row))
		if stmt.Limit >= 0 && int64(len(rows)) >= limitWindow(stmt.Limit, stmt.Offset) {
			return errAccessStop
		}
		return nil
	})
	if errors.Is(err, errAccessStop) {
		err = nil
	}
	return rows, true, err
}
