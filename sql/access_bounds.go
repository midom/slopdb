package sql

import (
	"bytes"
	"encoding/hex"
	"sort"
)

// Local physical readers use bare column names; evaluation keeps the original
// qualified predicate. Never descend into another query block's lexical scope.
func localAccessPredicate(expr Expression, table, alias string) Expression {
	out, _ := mapExpression(expr, func(expr Expression) (Expression, bool, error) {
		switch e := expr.(type) {
		case *ColumnRefExpr:
			if name := coveringColumn(e.Name, table, alias); name != "" {
				return &ColumnRefExpr{Name: name}, true, nil
			}
		case *SubqueryExpr, *ExistsExpr:
			return e, true, nil
		}
		return expr, false, nil
	})
	return out
}

// Unterminated secondary values need additional bands for shorter proper
// prefixes whose ':' delimiter sorts after the upper bound's next byte. Keep
// those bands separate instead of inflating one interval over unrelated values.
func accessRangeIntervals(start []byte, column string, info *TableInfo, idx *IndexInfo, c *tupleConstraint) ([]coveringInterval, bool) {
	if idx != nil && idx.prefixLength(column) > 0 {
		copy := *c
		copy.lowerStrict = false
		copy.upperStrict = false
		c = &copy
	}
	low, high, exact := accessRangeBounds(start, column, info, idx, c)
	var intervals []coveringInterval
	add := func(a, b []byte) {
		if bytes.Compare(a, b) < 0 {
			intervals = append(intervals, coveringInterval{a, b})
		}
	}
	var typ ColumnType
	for _, d := range info.Columns {
		if d.Name == column {
			typ = d.ColType
			break
		}
	}
	// A normalization can expand or contract bytes. Legacy secondary TEXT
	// components lack a terminator, so their prefix bands cannot be derived
	// from the original literal's byte length. Scan the TEXT band and retain
	// the SQL predicate as a residual instead.
	if idx != nil && typ == ColText && !idx.binaryColumns[column] && idx.collation != "" && idx.collation != BinaryCollation {
		band := []byte{3}
		if len(idx.columnNames()) > 1 || idx.hasPrefixes() {
			band = []byte(hex.EncodeToString(band))
		}
		low := append(copyBytes(start), band...)
		return []coveringInterval{{low, prefixSuccessor(low)}}, false
	}
	if idx == nil || (typ != ColText && typ != ColBlob) || c.upper == nil {
		add(low, high)
		return intervals, exact
	}
	upper := *c.upper
	length := len(upper.Text)
	if typ == ColBlob {
		length = len(upper.Blob)
	}
	// Pathological long bounds keep the existing conservative interval.
	// They must not create plans proportional to megabytes of input text.
	if length > 64 {
		add(low, high)
		return intervals, false
	}
	encoded := appendAccessValueColumn(start, upper, idx, info, column)
	add(low, encoded[:len(encoded)-1])
	if !c.upperStrict {
		add(encoded, prefixSuccessor(encoded))
	}
	for n := 0; n < length; n++ {
		var prefix Value
		if typ == ColBlob {
			prefix = NewBlobValue(upper.Blob[:n])
		} else {
			prefix = NewTextValue(upper.Text[:n])
		}
		if c.lower != nil {
			cmp := ValueCompare(prefix, *c.lower)
			if cmp < 0 || cmp == 0 && c.lowerStrict {
				continue
			}
		}
		key := appendAccessValueColumn(start, prefix, idx, info, column)
		add(key, prefixSuccessor(key))
	}
	return normalizeAccessIntervals(intervals), false
}

func normalizeAccessIntervals(intervals []coveringInterval) []coveringInterval {
	sort.Slice(intervals, func(i, j int) bool { return bytes.Compare(intervals[i].lower, intervals[j].lower) < 0 })
	out := intervals[:0]
	for _, interval := range intervals {
		if len(out) > 0 && bytes.Compare(interval.lower, out[len(out)-1].upper) <= 0 {
			if bytes.Compare(interval.upper, out[len(out)-1].upper) > 0 {
				out[len(out)-1].upper = interval.upper
			}
		} else {
			out = append(out, interval)
		}
	}
	return out
}

// Every physical reader shares these bounds. Use the type band rather than an
// artificial minimum value, so NULLs are excluded without losing -Inf, empty
// strings, embedded NULs, or empty blobs.
func accessRangeBounds(start []byte, column string, info *TableInfo, idx *IndexInfo, c *tupleConstraint) (low, high []byte, exact bool) {
	var typ ColumnType
	for _, def := range info.Columns {
		if def.Name == column {
			typ = def.ColType
			break
		}
	}
	tag := byte(1)
	switch typ {
	case ColReal:
		tag = 2
	case ColText:
		tag = 3
	case ColBlob:
		tag = 4
	}
	band := []byte{tag}
	if idx != nil && (len(idx.columnNames()) > 1 || idx.hasPrefixes()) {
		band = []byte(hex.EncodeToString(band))
	}
	low = append(copyBytes(start), band...)
	high = prefixSuccessor(low)
	variable := idx != nil && (typ == ColText || typ == ColBlob)
	exact = !variable
	if c.lower != nil {
		low = appendAccessValueColumn(start, *c.lower, idx, info, column)
		if variable {
			low = low[:len(low)-1]
		} else if c.lowerStrict {
			low = prefixSuccessor(low)
		}
		// Old secondary REAL encodings place negative zero at the start of the
		// type band. A negative lower bound must include it and recheck SQL values.
		if idx != nil && typ == ColReal && c.lower.Real < 0 {
			low = append(copyBytes(start), band...)
			exact = false
		}
	}
	if c.upper != nil {
		high = appendAccessValueColumn(start, *c.upper, idx, info, column)
		if variable {
			encoded := high[:len(high)-1]
			high = prefixSuccessor(encoded)
			// A shorter value ends in ':' rather than an ordered terminator.
			// In the legacy encoding that delimiter can sort after the next
			// byte of the bound, even though the shorter SQL value is smaller.
			// Include every proper prefix's delimiter band, then recheck SQL.
			step := 1
			if len(idx.columnNames()) > 1 || idx.hasPrefixes() {
				step = 2
			}
			if len(encoded)-len(start)-len(band) > 64*step {
				// Avoid quadratic prefix copying for huge bound literals.
				high = prefixSuccessor(append(copyBytes(start), band...))
				return low, high, false
			}
			for n := len(start) + len(band); n < len(encoded); n += step {
				candidate := prefixSuccessor(append(copyBytes(encoded[:n]), ':'))
				if bytes.Compare(candidate, high) > 0 {
					high = candidate
				}
			}
		} else if !c.upperStrict {
			high = prefixSuccessor(high)
		}
	}
	return low, high, exact
}

func variableIndexColumns(info *TableInfo, idx *IndexInfo) bool {
	if idx == nil {
		return false
	}
	for _, name := range idx.columnNames() {
		for _, col := range info.Columns {
			if name == col.Name && (col.ColType == ColText || col.ColType == ColBlob) {
				return true
			}
		}
	}
	return false
}

// Bound intersection currently compares literal TEXT bytewise. Until that
// intersection uses the configured collation, avoid narrowing a Unicode TEXT
// range and let the full scan evaluate the SQL predicate.
func collatedTextRange(info *TableInfo, idx *IndexInfo, column string) bool {
	if info == nil {
		return false
	}
	mode := info.collation
	if idx != nil {
		mode = idx.collation
		if idx.binaryColumns[column] {
			return false
		}
	} else if info.binaryColumn(column) {
		return false
	}
	if mode == "" || mode == BinaryCollation {
		return false
	}
	for _, def := range info.Columns {
		if def.Name == column {
			return def.ColType == ColText
		}
	}
	return false
}
