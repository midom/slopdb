package sql

import (
	"bytes"
	"encoding/hex"
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
	if idx != nil && len(idx.columnNames()) > 1 {
		band = []byte(hex.EncodeToString(band))
	}
	low = append(copyBytes(start), band...)
	high = prefixSuccessor(low)
	variable := idx != nil && (typ == ColText || typ == ColBlob)
	exact = !variable
	if c.lower != nil {
		low = appendAccessValue(start, *c.lower, idx)
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
		high = appendAccessValue(start, *c.upper, idx)
		if variable {
			encoded := high[:len(high)-1]
			high = prefixSuccessor(encoded)
			// A shorter value ends in ':' rather than an ordered terminator.
			// In the legacy encoding that delimiter can sort after the next
			// byte of the bound, even though the shorter SQL value is smaller.
			// Include every proper prefix's delimiter band, then recheck SQL.
			step := 1
			if len(idx.columnNames()) > 1 {
				step = 2
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
