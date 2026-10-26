package sql

// Tuple access has already enforced the index/primary-key terms. Retain only
// fields needed by downstream expressions and its residual predicate. Every
// returned row and BLOB still owns its bytes; uncertain shapes keep full rows.
func (s *Schema) tupleProjection(stmt *SelectStmt, plan *tupleReadPlan) (map[string]bool, bool) {
	if stmt.SelectAll || stmt.trackRows || stmt.With != nil || stmt.Compound != nil || stmt.FromExpr != nil || stmt.Virtual != nil || len(stmt.Joins) != 0 || hasWindows(stmt) || needsQualifiedRows(stmt) {
		return nil, false
	}
	available := make(map[string]bool, len(plan.info.Columns))
	for _, col := range plan.info.Columns {
		available[col.Name] = true
	}
	needed := make(map[string]bool)
	safe := true
	collect := func(expr Expression) {
		_ = walkExpression(expr, func(e Expression) (bool, error) {
			switch e := e.(type) {
			case *ColumnRefExpr:
				if e.Name != "*" {
					if !available[e.Name] {
						safe = false
					} else {
						needed[e.Name] = true
					}
				}
			case *SubqueryExpr, *ExistsExpr:
				safe = false
				return true, nil
			case *FuncCallExpr:
				if !s.deterministicFunction(e.Name) {
					safe = false
				}
			}
			return false, nil
		})
	}
	for _, col := range stmt.Columns {
		if col.Star {
			return nil, false
		}
		collect(col.Expr)
	}
	collect(plan.residual)
	collect(stmt.Having)
	for _, expr := range stmt.GroupBy {
		collect(expr)
	}
	for _, order := range stmt.OrderBy {
		collect(order.Expr)
	}
	return needed, safe
}
func tupleProjectionColumns(info *TableInfo, needed map[string]bool) []pointDecodeMetadata {
	columns := make([]pointDecodeMetadata, len(info.Columns))
	for i, col := range info.Columns {
		columns[i] = pointDecodeMetadata{name: col.Name, decode: needed[col.Name], kind: col.LogicalType().Kind, binary: col.Binary, collation: col.Collation, enums: col.EnumValues}
	}
	return columns
}
func decodeTupleProjection(data []byte, columns []pointDecodeMetadata, count int) map[string]Value {
	row := make(map[string]Value, count)
	offset := 0
	for _, col := range columns {
		if !col.decode {
			if offset < len(data) {
				offset += skipPointValue(data[offset:])
			}
			continue
		}
		value := NewNullValue()
		if offset < len(data) {
			var n int
			value, n = decodeValue(data[offset:])
			offset += n
			value = decorateTypedValue(value, col.kind, col.binary, col.collation, col.enums)
		}
		row[col.name] = value
	}
	return row
}
