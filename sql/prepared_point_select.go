package sql

import (
	"encoding/binary"
	"strings"
)

// Each prepared AST belongs to one execution at a time. Cache only immutable
// projection metadata, invalidated together with its catalog/plan recipe.
func (s *Schema) selectPreparedPoint(stmt *SelectStmt, info *TableInfo, dst *ResultSet) (*ResultSet, bool, error) {
	if stmt.CalcFoundRows {
		return nil, false, nil
	}
	p := stmt.prepared
	if !p.pointProjectionReady {
		p.pointProjectionReady = true
		if !p.pointComplete() || stmt.FromExpr != nil || stmt.With != nil || stmt.Compound != nil || stmt.Virtual != nil || stmt.materialized != nil || len(stmt.Joins) != 0 || len(stmt.Windows) != 0 || stmt.Lock != nil || stmt.trackRows || stmt.Distinct || len(stmt.GroupBy) != 0 || stmt.Having != nil || len(stmt.OrderBy) != 0 || needsQualifiedRows(stmt) {
			return nil, false, nil
		}
		if stmt.SelectAll {
			if len(stmt.Columns) > 1 || (len(stmt.Columns) == 1 && !stmt.Columns[0].Star) {
				return nil, false, nil
			}
			p.pointProjection = append([]ColumnDef(nil), info.Columns...)
			for i := range p.pointProjection {
				c := &p.pointProjection[i]
				c.SemanticType = c.LogicalType().Kind
				c.Default, c.Check = nil, nil
			}
		} else {
			if len(stmt.Columns) == 0 || len(stmt.Columns) > 8 {
				return nil, false, nil
			}
			columns := make([]ColumnDef, 0, len(stmt.Columns))
			for _, col := range stmt.Columns {
				ref, ok := col.Expr.(*ColumnRefExpr)
				if !ok || col.Alias != "" || col.Star {
					return nil, false, nil
				}
				found := false
				var typed ColumnDef
				for _, physical := range info.Columns {
					if physical.Name == ref.Name {
						typed = physical
						typed.SemanticType = physical.LogicalType().Kind
						typed.Default, typed.Check = nil, nil
						found = true
						break
					}
				}
				if !found {
					return nil, false, nil
				}
				columns = append(columns, typed)
			}
			if uniqueResultColumns(columns, nil) {
				return nil, false, nil
			}
			p.pointProjection = columns
		}
		// WHERE can still contain an invalid aggregate alongside the PK equality.
		// Preserve the general executor's grouping validation, including empty data.
		if err := s.validateQuery(stmt, info); err != nil {
			p.pointProjection = nil
			p.pointProjectionReady = false
			return nil, true, err
		}
		if err := s.validateGrouping(stmt, info, p.pointProjection); err != nil {
			p.pointProjection = nil
			p.pointProjectionReady = false
			return nil, true, err
		}
		p.pointDecodeColumns = make([]bool, len(info.Columns))
		mark := func(name string) {
			if _, rest, qualified := strings.Cut(name, "."); qualified {
				name = rest
			}
			for i, col := range info.Columns {
				if col.Name == name && !p.pointDecodeColumns[i] {
					p.pointDecodeColumns[i] = true
					p.pointDecodeCount++
				}
			}
		}
		for _, col := range p.pointProjection {
			mark(col.Name)
		}
		_ = walkExpression(stmt.Where, func(e Expression) (bool, error) {
			if col, ok := e.(*ColumnRefExpr); ok {
				mark(col.Name)
			}
			return false, nil
		})
		p.pointDecodeMetadata = make([]pointDecodeMetadata, len(info.Columns))
		for i, col := range info.Columns {
			m := &p.pointDecodeMetadata[i]
			m.name = col.Name
			m.decode = p.pointDecodeColumns[i]
			if m.decode {
				m.kind = col.LogicalType().Kind
				m.binary = col.Binary
				m.collation = col.Collation
				m.enums = col.EnumValues
			}
			for _, projected := range p.pointProjection {
				if projected.Name == col.Name {
					m.project = true
					break
				}
			}
		}
	}
	if len(p.pointProjection) == 0 || s.profile != nil {
		return nil, false, nil
	}
	key, ok := p.primaryKey(info.Name, info)
	if !ok {
		return nil, false, nil
	}
	result := dst
	if result == nil {
		result = &ResultSet{}
	}
	var row map[string]Value
	if dst != nil && cap(result.Rows) > 0 {
		row = result.Rows[:1][0]
	}
	result.Profile, result.rowKeys = nil, nil
	result.Columns = append(result.Columns[:0], p.pointProjection...)
	result.Rows = result.Rows[:0]
	var data []byte
	var found bool
	var err error
	if !s.inTx && s.statementActive && !s.temporaryKey(key) {
		data, found, err = s.lsm.GetAtForReaderInto(key, s.statementSnapshot, s.snapshotReader, p.pointValueScratch)
		p.pointValueScratch = data
	} else {
		data, found, err = s.schemaGet(key)
	}
	if err != nil {
		return nil, true, err
	}
	if !found {
		return result, true, nil
	}
	conditionRow := row
	if p.pointDecodeCount > len(p.pointProjection) {
		if p.pointSelectScratch == nil {
			p.pointSelectScratch = make(map[string]Value, p.pointDecodeCount)
		}
		conditionRow = p.pointSelectScratch
		row = decodePointProjectionInto(data, info, p, row, conditionRow)
	} else {
		row = decodePointRowInto(data, info, p, row)
		conditionRow = row
	}
	// An empty/filter-mismatched Into result still owns its reusable row map.
	if dst != nil && cap(result.Rows) == 0 {
		result.Rows = append(result.Rows, row)[:0]
	}
	matches, err := s.evalCondition(stmt.Where, conditionRow)
	if conditionRow != nil && p.pointDecodeCount > len(p.pointProjection) {
		for _, col := range p.pointProjection {
			conditionRow[col.Name] = NewNullValue()
		}
	}
	if err != nil {
		return nil, true, err
	}
	if !matches || stmt.Limit == 0 || stmt.Offset > 0 {
		return result, true, nil
	}
	if err := s.checkInterrupt(); err != nil {
		return nil, true, err
	}
	result.Rows = append(result.Rows, row)
	return result, true, nil
}

// Row buffers are reused only when the caller explicitly supplies a destination.
// BLOBs remain independent of storage scratch and can reuse their old capacity.
func decodePointRowInto(data []byte, info *TableInfo, p *preparedExecutionPlan, row map[string]Value) map[string]Value {
	reuse := row != nil
	if row == nil {
		row = make(map[string]Value, p.pointDecodeCount)
	}
	// The destination can have come from another query or an earlier catalog.
	for name := range row {
		found := false
		for _, col := range p.pointDecodeMetadata {
			if col.name == name && col.decode {
				found = true
				break
			}
		}
		if !found {
			delete(row, name)
		}
	}
	offset := 0
	for _, col := range p.pointDecodeMetadata {
		if !col.decode {
			if offset < len(data) {
				offset += skipPointValue(data[offset:])
			}
			continue
		}
		if offset >= len(data) {
			row[col.name] = NewNullValue()
			continue
		}
		var previous Value
		if reuse {
			previous = row[col.name]
		}
		v, n := decodePointValueInto(data[offset:], previous)
		v = decorateTypedValue(v, col.kind, col.binary, col.collation, col.enums)
		row[col.name] = v
		offset += n
	}
	return row
}

// Match decodeValue's advance through an unneeded field without copying its
// text or BLOB. The normal decoder still handles every field used by WHERE.
func skipPointValue(data []byte) int {
	switch data[0] {
	case binTypeInt64, binTypeFloat64, binTypeUint64:
		if len(data) >= 9 {
			return 9
		}
	case binTypeText, binTypeBlob, binTypeDecimal, binTypeJSON:
		length, n := binary.Uvarint(data[1:])
		if n <= 0 || length > uint64(len(data)-1-n) {
			return 1 + n
		}
		return 1 + n + int(length)
	}
	return 1
}

// Keep WHERE-only values private instead of repeatedly inserting/removing them
// in the result map. Selected BLOB buffers always come from the current caller.
func decodePointProjectionInto(data []byte, info *TableInfo, p *preparedExecutionPlan, row, condition map[string]Value) map[string]Value {
	if row == nil {
		row = make(map[string]Value, len(p.pointProjection))
	}
	for name := range row {
		selected := false
		for _, col := range p.pointProjection {
			if col.Name == name {
				selected = true
				break
			}
		}
		if !selected {
			delete(row, name)
		}
	}
	offset := 0
	for _, col := range p.pointDecodeMetadata {
		if !col.decode {
			if offset < len(data) {
				offset += skipPointValue(data[offset:])
			}
			continue
		}
		selected := col.project
		previous := condition[col.name]
		if selected {
			previous = row[col.name]
		}
		value := NewNullValue()
		if offset < len(data) {
			var n int
			value, n = decodePointValueInto(data[offset:], previous)
			offset += n
		}
		value = decorateTypedValue(value, col.kind, col.binary, col.collation, col.enums)
		condition[col.name] = value
		if selected {
			row[col.name] = value
		}
	}
	return row
}

func decodePointValueInto(data []byte, previous Value) (Value, int) {
	if len(data) > 0 {
		typ := ColText
		switch data[0] {
		case binTypeDecimal:
			typ = ColDecimal
		case binTypeJSON:
			typ = ColJSON
		}
		if data[0] == binTypeText || data[0] == binTypeDecimal || data[0] == binTypeJSON {
			length, n := binary.Uvarint(data[1:])
			if n > 0 && length <= uint64(len(data)-1-n) && previous.Type == typ && !previous.Null && length == uint64(len(previous.Text)) && string(data[1+n:1+n+int(length)]) == previous.Text {
				return Value{Type: typ, Text: previous.Text}, 1 + n + int(length)
			}
		}
	}
	return decodeValueInto(data, previous.Blob)
}

type pointDecodeMetadata struct {
	name                    string
	kind                    TypeID
	binary, decode, project bool
	collation               Collation
	enums                   []string
}
