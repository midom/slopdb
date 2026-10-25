package sql

import (
	"bytes"
	"strings"

	"slopdb/storage"
)

const literalReadRows = 64
const literalReadBytes = 64 << 10

// Syntax classification contains no catalog assumptions. Name resolution may
// skip reflection only when every reference is bare and every value is literal.
func literalBoundedSelectEligible(stmt *SelectStmt) bool {
	if !literalReadProjectionEligible(stmt) || len(stmt.OrderBy) > 1 || stmt.Offset < 0 {
		return false
	}
	for _, order := range stmt.OrderBy {
		ref, ok := order.Expr.(*ColumnRefExpr)
		if !ok || strings.Contains(ref.Name, ".") {
			return false
		}
	}
	if in, ok := stmt.Where.(*InListExpr); ok {
		return !in.Not && len(in.List) <= literalReadRows && literalPrimaryPredicate(in, "", true)
	}
	return stmt.Limit >= 0 && stmt.Limit <= literalReadRows && stmt.Offset <= 1024 && literalPrimaryPredicate(stmt.Where, "", false)
}

func literalPrimaryPredicate(expr Expression, primary string, allowIn bool) bool {
	column := func(expr Expression) bool {
		ref, ok := expr.(*ColumnRefExpr)
		return ok && !strings.Contains(ref.Name, ".") && (primary == "" || ref.Name == primary)
	}
	value := func(expr Expression, allowNull bool) bool {
		// Arithmetic constants must be evaluated in the session's dialect:
		// MySQL 5/2 is DECIMAL 2.5, unlike native integer division. Only unary
		// signs over actual literals are dialect-independent here.
		leaf := expr
		for {
			unary, ok := leaf.(*UnaryExpr)
			if !ok {
				break
			}
			if unary.Op != "+" && unary.Op != "-" {
				return false
			}
			leaf = unary.Right
		}
		if _, ok := leaf.(*LiteralExpr); !ok {
			return false
		}
		lit, ok := tupleLiteral(expr)
		return ok && ((allowNull && lit.Value.Null) || preparedPlainSignedInteger(lit.Value))
	}
	switch e := expr.(type) {
	case nil:
		return true
	case *InListExpr:
		if !allowIn || e.Not || !column(e.Expr) {
			return false
		}
		for _, item := range e.List {
			if !value(item, true) {
				return false
			}
		}
		return true
	case *BinaryExpr:
		if e.Op == "AND" {
			return literalPrimaryPredicate(e.Left, primary, false) && literalPrimaryPredicate(e.Right, primary, false)
		}
		switch e.Op {
		case "=", "<", "<=", ">", ">=":
			return column(e.Left) && value(e.Right, false) || column(e.Right) && value(e.Left, false)
		}
	case *BetweenExpr:
		return !e.Not && column(e.Expr) && value(e.Low, false) && value(e.High, false)
	}
	return false
}

// TrySelectReadInto selects small, proven primary-key reads without creating a
// streaming cursor. Larger/unsupported reads keep their normal streaming path.
// The caller holds the statement snapshot and catalog lock. Both result buffers
// and storage leases follow the ownership rules of TrySelectPointInto.
func (s *Schema) TrySelectReadInto(stmt *SelectStmt, dst *ResultSet) (*ResultSet, bool, error) {
	if LiteralPointSelectEligible(stmt) {
		return s.TrySelectPointInto(stmt, dst)
	}
	if !literalBoundedSelectEligible(stmt) || s.cteScope != nil || s.profile != nil || catalogRelationName(stmt.From) {
		return nil, false, nil
	}
	info := s.tables[strings.ToLower(stmt.From)]
	if info == nil {
		return nil, false, nil
	}
	if err := s.checkInterrupt(); err != nil {
		return nil, true, err
	}
	restore, err := s.selectReadView(stmt)
	if err != nil {
		return nil, true, err
	}
	defer restore()
	defer s.clearSelectCaches()
	return s.selectLiteralBounded(stmt, info, dst)
}

func (s *Schema) selectLiteralBounded(stmt *SelectStmt, info *TableInfo, dst *ResultSet) (*ResultSet, bool, error) {
	if !literalBoundedSelectEligible(stmt) || s.cteScope != nil || s.profile != nil || len(info.PKColumns) != 1 {
		return nil, false, nil
	}
	primary := info.PKColumns[0]
	signed := false
	for _, col := range info.Columns {
		if col.Name == primary {
			kind := col.LogicalType().Kind
			declared := LogicalTypeFromDeclaration(col.Declared).Kind
			signed = col.ColType == ColInteger && !col.Unsigned && !col.Binary && kind >= TypeInt8 && kind <= TypeInt64 && (declared == TypeUnknown || declared >= TypeInt8 && declared <= TypeInt64)
			break
		}
	}
	if !signed || !literalPrimaryPredicate(stmt.Where, primary, true) {
		return nil, false, nil
	}
	for _, order := range stmt.OrderBy {
		if order.Expr.(*ColumnRefExpr).Name != primary {
			return nil, false, nil
		}
	}
	p := s.directBoundedPlan
	if p == nil {
		p = &preparedExecutionPlan{}
		s.directBoundedPlan = p
	}
	if !sameLiteralProjection(stmt, info, p.pointProjection) {
		*p = preparedExecutionPlan{}
	}
	p.where = stmt.Where
	defer func() {
		p.releaseValues()
		p.where = nil
		for i := range p.equalities {
			p.equalities[i].literal = nil
		}
	}()
	defer s.restorePreparedPlan(s.usePreparedPlan(p, info))
	ready, err := s.preparePhysicalReadProjection(stmt, info, p)
	if err != nil {
		return nil, true, err
	}
	if !ready {
		return nil, false, nil
	}
	constraints := tupleConstraints(stmt.Where, info.Name, stmt.FromAlias, info)
	plan := buildTuplePlan(stmt, info, nil, constraints)
	if plan == nil || !plan.ordered || plan.residual != nil {
		return nil, false, nil
	}
	// Read at most a small result window before transferring any caller buffers.
	// Oversized values fall back without altering dst or restarting its snapshot.
	var raw [literalReadRows][]byte
	count, byteCount, skipped := 0, 0, int64(0)
	accept := func(data []byte) bool {
		if skipped < stmt.Offset {
			skipped++
			return true
		}
		if len(data) > literalReadBytes-byteCount || count == len(raw) {
			return false
		}
		raw[count] = data
		count++
		byteCount += len(data)
		return true
	}
	limited := func() bool { return stmt.Limit >= 0 && int64(count) >= stmt.Limit }
	if !limited() && plan.points != nil {
		// The tuple planner produces sorted keys, including normalized encodings.
		// SQL IN is a set even though storage's multi-get preserves duplicates.
		keys := plan.points[:0]
		for _, key := range plan.points {
			if len(keys) == 0 || !bytes.Equal(key, keys[len(keys)-1]) {
				keys = append(keys, key)
			}
		}
		values, fits, e := s.schemaMultiGetBounded(keys, literalReadBytes)
		if e != nil {
			return nil, true, e
		}
		if !fits {
			return nil, false, nil
		}
		for i := range values {
			if err := s.checkInterrupt(); err != nil {
				return nil, true, err
			}
			position := i
			if plan.reverse {
				position = len(values) - 1 - i
			}
			if values[position].Found && !accept(values[position].Value) {
				return nil, false, nil
			}
			if limited() {
				break
			}
		}
	} else if !limited() {
		for i := range plan.intervals {
			position := i
			if plan.reverse {
				position = len(plan.intervals) - 1 - i
			}
			interval := plan.intervals[position]
			it := s.newRangeIterator(interval.lower, interval.upper, plan.reverse)
			oversized := false
			for it.Valid() && !limited() {
				if err = s.checkInterrupt(); err != nil {
					break
				}
				if !accept(it.Value()) {
					oversized = true
					break
				}
				if limited() {
					break
				}
				if plan.reverse {
					it.Prev()
				} else {
					it.Next()
				}
			}
			if err == nil {
				err = storage.IteratorError(it)
			}
			it.Close()
			if err != nil {
				return nil, true, err
			}
			if oversized {
				return nil, false, nil
			}
			if limited() {
				break
			}
		}
	}
	if err := s.checkInterrupt(); err != nil {
		return nil, true, err
	}
	result := dst
	if result == nil {
		result = &ResultSet{}
	}
	result.Columns = append(result.Columns[:0], p.pointProjection...)
	result.Profile, result.rowKeys = nil, nil
	result.Rows = result.Rows[:0]
	for i := 0; i < count; i++ {
		var row map[string]Value
		if dst != nil && i < cap(result.Rows) {
			row = result.Rows[:cap(result.Rows)][i]
		}
		if p.pointDecodeCount > len(p.pointProjection) {
			if p.pointSelectScratch == nil {
				p.pointSelectScratch = make(map[string]Value, p.pointDecodeCount)
			}
			row = decodePointProjectionInto(raw[i], info, p, row, p.pointSelectScratch)
			for _, col := range p.pointProjection {
				p.pointSelectScratch[col.Name] = NewNullValue()
			}
		} else {
			row = decodePointRowInto(raw[i], info, p, row)
		}
		result.Rows = append(result.Rows, row)
	}
	return result, true, nil
}
