package sql

import "strings"

// LiteralPointSelectEligible is a syntactic filter, not a catalog or semantic
// validation. The engine must still prove that the equality covers a primary
// key before choosing point execution instead of a potentially multi-row scan.
func LiteralPointSelectEligible(stmt *SelectStmt) bool {
	if stmt == nil || stmt.prepared != nil || stmt.From == "" || stmt.FromExpr != nil || stmt.With != nil || stmt.Compound != nil || stmt.Virtual != nil || stmt.materialized != nil || stmt.JoinGroup || len(stmt.Joins) != 0 || len(stmt.Windows) != 0 || stmt.Lock != nil || stmt.trackRows || stmt.CalcFoundRows || stmt.Distinct || len(stmt.GroupBy) != 0 || stmt.Having != nil || len(stmt.OrderBy) != 0 || stmt.definitionDialect != 0 {
		return false
	}
	if stmt.SelectAll {
		if len(stmt.Columns) > 1 || len(stmt.Columns) == 1 && (!stmt.Columns[0].Star || stmt.Columns[0].StarQualifier != "") {
			return false
		}
	} else {
		if len(stmt.Columns) == 0 || len(stmt.Columns) > 8 {
			return false
		}
		for _, col := range stmt.Columns {
			ref, ok := col.Expr.(*ColumnRefExpr)
			if !ok || col.Alias != "" || col.Star || strings.Contains(ref.Name, ".") {
				return false
			}
		}
	}
	_, _, ok := literalPointEquality(stmt.Where)
	return ok
}

func literalPointEquality(where Expression) (*ColumnRefExpr, *LiteralExpr, bool) {
	b, ok := where.(*BinaryExpr)
	if !ok || b.Op != "=" {
		return nil, nil, false
	}
	ref, rOK := b.Left.(*ColumnRefExpr)
	lit, lOK := b.Right.(*LiteralExpr)
	if !rOK || !lOK {
		ref, rOK = b.Right.(*ColumnRefExpr)
		lit, lOK = b.Left.(*LiteralExpr)
	}
	return ref, lit, rOK && lOK && !strings.Contains(ref.Name, ".")
}

// TrySelectPointInto runs only a catalog-proven, literal primary-key SELECT.
// The caller must hold the normal statement snapshot and catalog locks. A false
// handled result has not evaluated SQL; streaming execution may safely follow.
// dst follows the same exclusive ownership rules as SelectInto.
func (s *Schema) TrySelectPointInto(stmt *SelectStmt, dst *ResultSet) (*ResultSet, bool, error) {
	if !LiteralPointSelectEligible(stmt) || s.cteScope != nil || s.profile != nil || catalogRelationName(stmt.From) {
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
	return s.selectLiteralPoint(stmt, info, dst)
}

func (s *Schema) selectLiteralPoint(stmt *SelectStmt, info *TableInfo, dst *ResultSet) (*ResultSet, bool, error) {
	if !LiteralPointSelectEligible(stmt) || s.cteScope != nil || s.profile != nil || len(info.PKColumns) != 1 {
		return nil, false, nil
	}
	ref, lit, _ := literalPointEquality(stmt.Where)
	if ref.Name != info.PKColumns[0] {
		return nil, false, nil
	}
	p := s.directPointPlan
	if p == nil {
		p = &preparedExecutionPlan{}
		s.directPointPlan = p
	}
	// The cache holds one projection recipe, never a parsed query or an answer.
	// A changed projection is rebuilt; table/dialect/catalog/statistics changes
	// use exactly the prepared executor's metadata invalidation checks.
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
	if len(p.equalities) != 1 {
		return nil, false, nil
	}
	p.equalities[0].name = info.PKColumns[0]
	p.equalities[0].literal = lit
	return s.selectPoint(stmt, info, p, dst)
}

func sameLiteralProjection(stmt *SelectStmt, info *TableInfo, columns []ColumnDef) bool {
	if stmt.SelectAll {
		if len(columns) != len(info.Columns) {
			return false
		}
		for i := range columns {
			if columns[i].Name != info.Columns[i].Name {
				return false
			}
		}
		return true
	}
	if len(columns) != len(stmt.Columns) {
		return false
	}
	for i, col := range stmt.Columns {
		if columns[i].Name != col.Expr.(*ColumnRefExpr).Name {
			return false
		}
	}
	return true
}
