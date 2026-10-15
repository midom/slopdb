package sql

import (
	"math"
	"strings"
)

// PreparedPlanStats is lifetime instrumentation, not an execution result cache.
// Builds count private-tree compilation or metadata invalidation; hits count
// subsequent uses against the same catalog and statistics snapshot.
type PreparedPlanStats struct {
	Builds uint64 `json:"builds"`
	Hits   uint64 `json:"hits"`
}

func (q *PreparedQuery) PlanStats() PreparedPlanStats {
	return PreparedPlanStats{Builds: q.planBuilds.Load(), Hits: q.planHits.Load()}
}

type preparedEquality struct {
	name    string
	typ     ColumnType
	literal *LiteralExpr
}
type preparedRangeTerm struct {
	name, op string
	typ      ColumnType
	value    Expression
	term     Expression
}

// Each pooled BoundQuery owns this mutable cache exclusively until Release.
// Catalog objects and physical candidates are immutable; runtime keys, ranges,
// costs and typed registers stay private. No snapshots or iterators are retained.
type preparedExecutionPlan struct {
	owner                              *PreparedQuery
	where                              Expression
	fromAlias                          string
	info                               *TableInfo
	distribution                       *tableStatistics
	catalogVersion, stateVersion       uint64
	initialized, validated             bool
	equalities                         []preparedEquality
	pointPossible                      bool
	pointKey                           []byte
	pointKeyScratch, pointValueScratch []byte
	pointKeyReady, pointKeyOK          bool
	ranges                             []preparedRangeTerm
	rangeFallback                      bool
	coverage                           map[*IndexInfo]bool
	candidatesReady                    bool
	pointUpdate                        *PointUpdate
	pointUpdateReady                   bool
	aggregate                          *typedAggregate
	aggregateReady                     bool
	pointProjectionReady               bool
	pointProjection                    []ColumnDef
}

func preparedSelectEligible(stmt *SelectStmt) bool {
	if hasWindows(stmt) || stmt.With != nil || stmt.Compound != nil || stmt.materialized != nil || stmt.FromExpr != nil || stmt.Virtual != nil || len(stmt.Joins) != 0 || stmt.From == "" {
		return false
	}
	safe := true
	check := func(expr Expression) {
		_ = walkExpression(expr, func(e Expression) (bool, error) {
			switch e.(type) {
			case *SubqueryExpr, *ExistsExpr:
				safe = false
				return true, nil
			}
			return false, nil
		})
	}
	check(stmt.Where)
	check(stmt.Having)
	for _, c := range stmt.Columns {
		check(c.Expr)
	}
	for _, e := range stmt.GroupBy {
		check(e)
	}
	for _, o := range stmt.OrderBy {
		check(o.Expr)
	}
	return safe
}

func (s *Schema) restorePreparedPlan(previous *preparedExecutionPlan) { s.preparedPlan = previous }

func (s *Schema) usePreparedPlan(p *preparedExecutionPlan, info *TableInfo) *preparedExecutionPlan {
	previous := s.preparedPlan
	if s.cteScope != nil {
		s.preparedPlan = nil
		return previous
	}
	catalog, state := s.readCatalogVersion, s.readStateVersion
	if s.parent == nil {
		catalog, state = s.catalogVersion, s.stateVersion
	}
	distribution := s.distributions[info.Name]
	if !p.initialized || p.info != info || p.distribution != distribution || p.catalogVersion != catalog || p.stateVersion != state {
		owner, where, alias := p.owner, p.where, p.fromAlias
		*p = preparedExecutionPlan{owner: owner, where: where, fromAlias: alias, info: info, distribution: distribution, catalogVersion: catalog, stateVersion: state, initialized: true}
		p.compilePointBindings()
		p.compileRangeTerms(info.Name)
		owner.planBuilds.Add(1)
	} else {
		p.owner.planHits.Add(1)
	}
	p.pointKey, p.pointKeyReady, p.pointKeyOK = nil, false, false
	s.preparedPlan = p
	return previous
}

func (p *preparedExecutionPlan) compilePointBindings() {
	types := make(map[string]ColumnType, len(p.info.PKColumns))
	for _, name := range p.info.PKColumns {
		for _, c := range p.info.Columns {
			if c.Name == name {
				types[name] = c.ColType
				break
			}
		}
	}
	var visit func(Expression)
	visit = func(e Expression) {
		b, ok := e.(*BinaryExpr)
		if !ok {
			return
		}
		if b.Op == "AND" {
			visit(b.Left)
			visit(b.Right)
			return
		}
		if b.Op != "=" {
			return
		}
		col, cOK := b.Left.(*ColumnRefExpr)
		lit, lOK := b.Right.(*LiteralExpr)
		if !cOK || !lOK {
			col, cOK = b.Right.(*ColumnRefExpr)
			lit, lOK = b.Left.(*LiteralExpr)
		}
		if !cOK || !lOK {
			return
		}
		name := strings.ToLower(col.Name)
		if prefix, rest, qualified := strings.Cut(name, "."); qualified {
			if prefix != p.info.Name {
				return
			}
			name = rest
		}
		if typ, ok := types[name]; ok {
			p.equalities = append(p.equalities, preparedEquality{name, typ, lit})
		}
	}
	visit(p.where)
	p.pointPossible = p.completePointBindings()
}

func (p *preparedExecutionPlan) pointComplete() bool { return p.pointPossible }
func (p *preparedExecutionPlan) completePointBindings() bool {
	for _, name := range p.info.PKColumns {
		found := false
		for _, b := range p.equalities {
			if b.name == name {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return len(p.info.PKColumns) != 0
}

func preparedExactValue(v Value, typ ColumnType) bool {
	return !v.Null && !v.Binary && v.Type == typ && (typ != ColReal || (v.Real != 0 && !math.IsNaN(v.Real)))
}
func (p *preparedExecutionPlan) primaryKey(table string, info *TableInfo) ([]byte, bool) {
	if p.pointKeyReady {
		return p.pointKey, p.pointKeyOK
	}
	p.pointKeyReady = true
	if !p.pointComplete() {
		return nil, false
	}
	key := append(p.pointKeyScratch[:0], info.storagePrefix(table)...)
	for _, name := range info.PKColumns {
		var value Value
		found := false
		for _, binding := range p.equalities {
			if binding.name == name && preparedExactValue(binding.literal.Value, binding.typ) {
				value, found = binding.literal.Value, true
			}
		}
		if !found {
			return nil, false
		}
		if value.Type == ColText && !info.binaryColumn(name) {
			value.Text = info.collation.Key(value.Text)
		}
		key = appendClusteredValue(key, value)
	}
	p.pointKeyScratch = key
	p.pointKey, p.pointKeyOK = key, true
	return p.pointKey, true
}

// Compile predicate traversal, column qualification and declared types once.
// Runtime binding still intersects bounds, validates types and preserves terms
// for residual evaluation. IN lists retain the general planner's deduplication.
func (p *preparedExecutionPlan) compileRangeTerms(table string) {
	types := make(map[string]ColumnType, len(p.info.Columns))
	for _, col := range p.info.Columns {
		types[col.Name] = col.ColType
	}
	var visit func(Expression)
	visit = func(expr Expression) {
		if e, ok := expr.(*BetweenExpr); ok && !e.Not {
			visit(&BinaryExpr{Op: ">=", Left: e.Expr, Right: e.Low})
			visit(&BinaryExpr{Op: "<=", Left: e.Expr, Right: e.High})
			return
		}
		if _, ok := expr.(*InListExpr); ok {
			p.rangeFallback = true
			return
		}
		b, ok := expr.(*BinaryExpr)
		if !ok {
			return
		}
		if b.Op == "AND" {
			visit(b.Left)
			visit(b.Right)
			return
		}
		op := b.Op
		switch op {
		case "=", ">", ">=", "<", "<=":
		default:
			return
		}
		col, ok := b.Left.(*ColumnRefExpr)
		value := b.Right
		if !ok {
			col, ok = b.Right.(*ColumnRefExpr)
			value = b.Left
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
		if !ok {
			return
		}
		// Literal arithmetic/sign folding is rerun with current parameters.
		name := coveringColumn(col.Name, table, p.alias())
		typ, ok := types[name]
		if !ok {
			return
		}
		p.ranges = append(p.ranges, preparedRangeTerm{name, op, typ, value, expr})
	}
	visit(p.where)
}

// alias is set from the SELECT before compiling; UPDATE uses the table name.
func (p *preparedExecutionPlan) alias() string { return p.fromAlias }

func (p *preparedExecutionPlan) constraints(stmt *SelectStmt) map[string]*tupleConstraint {
	if p.rangeFallback {
		return tupleConstraints(stmt.Where, p.info.Name, stmt.FromAlias, p.info)
	}
	result := make(map[string]*tupleConstraint, len(p.ranges))
	for _, term := range p.ranges {
		literal, ok := tupleLiteral(term.value)
		if !ok || !preparedExactValue(literal.Value, term.typ) {
			continue
		}
		v := literal.Value
		c := result[term.name]
		if c == nil {
			c = &tupleConstraint{}
			result[term.name] = c
		}
		switch term.op {
		case "=":
			if c.equal == nil {
				c.equal, c.equalTerm = &v, term.term
			}
		case ">", ">=":
			strict := term.op == ">"
			if c.lower == nil || ValueCompare(v, *c.lower) > 0 || ValueCompare(v, *c.lower) == 0 && strict {
				c.lower, c.lowerStrict, c.lowerTerm = &v, strict, term.term
			}
		case "<", "<=":
			strict := term.op == "<"
			if c.upper == nil || ValueCompare(v, *c.upper) < 0 || ValueCompare(v, *c.upper) == 0 && strict {
				c.upper, c.upperStrict, c.upperTerm = &v, strict, term.term
			}
		}
	}
	return result
}

func (p *preparedExecutionPlan) tupleCoverage(s *Schema, stmt *SelectStmt, idx *IndexInfo) bool {
	if !p.candidatesReady {
		p.coverage = make(map[*IndexInfo]bool)
		for _, index := range s.tableIndexes(p.info.Name) {
			if s.isClusteredIndex(index) {
				continue
			}
			payload := s.lookups().covering[index]
			p.coverage[index] = payload != nil && queryCovered(stmt, p.info.Name, stmt.FromAlias, p.info, payload)
		}
		p.candidatesReady = true
	}
	return p.coverage[idx]
}
func (p *preparedExecutionPlan) releaseValues() {
	p.pointKey, p.pointKeyReady, p.pointKeyOK = nil, false, false
	// Plans are pooled. Retain bounded private scratch, never caller results.
	if cap(p.pointKeyScratch) > 64<<10 {
		p.pointKeyScratch = nil
	}
	if cap(p.pointValueScratch) > 64<<10 {
		p.pointValueScratch = nil
	}
	if p.aggregate != nil {
		if p.aggregate.where != nil {
			clear(p.aggregate.where.registers)
		}
		for _, program := range p.aggregate.group {
			clear(program.registers)
		}
		for _, program := range p.aggregate.args {
			if program != nil {
				clear(program.registers)
			}
		}
	}
}
