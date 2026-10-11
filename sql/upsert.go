package sql

import (
	"fmt"
	"strings"
)

// OnConflictClause applies only to a PRIMARY KEY or UNIQUE violation. Target
// omission matches any unique constraint. Partial and expression targets are
// deliberately unsupported rather than approximated by their column names.
type OnConflictClause struct {
	Target    []string
	DoNothing bool
	Set       []UpdateSet
	Where     Expression
}

func (p *Parser) parseOnConflict() (*OnConflictClause, error) {
	p.advance()
	if err := p.expectKeyword("CONFLICT"); err != nil {
		return nil, err
	}
	clause := &OnConflictClause{}
	if p.cur.Type == TokLParen {
		p.advance()
		for {
			if p.cur.Type != TokIdent && p.cur.Type != TokKeyword {
				return nil, fmt.Errorf("ON CONFLICT target requires column names")
			}
			clause.Target = append(clause.Target, strings.ToLower(p.cur.Value))
			p.advance()
			if p.cur.Type != TokComma {
				break
			}
			p.advance()
		}
		if err := p.expect(TokRParen); err != nil {
			return nil, fmt.Errorf("ON CONFLICT supports only column targets: %w", err)
		}
	}
	if err := p.expectKeyword("DO"); err != nil {
		return nil, err
	}
	if p.cur.Value == "NOTHING" {
		clause.DoNothing = true
		p.advance()
		return clause, nil
	}
	if err := p.expectKeyword("UPDATE"); err != nil {
		return nil, err
	}
	if err := p.expectKeyword("SET"); err != nil {
		return nil, err
	}
	for {
		if p.cur.Type != TokIdent && p.cur.Type != TokKeyword {
			return nil, fmt.Errorf("expected ON CONFLICT update column")
		}
		name := strings.ToLower(p.cur.Value)
		p.advance()
		if err := p.expect(TokEq); err != nil {
			return nil, err
		}
		value, err := p.parseExpression()
		if err != nil {
			return nil, err
		}
		clause.Set = append(clause.Set, UpdateSet{Column: name, Value: value})
		if p.cur.Type != TokComma {
			break
		}
		p.advance()
	}
	if p.cur.Value == "WHERE" {
		p.advance()
		var err error
		clause.Where, err = p.parseExpression()
		if err != nil {
			return nil, err
		}
	}
	return clause, nil
}

func (s *Schema) validateOnConflict(clause *OnConflictClause, info *TableInfo) error {
	if len(clause.Target) > 0 {
		found := false
		for _, group := range s.uniqueGroups(info.Name, info) {
			found = found || candidateColumns(clause.Target, group)
		}
		if !found {
			return fmt.Errorf("ON CONFLICT target does not match a PRIMARY KEY or UNIQUE constraint")
		}
	}
	if clause.DoNothing {
		return nil
	}
	columns := make(map[string]bool)
	for _, col := range info.Columns {
		columns[col.Name] = true
	}
	seen := make(map[string]bool)
	for _, set := range clause.Set {
		if !columns[set.Column] {
			return fmt.Errorf("no such column: %s", set.Column)
		}
		if seen[set.Column] {
			return fmt.Errorf("duplicate ON CONFLICT assignment: %s", set.Column)
		}
		seen[set.Column] = true
	}
	// The pseudo-table is a read-only correlated scope, so ordinary DML
	// validation also checks subqueries and qualified references without mutating
	// the table's catalog or the prepared template.
	previous := s.outerRow
	scope := make(map[string]Value, len(previous)+len(info.Columns))
	for name, v := range previous {
		scope[name] = v
	}
	for _, col := range info.Columns {
		scope["excluded."+col.Name] = NewNullValue()
	}
	s.outerRow = scope
	defer func() { s.outerRow = previous }()
	return s.validateDMLExpressions(info, clause.Where, clause.Set, nil)
}

// handleInsertConflict runs after BEFORE INSERT and the insert locks. Unique
// lookups must see committed changes made while waiting for those locks, while
// row locking still validates the statement snapshot and asks the driver to
// retry if necessary. DO UPDATE never recursively handles its own violations.
func (s *Schema) handleInsertConflict(table string, info *TableInfo, attempted map[string]Value, clause *OnConflictClause) (bool, map[string]Value, error) {
	if err := s.checkNonUniqueConstraints(table, info, attempted); err != nil {
		return false, nil, err
	}
	groups := s.uniqueGroups(table, info)
	for _, group := range groups {
		if len(clause.Target) > 0 && !candidateColumns(clause.Target, group) {
			continue
		}
		var predicate Expression
		nullable := false
		for _, name := range group {
			v := attempted[name]
			if v.Null {
				nullable = true
				break
			}
			equal := &BinaryExpr{Op: "=", Left: &ColumnRefExpr{Name: name}, Right: &LiteralExpr{Value: v}}
			if predicate == nil {
				predicate = equal
			} else {
				predicate = &BinaryExpr{Op: "AND", Left: predicate, Right: equal}
			}
		}
		if nullable {
			continue
		}
		active := s.statementActive
		if s.lockHandler != nil {
			s.statementActive = false
		}
		entries, err := s.matchingDMLRows(table, info, predicate, 1, 0)
		s.statementActive = active
		if err != nil {
			return false, nil, err
		}
		if len(entries) == 0 {
			continue
		}
		entry := entries[0]
		original := decodeRow(entry.Value, info)
		if err := s.lockRow(table, info, original, entry.Key.UserKey); err != nil {
			return false, nil, err
		}
		if clause.DoNothing {
			return true, nil, nil
		}
		evaluation := make(map[string]Value, 3*len(original))
		for name, v := range original {
			evaluation[name] = v
			evaluation[table+"."+name] = v
		}
		for name, v := range attempted {
			evaluation["excluded."+name] = v
		}

		previous := s.outerRow
		// Correlated SELECTs in the SET/WHERE expressions can reference excluded.
		scope := make(map[string]Value, len(previous)+len(evaluation))
		for name, v := range previous {
			scope[name] = v
		}
		for name, v := range evaluation {
			scope[name] = v
		}
		s.outerRow = scope

		defer func() { s.outerRow = previous }()
		if clause.Where != nil {
			match, err := s.evalCondition(clause.Where, evaluation)
			if err != nil {
				return false, nil, err
			}
			if !match {
				return true, nil, nil
			}
		}
		updated, err := s.applyRowUpdate(table, info, entry.Key.UserKey, original, clause.Set, evaluation)
		s.outerRow = previous
		return true, updated, err
	}
	return false, nil, nil
}
