package sql

import (
	"fmt"
	"strings"
)

// WithClause is scoped to its owning statement. CTE results are materialized
// once per statement, so repeated references do not repeat volatile functions.
type WithClause struct {
	Recursive bool
	Entries   []CommonTableExpr
}
type CommonTableExpr struct {
	Name         string
	Columns      []string
	Query        Statement
	Materialized bool
}

func selectQuery(query Statement) *SelectStmt {
	if sel, ok := query.(*SelectStmt); ok {
		return sel
	}
	union := *query.(*UnionStmt)
	with := union.With
	union.With = nil
	return &SelectStmt{With: with, Compound: &union, Limit: -1}
}

func (p *Parser) parseWith() (Statement, error) {
	if err := p.expectKeyword("WITH"); err != nil {
		return nil, err
	}
	w := &WithClause{}
	if p.cur.Value == "RECURSIVE" {
		w.Recursive = true
		p.advance()
	}
	names := make(map[string]bool)
	for {
		if p.cur.Type != TokIdent {
			return nil, fmt.Errorf("expected CTE name, got %q", p.cur.Value)
		}
		c := CommonTableExpr{Name: strings.ToLower(p.cur.Value)}
		if names[c.Name] {
			return nil, fmt.Errorf("duplicate CTE name %q", c.Name)
		}
		names[c.Name] = true
		p.advance()
		if p.cur.Type == TokLParen {
			p.advance()
			columns := make(map[string]bool)
			for {
				if p.cur.Type != TokIdent {
					return nil, fmt.Errorf("expected CTE column name")
				}
				name := strings.ToLower(p.cur.Value)
				if columns[name] {
					return nil, fmt.Errorf("duplicate CTE column name %q", name)
				}
				columns[name] = true
				c.Columns = append(c.Columns, name)
				p.advance()
				if p.cur.Type != TokComma {
					break
				}
				p.advance()
			}
			if err := p.expect(TokRParen); err != nil {
				return nil, err
			}
		}
		if err := p.expectKeyword("AS"); err != nil {
			return nil, err
		}
		if p.cur.Value == "MATERIALIZED" {
			c.Materialized = true
			p.advance()
		}
		if p.cur.Value == "NOT" {
			return nil, fmt.Errorf("NOT MATERIALIZED CTEs are not supported")
		}
		if err := p.expect(TokLParen); err != nil {
			return nil, err
		}
		query, err := p.parseSelect()
		if err != nil {
			return nil, err
		}
		c.Query = query
		if err := p.expect(TokRParen); err != nil {
			return nil, err
		}
		w.Entries = append(w.Entries, c)
		if p.cur.Type != TokComma {
			break
		}
		p.advance()
	}
	if p.cur.Value == "WITH" {
		return nil, fmt.Errorf("expected SELECT, INSERT, UPDATE or DELETE after CTE definitions")
	}
	body, err := p.parseStatement()
	if err != nil {
		return nil, err
	}
	switch stmt := body.(type) {
	case *SelectStmt:
		stmt.With = w
	case *UnionStmt:
		stmt.With = w
	case *InsertStmt:
		stmt.With = w
	case *UpdateStmt:
		stmt.With = w
	case *DeleteStmt:
		stmt.With = w
	default:
		return nil, fmt.Errorf("WITH requires SELECT, INSERT, UPDATE or DELETE")
	}
	return body, nil
}

func formatWithSQL(w *WithClause) string {
	var b strings.Builder
	b.WriteString("WITH ")
	if w.Recursive {
		b.WriteString("RECURSIVE ")
	}
	for i, c := range w.Entries {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(quoteIdent(c.Name))
		if len(c.Columns) > 0 {
			b.WriteString("(")
			for j, col := range c.Columns {
				if j > 0 {
					b.WriteString(", ")
				}
				b.WriteString(quoteIdent(col))
			}
			b.WriteString(")")
		}
		b.WriteString(" AS ")
		if c.Materialized {
			b.WriteString("MATERIALIZED ")
		}
		b.WriteString("(")
		b.WriteString(formatCTEQuery(c.Query))
		b.WriteString(")")
	}
	b.WriteString(" ")
	return b.String()
}
func formatCTEQuery(query Statement) string {
	switch q := query.(type) {
	case *SelectStmt:
		return formatSelectSQL(q)
	case *UnionStmt:
		prefix := ""
		if q.With != nil {
			prefix = formatWithSQL(q.With)
		}
		op := q.Op
		if q.All {
			op += " ALL"
		}
		// Reuse SELECT formatting for the compound's final ORDER/LIMIT/OFFSET.
		tail := &SelectStmt{Limit: q.Limit, Offset: q.Offset, OrderBy: q.OrderBy}
		formatted := formatSelectSQL(tail)
		suffix := strings.TrimPrefix(formatted, "SELECT ")
		return prefix + formatSelectSQL(q.Left) + " " + op + " " + formatCTEQuery(q.Right) + suffix
	}
	panic("not a query")
}
