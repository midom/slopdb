package sql

import (
	"fmt"
	"strings"
)

// Window bounds use expression values so prepared bindings remain int64 and
// receive the same private AST ownership as ordinary expressions.
type WindowBound struct {
	Kind   string
	Offset Expression
}
type WindowFrame struct {
	Unit       string
	Start, End WindowBound
}
type WindowSpec struct {
	Name        string
	DirectName  bool
	PartitionBy []Expression
	OrderBy     []OrderByClause
	Frame       *WindowFrame
}
type WindowDefinition struct {
	Name string
	Spec *WindowSpec
}

func (p *Parser) windowKeyword(word string) bool { return strings.EqualFold(p.cur.Value, word) }
func (p *Parser) parseWindowCall(f *FuncCallExpr) (Expression, error) {
	if !p.windowKeyword("OVER") {
		return f, nil
	}
	p.advance()
	if p.cur.Type != TokLParen {
		if p.cur.Type != TokIdent {
			return nil, fmt.Errorf("expected window name or specification after OVER")
		}
		f.Window = &WindowSpec{Name: strings.ToLower(p.cur.Value), DirectName: true}
		p.advance()
		return f, nil
	}
	spec, err := p.parseWindowSpec()
	f.Window = spec
	return f, err
}
func (p *Parser) parseWindowDefinitions(s *SelectStmt) error {
	if !p.windowKeyword("WINDOW") {
		return nil
	}
	p.advance()
	for {
		if p.cur.Type != TokIdent {
			return fmt.Errorf("expected window name")
		}
		name := strings.ToLower(p.cur.Value)
		p.advance()
		if err := p.expectKeyword("AS"); err != nil {
			return err
		}
		spec, err := p.parseWindowSpec()
		if err != nil {
			return err
		}
		s.Windows = append(s.Windows, WindowDefinition{name, spec})
		if p.cur.Type != TokComma {
			return nil
		}
		p.advance()
	}
}
func (p *Parser) parseWindowSpec() (*WindowSpec, error) {
	if err := p.expect(TokLParen); err != nil {
		return nil, err
	}
	w := &WindowSpec{}
	if p.cur.Type == TokIdent {
		w.Name = strings.ToLower(p.cur.Value)
		p.advance()
	}
	if p.windowKeyword("PARTITION") {
		p.advance()
		if err := p.expectKeyword("BY"); err != nil {
			return nil, err
		}
		for {
			e, err := p.parseExpression()
			if err != nil {
				return nil, err
			}
			w.PartitionBy = append(w.PartitionBy, e)
			if p.cur.Type != TokComma {
				break
			}
			p.advance()
		}
	}
	if p.windowKeyword("ORDER") {
		p.advance()
		if err := p.expectKeyword("BY"); err != nil {
			return nil, err
		}
		for {
			e, err := p.parseExpression()
			if err != nil {
				return nil, err
			}
			o := OrderByClause{Expr: e}
			if p.windowKeyword("ASC") || p.windowKeyword("DESC") {
				o.Desc = p.windowKeyword("DESC")
				p.advance()
			}
			if p.windowKeyword("NULLS") {
				p.advance()
				if !p.windowKeyword("FIRST") && !p.windowKeyword("LAST") {
					return nil, fmt.Errorf("expected FIRST or LAST after NULLS")
				}
				o.NullsFirst = p.windowKeyword("FIRST")
				o.NullsLast = p.windowKeyword("LAST")
				p.advance()
			}
			w.OrderBy = append(w.OrderBy, o)
			if p.cur.Type != TokComma {
				break
			}
			p.advance()
		}
	}
	if p.windowKeyword("ROWS") || p.windowKeyword("RANGE") || p.windowKeyword("GROUPS") {
		f := &WindowFrame{Unit: strings.ToUpper(p.cur.Value), End: WindowBound{Kind: "CURRENT ROW"}}
		p.advance()
		between := p.windowKeyword("BETWEEN")
		if between {
			p.advance()
		}
		var err error
		f.Start, err = p.parseWindowBound()
		if err != nil {
			return nil, err
		}
		if between {
			if err = p.expectKeyword("AND"); err != nil {
				return nil, err
			}
			f.End, err = p.parseWindowBound()
			if err != nil {
				return nil, err
			}
		}
		if f.Start.Kind == "UNBOUNDED FOLLOWING" || f.End.Kind == "UNBOUNDED PRECEDING" {
			return nil, fmt.Errorf("invalid window frame bounds")
		}
		boundOrder := map[string]int{"UNBOUNDED PRECEDING": 0, "PRECEDING": 1, "CURRENT ROW": 2, "FOLLOWING": 3, "UNBOUNDED FOLLOWING": 4}
		if boundOrder[f.Start.Kind] > boundOrder[f.End.Kind] {
			return nil, fmt.Errorf("window frame end precedes start")
		}
		w.Frame = f
	}
	if p.windowKeyword("EXCLUDE") {
		return nil, fmt.Errorf("window EXCLUDE is not supported")
	}
	if err := p.expect(TokRParen); err != nil {
		return nil, err
	}
	return w, nil
}
func (p *Parser) parseWindowBound() (WindowBound, error) {
	b := WindowBound{}
	if p.windowKeyword("UNBOUNDED") {
		p.advance()
		if !p.windowKeyword("PRECEDING") && !p.windowKeyword("FOLLOWING") {
			return b, fmt.Errorf("expected PRECEDING or FOLLOWING")
		}
		b.Kind = "UNBOUNDED " + strings.ToUpper(p.cur.Value)
		p.advance()
		return b, nil
	}
	if p.windowKeyword("CURRENT") {
		p.advance()
		if err := p.expectKeyword("ROW"); err != nil {
			return b, err
		}
		b.Kind = "CURRENT ROW"
		return b, nil
	}
	// A frame offset is a scalar expression, not a boolean BETWEEN expression.
	e, err := p.parseAdditive()
	if err != nil {
		return b, err
	}
	b.Offset = e
	if !p.windowKeyword("PRECEDING") && !p.windowKeyword("FOLLOWING") {
		return b, fmt.Errorf("expected PRECEDING or FOLLOWING after frame offset")
	}
	b.Kind = strings.ToUpper(p.cur.Value)
	p.advance()
	return b, nil
}
func formatWindowCall(w *WindowSpec) string {
	if w == nil {
		return ""
	}
	if w.DirectName {
		return " OVER " + formatRef(w.Name)
	}
	return " OVER (" + formatWindowSpec(w) + ")"
}
func formatWindowSpec(w *WindowSpec) string {
	var parts []string
	if w.Name != "" {
		parts = append(parts, formatRef(w.Name))
	}
	if len(w.PartitionBy) > 0 {
		a := make([]string, len(w.PartitionBy))
		for i, e := range w.PartitionBy {
			a[i] = formatExpr(e)
		}
		parts = append(parts, "PARTITION BY "+strings.Join(a, ", "))
	}
	if len(w.OrderBy) > 0 {
		var a []string
		for _, o := range w.OrderBy {
			x := formatExpr(o.Expr)
			if o.Desc {
				x += " DESC"
			}
			if o.NullsFirst {
				x += " NULLS FIRST"
			}
			if o.NullsLast {
				x += " NULLS LAST"
			}
			a = append(a, x)
		}
		parts = append(parts, "ORDER BY "+strings.Join(a, ", "))
	}
	if f := w.Frame; f != nil {
		parts = append(parts, f.Unit+" BETWEEN "+formatWindowBound(f.Start)+" AND "+formatWindowBound(f.End))
	}
	return strings.Join(parts, " ")
}
func formatWindowBound(b WindowBound) string {
	if b.Offset != nil {
		return formatExpr(b.Offset) + " " + b.Kind
	}
	return b.Kind
}
func formatWindowDefinitions(defs []WindowDefinition) string {
	var a []string
	for _, d := range defs {
		a = append(a, formatRef(d.Name)+" AS ("+formatWindowSpec(d.Spec)+")")
	}
	return " WINDOW " + strings.Join(a, ", ")
}
