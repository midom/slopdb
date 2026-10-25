package sql

import (
	"fmt"
	"strings"
)

// MATCH is represented by an ordinary private expression tree so prepared
// binding, expression walking and stored-query reparsing retain their usual
// ownership. The final two arguments are the search expression and mode.
func (p *Parser) parseFulltextMatch() (Expression, error) {
	p.advance()
	if err := p.expect(TokLParen); err != nil {
		return nil, err
	}
	f := &FuncCallExpr{Name: fulltextFunction}
	for {
		if p.cur.Type != TokIdent {
			return nil, fmt.Errorf("MATCH requires column names")
		}
		name := strings.ToLower(p.cur.Value)
		p.advance()
		if p.cur.Type == TokDot {
			p.advance()
			if p.cur.Type != TokIdent {
				return nil, fmt.Errorf("MATCH requires a column after qualifier")
			}
			name += "." + strings.ToLower(p.cur.Value)
			p.advance()
		}
		f.Args = append(f.Args, &ColumnRefExpr{Name: name})
		if p.cur.Type != TokComma {
			break
		}
		p.advance()
	}
	if err := p.expect(TokRParen); err != nil {
		return nil, err
	}
	if !p.optionalAlterWord("AGAINST") {
		return nil, fmt.Errorf("expected AGAINST after MATCH")
	}
	if err := p.expect(TokLParen); err != nil {
		return nil, err
	}
	q, err := p.parseBitwise()
	if err != nil {
		return nil, err
	}
	mode := "NATURAL"
	if p.optionalAlterWord("IN") {
		switch {
		case p.optionalAlterWord("BOOLEAN"):
			mode = "BOOLEAN"
		case p.optionalAlterWord("NATURAL"):
			if !p.optionalAlterWord("LANGUAGE") {
				return nil, fmt.Errorf("expected LANGUAGE in fulltext mode")
			}
		default:
			return nil, fmt.Errorf("expected BOOLEAN or NATURAL LANGUAGE mode")
		}
		if !p.optionalAlterWord("MODE") {
			return nil, fmt.Errorf("expected MODE in AGAINST")
		}
	}
	if err := p.expect(TokRParen); err != nil {
		return nil, err
	}
	f.Args = append(f.Args, q, &LiteralExpr{Value: NewTextValue(mode)})
	return f, nil
}

func formatFulltextMatch(f *FuncCallExpr) string {
	if len(f.Args) < 3 {
		return "MATCH() AGAINST('')"
	}
	names := make([]string, len(f.Args)-2)
	for i, e := range f.Args[:len(names)] {
		names[i] = formatExpr(e)
	}
	mode := "IN NATURAL LANGUAGE MODE"
	if l, ok := f.Args[len(f.Args)-1].(*LiteralExpr); ok && l.Value.Text == "BOOLEAN" {
		mode = "IN BOOLEAN MODE"
	}
	return "MATCH(" + strings.Join(names, ", ") + ") AGAINST(" + formatExpr(f.Args[len(f.Args)-2]) + " " + mode + ")"
}
