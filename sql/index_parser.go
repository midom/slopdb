package sql

import (
	"fmt"
	"strings"
)

func (p *Parser) parseIndexColumns() ([]string, error) {
	if err := p.expect(TokLParen); err != nil {
		return nil, err
	}
	var columns []string
	for {
		name := strings.ToLower(p.cur.Value)
		if err := p.expect(TokIdent); err != nil {
			return nil, err
		}
		columns = append(columns, name)
		if p.showWord("ASC") {
			p.advance()
		} else if p.showWord("DESC") {
			return nil, fmt.Errorf("descending index columns are not supported")
		}
		if p.cur.Type != TokComma {
			break
		}
		p.advance()
	}
	if err := p.expect(TokRParen); err != nil {
		return nil, err
	}
	return columns, nil
}

// The leading INDEX/KEY (or UNIQUE) has already been consumed.
func (p *Parser) parseInlineIndex(unique bool, name string) (IndexDef, error) {
	if unique && (p.showWord("INDEX") || p.showWord("KEY")) {
		p.advance()
	}
	if p.cur.Type == TokIdent {
		name = strings.ToLower(p.cur.Value)
		p.advance()
	}
	columns, err := p.parseIndexColumns()
	return IndexDef{Name: name, Columns: columns, Unique: unique}, err
}
