package sql

import (
	"fmt"
	"strings"
)

func (p *Parser) parseIndexColumns() ([]string, error) {
	columns, _, err := p.parseIndexColumnsDetailed()
	return columns, err
}

func (p *Parser) parseIndexColumnsDetailed() ([]string, []int64, error) {
	if err := p.expect(TokLParen); err != nil {
		return nil, nil, err
	}
	var columns []string
	var prefixes []int64
	for {
		name := strings.ToLower(p.cur.Value)
		if err := p.expect(TokIdent); err != nil {
			return nil, nil, err
		}
		columns = append(columns, name)
		var prefix int64
		if p.cur.Type == TokLParen {
			n, err := p.parseStringLength()
			if err != nil {
				return nil, nil, err
			}
			prefix = n
		}
		prefixes = append(prefixes, prefix)
		if p.showWord("ASC") {
			p.advance()
		} else if p.showWord("DESC") {
			return nil, nil, fmt.Errorf("descending index columns are not supported")
		}
		if p.cur.Type != TokComma {
			break
		}
		p.advance()
	}
	if err := p.expect(TokRParen); err != nil {
		return nil, nil, err
	}
	return columns, prefixes, nil
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
	columns, prefixes, err := p.parseIndexColumnsDetailed()
	return IndexDef{Name: name, Columns: columns, Unique: unique, PrefixLengths: prefixes}, err
}
