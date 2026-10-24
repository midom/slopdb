package sql

import (
	"fmt"
	"strings"
)

type ReadBarrierStmt struct{ Mode string }

func (*ReadBarrierStmt) stmt()               {}
func (s *Schema) ReadBarrier() string        { return s.readBarrier }
func (s *Schema) SetReadBarrier(mode string) { s.readBarrier = mode }

func (p *Parser) parseReadBarrierSetting() (Statement, error) {
	p.advance()
	for _, word := range []string{"READ", "BARRIER"} {
		if !p.showWord(word) {
			return nil, fmt.Errorf("expected %s in SET READ BARRIER", word)
		}
		p.advance()
	}
	mode := strings.ToLower(p.cur.Value)
	if p.cur.Type != TokKeyword && p.cur.Type != TokIdent {
		return nil, fmt.Errorf("expected PREVIOUS, NEXT, COMMITTED or DEFAULT")
	}
	switch mode {
	case "previous", "next", "committed", "default":
	default:
		return nil, fmt.Errorf("expected PREVIOUS, NEXT, COMMITTED or DEFAULT")
	}
	p.advance()
	if mode == "default" {
		mode = ""
	}
	return &ReadBarrierStmt{Mode: mode}, nil
}
