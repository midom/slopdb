package sql

import (
	"fmt"
	"strings"
)

// NamespaceStmt changes a virtual namespace in the same physical database.
type NamespaceStmt struct {
	Operation                      string
	Names                          []string
	IfNotExists, IfExists, Cascade bool
}

func (*NamespaceStmt) stmt() {}

func (p *Parser) qualifiedName() (string, error) {
	name, err := p.alterName()
	if err != nil {
		return "", err
	}
	if p.cur.Type == TokDot && p.tok.Peek().Type != TokStar {
		p.advance()
		part, err := p.alterName()
		if err != nil {
			return "", err
		}
		name += "." + part
	}
	return name, nil
}
func (p *Parser) parseNamespaceCreate() (Statement, error) {
	p.advance()
	conditional, err := p.alterIfNotExists()
	if err != nil {
		return nil, err
	}
	name, err := p.alterName()
	if err != nil {
		return nil, err
	}
	if strings.ContainsAny(name, ".\x00") || name == "" {
		return nil, fmt.Errorf("invalid schema name")
	}
	var options CreateTableStmt
	if err = p.parseMySQLTableOptions(&options); err != nil {
		return nil, err
	}
	return &NamespaceStmt{Operation: "create", Names: []string{name}, IfNotExists: conditional}, nil
}
func (p *Parser) parseNamespaceDrop() (Statement, error) {
	p.advance()
	conditional, err := p.alterIfExists()
	if err != nil {
		return nil, err
	}
	name, err := p.alterName()
	if err != nil {
		return nil, err
	}
	cascade := p.optionalAlterWord("CASCADE")
	if !cascade {
		p.optionalAlterWord("RESTRICT")
	}
	return &NamespaceStmt{Operation: "drop", Names: []string{name}, IfExists: conditional, Cascade: cascade}, nil
}
func (p *Parser) parseNamespaceSetting() (Statement, error) {
	use := p.showWord("USE")
	p.advance()
	if !use && p.showWord("SESSION") {
		p.advance()
	}
	operation := "use"
	if !use {
		if p.showWord("SEARCH_PATH") {
			operation = "search_path"
		} else if !p.showWord("SCHEMA") {
			return nil, fmt.Errorf("expected SCHEMA or search_path")
		}
		p.advance()
		if p.cur.Type == TokEq || p.showWord("TO") {
			p.advance()
		}
	}
	var names []string
	for {
		if p.cur.Type != TokIdent && p.cur.Type != TokKeyword && p.cur.Type != TokString {
			return nil, fmt.Errorf("expected schema name")
		}
		names = append(names, strings.ToLower(p.cur.Value))
		p.advance()
		if operation != "search_path" || p.cur.Type != TokComma {
			break
		}
		p.advance()
	}
	return &NamespaceStmt{Operation: operation, Names: names}, nil
}

func (p *Parser) namespaceSettingAhead() bool {
	t := NewTokenizer(p.tok.Input()[p.cur.Start:])
	_ = t.Next()
	token := t.Next()
	if strings.EqualFold(token.Value, "SESSION") {
		token = t.Next()
	}
	return strings.EqualFold(token.Value, "SCHEMA") || strings.EqualFold(token.Value, "SEARCH_PATH")
}
