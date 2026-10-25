package sql

import (
	"fmt"
	"strings"
)

// SHOW's contextual words remain usable as ordinary identifiers elsewhere.
func (p *Parser) showWord(word string) bool {
	return (p.cur.Type == TokIdent || p.cur.Type == TokKeyword) && strings.EqualFold(p.cur.Value, word)
}

func (p *Parser) showName() (string, error) {
	if p.cur.Type != TokIdent {
		return "", fmt.Errorf("expected identifier, got %q", p.cur.Value)
	}
	name := strings.ToLower(p.cur.Value)
	p.advance()
	return name, nil
}

func (p *Parser) showObject(stmt *ShowStmt) error {
	name, err := p.showName()
	if err != nil {
		return err
	}
	if p.cur.Type == TokDot {
		p.advance()
		stmt.Database = name
		name, err = p.showName()
		if err != nil {
			return err
		}
	}
	stmt.Object = name
	return nil
}

func (p *Parser) showDatabase(stmt *ShowStmt) error {
	if !p.showWord("FROM") && !p.showWord("IN") {
		return nil
	}
	p.advance()
	name, err := p.showName()
	if err != nil {
		return err
	}
	if stmt.Database != "" && stmt.Database != name {
		return fmt.Errorf("conflicting database qualifiers: %q and %q", stmt.Database, name)
	}
	stmt.Database = name
	return nil
}

func (p *Parser) parseShowFilter(stmt *ShowStmt, allowLike bool) error {
	if allowLike && p.showWord("LIKE") {
		p.advance()
		if p.cur.Type != TokString {
			return fmt.Errorf("expected string pattern after LIKE, got %q", p.cur.Value)
		}
		pattern := p.cur.Value
		stmt.Like = &pattern
		p.advance()
	} else if p.showWord("WHERE") {
		p.advance()
		previous := p.showStatusFilter
		p.showStatusFilter = stmt.Kind == "TABLE STATUS"
		defer func() { p.showStatusFilter = previous }()
		var err error
		stmt.Where, err = p.parseExpression()
		return err
	}
	return nil
}

func (p *Parser) parseShow() (Statement, error) {
	p.advance()
	stmt := &ShowStmt{}
	if p.showWord("FULL") {
		stmt.Full = true
		p.advance()
	}
	kind := strings.ToUpper(p.cur.Value)
	if p.cur.Type != TokKeyword && p.cur.Type != TokIdent {
		return nil, fmt.Errorf("expected SHOW command, got %q", p.cur.Value)
	}
	p.advance()
	if stmt.Full && kind != "TABLES" && kind != "COLUMNS" && kind != "FIELDS" {
		return nil, fmt.Errorf("FULL is only supported for SHOW TABLES and SHOW COLUMNS")
	}
	switch kind {
	case "RAFT":
		if !p.showWord("STATUS") {
			return nil, fmt.Errorf("expected STATUS after SHOW RAFT")
		}
		p.advance()
		stmt.Kind = "RAFT STATUS"
	case "GLOBAL":
		if !p.showWord("VARIABLES") {
			return nil, fmt.Errorf("expected VARIABLES after SHOW GLOBAL")
		}
		p.advance()
		stmt.Kind = "GLOBAL VARIABLES"
	case "REPLICATION":
		if !p.showWord("POSITION") {
			return nil, fmt.Errorf("expected POSITION after SHOW REPLICATION")
		}
		p.advance()
		return &ReplicationPositionStmt{Operation: "current"}, nil
	case "LAST":
		if !p.showWord("COMMIT") {
			return nil, fmt.Errorf("expected COMMIT after SHOW LAST")
		}
		p.advance()
		if !p.showWord("POSITION") {
			return nil, fmt.Errorf("expected POSITION after SHOW LAST COMMIT")
		}
		p.advance()
		return &ReplicationPositionStmt{Operation: "last"}, nil
	case "TABLE":
		if !p.showWord("STATUS") {
			return nil, fmt.Errorf("expected STATUS after SHOW TABLE")
		}
		p.advance()
		stmt.Kind = "TABLE STATUS"
		if err := p.showDatabase(stmt); err != nil {
			return nil, err
		}
	case "DATABASES", "SCHEMAS":
		stmt.Kind = "DATABASES"
	case "TABLES", "TRIGGERS":
		stmt.Kind = kind
		if err := p.showDatabase(stmt); err != nil {
			return nil, err
		}
	case "COLUMNS", "FIELDS", "INDEX", "INDEXES", "KEYS":
		stmt.Kind = "COLUMNS"
		if kind == "INDEX" || kind == "INDEXES" || kind == "KEYS" {
			stmt.Kind = "INDEX"
		}
		if !p.showWord("FROM") && !p.showWord("IN") {
			return nil, fmt.Errorf("expected FROM or IN after SHOW %s", kind)
		}
		p.advance()
		if err := p.showObject(stmt); err != nil {
			return nil, err
		}
		if err := p.showDatabase(stmt); err != nil {
			return nil, err
		}
	case "CREATE":
		objectKind := strings.ToUpper(p.cur.Value)
		if !p.showWord("TABLE") && !p.showWord("VIEW") && !p.showWord("TRIGGER") {
			return nil, fmt.Errorf("expected TABLE, VIEW, or TRIGGER after SHOW CREATE, got %q", p.cur.Value)
		}
		p.advance()
		stmt.Kind = "CREATE " + objectKind
		if err := p.showObject(stmt); err != nil {
			return nil, err
		}
		return stmt, nil
	default:
		return nil, fmt.Errorf("unsupported SHOW command: %s", kind)
	}
	if err := p.parseShowFilter(stmt, stmt.Kind != "INDEX"); err != nil {
		return nil, err
	}
	return stmt, nil
}

func (p *Parser) parseDescribe() (Statement, error) {
	// MySQL also accepts DESCRIBE/DESC for query execution plans.
	if next := p.tok.Peek(); next.Type == TokKeyword {
		switch strings.ToUpper(next.Value) {
		case "SELECT", "INSERT", "REPLACE", "UPDATE", "DELETE", "ANALYZE":
			return p.parseExplain()
		}
	}
	p.advance()
	stmt := &ShowStmt{Kind: "COLUMNS"}
	if err := p.showObject(stmt); err != nil {
		return nil, err
	}
	if p.cur.Type == TokString {
		pattern := p.cur.Value
		stmt.Like = &pattern
		p.advance()
	} else if p.cur.Type == TokIdent {
		var err error
		stmt.Column, err = p.showName()
		if err != nil {
			return nil, err
		}
	}
	return stmt, nil
}
