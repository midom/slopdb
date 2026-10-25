package sql

import (
	"fmt"
	"strings"
)

func (p *Parser) parseMySQLMutationTables() (*SelectStmt, error) {
	stmt := &SelectStmt{SelectAll: true, Columns: []SelectColumn{{Star: true}}, Limit: -1}
	stmt.From, stmt.FromAlias, stmt.FromExpr = p.parseTableRef()
	if stmt.FromAlias == stmt.From {
		stmt.FromAlias = ""
	}
	if stmt.From == "" || stmt.FromExpr != nil {
		return nil, fmt.Errorf("expected physical table in mutation")
	}
	for p.cur.Type == TokComma || mysqlJoinWord(p.cur.Value) {
		var jc JoinClause
		if p.cur.Type == TokComma {
			p.advance()
			table, alias, expr := p.parseTableRef()
			if table == "" || expr != nil {
				return nil, fmt.Errorf("expected physical join table in mutation")
			}
			jc = JoinClause{Type: "CROSS", Table: table, Alias: alias}
		} else {
			kind := strings.ToUpper(p.cur.Value)
			if kind == "JOIN" {
				kind = "INNER"
			}
			var err error
			jc, err = p.parseJoinClause(kind)
			if err != nil {
				return nil, err
			}
		}
		if jc.Subquery != nil {
			return nil, fmt.Errorf("derived joined mutation tables are not supported")
		}
		stmt.Joins = append(stmt.Joins, jc)
	}
	return stmt, p.tableRefError
}

func (p *Parser) parseMySQLDeleteTargets() ([]string, error) {
	var targets []string
	for {
		if p.cur.Type != TokIdent {
			return nil, fmt.Errorf("expected DELETE target table")
		}
		name := strings.ToLower(p.cur.Value)
		p.advance()
		if p.cur.Type == TokDot {
			p.advance()
			if p.cur.Type != TokStar {
				return nil, fmt.Errorf("expected * after DELETE target qualifier")
			}
			p.advance()
		}
		targets = append(targets, name)
		if p.cur.Type != TokComma {
			return targets, nil
		}
		p.advance()
	}
}

func (p *Parser) parseMySQLDeleteTables(stmt *DeleteStmt) error {
	if strings.EqualFold(p.cur.Value, "FROM") {
		p.advance()
		// A single-table DELETE and the USING form share their initial target.
		if p.cur.Type != TokIdent {
			return fmt.Errorf("expected DELETE table")
		}
		var err error
		stmt.From, err = p.qualifiedName()
		if err != nil {
			return err
		}
		if p.cur.Type == TokComma || strings.EqualFold(p.cur.Value, "USING") || p.cur.Type == TokDot && p.tok.Peek().Type == TokStar {
			stmt.Targets = []string{stmt.From}
			if p.cur.Type == TokDot {
				p.advance()
				p.advance()
			}
			if p.cur.Type == TokComma {
				p.advance()
				more, err := p.parseMySQLDeleteTargets()
				if err != nil {
					return err
				}
				stmt.Targets = append(stmt.Targets, more...)
			}
			if err := p.expectKeyword("USING"); err != nil {
				return err
			}
			from, err := p.parseMySQLMutationTables()
			if err != nil {
				return err
			}
			stmt.From, stmt.Alias, stmt.Joins = from.From, from.FromAlias, from.Joins
			return nil
		}
		if strings.EqualFold(p.cur.Value, "AS") {
			p.advance()
			if p.cur.Type != TokIdent {
				return fmt.Errorf("expected DELETE alias")
			}
			stmt.Alias = strings.ToLower(p.cur.Value)
			p.advance()
		} else if p.cur.Type == TokIdent && !isReservedWord(p.cur.Value) {
			stmt.Alias = strings.ToLower(p.cur.Value)
			p.advance()
		}
		return nil
	}
	targets, err := p.parseMySQLDeleteTargets()
	if err != nil {
		return err
	}
	if err := p.expectKeyword("FROM"); err != nil {
		return err
	}
	from, err := p.parseMySQLMutationTables()
	if err != nil {
		return err
	}
	stmt.Targets, stmt.From, stmt.Alias, stmt.Joins = targets, from.From, from.FromAlias, from.Joins
	return nil
}
