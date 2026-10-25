package sql

import (
	"fmt"
	"strconv"
	"strings"
)

func (p *Parser) parseMySQLPrimary() (Expression, bool, error) {
	if expr, handled, err := p.parseMySQLTemporalPrimary(); handled {
		return expr, true, err
	}
	if expr, handled, err := p.parseMySQLStringPrimary(); handled {
		return expr, handled, err
	}
	if strings.EqualFold(p.cur.Value, "CURRENT_TIMESTAMP") && p.tok.Peek().Type != TokLParen {
		p.advance()
		return &FuncCallExpr{Name: "CURRENT_TIMESTAMP"}, true, nil
	}
	if !strings.EqualFold(p.cur.Value, "CONVERT") || p.tok.Peek().Type != TokLParen {
		return nil, false, nil
	}
	p.advance()
	p.advance()
	value, err := p.parseExpression()
	if err != nil {
		return nil, true, err
	}
	if !strings.EqualFold(p.cur.Value, "USING") {
		return nil, true, fmt.Errorf("expected USING in CONVERT")
	}
	p.advance()
	if p.cur.Type != TokIdent && p.cur.Type != TokKeyword {
		return nil, true, fmt.Errorf("expected character set in CONVERT")
	}
	charset := strings.ToLower(p.cur.Value)
	switch charset {
	case "utf8", "utf8mb3", "utf8mb4", "binary":
	default:
		return nil, true, fmt.Errorf("unsupported character set: %s", charset)
	}
	p.advance()
	if err = p.expect(TokRParen); err != nil {
		return nil, true, err
	}
	return &FuncCallExpr{Name: "__MYSQL_CONVERT", Args: []Expression{value, &LiteralExpr{Value: NewTextValue(charset)}}}, true, nil
}

func (p *Parser) parseFunctionCall(name string) (Expression, error) {
	if err := p.expect(TokLParen); err != nil {
		return nil, err
	}
	f := &FuncCallExpr{Name: strings.ToUpper(name)}
	if strings.EqualFold(p.cur.Value, "DISTINCT") {
		f.Distinct = true
		p.advance()
	} else if strings.EqualFold(p.cur.Value, "ALL") {
		p.advance()
	}
	concat := f.Name == "GROUP_CONCAT"
	if concat && p.tok.options.MySQLQuoting {
		f.MySQLGroupConcat = true
	}
	for p.cur.Type != TokRParen && p.cur.Type != TokEOF {
		arg, err := p.parseExpression()
		if err != nil {
			return nil, err
		}
		f.Args = append(f.Args, arg)
		if p.cur.Type != TokComma {
			break
		}
		p.advance()
	}
	if concat && strings.EqualFold(p.cur.Value, "ORDER") {
		f.MySQLGroupConcat = true
		p.advance()
		if err := p.expectKeyword("BY"); err != nil {
			return nil, err
		}
		for {
			expr, err := p.parseExpression()
			if err != nil {
				return nil, err
			}
			order := OrderByClause{Expr: expr}
			if strings.EqualFold(p.cur.Value, "ASC") || strings.EqualFold(p.cur.Value, "DESC") {
				order.Desc = strings.EqualFold(p.cur.Value, "DESC")
				p.advance()
			}
			f.GroupConcatOrder = append(f.GroupConcatOrder, order)
			if p.cur.Type != TokComma {
				break
			}
			p.advance()
		}
	}
	if concat && strings.EqualFold(p.cur.Value, "SEPARATOR") {
		f.MySQLGroupConcat = true
		p.advance()
		expr, err := p.parseExpression()
		if err != nil {
			return nil, err
		}
		if _, ok := expr.(*LiteralExpr); !ok {
			if _, ok := expr.(*ParameterExpr); !ok {
				return nil, fmt.Errorf("GROUP_CONCAT separator must be a string or parameter")
			}
		}
		f.GroupConcatSeparator = expr
	}
	if err := p.expect(TokRParen); err != nil {
		return nil, err
	}
	return p.parseWindowCall(f)
}

// Hints may affect plan preference, but never alter the SQL answer. Retain
// normal cost-based planning while validating the complete hint grammar.
func (p *Parser) parseMySQLIndexHints() error {
	for strings.EqualFold(p.cur.Value, "FORCE") || strings.EqualFold(p.cur.Value, "IGNORE") || strings.EqualFold(p.cur.Value, "USE") {
		p.advance()
		if !strings.EqualFold(p.cur.Value, "INDEX") && !strings.EqualFold(p.cur.Value, "KEY") {
			return fmt.Errorf("expected INDEX after index hint")
		}
		p.advance()
		if strings.EqualFold(p.cur.Value, "FOR") {
			p.advance()
			if strings.EqualFold(p.cur.Value, "JOIN") {
				p.advance()
			} else if strings.EqualFold(p.cur.Value, "ORDER") || strings.EqualFold(p.cur.Value, "GROUP") {
				p.advance()
				if err := p.expectKeyword("BY"); err != nil {
					return err
				}
			} else {
				return fmt.Errorf("expected JOIN, ORDER BY or GROUP BY after hint FOR")
			}
		}
		if err := p.expect(TokLParen); err != nil {
			return err
		}
		for p.cur.Type != TokRParen {
			if p.cur.Type != TokIdent && p.cur.Type != TokKeyword {
				return fmt.Errorf("expected index name in hint")
			}
			p.advance()
			if p.cur.Type != TokComma {
				break
			}
			p.advance()
		}
		if err := p.expect(TokRParen); err != nil {
			return err
		}
	}
	return nil
}

func mysqlJoinWord(word string) bool {
	switch strings.ToUpper(word) {
	case "JOIN", "INNER", "LEFT", "RIGHT", "CROSS", "NATURAL", "STRAIGHT_JOIN":
		return true
	}
	return false
}

func (p *Parser) parseMySQLJoinGroup() (*SelectStmt, error) {
	if err := p.expect(TokLParen); err != nil {
		return nil, err
	}
	group := &SelectStmt{JoinGroup: true, SelectAll: true, Columns: []SelectColumn{{Star: true}}, Limit: -1}
	group.From, group.FromAlias, group.FromExpr = p.parseTableRef()
	if group.From == "" && group.FromExpr == nil {
		return nil, fmt.Errorf("expected table in joined group")
	}
	for p.cur.Type == TokComma || mysqlJoinWord(p.cur.Value) {
		if p.cur.Type == TokComma {
			p.advance()
			table, alias, expr := p.parseTableRef()
			j := JoinClause{Type: "CROSS", Table: table, Alias: alias}
			if sub, ok := expr.(*SubqueryExpr); ok {
				j.Subquery = sub.Stmt
			}
			group.Joins = append(group.Joins, j)
		} else {
			kind := strings.ToUpper(p.cur.Value)
			if kind == "JOIN" {
				kind = "INNER"
			}
			j, err := p.parseJoinClause(kind)
			if err != nil {
				return nil, err
			}
			group.Joins = append(group.Joins, j)
		}
	}
	if err := p.expect(TokRParen); err != nil {
		return nil, err
	}
	return group, nil
}

func (p *Parser) parseBinaryCastLength() (string, error) {
	if err := p.expect(TokLParen); err != nil {
		return "", err
	}
	if p.cur.Type != TokInt {
		return "", fmt.Errorf("expected nonnegative BINARY cast length")
	}
	n, err := strconv.ParseInt(p.cur.Value, 10, 64)
	if err != nil || n < 0 || n > 64<<20 {
		return "", fmt.Errorf("invalid BINARY cast length")
	}
	p.advance()
	if err := p.expect(TokRParen); err != nil {
		return "", err
	}
	return fmt.Sprintf("BINARY(%d)", n), nil
}

func (p *Parser) parseMySQLTableRef() (string, string, Expression, error) {
	name, alias := "", ""
	var expr Expression
	if p.cur.Type == TokLParen {
		var sub *SelectStmt
		var err error
		if strings.EqualFold(p.tok.Peek().Value, "SELECT") || strings.EqualFold(p.tok.Peek().Value, "WITH") {
			sub, err = p.parseSubqueryParens()
		} else {
			sub, err = p.parseMySQLJoinGroup()
		}
		if err != nil {
			return "", "", nil, err
		}
		expr = &SubqueryExpr{Stmt: sub}
	} else {
		if p.cur.Type != TokIdent && p.cur.Type != TokKeyword {
			return "", "", nil, fmt.Errorf("expected table name")
		}
		name = strings.ToLower(p.cur.Value)
		p.advance()
		alias = name
		if p.cur.Type == TokDot {
			p.advance()
			if p.cur.Type != TokIdent && p.cur.Type != TokKeyword {
				return "", "", nil, fmt.Errorf("expected table name after database qualifier")
			}
			alias = strings.ToLower(p.cur.Value)
			name += "." + alias
			p.advance()
		}
	}
	if strings.EqualFold(p.cur.Value, "AS") {
		p.advance()
		if p.cur.Type != TokIdent && p.cur.Type != TokKeyword {
			return "", "", nil, fmt.Errorf("expected table alias")
		}
		alias = strings.ToLower(p.cur.Value)
		p.advance()
	} else if (p.cur.Type == TokIdent || p.cur.Type == TokKeyword) && !isReservedWord(p.cur.Value) {
		alias = strings.ToLower(p.cur.Value)
		p.advance()
	}
	if err := p.parseMySQLIndexHints(); err != nil {
		return "", "", nil, err
	}
	return name, alias, expr, nil
}
