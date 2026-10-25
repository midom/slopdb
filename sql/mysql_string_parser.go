package sql

import (
	"fmt"
	"strings"
)

// Standard spelling of string functions has delimiter words that are not
// ordinary comma-separated function arguments. Canonical ASTs retain only
// ordinary function calls so prepared binding and persisted views share them.
func (p *Parser) parseMySQLStringPrimary() (Expression, bool, error) {
	name := strings.ToUpper(p.cur.Value)
	if p.tok.Peek().Type != TokLParen {
		return nil, false, nil
	}
	switch name {
	case "POSITION", "CHAR", "SUBSTRING", "MID", "TRIM":
	default:
		return nil, false, nil
	}
	p.advance()
	p.advance()
	if name == "POSITION" {
		needle, err := p.parseBitwise()
		if err != nil {
			return nil, true, err
		}
		if p.cur.Type == TokComma {
			p.advance()
		} else if strings.EqualFold(p.cur.Value, "IN") {
			p.advance()
		} else {
			return nil, true, fmt.Errorf("expected IN in POSITION")
		}
		haystack, err := p.parseExpression()
		if err != nil {
			return nil, true, err
		}
		if err = p.expect(TokRParen); err != nil {
			return nil, true, err
		}
		return &FuncCallExpr{Name: "LOCATE", Args: []Expression{needle, haystack}}, true, nil
	}
	if name == "TRIM" {
		expr, err := p.parseMySQLTrim()
		return expr, true, err
	}
	var args []Expression
	for p.cur.Type != TokRParen && p.cur.Type != TokEOF {
		arg, err := p.parseExpression()
		if err != nil {
			return nil, true, err
		}
		args = append(args, arg)
		if (name == "SUBSTRING" || name == "MID") && strings.EqualFold(p.cur.Value, "FROM") {
			if len(args) != 1 {
				return nil, true, fmt.Errorf("unexpected FROM in %s", name)
			}
			p.advance()
			position, err := p.parseBitwise()
			if err != nil {
				return nil, true, err
			}
			args = append(args, position)
			if strings.EqualFold(p.cur.Value, "FOR") {
				p.advance()
				length, err := p.parseExpression()
				if err != nil {
					return nil, true, err
				}
				args = append(args, length)
			}
			if err = p.expect(TokRParen); err != nil {
				return nil, true, err
			}
			return &FuncCallExpr{Name: "MID", Args: args}, true, nil
		}
		if name == "CHAR" && strings.EqualFold(p.cur.Value, "USING") {
			p.advance()
			charset := strings.ToLower(p.cur.Value)
			if p.cur.Type != TokIdent && p.cur.Type != TokKeyword && p.cur.Type != TokString {
				return nil, true, fmt.Errorf("expected character set in CHAR")
			}
			if charset != "binary" && charset != "utf8" && charset != "utf8mb3" && charset != "utf8mb4" {
				return nil, true, fmt.Errorf("unsupported character set: %s", charset)
			}
			p.advance()
			if err = p.expect(TokRParen); err != nil {
				return nil, true, err
			}
			if charset != "binary" {
				name = "__MYSQL_CHAR_UTF8"
			}
			return &FuncCallExpr{Name: name, Args: args}, true, nil
		}
		if p.cur.Type != TokComma {
			break
		}
		p.advance()
		if p.cur.Type == TokRParen {
			return nil, true, fmt.Errorf("expected argument after comma")
		}
	}
	if err := p.expect(TokRParen); err != nil {
		return nil, true, err
	}
	return &FuncCallExpr{Name: name, Args: args}, true, nil
}

func (p *Parser) parseMySQLTrim() (Expression, error) {
	mode := "BOTH"
	special := false
	if strings.EqualFold(p.cur.Value, "BOTH") || strings.EqualFold(p.cur.Value, "LEADING") || strings.EqualFold(p.cur.Value, "TRAILING") {
		mode = strings.ToUpper(p.cur.Value)
		p.advance()
		special = true
	}
	var first Expression
	if strings.EqualFold(p.cur.Value, "FROM") {
		first = &LiteralExpr{Value: NewTextValue(" ")}
	} else {
		var err error
		first, err = p.parseExpression()
		if err != nil {
			return nil, err
		}
	}
	if strings.EqualFold(p.cur.Value, "FROM") {
		p.advance()
		value, err := p.parseExpression()
		if err != nil {
			return nil, err
		}
		if err = p.expect(TokRParen); err != nil {
			return nil, err
		}
		return &FuncCallExpr{Name: "__MYSQL_TRIM", Args: []Expression{value, first, &LiteralExpr{Value: NewTextValue(mode)}}}, nil
	}
	if special {
		return nil, fmt.Errorf("expected FROM in TRIM")
	}
	args := []Expression{first}
	if p.cur.Type == TokComma {
		p.advance()
		arg, err := p.parseExpression()
		if err != nil {
			return nil, err
		}
		args = append(args, arg)
	}
	if err := p.expect(TokRParen); err != nil {
		return nil, err
	}
	return &FuncCallExpr{Name: "TRIM", Args: args}, nil
}
