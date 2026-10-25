package sql

import (
	"fmt"
	"strings"
)

func (p *Parser) parseMySQLTemporalPrimary() (Expression, bool, error) {
	name := strings.ToUpper(p.cur.Value)
	if p.cur.Start < len(p.tok.input) && strings.ContainsRune("`\"[", rune(p.tok.input[p.cur.Start])) {
		return nil, false, nil
	}
	switch name {
	case "CURRENT_DATE", "CURRENT_TIME", "LOCALTIME", "LOCALTIMESTAMP", "UTC_DATE", "UTC_TIME", "UTC_TIMESTAMP":
		if p.tok.Peek().Type != TokLParen {
			p.advance()
			return &FuncCallExpr{Name: name}, true, nil
		}
	}
	switch name {
	case "EXTRACT", "TIMESTAMPADD", "TIMESTAMPDIFF", "DATE_ADD", "DATE_SUB", "ADDDATE", "SUBDATE":
	default:
		return nil, false, nil
	}
	if p.tok.Peek().Type != TokLParen {
		return nil, false, nil
	}
	p.advance()
	p.advance()
	literalUnit := func() (Expression, error) {
		if p.cur.Type != TokIdent && p.cur.Type != TokKeyword && p.cur.Type != TokString {
			return nil, fmt.Errorf("expected temporal interval unit")
		}
		unit := strings.ToUpper(p.cur.Value)
		switch strings.TrimPrefix(unit, "SQL_TSI_") {
		case "MICROSECOND", "SECOND", "MINUTE", "HOUR", "DAY", "WEEK", "MONTH", "QUARTER", "YEAR", "SECOND_MICROSECOND", "MINUTE_MICROSECOND", "MINUTE_SECOND", "HOUR_MICROSECOND", "HOUR_SECOND", "HOUR_MINUTE", "DAY_MICROSECOND", "DAY_SECOND", "DAY_MINUTE", "DAY_HOUR", "YEAR_MONTH":
		default:
			return nil, fmt.Errorf("unsupported temporal interval unit %s", unit)
		}
		p.advance()
		return &LiteralExpr{Value: NewTextValue(unit)}, nil
	}
	var args []Expression
	if name == "EXTRACT" {
		unit, err := literalUnit()
		if err != nil {
			return nil, true, err
		}
		if p.cur.Type == TokComma {
			p.advance()
		} else if err = p.expectKeyword("FROM"); err != nil {
			return nil, true, err
		}
		value, err := p.parseExpression()
		if err != nil {
			return nil, true, err
		}
		args = []Expression{unit, value}
	} else if name == "TIMESTAMPADD" || name == "TIMESTAMPDIFF" {
		unit, err := literalUnit()
		if err != nil {
			return nil, true, err
		}
		args = append(args, unit)
		for i := 0; i < 2; i++ {
			if err = p.expect(TokComma); err != nil {
				return nil, true, err
			}
			value, err := p.parseExpression()
			if err != nil {
				return nil, true, err
			}
			args = append(args, value)
		}
	} else {
		value, err := p.parseExpression()
		if err != nil {
			return nil, true, err
		}
		args = append(args, value)
		if err = p.expect(TokComma); err != nil {
			return nil, true, err
		}
		interval := strings.EqualFold(p.cur.Value, "INTERVAL")
		if interval {
			p.advance()
		}
		amount, err := p.parseExpression()
		if err != nil {
			return nil, true, err
		}
		args = append(args, amount)
		if interval {
			unit, err := literalUnit()
			if err != nil {
				return nil, true, err
			}
			args = append(args, unit)
		} else if p.cur.Type == TokComma {
			p.advance()
			unit, err := p.parseExpression()
			if err != nil {
				return nil, true, err
			}
			args = append(args, unit)
		}
	}
	if err := p.expect(TokRParen); err != nil {
		return nil, true, err
	}
	return &FuncCallExpr{Name: name, Args: args}, true, nil
}
