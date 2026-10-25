package sql

import (
	"fmt"
	"strconv"
	"strings"
	"unicode/utf8"
)

func (p *Parser) parsePostgresCastType() (string, error) {
	if p.cur.Type != TokIdent && p.cur.Type != TokKeyword {
		return "", fmt.Errorf("expected cast type")
	}
	name := strings.ToUpper(p.cur.Value)
	p.advance()
	if name == "PG_CATALOG" && p.cur.Type == TokDot {
		p.advance()
		name = strings.ToUpper(p.cur.Value)
		p.advance()
	}
	if name == "DOUBLE" && p.showWord("PRECISION") {
		name += " PRECISION"
		p.advance()
	}
	if name == "CHARACTER" && p.showWord("VARYING") {
		name = "VARCHAR"
		p.advance()
	}
	if p.cur.Type == TokLParen {
		switch name {
		case "NUMERIC", "DECIMAL":
			precision, scale, e := p.parseDecimalParameters()
			if e != nil {
				return "", e
			}
			name = fmt.Sprintf("%s(%d,%d)", name, precision, scale)
		case "VARCHAR", "VARBINARY":
			n, e := p.parseStringLength()
			if e != nil {
				return "", e
			}
			name = fmt.Sprintf("%s(%d)", name, n)
		case "BINARY":
			return p.parseBinaryCastLength()
		case "TIMESTAMP", "TIMESTAMPTZ", "TIME", "TIMETZ":
			p.advance()
			n, e := strconv.Atoi(p.cur.Value)
			if e != nil || n < 0 || n > 6 {
				return "", fmt.Errorf("time precision must be 0 through 6")
			}
			p.advance()
			if e = p.expect(TokRParen); e != nil {
				return "", e
			}
			// Cast precision is carried in the spelling and applied by the type converter.
			name = fmt.Sprintf("%s(%d)", name, n)
		}
	}
	base := name
	if i := strings.IndexByte(base, '('); i >= 0 {
		base = base[:i]
	}
	if (base == "TIME" || base == "TIMESTAMP") && (p.showWord("WITH") || p.showWord("WITHOUT")) {
		zone := strings.ToUpper(p.cur.Value)
		p.advance()
		if !p.showWord("TIME") {
			return "", fmt.Errorf("expected TIME ZONE")
		}
		p.advance()
		if !p.showWord("ZONE") {
			return "", fmt.Errorf("expected TIME ZONE")
		}
		p.advance()
		name += " " + zone + " TIME ZONE"
	}
	if name == "SIGNED" && p.showWord("INTEGER") {
		p.advance()
	}
	if name == "UNSIGNED" && p.showWord("INTEGER") {
		p.advance()
		name = "UNSIGNED INTEGER"
	}
	return name, nil
}

func (p *Parser) parsePostgresColumnType(col *ColumnDef) (bool, error) {
	name := strings.ToUpper(p.cur.Value)
	switch name {
	case "OID", "NAME", "REGCLASS", "REGTYPE", "REGNAMESPACE", "UUID", "BYTEA", "TIME", "TIMETZ", "TIMESTAMPTZ", "INTERVAL", "INT2", "INT4", "INT8", "FLOAT4", "FLOAT8", "SERIAL", "BIGSERIAL", "SMALLSERIAL", "SERIAL2", "SERIAL4", "SERIAL8":
	default:
		return false, nil
	}
	typ, e := p.parsePostgresCastType()
	if e != nil {
		return true, e
	}
	col.Postgres = true
	col.TypeModifiers = strings.Contains(typ, "(")
	if i := strings.IndexByte(typ, '('); i >= 0 {
		fmt.Sscanf(typ[i+1:], "%d", &col.TemporalPrecision)
	}
	col.Declared = typ
	switch PostgresColumnOID(*col) {
	case 16, 20, 21, 23, 26, 2205, 2206, 4089:
		col.ColType = ColInteger
	case 700, 701:
		col.ColType = ColReal
	case 17, 2950:
		col.ColType = ColBlob
	default:
		col.ColType = ColText
	}
	postgresColumnDefinition(col)
	if strings.Contains(name, "SERIAL") {
		col.AutoIncrement = true
		col.NotNull = true
	}
	return true, nil
}

func (t *Tokenizer) scanPostgresDollarString() (Token, bool) {
	start := t.pos
	end := start + 1
	for end < len(t.input) && t.input[end] != '$' {
		c := t.input[end]
		if !isIdentStart(c) && !(end > start+1 && c >= '0' && c <= '9') {
			return Token{}, false
		}
		end++
	}
	if end >= len(t.input) {
		return Token{}, false
	}
	delimiter := t.input[start : end+1]
	content := end + 1
	i := strings.Index(t.input[content:], delimiter)
	if i < 0 {
		t.pos = len(t.input)
		return Token{Type: TokInvalid, Value: "unterminated dollar-quoted string"}, true
	}
	t.pos = content + i + len(delimiter)
	return Token{Type: TokString, Value: t.input[content : content+i]}, true
}
func (t *Tokenizer) scanPostgresEscapeString() Token {
	t.pos += 2
	var out strings.Builder
	for t.pos < len(t.input) {
		c := t.input[t.pos]
		t.pos++
		if c == '\'' {
			if t.pos < len(t.input) && t.input[t.pos] == '\'' {
				t.pos++
				out.WriteByte(c)
				continue
			}
			if !utf8.ValidString(out.String()) || strings.IndexByte(out.String(), 0) >= 0 {
				return Token{Type: TokInvalid, Value: "invalid byte sequence in escape string"}
			}
			return Token{Type: TokString, Value: out.String()}
		}
		if c != '\\' {
			out.WriteByte(c)
			continue
		}
		if t.pos >= len(t.input) {
			break
		}
		c = t.input[t.pos]
		t.pos++
		switch c {
		case 'b':
			out.WriteByte('\b')
		case 'f':
			out.WriteByte('\f')
		case 'n':
			out.WriteByte('\n')
		case 'r':
			out.WriteByte('\r')
		case 't':
			out.WriteByte('\t')
		case 'u', 'U', 'x':
			limit := 2
			if c == 'u' {
				limit = 4
			}
			if c == 'U' {
				limit = 8
			}
			begin := t.pos
			for t.pos < len(t.input) && t.pos-begin < limit && strings.ContainsRune("0123456789abcdefABCDEF", rune(t.input[t.pos])) {
				t.pos++
			}
			if begin == t.pos || (c != 'x' && t.pos-begin != limit) {
				return Token{Type: TokInvalid, Value: "invalid Unicode/hex escape"}
			}
			n, e := strconv.ParseUint(t.input[begin:t.pos], 16, 32)
			if e != nil {
				return Token{Type: TokInvalid, Value: "invalid escape"}
			}
			if c == 'x' {
				out.WriteByte(byte(n))
			} else if n == 0 || !utf8.ValidRune(rune(n)) {
				return Token{Type: TokInvalid, Value: "invalid Unicode escape"}
			} else {
				out.WriteRune(rune(n))
			}
		default:
			if c >= '0' && c <= '7' {
				n := int(c - '0')
				for k := 0; k < 2 && t.pos < len(t.input) && t.input[t.pos] >= '0' && t.input[t.pos] <= '7'; k++ {
					n = n*8 + int(t.input[t.pos]-'0')
					t.pos++
				}
				out.WriteByte(byte(n))
			} else {
				out.WriteByte(c)
			}
		}
	}
	return Token{Type: TokInvalid, Value: "unterminated escape string"}
}

func (p *Parser) parsePostgresPrimary() (Expression, bool, error) {
	name := strings.ToUpper(p.cur.Value)
	switch name {
	case "CURRENT_DATABASE", "CURRENT_SCHEMA", "CURRENT_USER", "SESSION_USER", "USER":
		if p.tok.Peek().Type != TokLParen && p.cur.Start < len(p.tok.input) && !strings.ContainsRune("`\"[", rune(p.tok.input[p.cur.Start])) {
			p.advance()
			return &FuncCallExpr{Name: name}, true, nil
		}
	}
	if name == "OVERLAY" && p.tok.Peek().Type == TokLParen {
		p.advance()
		p.advance()
		first, e := p.parseExpression()
		if e != nil {
			return nil, true, e
		}
		args := []Expression{first}
		if p.showWord("PLACING") {
			p.advance()
			replacement, e := p.parseExpression()
			if e != nil {
				return nil, true, e
			}
			if e = p.expectKeyword("FROM"); e != nil {
				return nil, true, e
			}
			start, e := p.parseExpression()
			if e != nil {
				return nil, true, e
			}
			args = append(args, replacement, start)
			if p.showWord("FOR") {
				p.advance()
				n, e := p.parseExpression()
				if e != nil {
					return nil, true, e
				}
				args = append(args, n)
			}
		} else {
			for p.cur.Type == TokComma {
				p.advance()
				a, e := p.parseExpression()
				if e != nil {
					return nil, true, e
				}
				args = append(args, a)
			}
		}
		if e = p.expect(TokRParen); e != nil {
			return nil, true, e
		}
		return &FuncCallExpr{Name: name, Args: args}, true, nil
	}
	switch name {
	case "DATE", "TIME", "TIMESTAMP", "TIMESTAMPTZ", "INTERVAL", "UUID", "JSON", "JSONB", "BOOLEAN":
	default:
		return nil, false, nil
	}
	if p.tok.Peek().Type != TokString {
		return nil, false, nil
	}
	p.advance()
	value := NewTextValue(p.cur.Value)
	p.advance()
	return &FuncCallExpr{Name: "CAST", Args: []Expression{&LiteralExpr{Value: value}, &LiteralExpr{Value: NewTextValue(name)}}}, true, nil
}
