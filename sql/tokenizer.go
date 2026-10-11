package sql

import "strings"

type TokenType int

const (
	TokEOF TokenType = iota
	TokIdent
	TokString
	TokInt
	TokFloat
	TokComma
	TokLParen
	TokRParen
	TokSemicolon
	TokEq
	TokLt
	TokGt
	TokLe
	TokGe
	TokNe
	TokPlus
	TokMinus
	TokStar
	TokSlash
	TokDot
	TokConcat
	TokBitOr
	TokAmp
	TokLShift
	TokRShift
	TokTilde
	TokBlob
	TokKeyword
	TokInvalid
	TokPercent
	TokParameter
)

type Token struct {
	Type  TokenType
	Value string
	Start int
}

var keywords = map[string]bool{
	"CREATE": true, "TABLE": true, "INSERT": true, "INTO": true,
	"VALUES": true, "SELECT": true, "FROM": true, "WHERE": true,
	"DELETE": true, "UPDATE": true, "SET": true, "DROP": true,
	"AND": true, "OR": true, "NOT": true, "TRUE": true, "FALSE": true,
	"NULL":    true,
	"INTEGER": true, "INT": true, "TEXT": true, "REAL": true,
	"BLOB": true, "PRIMARY": true, "KEY": true, "ORDER": true,
	"BY": true, "ASC": true, "DESC": true, "LIMIT": true,
	"OFFSET": true, "AS": true, "ON": true, "JOIN": true,
	"LEFT": true, "RIGHT": true, "INNER": true, "OUTER": true,
	"GROUP": true, "HAVING": true, "COUNT": true, "SUM": true,
	"AVG": true, "MIN": true, "MAX": true, "DISTINCT": true,
	"LIKE": true, "IN": true, "BETWEEN": true, "IS": true,
	"GLOB": true, "REGEXP": true,
	"EXISTS": true, "CASE": true, "WHEN": true, "THEN": true,
	"COLLATE": true,
	"ELSE":    true, "END": true, "CAST": true, "UNION": true,
	"INTERSECT": true, "EXCEPT": true,
	"ALL": true, "ALTER": true, "INDEX": true, "UNIQUE": true,
	"DEFAULT": true, "CHECK": true, "FOREIGN": true, "REFERENCES": true, "CONSTRAINT": true,
	"AUTOINCREMENT": true, "BEGIN": true, "COMMIT": true, "ROLLBACK": true,
	"TRANSACTION": true, "START": true, "IF": true, "EXPLAIN": true, "VACUUM": true,
	"REPLACE": true, "RETURNING": true, "PRAGMA": true, "ANALYZE": true,
	"VIEW": true, "TRIGGER": true, "INSTEAD": true, "OF": true,
	"FOR": true, "EACH": true, "ROW": true,
	"LOCK": true, "SHARE": true, "MODE": true, "NOWAIT": true, "SKIP": true, "LOCKED": true,
	"BEFORE": true, "AFTER": true, "COLUMN": true,
	"SAVEPOINT": true, "RELEASE": true, "TO": true,
	"USING": true, "NATURAL": true, "CROSS": true,
	"SHOW": true, "DESCRIBE": true,
	"OVER": true, "PARTITION": true, "WINDOW": true, "ROWS": true, "RANGE": true, "GROUPS": true, "UNBOUNDED": true, "PRECEDING": true, "FOLLOWING": true, "CURRENT": true, "EXCLUDE": true,
	"NULLS": true, "FIRST": true, "LAST": true, "ESCAPE": true}

func isKeyword(s string) bool {
	return keywords[strings.ToUpper(s)]
}

type Tokenizer struct {
	input string
	pos   int
}

func NewTokenizer(input string) *Tokenizer {
	return &Tokenizer{input: input}
}

func (t *Tokenizer) Next() (tok Token) {
	t.skipWhitespace()
	start := t.pos
	defer func() { tok.Start = start }()
	if t.pos >= len(t.input) {
		return Token{Type: TokEOF}
	}

	ch := t.input[t.pos]

	if (ch == 'X' || ch == 'x') && t.pos+1 < len(t.input) && t.input[t.pos+1] == '\'' {
		t.pos += 2
		return t.scanBlobLiteral()
	}

	if ch == '\'' || ch == '"' || ch == '`' || ch == '[' {
		return t.scanString(ch)
	}
	if (ch >= '0' && ch <= '9') || (ch == '.' && t.pos+1 < len(t.input) && t.input[t.pos+1] >= '0' && t.input[t.pos+1] <= '9') {
		return t.scanNumber()
	}
	if isIdentStart(ch) {
		return t.scanIdent()
	}

	switch ch {
	case '?':
		t.pos++
		return Token{Type: TokParameter, Value: "?"}
	case ',':
		t.pos++
		return Token{Type: TokComma, Value: ","}
	case '(':
		t.pos++
		return Token{Type: TokLParen, Value: "("}
	case ')':
		t.pos++
		return Token{Type: TokRParen, Value: ")"}
	case ';':
		t.pos++
		return Token{Type: TokSemicolon, Value: ";"}
	case '=':
		t.pos++
		return Token{Type: TokEq, Value: "="}
	case '<':
		t.pos++
		if t.pos < len(t.input) {
			if t.input[t.pos] == '=' {
				t.pos++
				return Token{Type: TokLe, Value: "<="}
			}
			if t.input[t.pos] == '>' {
				t.pos++
				return Token{Type: TokNe, Value: "<>"}
			}
			if t.input[t.pos] == '<' {
				t.pos++
				return Token{Type: TokLShift, Value: "<<"}
			}
		}
		return Token{Type: TokLt, Value: "<"}
	case '>':
		t.pos++
		if t.pos < len(t.input) {
			if t.input[t.pos] == '=' {
				t.pos++
				return Token{Type: TokGe, Value: ">="}
			}
			if t.input[t.pos] == '>' {
				t.pos++
				return Token{Type: TokRShift, Value: ">>"}
			}
		}
		return Token{Type: TokGt, Value: ">"}
	case '|':
		t.pos++
		if t.pos < len(t.input) && t.input[t.pos] == '|' {
			t.pos++
			return Token{Type: TokConcat, Value: "||"}
		}
		return Token{Type: TokBitOr, Value: "|"}
	case '&':
		t.pos++
		return Token{Type: TokAmp, Value: "&"}
	case '~':
		t.pos++
		return Token{Type: TokTilde, Value: "~"}
	case '+':
		t.pos++
		return Token{Type: TokPlus, Value: "+"}
	case '-':
		t.pos++
		return Token{Type: TokMinus, Value: "-"}
	case '*':
		t.pos++
		return Token{Type: TokStar, Value: "*"}
	case '%':
		t.pos++
		return Token{Type: TokPercent, Value: "%"}
	case '!':
		t.pos++
		if t.pos < len(t.input) && t.input[t.pos] == '=' {
			t.pos++
			return Token{Type: TokNe, Value: "<>"}
		}
		return Token{Type: TokInvalid, Value: "!"}
	case '/':
		t.pos++
		return Token{Type: TokSlash, Value: "/"}
	case '.':
		t.pos++
		return Token{Type: TokDot, Value: "."}
	}

	t.pos++
	return Token{Type: TokInvalid, Value: string(ch)}
}

func (t *Tokenizer) skipWhitespace() {
	for t.pos < len(t.input) {
		ch := t.input[t.pos]
		if ch == ' ' || ch == '\t' || ch == '\n' || ch == '\r' {
			t.pos++
			continue
		}
		if ch == '-' && t.pos+1 < len(t.input) && t.input[t.pos+1] == '-' {
			t.pos += 2
			for t.pos < len(t.input) && t.input[t.pos] != '\n' {
				t.pos++
			}
			continue
		}
		if ch == '/' && t.pos+1 < len(t.input) && t.input[t.pos+1] == '*' {
			t.pos += 2
			for t.pos+1 < len(t.input) && !(t.input[t.pos] == '*' && t.input[t.pos+1] == '/') {
				t.pos++
			}
			if t.pos+1 < len(t.input) {
				t.pos += 2
			} else {
				t.pos = len(t.input)
			}
			continue
		}
		break
	}
}

func (t *Tokenizer) scanString(quote byte) Token {
	typ := TokString
	end := quote
	if quote != '\'' {
		typ = TokIdent
	}
	if quote == '[' {
		end = ']'
	}
	t.pos++
	var value strings.Builder
	for t.pos < len(t.input) {
		ch := t.input[t.pos]
		t.pos++
		if ch == end {
			if t.pos < len(t.input) && t.input[t.pos] == end && quote != '[' {
				value.WriteByte(end)
				t.pos++
				continue
			}
			return Token{Type: typ, Value: value.String()}
		}
		value.WriteByte(ch)
	}
	return Token{Type: TokInvalid, Value: "unterminated string or identifier"}
}
func (t *Tokenizer) scanNumber() Token {
	start := t.pos
	floating := false
	for t.pos < len(t.input) && t.input[t.pos] >= '0' && t.input[t.pos] <= '9' {
		t.pos++
	}
	if t.pos < len(t.input) && t.input[t.pos] == '.' {
		floating = true
		t.pos++
		for t.pos < len(t.input) && t.input[t.pos] >= '0' && t.input[t.pos] <= '9' {
			t.pos++
		}
	}
	if t.pos < len(t.input) && (t.input[t.pos] == 'e' || t.input[t.pos] == 'E') {
		floating = true
		t.pos++
		if t.pos < len(t.input) && (t.input[t.pos] == '+' || t.input[t.pos] == '-') {
			t.pos++
		}
		digits := t.pos
		for t.pos < len(t.input) && t.input[t.pos] >= '0' && t.input[t.pos] <= '9' {
			t.pos++
		}
		if t.pos == digits {
			return Token{Type: TokInvalid, Value: "invalid exponent"}
		}
	}
	typ := TokInt
	if floating {
		typ = TokFloat
	}
	return Token{Type: typ, Value: t.input[start:t.pos]}
}

func isIdentStart(ch byte) bool {
	return (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || ch == '_'
}

func isIdentPart(ch byte) bool {
	return isIdentStart(ch) || (ch >= '0' && ch <= '9')
}

func (t *Tokenizer) scanIdent() Token {
	start := t.pos
	for t.pos < len(t.input) && isIdentPart(t.input[t.pos]) {
		t.pos++
	}
	val := t.input[start:t.pos]
	if isKeyword(val) {
		return Token{Type: TokKeyword, Value: strings.ToUpper(val)}
	}
	return Token{Type: TokIdent, Value: val}
}

func (t *Tokenizer) scanBlobLiteral() Token {
	start := t.pos
	for t.pos < len(t.input) && t.input[t.pos] != '\'' {
		t.pos++
	}
	hexStr := t.input[start:t.pos]
	if t.pos == len(t.input) {
		return Token{Type: TokInvalid, Value: "unterminated blob"}
	}
	if t.pos < len(t.input) {
		t.pos++
	}
	return Token{Type: TokBlob, Value: hexStr}
}

func (t *Tokenizer) Peek() Token {
	saved := t.pos
	tok := t.Next()
	t.pos = saved
	return tok
}

func (t *Tokenizer) Pos() int      { return t.pos }
func (t *Tokenizer) Input() string { return t.input }
