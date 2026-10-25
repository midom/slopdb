package sql

import "strings"

// Catalog definitions use canonical SQL. Only SQL exposed to callers is
// rendered for MySQL input, preserving every literal byte and identifier.
func (d Dialect) RenderSQL(text string) string {
	if d != DialectMySQL {
		return text
	}
	tokens := NewTokenizer(text)
	var result strings.Builder
	previous := 0
	for {
		token := tokens.Next()
		if token.Type == TokEOF {
			result.WriteString(text[previous:])
			break
		}
		end := tokens.Pos()
		result.WriteString(text[previous:token.Start])
		switch {
		case token.Type == TokString:
			result.WriteString(quoteMySQLString(token.Value))
		case token.Type == TokIdent && token.Start < len(text) && (text[token.Start] == '"' || text[token.Start] == '`' || text[token.Start] == '['):
			result.WriteByte('`')
			result.WriteString(strings.ReplaceAll(token.Value, "`", "``"))
			result.WriteByte('`')
		default:
			result.WriteString(text[token.Start:end])
		}
		previous = end
	}
	return result.String()
}

func quoteMySQLString(text string) string {
	var result strings.Builder
	result.WriteByte('\'')
	for i := 0; i < len(text); i++ {
		switch text[i] {
		case 0:
			result.WriteString(`\0`)
		case '\b':
			result.WriteString(`\b`)
		case '\n':
			result.WriteString(`\n`)
		case '\r':
			result.WriteString(`\r`)
		case '\t':
			result.WriteString(`\t`)
		case 26:
			result.WriteString(`\Z`)
		case '\\':
			result.WriteString(`\\`)
		case '\'':
			result.WriteString(`\'`)
		default:
			result.WriteByte(text[i])
		}
	}
	result.WriteByte('\'')
	return result.String()
}
