package db

import (
	"strings"

	"slopdb/sql"
)

// ParseOptions returns the native API input preference chosen at Open. Sessions
// derive their own effective syntax from their selected SQL dialect. Neither
// setting changes canonical catalog SQL or the physical database format.
func (db *Database) ParseOptions() sql.ParseOptions {
	return sql.ParseOptions{MySQLQuoting: db.opt.MySQLQuoting}
}

func (db *Database) renderSQL(text string) string {
	if !db.opt.MySQLQuoting {
		return text
	}
	return renderMySQLSQL(text)
}

// Catalog definitions use canonical SQL. Only SQL exposed to callers is
// rendered for MySQL input, preserving every literal byte and identifier.
func renderMySQLSQL(text string) string {
	tokens := sql.NewTokenizer(text)
	var result strings.Builder
	previous := 0
	for {
		token := tokens.Next()
		if token.Type == sql.TokEOF {
			result.WriteString(text[previous:])
			break
		}
		end := tokens.Pos()
		result.WriteString(text[previous:token.Start])
		switch {
		case token.Type == sql.TokString:
			result.WriteString(quoteMySQLString(token.Value))
		case token.Type == sql.TokIdent && token.Start < len(text) && (text[token.Start] == '"' || text[token.Start] == '`' || text[token.Start] == '['):
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

// effectiveParseOptions is read only while the session gate is held.
func (s *Session) effectiveParseOptions() sql.ParseOptions {
	return s.schema.Dialect().ParseOptions(s.db.ParseOptions())
}

// ParseOptions returns a serialized snapshot of this session's input syntax.
// Protocol adapters know their frontend dialect and must not re-enter this
// getter from a running SQL callback or while a streaming Rows holds the gate.
func (s *Session) ParseOptions() sql.ParseOptions {
	s.gate.Lock()
	defer s.gate.Unlock()
	return s.effectiveParseOptions()
}
