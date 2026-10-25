package db

import "slopdb/sql"

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

func renderMySQLSQL(text string) string { return sql.DialectMySQL.RenderSQL(text) }

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
