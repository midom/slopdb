package db

import (
	"context"
	"slopdb/sql"
)

func (s *Session) SetPostgresMode(enabled bool) error {
	s.gate.Lock()
	defer s.gate.Unlock()
	if s.closed || s.closing.Load() || s.db.closed.Load() {
		return ErrSessionClosed
	}
	if enabled {
		return s.schema.SetDialect(sql.DialectPostgreSQL)
	}
	if s.schema.Dialect() == sql.DialectPostgreSQL {
		return s.schema.SetDialect(sql.DialectNative)
	}
	return nil
}

// PostgresCatalog reads virtual system metadata in the same catalog execution
// view as DescribePrepared; it never executes a user query.
func (s *Session) PostgresCatalog(ctx context.Context, name string) (*sql.ResultSet, error) {
	var result *sql.ResultSet
	err := s.readCatalog(ctx, func(schema *sql.Schema) error {
		var err error
		result, err = schema.PostgresCatalogTable(name)
		return err
	})
	return result, err
}
