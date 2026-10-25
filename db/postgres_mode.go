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
	if ctx == nil {
		ctx = context.Background()
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	if err := s.gate.LockContext(ctx); err != nil {
		return nil, err
	}
	defer s.gate.Unlock()
	if err := s.db.execution.RLockContextForReader(ctx, s.executionReader); err != nil {
		return nil, err
	}
	defer s.db.execution.RUnlockForReader(s.executionReader)
	if err := s.registerActive(cancel); err != nil {
		return nil, err
	}
	defer s.clearActive()
	if err := s.schema.RefreshReadCommitted(); err != nil {
		return nil, err
	}
	s.schema.SetContext(ctx)
	return s.schema.PostgresCatalogTable(name)
}
