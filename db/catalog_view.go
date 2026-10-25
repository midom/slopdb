package db

import (
	"context"
	"slopdb/sql"
)

// readCatalog supplies the same cancellable session/catalog execution view used
// by static preparation. The callback must not execute SQL or re-enter Session.
func (s *Session) readCatalog(ctx context.Context, read func(*sql.Schema) error) error {
	if ctx == nil {
		ctx = context.Background()
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	if err := s.gate.LockContext(ctx); err != nil {
		return err
	}
	defer s.gate.Unlock()
	if err := s.db.execution.RLockContextForReader(ctx, s.executionReader); err != nil {
		return err
	}
	defer s.db.execution.RUnlockForReader(s.executionReader)
	if err := s.registerActive(cancel); err != nil {
		return err
	}
	defer s.clearActive()
	if err := s.schema.RefreshReadCommitted(); err != nil {
		return err
	}
	s.schema.SetContext(ctx)
	return read(s.schema)
}

// CatalogSnapshot exposes protocol-neutral facts in this session's catalog
// view, including its own temporary/uncommitted objects. It does not pin rows.
func (s *Session) CatalogSnapshot(ctx context.Context) (*sql.CatalogSnapshot, error) {
	var result *sql.CatalogSnapshot
	err := s.readCatalog(ctx, func(schema *sql.Schema) error { result = schema.CatalogSnapshot(); return nil })
	return result, err
}
