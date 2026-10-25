package db

import (
	"context"
	"slopdb/sql"
)

// SetMySQLMode enables MySQL value affinity and result-count semantics for this
// session. It is independent of input quoting and does not alter other sessions.
func (s *Session) SetMySQLMode(enabled bool) error {
	s.gate.Lock()
	defer s.gate.Unlock()
	if s.closed || s.closing.Load() || s.db.closed.Load() {
		return ErrSessionClosed
	}
	s.schema.SetMySQLMode(enabled)
	return nil
}

// TemporaryCatalog returns a private catalog snapshot for protocol permission
// checks. Session serialization and the lifecycle reader protect it from SQL,
// rollback and concurrent closure; it never enters an executing SQL callback.
func (s *Session) TemporaryCatalog(ctx context.Context) (sql.TemporaryCatalogInfo, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	if err := s.gate.LockContext(ctx); err != nil {
		return sql.TemporaryCatalogInfo{}, err
	}
	defer s.gate.Unlock()
	if err := s.db.execution.RLockContextForReader(ctx, s.executionReader); err != nil {
		return sql.TemporaryCatalogInfo{}, err
	}
	defer s.db.execution.RUnlockForReader(s.executionReader)
	if err := s.registerActive(cancel); err != nil {
		return sql.TemporaryCatalogInfo{}, err
	}
	defer s.clearActive()
	return s.schema.TemporaryCatalog(), nil
}
