package db

import "slopdb/sql"

// SetDialect selects mutually exclusive connection-local SQL semantics.
// Existing prepared statements are rebound against the selected dialect and
// their cached execution recipes are invalidated on their next execution.
func (s *Session) SetDialect(d sql.Dialect) error {
	s.gate.Lock()
	defer s.gate.Unlock()
	if s.closed || s.closing.Load() || s.db.closed.Load() {
		return ErrSessionClosed
	}
	return s.schema.SetDialect(d)
}

// Dialect reports this session's selection, serialized with calls on it.
func (s *Session) Dialect() sql.Dialect {
	s.gate.Lock()
	defer s.gate.Unlock()
	return s.schema.Dialect()
}
