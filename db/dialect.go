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
	err := s.schema.SetDialect(d)
	if err == nil {
		if r := s.db.activityRecord(s.schema); r != nil {
			r.mu.Lock()
			r.value.Dialect = d
			r.mu.Unlock()
		}
	}
	return err
}

// Dialect reports this session's selection, serialized with calls on it.
func (s *Session) Dialect() sql.Dialect {
	s.gate.Lock()
	defer s.gate.Unlock()
	return s.schema.Dialect()
}
