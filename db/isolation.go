package db

import "slopdb/sql"

// SetIsolationLevel sets the session default for future transactions.
func (s *Session) SetIsolationLevel(level sql.IsolationLevel) error {
	s.gate.Lock()
	defer s.gate.Unlock()
	if s.closed || s.closing.Load() || s.db.closed.Load() {
		return ErrSessionClosed
	}
	return s.schema.SetIsolationLevel(level)
}

func (s *Session) SetNextTransactionIsolation(level sql.IsolationLevel) error {
	s.gate.Lock()
	defer s.gate.Unlock()
	if s.closed || s.closing.Load() || s.db.closed.Load() {
		return ErrSessionClosed
	}
	return s.schema.SetNextTransactionIsolation(level)
}

func (s *Session) IsolationLevel() sql.IsolationLevel {
	s.gate.Lock()
	defer s.gate.Unlock()
	return s.schema.IsolationLevel()
}
