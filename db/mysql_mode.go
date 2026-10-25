package db

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
