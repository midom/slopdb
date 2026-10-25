package db

func (s *Session) SetPostgresMode(enabled bool) error {
	s.gate.Lock()
	defer s.gate.Unlock()
	if s.closed || s.closing.Load() || s.db.closed.Load() {
		return ErrSessionClosed
	}
	s.schema.SetPostgresMode(enabled)
	return nil
}
