package db

import "context"

// InTransaction reports whether this session has an active SQL transaction.
// It serializes with execution and includes transactions started by SAVEPOINT.
func (s *Session) InTransaction() bool {
	s.gate.Lock()
	defer s.gate.Unlock()
	_ = s.db.execution.RLockContextForReader(context.Background(), s.executionReader)
	defer s.db.execution.RUnlockForReader(s.executionReader)
	return s.schema.InTransaction()
}
