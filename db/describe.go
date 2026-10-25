package db

import (
	"context"
	"fmt"
	"slopdb/sql"
)

// DescribePrepared inspects a statement's current result metadata. It never
// executes user SQL, evaluates defaults/functions, or obtains row locks.
func (s *Session) DescribePrepared(ctx context.Context, stmt *Stmt) ([]sql.ColumnDef, error) {
	if stmt == nil || stmt.db != s.db {
		return nil, fmt.Errorf("statement belongs to another database")
	}
	if stmt.closed.Load() {
		return nil, ErrStmtClosed
	}
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
	args := make([]sql.Value, stmt.query.NumInput())
	for i := range args {
		args[i] = sql.NewIntValue(0)
	}
	bound, err := stmt.query.Bind(args)
	if err != nil {
		return nil, err
	}
	defer bound.Release()
	if len(bound.Statements()) != 1 {
		return nil, fmt.Errorf("metadata requires one statement")
	}
	resolved, err := s.schema.ResolveNamespaces(bound.Statements()[0])
	if err != nil {
		return nil, err
	}
	return s.schema.Describe(resolved)
}
