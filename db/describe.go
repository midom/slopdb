package db

import (
	"context"
	"fmt"
	"slopdb/sql"
)

// DescribePrepared inspects a statement's current result metadata. It never
// executes user SQL, evaluates defaults/functions, or obtains row locks.
func (s *Session) DescribePrepared(ctx context.Context, stmt *Stmt) ([]sql.ColumnDef, error) {
	d, err := s.DescribePreparedTypes(ctx, stmt, sql.DescribeOptions{})
	return d.Results, err
}

// DescribePreparedTypes supplies one shared, protocol-neutral metadata result.
// Parameter types are constraints; no placeholder values or SQL are executed.
func (s *Session) DescribePreparedTypes(ctx context.Context, stmt *Stmt, options sql.DescribeOptions) (sql.PreparedDescription, error) {
	if stmt == nil || stmt.db != s.db {
		return sql.PreparedDescription{}, fmt.Errorf("statement belongs to another database")
	}
	if stmt.closed.Load() {
		return sql.PreparedDescription{}, ErrStmtClosed
	}
	if ctx == nil {
		ctx = context.Background()
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	if err := s.gate.LockContext(ctx); err != nil {
		return sql.PreparedDescription{}, err
	}
	defer s.gate.Unlock()
	if err := s.db.execution.RLockContextForReader(ctx, s.executionReader); err != nil {
		return sql.PreparedDescription{}, err
	}
	defer s.db.execution.RUnlockForReader(s.executionReader)
	if err := s.registerActive(cancel); err != nil {
		return sql.PreparedDescription{}, err
	}
	defer s.clearActive()
	if err := s.schema.RefreshReadCommitted(); err != nil {
		return sql.PreparedDescription{}, err
	}
	s.schema.SetContext(ctx)
	resolved, err := s.schema.ResolveNamespaces(stmt.query.MetadataStatement())
	if err != nil {
		return sql.PreparedDescription{}, err
	}
	return s.schema.DescribePrepared(resolved, options)
}
