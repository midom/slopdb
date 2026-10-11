package db

import (
	"context"
	"sync"

	"slopdb/sql"
)

// executionGate lets active statements run together and keeps database Close
// outside execution. Writer preference bounds shutdown waits; acquisition
// respects query cancellation.
type executionGate struct {
	mu               sync.Mutex
	readers, writers int
	activeWriter     bool
	changed          chan struct{}
}

func (g *executionGate) init() {
	if g.changed == nil {
		g.changed = make(chan struct{})
	}
}
func (g *executionGate) notify() { close(g.changed); g.changed = make(chan struct{}) }
func (g *executionGate) Lock()   { _ = g.LockContext(context.Background()) }
func (g *executionGate) LockContext(ctx context.Context) error {
	g.mu.Lock()
	g.init()
	g.writers++
	for g.activeWriter || g.readers > 0 {
		wait := g.changed
		g.mu.Unlock()
		select {
		case <-wait:
		case <-ctx.Done():
			g.mu.Lock()
			g.writers--
			g.notify()
			g.mu.Unlock()
			return ctx.Err()
		}
		g.mu.Lock()
	}
	g.writers--
	if err := ctx.Err(); err != nil {
		g.notify()
		g.mu.Unlock()
		return err
	}
	g.activeWriter = true
	g.mu.Unlock()
	return nil
}
func (g *executionGate) Unlock() { g.mu.Lock(); g.activeWriter = false; g.notify(); g.mu.Unlock() }
func (g *executionGate) RLockContext(ctx context.Context) error {
	g.mu.Lock()
	g.init()
	for g.activeWriter || g.writers > 0 {
		wait := g.changed
		g.mu.Unlock()
		select {
		case <-wait:
		case <-ctx.Done():
			return ctx.Err()
		}
		g.mu.Lock()
	}
	if err := ctx.Err(); err != nil {
		g.mu.Unlock()
		return err
	}
	g.readers++
	g.mu.Unlock()
	return nil
}
func (g *executionGate) RUnlock() {
	g.mu.Lock()
	g.readers--
	if g.readers == 0 {
		g.notify()
	}
	g.mu.Unlock()
}

// The session gate remains held across both paths. A failed lock acquisition
// leaves no locks behind, then uses the full executor's timeout/deadlock retry.
func (s *Session) tryPointUpdate(ctx context.Context, statements []sql.Statement) (*ExecResult, bool, error) {
	if len(statements) != 1 {
		return nil, false, nil
	}
	stmt, ok := statements[0].(*sql.UpdateStmt)
	if !ok {
		return nil, false, nil
	}
	d := s.db
	if s.schema.InTransaction() {
		return nil, false, nil
	}
	held := d.locks.snapshot(s.schema)
	if err := d.locks.acquire(s.schema, sql.LockRequest{Resource: "catalog"}); err != nil {
		return nil, false, nil
	}
	defer d.locks.restore(s.schema, held)
	s.schema.SetContext(ctx)
	if err := s.schema.RefreshReadCommitted(); err != nil {
		return nil, true, err
	}
	plan := s.schema.PreparePointUpdate(stmt)
	if plan == nil {
		return nil, false, nil
	}
	if err := d.locks.acquire(s.schema, sql.LockRequest{Resource: "row:" + string(plan.Key()), Exclusive: true}); err != nil {
		return nil, false, nil
	}
	changes, err := plan.Run(ctx)
	if err == nil {
		err = ctx.Err()
	}
	if err != nil {
		return nil, true, err
	}
	return &ExecResult{Changes: changes}, true, nil
}
