package db

import (
	"context"
	"sync"
	"sync/atomic"

	"slopdb/sql"
)

// executionGate lets active statements run together and keeps database Close
// outside execution. Writer preference bounds shutdown waits; acquisition
// respects query cancellation.
type executionGate struct {
	mu           sync.Mutex
	readers      atomic.Int64
	pending      atomic.Bool
	writers      int
	activeWriter bool
	changed      chan struct{}
	padding      [16]byte // separate writer intent from the first reader shard
	shards       [128]executionReaders
}

type executionReaders struct {
	count   atomic.Int64
	padding [56]byte
}

func (g *executionGate) hasReaders() bool {
	if g.readers.Load() != 0 {
		return true
	}
	for i := range g.shards {
		if g.shards[i].count.Load() != 0 {
			return true
		}
	}
	return false
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
	g.pending.Store(true)
	for g.activeWriter || g.hasReaders() {
		wait := g.changed
		g.mu.Unlock()
		select {
		case <-wait:
		case <-ctx.Done():
			g.mu.Lock()
			g.writers--
			g.pending.Store(g.activeWriter || g.writers > 0)
			g.notify()
			g.mu.Unlock()
			return ctx.Err()
		}
		g.mu.Lock()
	}
	g.writers--
	if err := ctx.Err(); err != nil {
		g.pending.Store(g.activeWriter || g.writers > 0)
		g.notify()
		g.mu.Unlock()
		return err
	}
	g.activeWriter = true
	g.mu.Unlock()
	return nil
}
func (g *executionGate) Unlock() {
	g.mu.Lock()
	g.activeWriter = false
	g.pending.Store(g.writers > 0)
	g.notify()
	g.mu.Unlock()
}
func (g *executionGate) RLockContext(ctx context.Context) error {
	return g.rLockContext(ctx, &g.readers)
}

func (g *executionGate) RLockContextForReader(ctx context.Context, reader uint64) error {
	return g.rLockContext(ctx, &g.shards[reader%uint64(len(g.shards))].count)
}

func (g *executionGate) rLockContext(ctx context.Context, readers *atomic.Int64) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	// Register before rechecking writer intent. A racing writer either sees
	// this reader and waits, or intent is visible and this reader backs out
	// before touching the database. Slow waiters retain writer preference.
	if !g.pending.Load() {
		readers.Add(1)
		if !g.pending.Load() {
			if err := ctx.Err(); err != nil {
				g.rUnlock(readers)
				return err
			}
			return nil
		}
		g.rUnlock(readers)
	}
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
	readers.Add(1)
	g.mu.Unlock()
	return nil
}
func (g *executionGate) RUnlock() {
	g.rUnlock(&g.readers)
}

func (g *executionGate) RUnlockForReader(reader uint64) {
	g.rUnlock(&g.shards[reader%uint64(len(g.shards))].count)
}

func (g *executionGate) rUnlock(readers *atomic.Int64) {
	if readers.Add(-1) == 0 && g.pending.Load() {
		g.mu.Lock()
		if !g.hasReaders() && g.writers > 0 {
			g.notify()
		}
		g.mu.Unlock()
	}
}

// The session gate remains held across both paths. A failed lock acquisition
// leaves no locks behind, then uses the full executor's timeout/deadlock retry.
func (s *Session) tryPointUpdate(ctx context.Context, statements []sql.Statement, dst *ExecResult) (*ExecResult, bool, error) {
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
	catalog, err := d.locks.acquirePointCatalog(s.schema)
	if err != nil {
		return nil, false, nil
	}
	defer d.locks.releasePointCatalog(catalog)
	defer d.locks.restore(s.schema, held)
	s.schema.SetContext(ctx)
	if err := s.schema.RefreshReadCommitted(); err != nil {
		return nil, true, err
	}
	plan := s.schema.PreparePointUpdateInto(stmt, &s.pointTarget)
	defer func() { s.pointTarget = sql.PointUpdate{} }()
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
	if dst == nil {
		dst = new(ExecResult)
	}
	*dst = ExecResult{Changes: changes}
	return dst, true, nil
}
