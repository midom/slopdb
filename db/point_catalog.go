package db

import (
	"slopdb/sql"
	"sync/atomic"
)

// Autocommit inserts and point updates hold one shared grant for each attempt. Stable owner
// IDs spread grants across counters; row and predicate ownership still uses
// the ordinary lock manager. Exclusive catalog locks stop new grants before
// checking the counters, and include these owners in waits and diagnostics.
// Failed attempts release their grant before entering a lock wait. This path
// is used only outside explicit transactions, so a grant cannot be
// upgraded, saved or retained across calls.
type pointCatalogGate struct {
	pending atomic.Bool
	padding [56]byte
	readers [128]executionReaders
}

func (m *lockManager) acquirePointCatalog(owner *sql.Schema) (*ownerLocks, error) {
	state := m.state(owner)
	gate := m.pointCatalog
	if !gate.pending.Load() {
		counter := &gate.readers[state.id%128].count
		counter.Add(1)
		if !gate.pending.Load() {
			state.pointCatalog.Store(true)
			return state, nil
		}
		m.releasePointCatalogCounter(state)
	}
	// The regular path supplies queued-writer fairness and the normal wait
	// request. Its ownership is released by the statement's lock journal.
	return nil, m.acquire(owner, sql.LockRequest{Resource: "catalog"})
}

func (m *lockManager) releasePointCatalog(state *ownerLocks) {
	if state == nil {
		return
	}
	state.pointCatalog.Store(false)
	m.releasePointCatalogCounter(state)
}

func (m *lockManager) releasePointCatalogCounter(state *ownerLocks) {
	gate := m.pointCatalog
	if gate.readers[state.id%128].count.Add(-1) == 0 && gate.pending.Load() {
		shard := m.shards[0]
		shard.mu.Lock()
		if !shard.pointCatalogBlocked(sql.LockRequest{Resource: "catalog", Exclusive: true}) {
			shard.notifyResource(sql.LockRequest{Resource: "catalog"})
		}
		shard.mu.Unlock()
	}
}

// The catalog shard mutex protects exclusive ownership and the waiter list.
func (m *lockManager) refreshPointCatalogPendingLocked() {
	if m.root == nil || m != m.root.shards[0] {
		return
	}
	pending := false
	if entry := m.entry(sql.LockRequest{Resource: "catalog"}); entry != nil && entry.exclusiveOwner != nil {
		pending = true
	}
	for _, queued := range m.waiting {
		if queued.request.Resource == "catalog" && queued.request.Exclusive {
			pending = true
			break
		}
	}
	m.root.pointCatalog.pending.Store(pending)
}

func (m *lockManager) pointCatalogBlocked(request sql.LockRequest) bool {
	if m.root == nil || request.Resource != "catalog" || !request.Exclusive {
		return false
	}
	for i := range m.root.pointCatalog.readers {
		if m.root.pointCatalog.readers[i].count.Load() != 0 {
			return true
		}
	}
	return false
}
