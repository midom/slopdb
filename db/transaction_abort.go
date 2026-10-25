package db

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"

	"slopdb/sql"
)

// ErrTransactionAborted identifies a full transaction rollback requested by
// index publication. Retry the transaction using a new Tx or BEGIN.
var ErrTransactionAborted = errors.New("transaction rolled back for index publication")

type TransactionAbortedError struct {
	SessionID          uint64
	PublisherSessionID uint64
	transactionID      uint64
}

func (e *TransactionAbortedError) Error() string {
	return fmt.Sprintf("session %d: %v by session %d; retry the entire transaction", e.SessionID, ErrTransactionAborted, e.PublisherSessionID)
}

func (e *TransactionAbortedError) Unwrap() error { return ErrTransactionAborted }

// This mutex protects only transaction/cancellation state. Schema access always
// requires the session gate and execution view; the publisher never reads a
// victim's mutable SQL state without owning those guards.
type transactionControl struct {
	mu               sync.Mutex
	nextID           uint64
	id               atomic.Uint64
	request, pending *TransactionAbortedError
	activeCancel     context.CancelCauseFunc
	commitWon        bool
}

func (db *Database) syncManagedTransaction(schema *sql.Schema, statementErr error) {
	db.syncActivity(schema)
	state := db.locks.state(schema)
	c := &state.transaction
	c.mu.Lock()
	defer c.mu.Unlock()
	if schema.InTransaction() {
		if c.id.Load() == 0 {
			c.nextID++
			c.id.Store(c.nextID)
		}
		return
	}
	if c.id.Load() != 0 || c.request != nil {
		c.id.Store(0)
		// Commit/rollback can finish before a requested cancellation takes
		// effect. A request for that generation must never reach its successor.
		if c.request != nil {
			if statementErr == nil {
				c.commitWon = true
			} else {
				c.pending = c.request
			}
			c.request = nil
		}
	}
}

func (db *Database) beginManagedCall(schema *sql.Schema, cancel context.CancelCauseFunc) error {
	c := &db.locks.state(schema).transaction
	c.mu.Lock()
	c.activeCancel = cancel
	c.commitWon = false
	pending, request := c.pending, c.request
	c.pending = nil
	c.mu.Unlock()
	if pending != nil {
		return pending
	}
	if request != nil {
		return db.rollbackRequestedTransaction(schema, request, false)
	}
	return nil
}

// rollbackRequestedTransaction runs with the victim's session gate and an
// execution view held. Generation checks protect a subsequent transaction even
// if the original holder commits while the publisher is selecting victims.
func (db *Database) rollbackRequestedTransaction(schema *sql.Schema, request *TransactionAbortedError, pending bool) error {
	c := &db.locks.state(schema).transaction
	c.mu.Lock()
	eligible := c.id.Load() == request.transactionID && c.request == request
	c.mu.Unlock()
	if !eligible || !schema.InTransaction() {
		return nil
	}
	if err := schema.Rollback(); err != nil {
		return errors.Join(request, err)
	}
	db.locks.release(schema)
	c.mu.Lock()
	c.id.Store(0)
	c.request = nil
	if pending {
		c.pending = request
	}
	c.mu.Unlock()
	return request
}

func (db *Database) finishManagedCall(schema *sql.Schema, ctx context.Context, err error, pending bool) error {
	defer db.syncActivity(schema)
	c := &db.locks.state(schema).transaction
	c.mu.Lock()
	c.activeCancel = nil
	request, completed := c.request, c.pending
	if !pending {
		c.pending = nil
	}
	c.mu.Unlock()
	if request != nil {
		if aborted := db.rollbackRequestedTransaction(schema, request, pending); aborted != nil {
			return aborted
		}
	}
	db.syncManagedTransaction(schema, err)
	if completed != nil {
		return completed
	}
	return err
}

func (db *Database) publicationCommitWon(schema *sql.Schema, ctx context.Context) bool {
	if !errors.Is(context.Cause(ctx), ErrTransactionAborted) {
		return false
	}
	c := &db.locks.state(schema).transaction
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.commitWon
}

func (s *Session) finishTransactionAbort(err error) error {
	var aborted *TransactionAbortedError
	if !s.transactionHandle || !errors.As(err, &aborted) {
		return err
	}
	s.abortError.Store(aborted)
	s.db.mu.Lock()
	s.closeLocked()
	s.db.mu.Unlock()
	return err
}

func (s *Session) terminalTransactionError() error {
	if aborted := s.abortError.Load(); aborted != nil {
		return errors.Join(ErrTxDone, aborted)
	}
	return nil
}

type abortVictim struct {
	schema  *sql.Schema
	state   *ownerLocks
	id      uint64
	session *Session // nil means the default database session
}

type transactionVictimID struct {
	schema *sql.Schema
	id     uint64
}

// The shard lock protects ownership; atomic transaction IDs allow checking
// eligibility without reversing the transaction-control -> shard lock order.
func (db *Database) hasPublicationVictimsLocked(shard *lockManager, publisher *sql.Schema, request sql.LockRequest, processed map[transactionVictimID]bool) bool {
	if entry := shard.entry(request); entry != nil {
		found := false
		entry.eachOwner(func(owner *sql.Schema, exclusive bool) {
			if owner == publisher || (!exclusive && !request.Exclusive) {
				return
			}
			if raw, exists := db.locks.states.Load(owner); exists {
				id := raw.(*ownerLocks).transaction.id.Load()
				if id != 0 && !processed[transactionVictimID{owner, id}] {
					found = true
				}
			}
		})
		if found {
			return true
		}
	}
	return false
}

// selectPublicationVictims considers actual incompatible catalog owners, not
// fairness waiters. Autocommit operations and transactions holding no catalog
// lock are excluded.
func (db *Database) selectPublicationVictims(publisher *sql.Schema, request sql.LockRequest) []abortVictim {
	shard := db.locks.shards[db.locks.shardIndex(request.Resource)]
	shard.mu.Lock()
	var owners []*sql.Schema
	if entry := shard.entry(request); entry != nil {
		entry.eachOwner(func(owner *sql.Schema, exclusive bool) {
			if owner != publisher && (exclusive || request.Exclusive) {
				owners = append(owners, owner)
			}
		})
	}
	shard.mu.Unlock()
	var victims []abortVictim
	for _, owner := range owners {
		raw, exists := db.locks.states.Load(owner)
		if !exists {
			continue
		}
		state := raw.(*ownerLocks)
		id := state.transaction.id.Load()
		if id == 0 {
			continue
		}
		victim := abortVictim{schema: owner, state: state, id: id}
		db.mu.Lock()
		found := owner == db.schema
		if !found {
			for session := range db.sessions {
				if session.schema == owner {
					victim.session, found = session, true
					break
				}
			}
		}
		db.mu.Unlock()
		if found {
			victims = append(victims, victim)
		}
	}
	return victims
}

// abortPublicationBlockers runs while the publication waiter has released its
// execution view. Cancel all selected active calls first, then acquire their
// gates one at a time for idle-transaction rollback.
func (db *Database) abortPublicationBlockers(ctx context.Context, publisher *sql.Schema, request sql.LockRequest, processed map[transactionVictimID]bool) error {
	victims := db.selectPublicationVictims(publisher, request)
	victims = slices.DeleteFunc(victims, func(v abortVictim) bool { return processed[transactionVictimID{v.schema, v.id}] })
	if len(victims) != 0 && db.onlineIndexHook != nil {
		db.onlineIndexHook("abort-victims-selected")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	for _, victim := range victims {
		c := &victim.state.transaction
		c.mu.Lock()
		shard := db.locks.shards[db.locks.shardIndex(request.Resource)]
		shard.mu.Lock()
		owns := false
		if entry := shard.entry(request); entry != nil {
			mode, held := entry.ownerMode(victim.schema)
			owns = held && (mode || request.Exclusive)
		}
		if c.id.Load() == victim.id && owns {
			processed[transactionVictimID{victim.schema, victim.id}] = true
			if c.request == nil {
				c.request = &TransactionAbortedError{SessionID: victim.state.id, PublisherSessionID: db.lockSessionID(publisher), transactionID: victim.id}
				if c.activeCancel != nil {
					c.activeCancel(c.request)
				}
			}
		}
		shard.mu.Unlock()
		c.mu.Unlock()
	}
	// Once an abort request is published, finish its rollback even if the
	// publisher is cancelled. Active calls also process requests in their own
	// finalizer. This worker covers idle transactions and never accesses SQL
	// state without the victim's gate and lifecycle view.
	done := make(chan struct{})
	go func() {
		defer close(done)
		for _, victim := range victims {
			db.completePublicationVictim(victim)
		}
	}()
	if len(victims) != 0 && db.onlineIndexHook != nil {
		db.onlineIndexHook("abort-victims-signalled")
	}
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (db *Database) completePublicationVictim(victim abortVictim) {
	gate := &db.defaultGate
	if victim.session != nil {
		gate = &victim.session.gate
	}
	gate.Lock()
	defer gate.Unlock()
	_ = db.execution.RLockContext(context.Background())
	defer db.execution.RUnlock()
	if db.closed.Load() || (victim.session != nil && victim.session.closed) {
		return
	}
	victim.state.transaction.mu.Lock()
	request := victim.state.transaction.request
	victim.state.transaction.mu.Unlock()
	if request != nil && request.transactionID == victim.id {
		err := db.rollbackRequestedTransaction(victim.schema, request, true)
		if victim.session != nil {
			_ = victim.session.finishTransactionAbort(err)
		}
	}
}
