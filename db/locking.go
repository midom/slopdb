package db

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"slopdb/sql"
)

var (
	ErrDeadlock         = errors.New("transaction deadlock; retry transaction")
	ErrLockTimeout      = errors.New("transaction lock wait timeout")
	ErrLockNotAvailable = sql.ErrLockNotAvailable
)

// contextMutex also permits cancellation while queued behind SQL execution.
type contextMutex struct {
	once sync.Once
	gate chan struct{}
}

func (m *contextMutex) init()   { m.once.Do(func() { m.gate = make(chan struct{}, 1) }) }
func (m *contextMutex) Lock()   { m.init(); m.gate <- struct{}{} }
func (m *contextMutex) Unlock() { <-m.gate }
func (m *contextMutex) LockContext(ctx context.Context) error {
	m.init()
	if err := ctx.Err(); err != nil {
		return err
	}
	// Most sessions have a single caller. Avoid materializing the context's
	// cancellation channel unless acquisition actually has to wait.
	select {
	case m.gate <- struct{}{}:
		if err := ctx.Err(); err != nil {
			m.Unlock()
			return err
		}
		return nil
	default:
	}
	select {
	case m.gate <- struct{}{}:
		if err := ctx.Err(); err != nil {
			m.Unlock()
			return err
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

type lockEntry struct {
	request        sql.LockRequest
	owners         map[*sql.Schema]bool // true means exclusive
	exclusiveOwner *sql.Schema
	position       int
	valueKey       string
	keyed          bool
}

type lockWait struct{ request sql.LockRequest }

type queuedLock struct {
	request sql.LockRequest
	order   uint64
	started time.Time
}

func (w *lockWait) Error() string { return "transaction lock is held by another transaction" }

// Independent resource shards protect lock ownership. The root wait graph is
// used only for contended acquisitions; SQL evaluation and storage reads execute
// outside these locks.
type lockManager struct {
	mu            sync.Mutex
	shards        []*lockManager
	states        sync.Map
	graphMu       sync.Mutex
	order         atomic.Uint64
	nextSessionID atomic.Uint64
	resources     map[string]*lockBucket
	owned         map[*sql.Schema]map[*lockEntry]bool
	waiting       map[*sql.Schema]queuedLock
	notifications map[*sql.Schema]chan struct{}
}

type ownerLocks struct {
	transaction transactionControl
	mu          sync.Mutex
	id          uint64
	lastChange  [64]int // journal positions encoded as index + 1
	changes     []lockChange
	catalogMode int // 0 absent, 1 shared, 2 exclusive; protected by owner.mu
}

type lockChange struct {
	entry           *lockEntry
	previousShard   int
	shard           int
	held, exclusive bool
}

func emptyLockManager() *lockManager {
	return &lockManager{resources: make(map[string]*lockBucket), owned: make(map[*sql.Schema]map[*lockEntry]bool), waiting: make(map[*sql.Schema]queuedLock), notifications: make(map[*sql.Schema]chan struct{})}
}
func newLockManager() *lockManager {
	m := emptyLockManager()
	m.shards = make([]*lockManager, 64)
	for i := range m.shards {
		m.shards[i] = emptyLockManager()
	}
	return m
}
func (m *lockManager) shardIndex(resource string) int {
	if resource == "catalog" {
		return 0
	}
	h := uint64(14695981039346656037)
	for i := 0; i < len(resource); i++ {
		h = (h ^ uint64(resource[i])) * 1099511628211
	}
	return 1 + int(h%63)
}
func (m *lockManager) state(owner *sql.Schema) *ownerLocks {
	if s, ok := m.states.Load(owner); ok {
		return s.(*ownerLocks)
	}
	s, _ := m.states.LoadOrStore(owner, &ownerLocks{id: m.nextSessionID.Add(1)})
	return s.(*ownerLocks)
}
func (m *lockManager) forget(owner *sql.Schema) { m.states.Delete(owner) }

func (db *Database) installLocks(schema *sql.Schema) {
	db.locks.state(schema)
	schema.SetLockHandler(func(request sql.LockRequest) error { return db.locks.acquire(schema, request) })
	schema.SetLockGroupHandler(func(requests []sql.LockRequest) error { return db.locks.acquireAll(schema, requests) })
}

func (db *Database) statementCatalogLock(schema *sql.Schema, statement sql.Statement) error {
	exclusive := false
	switch statement.(type) {
	case *sql.CreateTableStmt, *sql.DropTableStmt, *sql.AlterTableStmt,
		*sql.CreateIndexStmt, *sql.DropIndexStmt, *sql.CreateViewStmt, *sql.DropViewStmt,
		*sql.CreateTriggerStmt, *sql.DropTriggerStmt, *sql.VacuumStmt:
		exclusive = true
	case *sql.PragmaStmt:
		pragma := statement.(*sql.PragmaStmt)
		setting := pragma.Value != nil || len(pragma.Args) > 0
		if !setting {
			return nil
		}
		if pragma.Name == "collation" {
			exclusive = true
		}
	case *sql.InsertStmt, *sql.UpdateStmt, *sql.DeleteStmt, *sql.AnalyzeStmt:
	default:
		return nil
	}
	if onlineIndexStatement(schema, statement) {
		exclusive = false
	}
	return db.locks.acquire(schema, sql.LockRequest{Resource: "catalog", Exclusive: exclusive})
}
func sameValues(a, b []sql.Value) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if sql.ValueCompare(a[i], b[i]) != 0 {
			return false
		}
	}
	return true
}
func (m *lockManager) entry(request sql.LockRequest) *lockEntry {
	if bucket := m.resources[request.Resource]; bucket != nil {
		return bucket.find(request.Values)
	}
	return nil
}
func (m *lockManager) blockers(owner *sql.Schema, request sql.LockRequest) []*sql.Schema {
	var owners []*sql.Schema
	if entry := m.entry(request); entry != nil {
		if mode, held := entry.owners[owner]; held && (mode || !request.Exclusive) {
			return nil // Existing owners can reuse locks despite queued waiters.
		}
		if !request.Exclusive {
			if entry.exclusiveOwner != nil && entry.exclusiveOwner != owner {
				owners = append(owners, entry.exclusiveOwner)
			}
		} else {
			for other := range entry.owners {
				if other != owner {
					owners = append(owners, other)
				}
			}
		}
	}
	order := ^uint64(0)
	if queued, ok := m.waiting[owner]; ok {
		order = queued.order
	}
	// Give earlier incompatible waiters priority, including pending catalog
	// upgrades. This prevents an active stream of writers from starving them.
	for other, queued := range m.waiting {
		if other != owner && queued.order < order && queued.request.Resource == request.Resource &&
			(queued.request.Exclusive || request.Exclusive) && sameValues(queued.request.Values, request.Values) {
			owners = append(owners, other)
		}
	}
	return owners
}
func (m *lockManager) acquireLocked(owner *sql.Schema, request sql.LockRequest) (*lockEntry, bool, bool, error) {
	if len(m.blockers(owner, request)) > 0 {
		if request.Wait != sql.LockWait {
			return nil, false, false, ErrLockNotAvailable
		}
		return nil, false, false, &lockWait{request: request}
	}
	entry := m.entry(request)
	if entry == nil {
		request.Values = append([]sql.Value(nil), request.Values...)
		for i := range request.Values {
			request.Values[i].Blob = append([]byte(nil), request.Values[i].Blob...)
		}
		entry = &lockEntry{request: request, owners: make(map[*sql.Schema]bool)}
		bucket := m.resources[request.Resource]
		if bucket == nil {
			bucket = &lockBucket{}
			m.resources[request.Resource] = bucket
		}
		bucket.add(entry)
	}
	previous, held := entry.owners[owner]
	entry.owners[owner] = previous || request.Exclusive
	if request.Exclusive {
		entry.exclusiveOwner = owner
	}
	if m.owned[owner] == nil {
		m.owned[owner] = make(map[*lockEntry]bool)
	}
	m.owned[owner][entry] = entry.owners[owner]
	return entry, held, previous, nil
}

func (m *lockManager) restoreEntryLocked(owner *sql.Schema, entry *lockEntry, held, exclusive bool) {
	if held {
		entry.owners[owner] = exclusive
		m.owned[owner][entry] = exclusive
		if exclusive {
			entry.exclusiveOwner = owner
		} else if entry.exclusiveOwner == owner {
			entry.exclusiveOwner = nil
		}
	} else {
		delete(entry.owners, owner)
		if entry.exclusiveOwner == owner {
			entry.exclusiveOwner = nil
		}
		delete(m.owned[owner], entry)
		if len(entry.owners) == 0 {
			bucket := m.resources[entry.request.Resource]
			bucket.remove(entry)
			if len(bucket.entries) == 0 {
				delete(m.resources, entry.request.Resource)
			}
		}
		if len(m.owned[owner]) == 0 {
			delete(m.owned, owner)
		}
	}
	m.notifyResource(entry.request)
}
func (state *ownerLocks) record(entry *lockEntry, shard int, held, previous, exclusive bool) {
	if !held || (!previous && exclusive) {
		state.changes = append(state.changes, lockChange{entry: entry, shard: shard, held: held, exclusive: previous, previousShard: state.lastChange[shard]})
		state.lastChange[shard] = len(state.changes)
	}
	if entry.request.Resource == "catalog" && len(entry.request.Values) == 0 {
		state.catalogMode = 1
		if previous || exclusive {
			state.catalogMode = 2
		}
	}
}

func (m *lockManager) notifyResource(request sql.LockRequest) {
	for owner, queued := range m.waiting {
		if queued.request.Resource == request.Resource && sameValues(queued.request.Values, request.Values) {
			if ch := m.notifications[owner]; ch != nil {
				close(ch)
				delete(m.notifications, owner)
			}
		}
	}
}
func (m *lockManager) removeWaiting(owner *sql.Schema) {
	if q, ok := m.waiting[owner]; ok {
		delete(m.waiting, owner)
		delete(m.notifications, owner)
		m.notifyResource(q.request)
	}
}

// Contended deadlock detection takes a coherent view of the wait graph and
// shards. Uncontended row-lock operations never acquire the graph mutex.
func (m *lockManager) deadlocked(owner *sql.Schema) bool {
	m.graphMu.Lock()
	defer m.graphMu.Unlock()
	// The graph mutex freezes waiter registration. Only resources mentioned
	// by that graph can contribute an edge; lock those shards in order to
	// capture coherent ownership without stopping unrelated resource shards.
	var mask uint64
	for _, q := range m.waiting {
		mask |= uint64(1) << m.shardIndex(q.request.Resource)
	}
	for i, shard := range m.shards {
		if mask&(uint64(1)<<i) != 0 {
			shard.mu.Lock()
		}
	}
	defer func() {
		for i := len(m.shards) - 1; i >= 0; i-- {
			if mask&(uint64(1)<<i) != 0 {
				m.shards[i].mu.Unlock()
			}
		}
	}()
	seen := make(map[*sql.Schema]bool)
	var visit func(*sql.Schema) bool
	visit = func(current *sql.Schema) bool {
		if seen[current] {
			return false
		}
		seen[current] = true
		q, ok := m.waiting[current]
		if !ok {
			return false
		}
		s := m.shards[m.shardIndex(q.request.Resource)]
		for _, next := range s.blockers(current, q.request) {
			if next == owner || visit(next) {
				return true
			}
		}
		return false
	}
	return visit(owner)
}
func (m *lockManager) acquire(owner *sql.Schema, r sql.LockRequest) error {
	state := m.state(owner)
	state.mu.Lock()
	defer state.mu.Unlock()
	// Existing catalog owners can reuse their lock even with a queued upgrade.
	// Ownership changes and release share this owner mutex, so this avoids the
	// global catalog shard on every statement within the same transaction.
	if r.Resource == "catalog" && len(r.Values) == 0 && (state.catalogMode == 2 || (state.catalogMode == 1 && !r.Exclusive)) {
		return nil
	}
	i := m.shardIndex(r.Resource)
	s := m.shards[i]
	s.mu.Lock()
	defer s.mu.Unlock()
	entry, held, previous, err := s.acquireLocked(owner, r)
	if err == nil {
		state.record(entry, i, held, previous, r.Exclusive)
	}
	return err
}
func (m *lockManager) acquireAll(owner *sql.Schema, requests []sql.LockRequest) error {
	state := m.state(owner)
	state.mu.Lock()
	defer state.mu.Unlock()
	var mask uint64
	for _, r := range requests {
		mask |= uint64(1) << m.shardIndex(r.Resource)
	}
	for i, s := range m.shards {
		if mask&(uint64(1)<<i) != 0 {
			s.mu.Lock()
		}
	}
	defer func() {
		for i := len(m.shards) - 1; i >= 0; i-- {
			if mask&(uint64(1)<<i) != 0 {
				m.shards[i].mu.Unlock()
			}
		}
	}()
	for _, r := range requests {
		if len(m.shards[m.shardIndex(r.Resource)].blockers(owner, r)) > 0 {
			if r.Wait != sql.LockWait {
				return ErrLockNotAvailable
			}
			return &lockWait{request: r}
		}
	}
	for _, r := range requests {
		i := m.shardIndex(r.Resource)
		entry, held, previous, err := m.shards[i].acquireLocked(owner, r)
		if err != nil {
			return err
		}
		state.record(entry, i, held, previous, r.Exclusive)
	}
	return nil
}

// A statement checkpoint is a journal offset, independent of transaction size.
func (m *lockManager) snapshot(owner *sql.Schema) int {
	state := m.state(owner)
	state.mu.Lock()
	defer state.mu.Unlock()
	return len(state.changes)
}
func (m *lockManager) restore(owner *sql.Schema, mark int) {
	state := m.state(owner)
	state.mu.Lock()
	defer state.mu.Unlock()
	if mark > len(state.changes) {
		mark = len(state.changes)
	}
	if mark == len(state.changes) {
		return
	}
	var mask uint64
	for _, change := range state.changes[mark:] {
		mask |= uint64(1) << change.shard
	}
	// SQL has already undone the failed statement or published the commit.
	// Resource shards can be restored independently; holding all of them at
	// once would block unrelated transactions throughout a large release.
	for i, shard := range m.shards {
		if mask&(uint64(1)<<i) == 0 {
			continue
		}
		shard.mu.Lock()
		for pos := state.lastChange[i]; pos > mark; pos = state.lastChange[i] {
			change := state.changes[pos-1]
			shard.restoreEntryLocked(owner, change.entry, change.held, change.exclusive)
			state.lastChange[i] = change.previousShard
			if change.entry.request.Resource == "catalog" && len(change.entry.request.Values) == 0 {
				state.catalogMode = 0
				if change.held {
					state.catalogMode = 1
					if change.exclusive {
						state.catalogMode = 2
					}
				}
			}
		}
		shard.mu.Unlock()
	}
	clear(state.changes[mark:])
	state.changes = state.changes[:mark]
}
func (m *lockManager) release(owner *sql.Schema) { m.restore(owner, 0) }

// Wait without an execution view so Close and the lock owner can proceed.
// Reacquiring the shared view before returning also protects schema rollback.
func (db *Database) waitForLock(ctx context.Context, owner *sql.Schema, request sql.LockRequest, deadline time.Time) (result error) {
	return db.waitForLockPolicy(ctx, owner, request, deadline, nil)
}

func (db *Database) waitForLockPolicy(ctx context.Context, owner *sql.Schema, request sql.LockRequest, deadline time.Time, abortAfter *time.Duration) (result error) {
	defer func() { result = db.lockDiagnosticError(owner, request, result) }()
	if owner.QueryProfile() != nil {
		started := time.Now()
		defer func() { owner.RecordLockWait(time.Since(started)) }()
	}
	reader := db.locks.state(owner).id
	db.execution.RUnlockForReader(reader)
	defer func() { _ = db.execution.RLockContextForReader(context.Background(), reader) }()
	root := db.locks
	shard := root.shards[root.shardIndex(request.Resource)]
	// Keep both waiter registries coherent with the detector's graph -> shard
	// lock order, including fairness ordering while a waiter enters or leaves.
	root.graphMu.Lock()
	shard.mu.Lock()
	queued := queuedLock{request: request, order: root.order.Add(1), started: time.Now()}
	root.waiting[owner] = queued
	shard.waiting[owner] = queued
	shard.mu.Unlock()
	root.graphMu.Unlock()
	defer func() {
		root.graphMu.Lock()
		shard.mu.Lock()
		shard.removeWaiting(owner)
		delete(root.waiting, owner)
		shard.mu.Unlock()
		root.graphMu.Unlock()
	}()
	var abortTimer *time.Timer
	var abortTimeout <-chan time.Time
	if abortAfter != nil {
		abortTimer = time.NewTimer(*abortAfter)
		abortTimeout = abortTimer.C
		defer abortTimer.Stop()
	}
	abortDue := false
	var processedVictims map[transactionVictimID]bool
	if abortAfter != nil {
		processedVictims = make(map[transactionVictimID]bool)
	}
	for {
		if db.closed.Load() {
			return errors.New("database closed")
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if !deadline.IsZero() && !time.Now().Before(deadline) {
			return fmt.Errorf("%w: %w", ErrLockTimeout, context.DeadlineExceeded)
		}
		state := root.state(owner)
		state.mu.Lock()
		shard.mu.Lock()
		if err := ctx.Err(); err != nil {
			shard.mu.Unlock()
			state.mu.Unlock()
			return err
		}
		if db.closed.Load() {
			shard.mu.Unlock()
			state.mu.Unlock()
			return errors.New("database closed")
		}
		if len(shard.blockers(owner, request)) == 0 {
			entry, held, previous, err := shard.acquireLocked(owner, request)
			if err == nil {
				state.record(entry, root.shardIndex(request.Resource), held, previous, request.Exclusive)
			}
			shard.mu.Unlock()
			state.mu.Unlock()
			return err
		}
		if abortDue && db.hasPublicationVictimsLocked(shard, owner, request, processedVictims) {
			shard.mu.Unlock()
			state.mu.Unlock()
			abortCtx := ctx
			cancel := func() {}
			if !deadline.IsZero() {
				abortCtx, cancel = context.WithDeadline(ctx, deadline)
			}
			err := db.abortPublicationBlockers(abortCtx, owner, request, processedVictims)
			cancel()
			if err != nil {
				if ctx.Err() == nil && !deadline.IsZero() && !time.Now().Before(deadline) {
					return fmt.Errorf("%w: %w", ErrLockTimeout, context.DeadlineExceeded)
				}
				return err
			}
			continue
		}
		ch := make(chan struct{})
		shard.notifications[owner] = ch
		shard.mu.Unlock()
		state.mu.Unlock()
		if db.opt.DeadlockDetection && root.deadlocked(owner) {
			return ErrDeadlock
		}
		var timer *time.Timer
		var timeout <-chan time.Time
		if !deadline.IsZero() {
			timer = time.NewTimer(time.Until(deadline))
			timeout = timer.C
		}
		var err error
		select {
		case <-ch:
		case <-ctx.Done():
			err = ctx.Err()
		case <-timeout:
			err = fmt.Errorf("%w: %w", ErrLockTimeout, context.DeadlineExceeded)
		case <-abortTimeout:
			abortDue = true
			abortTimeout = nil
		}
		if timer != nil {
			timer.Stop()
		}
		if err != nil {
			return err
		}
	}
}
