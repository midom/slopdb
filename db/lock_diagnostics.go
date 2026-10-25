package db

import (
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"slopdb/sql"
)

// LockWaitInfo is a point-in-time view of one session waiting for a logical
// catalog, row, or unique-value lock. Session IDs are scoped to this database
// instance. Resource is a hex-encoded physical key for row locks and a framed
// table/column name for unique locks. Values contains the unique predicate.
type LockWaitInfo struct {
	SessionID          uint64
	BlockingSessionIDs []uint64
	ResourceKind       string
	Resource           string
	Values             []sql.Value
	Exclusive          bool
	WaitDuration       time.Duration
}

// LockWaits reports current lock waits without requiring a SQL session or an
// execution gate. The result does not retain references to internal lock data.
func (db *Database) LockWaits() []LockWaitInfo {
	if db == nil {
		return nil
	}
	m := db.diagnosticLocks.Load()
	if m == nil {
		return nil
	}
	m.graphMu.Lock()
	defer m.graphMu.Unlock()
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
	now := time.Now()
	result := make([]LockWaitInfo, 0, len(m.waiting))
	for owner, q := range m.waiting {
		info := LockWaitInfo{Exclusive: q.request.Exclusive, WaitDuration: now.Sub(q.started)}
		if state, ok := m.states.Load(owner); ok {
			info.SessionID = state.(*ownerLocks).id
		}
		info.ResourceKind, info.Resource = describeLockResource(q.request.Resource)
		info.Values = make([]sql.Value, len(q.request.Values))
		copy(info.Values, q.request.Values)
		for i := range info.Values {
			info.Values[i].Blob = append([]byte(nil), info.Values[i].Blob...)
		}
		shard := m.shards[m.shardIndex(q.request.Resource)]
		seen := make(map[uint64]bool)
		for _, blocker := range shard.blockers(owner, q.request) {
			if state, ok := m.states.Load(blocker); ok {
				id := state.(*ownerLocks).id
				if !seen[id] {
					seen[id] = true
					info.BlockingSessionIDs = append(info.BlockingSessionIDs, id)
				}
			}
		}
		sort.Slice(info.BlockingSessionIDs, func(i, j int) bool { return info.BlockingSessionIDs[i] < info.BlockingSessionIDs[j] })
		result = append(result, info)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].SessionID < result[j].SessionID })
	return result
}

func describeLockResource(resource string) (kind, description string) {
	switch {
	case resource == "catalog":
		return "catalog", "catalog"
	case strings.HasPrefix(resource, "row:"):
		return "row", hex.EncodeToString([]byte(strings.TrimPrefix(resource, "row:")))
	case strings.HasPrefix(resource, "unique:"):
		return "unique", strings.TrimPrefix(resource, "unique:")
	default:
		return "other", resource
	}
}

// LockError describes a failed logical lock wait. Unwrap preserves sentinel
// errors such as ErrLockTimeout, ErrDeadlock and context.DeadlineExceeded.
type LockError struct {
	SessionID    uint64
	ResourceKind string
	Resource     string
	Err          error
}

func (e *LockError) Error() string {
	hint := "inspect Database.LockWaits() for active blockers or increase the lock timeout"
	if errors.Is(e.Err, ErrDeadlock) {
		hint = "retry the entire transaction; acquire shared resources in a consistent order"
	}
	return fmt.Sprintf("session %d waiting for %s lock on %q: %v; %s", e.SessionID, e.ResourceKind, e.Resource, e.Err, hint)
}

func (e *LockError) Unwrap() error { return e.Err }

func (db *Database) lockDiagnosticError(owner *sql.Schema, request sql.LockRequest, err error) error {
	if !errors.Is(err, ErrLockTimeout) && !errors.Is(err, ErrDeadlock) {
		return err
	}
	kind, resource := describeLockResource(request.Resource)
	return &LockError{SessionID: db.lockSessionID(owner), ResourceKind: kind, Resource: resource, Err: err}
}

func (db *Database) lockSessionID(owner *sql.Schema) uint64 {
	if db == nil || db.locks == nil || owner == nil {
		return 0
	}
	if state, ok := db.locks.states.Load(owner); ok {
		return state.(*ownerLocks).id
	}
	return 0
}

// SessionID returns the database-local ID of the default SQL session.
// IDs are diagnostic and are not persisted. A nil or closed handle returns 0.
func (db *Database) SessionID() uint64 {
	if db == nil || db.closed.Load() {
		return 0
	}
	return db.lockSessionID(db.schema)
}

// SessionID returns this session's database-local diagnostic ID, or 0 after
// closure. It can be called while Exec is waiting for a lock.
func (s *Session) SessionID() uint64 {
	if s == nil || s.closing.Load() {
		return 0
	}
	return s.db.lockSessionID(s.schema)
}

// SessionID returns the diagnostic ID of the session belonging to this Tx.
func (tx *Tx) SessionID() uint64 {
	if tx == nil {
		return 0
	}
	return tx.session.SessionID()
}
