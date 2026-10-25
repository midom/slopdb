package db

import (
	"context"
	"errors"
	"math"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"slopdb/sql"
)

type activityRecord struct {
	mu                   sync.Mutex
	value                sql.SessionActivity
	identified           bool
	inTransaction        bool
	failed               bool
	lastTransactionStart time.Time
	// Native facts are accessed only under the owner's gate and execution view.
	// Protocol reports use a generation to invalidate this cache without requiring
	// that gate; observers continue to read only value under mu.
	protocolVersion                  atomic.Uint64
	nativeVersion                    uint64
	nativeValid, nativeInTransaction bool
	nativeDialect                    sql.Dialect
	nativeDatabase                   string
}

func (d *Database) registerActivity(s *sql.Schema) {
	now := time.Now()
	r := &activityRecord{value: sql.SessionActivity{ID: d.activityIDs.Add(1), SessionID: d.locks.state(s).id, Dialect: s.Dialect(), StartedAt: now, StateChange: now, State: sql.ActivityIdle}}
	r.value.Database = s.CurrentSchema()
	d.activityRecords.Store(s, r)
	s.SetActivitySnapshot(d.Activity)
}
func (d *Database) activityRecord(s *sql.Schema) *activityRecord {
	v, ok := d.activityRecords.Load(s)
	if !ok {
		return nil
	}
	return v.(*activityRecord)
}

// ReserveBackendID allocates positive IDs shared by every frontend, and never
// reuses them when replication replaces the native catalog and lock manager.
func (d *Database) ReserveBackendID() (uint32, error) {
	if d.closed.Load() {
		return 0, errors.New("database closed")
	}
	id := d.activityIDs.Add(1)
	if id > math.MaxInt32 {
		return 0, errors.New("backend ID space exhausted")
	}
	return uint32(id), nil
}
func (s *Session) SetActivityIdentity(identity sql.ActivityIdentity) {
	r := s.db.activityRecord(s.schema)
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if identity.BackendID != 0 {
		r.value.ID = uint64(identity.BackendID)
	}
	r.identified = true
	if !identity.StartedAt.IsZero() {
		r.value.StartedAt = identity.StartedAt
	}
	r.value.Dialect = identity.Dialect
	r.value.User = identity.User
	r.value.Database = identity.Database
	r.value.ApplicationName = identity.ApplicationName
	r.value.Host = identity.Host
	r.value.ClientAddress = identity.ClientAddress
	r.value.ClientPort = identity.ClientPort
}

// SetActivityTransactionState publishes protocol transaction state even after
// PostgreSQL has rolled back the underlying transaction on a statement error.
func (s *Session) SetActivityTransactionState(active, failed bool) {
	r := s.db.activityRecord(s.schema)
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	now := time.Now()
	r.protocolVersion.Add(1)
	r.inTransaction, r.failed = active, failed
	if active {
		if r.value.TransactionStart.IsZero() {
			r.value.TransactionStart = r.lastTransactionStart
			if r.value.TransactionStart.IsZero() {
				r.value.TransactionStart = now
			}
		}
	} else {
		r.value.TransactionStart = time.Time{}
		r.lastTransactionStart = time.Time{}
	}
	if r.value.State != sql.ActivityActive {
		state := sql.ActivityIdle
		if active {
			state = sql.ActivityIdleInTransaction
			if failed {
				state = sql.ActivityIdleInTransactionAborted
			}
		}
		if state != r.value.State {
			r.value.State = state
			r.value.StateChange = now
		}
	}
}
func boundedActivityQuery(q string) string {
	if len(q) <= sql.ActivityQueryLimit {
		return q
	}
	n := sql.ActivityQueryLimit
	for n > 0 && !utf8.RuneStart(q[n]) {
		n--
	}
	return strings.Clone(q[:n])
}

// beginActivity is called while the execution owner's gate is held. Snapshot
// readers only lock this small record and never touch another session's schema.
func (d *Database) beginActivity(s *sql.Schema, ctx context.Context, query string) *activityRecord {
	r := d.activityRecord(s)
	if r == nil {
		return nil
	}
	if original, ok := sql.ActivityQueryFromContext(ctx); ok {
		query = original
	}
	now := time.Now()
	r.mu.Lock()
	r.value.State = sql.ActivityActive
	r.value.StateChange = now
	r.value.QueryStart = now
	r.value.Query = boundedActivityQuery(query)
	r.mu.Unlock()
	return r
}

// end needs only the retained record, so releasing an execution view before
// this call cannot race shutdown or a replacement catalog. Native callers
// defer this method directly instead of allocating a per-query closure.
func (r *activityRecord) end() {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	state := sql.ActivityIdle
	if r.inTransaction {
		state = sql.ActivityIdleInTransaction
		if r.failed {
			state = sql.ActivityIdleInTransactionAborted
		}
	}
	r.value.State = state
	r.value.StateChange = time.Now()
}

// syncActivity runs only under the owning session gate and lifecycle read view.
func (d *Database) syncActivity(s *sql.Schema) {
	r := d.activityRecord(s)
	if r == nil {
		return
	}
	inTx := s.InTransaction()
	dialect := s.Dialect()
	database := s.CurrentSchema()
	version := r.protocolVersion.Load()
	if r.nativeValid && r.nativeInTransaction == inTx && r.nativeDialect == dialect && r.nativeDatabase == database && r.nativeVersion == version {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.nativeValid = true
	r.nativeInTransaction, r.nativeDialect, r.nativeDatabase = inTx, dialect, database
	r.nativeVersion = r.protocolVersion.Load()
	r.inTransaction = inTx
	r.failed = false
	if inTx {
		if r.value.TransactionStart.IsZero() {
			r.value.TransactionStart = time.Now()
		}
		r.lastTransactionStart = r.value.TransactionStart
	} else {
		if !r.value.TransactionStart.IsZero() {
			r.lastTransactionStart = r.value.TransactionStart
		}
		r.value.TransactionStart = time.Time{}
	}
	if !r.identified {
		r.value.Dialect = dialect
		r.value.Database = database
	}
}

// Activity returns owned records and lock-wait facts without taking session
// gates, catalog locks or performing storage reads.
func (d *Database) Activity(ctx context.Context) ([]sql.SessionActivity, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	result := make([]sql.SessionActivity, 0)
	d.activityRecords.Range(func(_, value any) bool {
		r := value.(*activityRecord)
		r.mu.Lock()
		v := r.value
		r.mu.Unlock()
		result = append(result, v)
		return true
	})
	waits := d.LockWaits()
	bySession := make(map[uint64]LockWaitInfo, len(waits))
	for _, w := range waits {
		bySession[w.SessionID] = w
	}
	for i := range result {
		if w, ok := bySession[result[i].SessionID]; ok {
			result[i].Waiting = true
			result[i].WaitKind = w.ResourceKind
			result[i].WaitResource = w.Resource
		}
	}
	sort.Slice(result, func(i, j int) bool { return result[i].ID < result[j].ID })
	return result, ctx.Err()
}

// TrackActivity describes a protocol command handled without executing native
// SQL. Call the returned function when the command finishes. Adapters must not
// nest this scope around calls which already track their native execution.
func (s *Session) TrackActivity(ctx context.Context, source string) func() {
	return s.db.beginActivity(s.schema, ctx, source).end
}
