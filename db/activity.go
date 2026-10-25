package db

import (
	"context"
	"errors"
	"math"
	"sort"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"slopdb/sql"
)

type activityRecord struct {
	mu                   sync.Mutex
	value                sql.SessionActivity
	identified           bool
	lastTransactionStart time.Time
}

func (d *Database) registerActivity(s *sql.Schema) {
	now := time.Now()
	r := &activityRecord{value: sql.SessionActivity{ID: d.activityIDs.Add(1), SessionID: d.locks.state(s).id, Dialect: s.Dialect(), StartedAt: now, StateChange: now, State: sql.ActivityIdle}}
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
func (d *Database) beginActivity(s *sql.Schema, ctx context.Context, query string) func() {
	r := d.activityRecord(s)
	if r == nil {
		return func() {}
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
	return func() {
		inTx := s.InTransaction()
		dialect := s.Dialect()
		path := s.SessionSearchPath()
		r.mu.Lock()
		defer r.mu.Unlock()
		now := time.Now()
		state := sql.ActivityIdle
		if inTx {
			state = sql.ActivityIdleInTransaction
			if r.value.TransactionStart.IsZero() {
				r.value.TransactionStart = now
			}
			r.lastTransactionStart = r.value.TransactionStart
		} else {
			if !r.value.TransactionStart.IsZero() {
				r.lastTransactionStart = r.value.TransactionStart
			}
			r.value.TransactionStart = time.Time{}
		}
		r.value.State = state
		r.value.StateChange = now
		if !r.identified {
			r.value.Dialect = dialect
			if len(path) > 0 {
				r.value.Database = path[0]
			}
		}
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
	for i := range result {
		for _, w := range waits {
			if w.SessionID == result[i].SessionID {
				result[i].Waiting = true
				result[i].WaitKind = w.ResourceKind
				result[i].WaitResource = w.Resource
				break
			}
		}
	}
	sort.Slice(result, func(i, j int) bool { return result[i].ID < result[j].ID })
	return result, ctx.Err()
}
