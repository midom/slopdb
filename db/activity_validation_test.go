package db

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode/utf8"

	"slopdb/sql"
)

type activityValidationResult struct {
	rows []sql.SessionActivity
	err  error
}

// A blocked status implementation must fail this test without preventing its
// deferred query cancellation/gate release from cleaning up the database.
func activityValidationRows(t *testing.T, d *Database) []sql.SessionActivity {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	done := make(chan activityValidationResult, 1)
	go func() { rows, e := d.Activity(ctx); done <- activityValidationResult{rows, e} }()
	select {
	case result := <-done:
		if result.err != nil {
			t.Fatal(result.err)
		}
		return result.rows
	case <-ctx.Done():
		t.Fatal("activity inspection blocked behind a SQL session gate")
		return nil
	}
}
func activityValidationFind(rows []sql.SessionActivity, id uint64) (sql.SessionActivity, bool) {
	for _, row := range rows {
		if row.SessionID == id {
			return row, true
		}
	}
	return sql.SessionActivity{}, false
}
func activityValidationWait(t *testing.T, d *Database, id uint64, predicate func(sql.SessionActivity) bool) sql.SessionActivity {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		rows := activityValidationRows(t, d)
		if row, ok := activityValidationFind(rows, id); ok && predicate(row) {
			return row
		}
		runtime.Gosched()
	}
	t.Fatalf("activity state was not observed for session%d: %+v", id, activityValidationRows(t, d))
	return sql.SessionActivity{}
}
func activityValidationSession(t *testing.T, d *Database, dialect sql.Dialect) *Session {
	t.Helper()
	s, e := d.NewSession()
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { _ = s.Close() })
	if e = s.SetDialect(dialect); e != nil {
		t.Fatal(e)
	}
	return s
}
func activityValidationFinish(t *testing.T, done <-chan error, wantCancelled bool) {
	t.Helper()
	select {
	case e := <-done:
		if wantCancelled && !errors.Is(e, context.Canceled) {
			t.Fatalf("expected canceled query: %v", e)
		}
		if !wantCancelled && e != nil {
			t.Fatal(e)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("query did not finish after cancellation/unlock")
	}
}

func TestActivityIndependentBusySessionIdentityAndCancellation(t *testing.T) {
	d := projectionDatabase(t)
	s := activityValidationSession(t, d, sql.DialectMySQL)
	public, e := d.ReserveBackendID()
	if e != nil {
		t.Fatal(e)
	}
	s.SetActivityIdentity(sql.ActivityIdentity{BackendID: public, Dialect: sql.DialectMySQL, User: "alice", Database: "app_db", ApplicationName: "activity-validation", Host: "127.0.0.1:40201", ClientAddress: "127.0.0.1", ClientPort: 40201})
	owner := s.SessionID()
	initial := activityValidationWait(t, d, owner, func(a sql.SessionActivity) bool { return a.State == "idle" })
	if initial.StartedAt.IsZero() || initial.StateChange.Before(initial.StartedAt) {
		t.Fatal("connection timestamps missing", initial)
	}
	if initial.ID != uint64(public) || initial.User != "alice" || initial.Database != "app_db" || initial.ApplicationName != "activity-validation" || initial.ClientAddress != "127.0.0.1" || initial.ClientPort != 40201 {
		t.Fatalf("identity %+v", initial)
	}
	query := "SELECT SLEEP(60) /* active-marker */"
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { _, e := s.ExecContext(ctx, query); done <- e }()
	active := activityValidationWait(t, d, owner, func(a sql.SessionActivity) bool { return a.State == "active" && a.Query == query })
	if active.QueryStart.Before(initial.StartedAt) || active.QueryStart.IsZero() || active.StateChange.Before(active.QueryStart) {
		t.Fatal("active timestamps missing", active)
	}
	if active.ID != uint64(public) || active.Waiting {
		t.Fatalf("busy row %+v", active)
	}
	// Pollers use separate snapshots while the writer owns its session gate.
	var wg sync.WaitGroup
	failures := make(chan error, 4)
	for n := 0; n < 4; n++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 20; i++ {
				ctx, stop := context.WithTimeout(context.Background(), time.Second)
				rows, e := d.Activity(ctx)
				stop()
				if e != nil {
					failures <- e
					return
				}
				a, ok := activityValidationFind(rows, owner)
				if !ok || a.State != "active" || a.Query != query {
					failures <- fmt.Errorf("unstable active snapshot: %+v", a)
					return
				}
			}
		}()
	}
	wg.Wait()
	close(failures)
	for e := range failures {
		t.Error(e)
	}
	cancel()
	activityValidationFinish(t, done, true)
	idle := activityValidationWait(t, d, owner, func(a sql.SessionActivity) bool { return a.State == "idle" })
	if idle.Query != query || !idle.QueryStart.Equal(active.QueryStart) || idle.StateChange.Before(active.StateChange) {
		t.Fatal("idle state lost last query timestamps", idle)
	}
	if idle.Waiting || idle.WaitKind != "" || idle.WaitResource != "" {
		t.Fatalf("canceled query retained wait metadata %+v", idle)
	}
	// The status getter must not serialize against idle session gates either.
	s.gate.Lock()
	func() { defer s.gate.Unlock(); _ = activityValidationRows(t, d) }()
	if e := s.Close(); e != nil {
		t.Fatal(e)
	}
	if _, ok := activityValidationFind(activityValidationRows(t, d), owner); ok {
		t.Fatal("closed session retained in activity")
	}
}

func TestActivityReportsActualRowLockWaitAndTransaction(t *testing.T) {
	d := projectionDatabase(t)
	objectExec(t, d, "CREATE TABLE activity_rows(id INTEGER PRIMARY KEY,v INTEGER);INSERT INTO activity_rows VALUES(1,0)")
	owner := activityValidationSession(t, d, sql.DialectNative)
	waiter := activityValidationSession(t, d, sql.DialectMySQL)
	if _, e := owner.Exec("BEGIN"); e != nil {
		t.Fatal(e)
	}
	if _, e := owner.Exec("UPDATE activity_rows SET v=1 WHERE id=1"); e != nil {
		t.Fatal(e)
	}
	ownerID, waiterID := owner.SessionID(), waiter.SessionID()
	idle := activityValidationWait(t, d, ownerID, func(a sql.SessionActivity) bool { return a.State == "idle in transaction" })
	if idle.TransactionStart.IsZero() || idle.TransactionStart.After(idle.QueryStart) {
		t.Fatal("transaction start not retained", idle)
	}
	if idle.Waiting {
		t.Fatalf("idle transaction marked waiting %+v", idle)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	query := "UPDATE activity_rows SET v=2 WHERE id=1"
	go func() { _, e := waiter.ExecContext(ctx, query); done <- e }()
	registered := waitForReportedLock(t, d)
	if registered.SessionID != waiterID {
		t.Fatal("wrong SQL lock waiter", registered)
	}
	waiting := activityValidationWait(t, d, waiterID, func(a sql.SessionActivity) bool { return a.State == "active" && a.Waiting })
	if waiting.Query != query || waiting.WaitKind != "row" || waiting.WaitResource == "" {
		t.Fatalf("missing row-lock activity %+v", waiting)
	}
	if _, e := owner.Exec("ROLLBACK"); e != nil {
		t.Fatal(e)
	}
	activityValidationFinish(t, done, false)
	final := activityValidationWait(t, d, waiterID, func(a sql.SessionActivity) bool { return a.State == "idle" && !a.Waiting })
	if final.WaitKind != "" || final.WaitResource != "" {
		t.Fatalf("released wait remains visible %+v", final)
	}
}

func TestActivityPreparedQueryTextAndUTF8Bound(t *testing.T) {
	d := projectionDatabase(t)
	s := activityValidationSession(t, d, sql.DialectMySQL)
	id := s.SessionID()
	query := "SELECT SLEEP(?), ? /* prepared-original */"
	p, e := s.Prepare(query)
	if e != nil {
		t.Fatal(e)
	}
	defer p.Close()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { _, e := p.ExecContext(ctx, 60, "bound-value-must-not-appear"); done <- e }()
	active := activityValidationWait(t, d, id, func(a sql.SessionActivity) bool {
		return a.State == "active" && strings.Contains(a.Query, "prepared-original")
	})
	if active.Query != query || strings.Contains(active.Query, "bound-value-must-not-appear") {
		t.Fatalf("prepared binding expanded in activity: %q", active.Query)
	}
	cancel()
	activityValidationFinish(t, done, true)
	longQuery := "SELECT SLEEP(60) /*" + strings.Repeat("世", 23000) + "*/"
	ctx, cancel = context.WithCancel(context.Background())
	defer cancel()
	done = make(chan error, 1)
	go func() { _, e := s.ExecContext(ctx, longQuery); done <- e }()
	active = activityValidationWait(t, d, id, func(a sql.SessionActivity) bool {
		return a.State == "active" && strings.HasPrefix(a.Query, "SELECT SLEEP(60) /*")
	})
	if !utf8.ValidString(active.Query) || len(active.Query) > 64<<10 || len(active.Query) < (64<<10)-4 {
		t.Fatalf("query truncation split Unicode or was not bounded: bytes=%d valid=%v", len(active.Query), utf8.ValidString(active.Query))
	}
	cancel()
	activityValidationFinish(t, done, true)
}

func TestActivityStreamingLifetimeAndCancelClose(t *testing.T) {
	d := projectionDatabase(t)
	objectExec(t, d, "CREATE TABLE activity_stream(id INTEGER PRIMARY KEY,v TEXT);INSERT INTO activity_stream VALUES(1,'first'),(2,'second')")
	s := activityValidationSession(t, d, sql.DialectPostgreSQL)
	id := s.SessionID()
	if _, e := s.Exec("BEGIN"); e != nil {
		t.Fatal(e)
	}
	before := activityValidationWait(t, d, id, func(a sql.SessionActivity) bool { return a.State == sql.ActivityIdleInTransaction })
	if before.TransactionStart.IsZero() {
		t.Fatal("BEGIN omitted transaction start")
	}
	query := "SELECT id,v FROM activity_stream ORDER BY id"
	rows, e := s.Query(query)
	if e != nil {
		t.Fatal(e)
	}
	defer rows.Close()
	if rows.Materialized() {
		t.Fatal("fixture must retain an actual streaming snapshot")
	}
	active := activityValidationWait(t, d, id, func(a sql.SessionActivity) bool { return a.State == sql.ActivityActive && a.Query == query })
	if !active.TransactionStart.Equal(before.TransactionStart) {
		t.Fatal("stream changed transaction start", before, active)
	}
	if !rows.Next() {
		t.Fatal("empty stream", rows.Err())
	}
	if a, ok := activityValidationFind(activityValidationRows(t, d), id); !ok || a.State != sql.ActivityActive {
		t.Fatal("partially consumed cursor became idle", a)
	}
	if e = rows.Close(); e != nil {
		t.Fatal(e)
	}
	after := activityValidationWait(t, d, id, func(a sql.SessionActivity) bool { return a.State == sql.ActivityIdleInTransaction })
	if !after.TransactionStart.Equal(before.TransactionStart) {
		t.Fatal("closing stream changed transaction start")
	}
	if _, e = s.Exec("COMMIT"); e != nil {
		t.Fatal(e)
	}
	idle := activityValidationWait(t, d, id, func(a sql.SessionActivity) bool { return a.State == sql.ActivityIdle })
	if !idle.TransactionStart.IsZero() {
		t.Fatal("committed transaction retained start", idle)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rows, e = s.QueryContext(ctx, query)
	if e != nil {
		t.Fatal(e)
	}
	activityValidationWait(t, d, id, func(a sql.SessionActivity) bool { return a.State == sql.ActivityActive })
	cancel()
	activityValidationWait(t, d, id, func(a sql.SessionActivity) bool { return a.State == sql.ActivityIdle })
	if e = rows.Err(); !errors.Is(e, context.Canceled) {
		t.Fatal("stream cancellation missing", e)
	}
	if _, e = s.Exec("SELECT 1"); e != nil {
		t.Fatal("canceled stream poisoned session", e)
	}
	rows, e = s.Query(query)
	if e != nil {
		t.Fatal(e)
	}
	activityValidationWait(t, d, id, func(a sql.SessionActivity) bool { return a.State == sql.ActivityActive })
	closed := make(chan error, 1)
	go func() { closed <- s.Close() }()
	activityValidationFinish(t, closed, false)
	if _, ok := activityValidationFind(activityValidationRows(t, d), id); ok {
		t.Fatal("disconnect retained streaming backend")
	}
	_ = rows.Close()
}

func TestActivityPublicIDsAndOwnedSnapshots(t *testing.T) {
	d := projectionDatabase(t)
	first := activityValidationSession(t, d, sql.DialectNative)
	second := activityValidationSession(t, d, sql.DialectPostgreSQL)
	rows := activityValidationRows(t, d)
	byID := map[uint64]bool{}
	bySession := map[uint64]bool{}
	for _, row := range rows {
		if row.ID == 0 || row.SessionID == 0 || byID[row.ID] || bySession[row.SessionID] {
			t.Fatal("non-unique live identity", rows)
		}
		byID[row.ID] = true
		bySession[row.SessionID] = true
	}
	if !bySession[d.SessionID()] || !bySession[first.SessionID()] || !bySession[second.SessionID()] {
		t.Fatal("default or independent SQL session absent", rows)
	}
	row, ok := activityValidationFind(rows, first.SessionID())
	if !ok {
		t.Fatal("missing first session")
	}
	for i := range rows {
		rows[i].Query = "mutated snapshot"
		rows[i].User = "intruder"
		rows[i].ID = 0
	}
	current, ok := activityValidationFind(activityValidationRows(t, d), first.SessionID())
	if !ok || current.ID != row.ID || current.Query == "mutated snapshot" || current.User == "intruder" {
		t.Fatal("activity rows alias registry", current)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, e := d.Activity(ctx); !errors.Is(e, context.Canceled) {
		t.Fatal("canceled inspection admitted", e)
	}
	if _, e := d.Activity(nil); e != nil {
		t.Fatal("nil inspection context rejected", e)
	}
}
