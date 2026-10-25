package tpcc

import (
	"bytes"
	"context"
	"errors"
	"math/rand"
	"reflect"
	"slopdb/db"
	"slopdb/sql"
	"sync"
	"testing"
	"time"
)

func reuseConfig(dir string) Config {
	return Config{Engine: "slopdb", Dir: dir, Workers: 2, Warehouses: 2, Districts: 2, Customers: 30, Items: 100, Seed: 42, Prepared: true, LockTimeout: time.Second, TxTimeout: 5 * time.Second}
}
func reuseFixture(t testing.TB, c Config) Database {
	t.Helper()
	d, e := OpenSlopDB(c)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { d.Close() })
	if e = load(d, c); e != nil {
		t.Fatal(e)
	}
	return d
}
func TestPreparedWorkloadConsistencyAndRollback(t *testing.T) {
	for _, prepared := range []bool{false, true} {
		t.Run(map[bool]string{false: "text", true: "prepared"}[prepared], func(t *testing.T) {
			c := reuseConfig(t.TempDir())
			c.Prepared = prepared
			d := reuseFixture(t, c)
			var e error
			c.statements, e = prepareWorkload(d, c)
			if e != nil {
				t.Fatal(e)
			}
			defer c.statements.Close()
			var effects effect
			var payments int64
			rng := rand.New(rand.NewSource(c.Seed))
			for n := 0; n < 50; n++ {
				kind := n % 5
				p := makePlan(rng, c, kind)
				p.invalid = kind == 0 && n%10 == 0
				tx, e := d.BeginContext(context.Background(), n%2, kind == 2 || kind == 4)
				if e != nil {
					t.Fatal(e)
				}
				eff, e := execute(context.Background(), tx, p, c, int64(n+1))
				if p.invalid {
					if !errors.Is(e, invalidItem) {
						t.Fatalf("invalid item err=%v", e)
					}
					if e = tx.Rollback(); e != nil {
						t.Fatal(e)
					}
					continue
				}
				if e != nil {
					tx.Rollback()
					t.Fatal(e)
				}
				if prepared {
					_, e = tx.ExecPrepared(c.statements.statements["COMMIT"])
				} else {
					e = tx.Commit()
				}
				if e != nil {
					t.Fatal(e)
				}
				effects.add(eff)
				if kind == 1 {
					payments++
				}
			}
			for _, reopen := range []bool{false, true} {
				if reopen {
					if e = d.Close(); e != nil {
						t.Fatal(e)
					}
					d, e = OpenSlopDB(c)
					if e != nil {
						t.Fatal(e)
					}
					defer d.Close()
				}
				checks, e := validate(d, c, effects, payments)
				if e != nil || len(checks) != 15 {
					t.Fatalf("reopen=%v checks=%v err=%v", reopen, checks, e)
				}
			}
		})
	}
}

func BenchmarkPreparedTransactions(b *testing.B) {
	for kind, name := range names {
		b.Run(name, func(b *testing.B) {
			c := reuseConfig(b.TempDir())
			d := reuseFixture(b, c)
			var e error
			c.statements, e = prepareWorkload(d, c)
			if e != nil {
				b.Fatal(e)
			}
			defer c.statements.Close()
			p := makePlan(rand.New(rand.NewSource(c.Seed+int64(kind))), c, kind)
			p.invalid = false
			var scratch executionScratch
			b.ReportAllocs()
			b.ResetTimer()
			for n := 0; n < b.N; n++ {
				tx, e := d.BeginContext(context.Background(), 0, kind == 2 || kind == 4)
				if e != nil {
					b.Fatal(e)
				}
				_, e = executeWithScratch(context.Background(), tx, p, c, int64(n+1), &scratch)
				if e != nil {
					tx.Rollback()
					b.Fatal(e)
				}
				if e = tx.Rollback(); e != nil {
					b.Fatal(e)
				}
			}
			b.StopTimer()
		})
	}
}

func TestWorkerTransactionStateAndOwnedResults(t *testing.T) {
	c := reuseConfig(t.TempDir())
	d := reuseFixture(t, c)
	var e error
	c.statements, e = prepareWorkload(d, c)
	if e != nil {
		t.Fatal(e)
	}
	defer c.statements.Close()
	if _, e = d.Exec("CREATE TABLE owned_bytes(id INTEGER PRIMARY KEY,b BLOB);INSERT INTO owned_bytes VALUES(1,X'010203'),(2,X'')"); e != nil {
		t.Fatal(e)
	}
	stmt, e := d.Prepare("SELECT b FROM owned_bytes WHERE id=?")
	if e != nil {
		t.Fatal(e)
	}
	defer stmt.Close()
	if e = d.(*slopDatabase).workers[0].session.SetDialect(sql.DialectMySQL); e != nil {
		t.Fatal(e)
	}
	tx, e := d.BeginContext(context.Background(), 0, false)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = d.BeginContext(context.Background(), 0, false); e == nil {
		t.Fatal("concurrent worker transaction accepted")
	}
	other, e := d.BeginContext(context.Background(), 1, true)
	if e != nil {
		t.Fatal(e)
	}
	defer other.Rollback()
	first, e := tx.ExecPreparedContext(context.Background(), stmt, 1)
	if e != nil {
		t.Fatal(e)
	}
	empty, e := tx.ExecPreparedContext(context.Background(), stmt, 2)
	if e != nil {
		t.Fatal(e)
	}
	if !bytes.Equal(first.Rows[0]["b"].Blob, []byte{1, 2, 3}) || empty.Rows[0]["b"].Blob == nil || len(empty.Rows[0]["b"].Blob) != 0 {
		t.Fatal("ordinary prepared result ownership/empty BLOB changed")
	}
	if _, e = tx.ExecContext(context.Background(), "SET @marker=X'0405';CREATE TEMPORARY TABLE private_temp(id INTEGER PRIMARY KEY)"); e != nil {
		t.Fatal(e)
	}
	if e = tx.Commit(); e != nil {
		t.Fatal(e)
	}
	next, e := d.BeginContext(context.Background(), 0, false)
	if e != nil {
		t.Fatal(e)
	}
	defer next.Rollback()
	for _, call := range []func() error{tx.Commit, tx.Rollback, func() error { _, e := tx.ExecContext(context.Background(), "UPDATE warehouse SET ytd=100"); return e }, func() error { _, e := tx.ExecPrepared(stmt, 1); return e }} {
		if e = call(); !errors.Is(e, db.ErrTxDone) {
			t.Fatalf("stale transaction err=%v", e)
		}
	}
	if next.(*slopTransaction).worker.session.Dialect() != sql.DialectNative {
		t.Fatal("session dialect leaked across attempts")
	}
	if _, e = next.ExecContext(context.Background(), "SELECT * FROM private_temp"); e == nil {
		t.Fatal("temporary table leaked across transaction attempts")
	}
	r, e := other.ExecPrepared(stmt, 1)
	if e != nil {
		t.Fatal(e)
	}
	first.Rows[0]["b"].Blob[0] = 255
	if !bytes.Equal(r.Rows[0]["b"].Blob, []byte{1, 2, 3}) {
		t.Fatal("independent worker result aliases prior owned BLOB")
	}
	if e = next.Rollback(); e != nil {
		t.Fatal(e)
	}
	session := next.(*slopTransaction).worker.session
	if e = session.SetDialect(sql.DialectMySQL); e != nil {
		t.Fatal(e)
	}
	r, e = session.Exec("SELECT @marker AS marker")
	if e != nil || len(r.Rows) != 1 || !r.Rows[0]["marker"].Null {
		t.Fatalf("variable leaked %#v err=%v", r, e)
	}
	if empty.Rows[0]["b"].Blob == nil || first.Rows[0]["b"].Blob[0] != 255 {
		t.Fatal("retained caller results changed after session reset/rollback")
	}
}

func TestWorkerCancellationRollbackAndConcurrentBegin(t *testing.T) {
	c := reuseConfig(t.TempDir())
	d := reuseFixture(t, c)
	var e error
	c.statements, e = prepareWorkload(d, c)
	if e != nil {
		t.Fatal(e)
	}
	defer c.statements.Close()
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, e = d.BeginContext(cancelled, 0, false); !errors.Is(e, context.Canceled) {
		t.Fatalf("cancelled BEGIN=%v", e)
	}
	const count = 8
	start := make(chan struct{})
	out := make(chan Transaction, count)
	var wg sync.WaitGroup
	for i := 0; i < count; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			tx, e := d.BeginContext(context.Background(), 0, false)
			if e == nil {
				out <- tx
			}
		}()
	}
	close(start)
	wg.Wait()
	close(out)
	var winner Transaction
	for tx := range out {
		if winner != nil {
			t.Fatal("two active transactions on one worker")
		}
		winner = tx
	}
	if winner == nil {
		t.Fatal("no successful BEGIN")
	}
	if _, e = winner.ExecPreparedContext(cancelled, c.statements.statements[itemRead], 1); !errors.Is(e, context.Canceled) {
		t.Fatalf("cancelled SELECT=%v", e)
	}
	if e = winner.Rollback(); e != nil {
		t.Fatal(e)
	}
	// A cancelled commit keeps the handle active until explicit rollback.
	commitAttempt, e := d.BeginContext(context.Background(), 0, false)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = commitAttempt.ExecPreparedContext(cancelled, c.statements.statements["COMMIT"]); !errors.Is(e, context.Canceled) {
		t.Fatalf("cancelled COMMIT=%v", e)
	}
	if _, e = d.BeginContext(context.Background(), 0, false); e == nil {
		t.Fatal("failed commit released active handle")
	}
	if e = commitAttempt.Rollback(); e != nil {
		t.Fatal(e)
	}
	// Simulate a session lost before rollback. Cleanup must retire that handle
	// and replace its closed session before another transaction is admitted.
	lost, e := d.BeginContext(context.Background(), 0, false)
	if e != nil {
		t.Fatal(e)
	}
	if e = lost.(*slopTransaction).worker.session.Close(); e != nil {
		t.Fatal(e)
	}
	if e = lost.Rollback(); e == nil {
		t.Fatal("lost-session rollback unexpectedly succeeded")
	}
	if e = lost.Rollback(); !errors.Is(e, db.ErrTxDone) {
		t.Fatalf("failed rollback handle=%v", e)
	}
	holder, e := d.BeginContext(context.Background(), 0, false)
	if e != nil {
		t.Fatal(e)
	}
	q := queries{prepared: c.statements, ctx: context.Background(), tx: holder}
	q.one(stockLock, 1, 1)
	if q.err != nil {
		t.Fatal(q.err)
	}
	waiter, e := d.BeginContext(context.Background(), 1, false)
	if e != nil {
		t.Fatal(e)
	}
	ctx, stop := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer stop()
	q = queries{prepared: c.statements, ctx: ctx, tx: waiter}
	q.one(stockLock, 1, 1)
	if !errors.Is(q.err, context.DeadlineExceeded) && !errors.Is(q.err, db.ErrLockTimeout) {
		t.Fatalf("contended cancelled lock=%v", q.err)
	}
	if e = waiter.Rollback(); e != nil {
		t.Fatal(e)
	}
	if e = holder.Rollback(); e != nil {
		t.Fatal(e)
	}
	for worker := 0; worker < 2; worker++ {
		tx, e := d.BeginContext(context.Background(), worker, false)
		if e != nil {
			t.Fatal(e)
		}
		q = queries{prepared: c.statements, ctx: context.Background(), tx: tx}
		q.one(stockLock, 1, 1)
		if q.err != nil {
			t.Fatal(q.err)
		}
		if e = tx.Rollback(); e != nil {
			t.Fatal(e)
		}
	}
	checks, e := validate(d, c, effect{}, 0)
	if e != nil || len(checks) != 15 {
		t.Fatalf("after cancellation: checks=%v err=%v", checks, e)
	}
}

func TestNewOrderPlanScratchAndIndependentInputs(t *testing.T) {
	c := reuseConfig(t.TempDir())
	rng := rand.New(rand.NewSource(43))
	var scratch [15]orderItem
	owned := makePlan(rand.New(rand.NewSource(42)), c, 0)
	saved := append([]orderItem(nil), owned.items...)
	for n := 0; n < 200; n++ {
		p := makePlanInto(rng, c, 0, scratch[:0])
		if len(p.items) < 5 || len(p.items) > 15 {
			t.Fatalf("item count=%d", len(p.items))
		}
		seen := map[int]bool{}
		for i, item := range p.items {
			if item.id < 1 || item.id > c.Items || item.quantity < 1 || item.quantity > 10 || item.warehouse < 1 || item.warehouse > c.Warehouses || seen[item.id] {
				t.Fatalf("invalid/duplicate item: %+v", item)
			}
			seen[item.id] = true
			if i > 0 {
				prior := p.items[i-1]
				if prior.warehouse > item.warehouse || (prior.warehouse == item.warehouse && prior.id > item.id) {
					t.Fatal("stock lock order changed")
				}
			}
		}
	}
	if !reflect.DeepEqual(owned.items, saved) {
		t.Fatal("separate plan's retained inputs overwritten by scratch reuse")
	}
}
