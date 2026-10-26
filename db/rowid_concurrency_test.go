package db

import (
	"fmt"
	"sync"
	"testing"

	"slopdb/storage"
)

func TestConcurrentGeneratedIDsPreserveDeletedHighWaterAfterReopen(t *testing.T) {
	testConcurrentGeneratedIDs(t, false, false)
}
func TestVolatileGeneratedIDsReuseDeletedHighWaterAfterRestart(t *testing.T) {
	testConcurrentGeneratedIDs(t, true, false)
}
func TestDurableConcurrentGeneratedIDsPreserveDeletedHighWaterAfterReopen(t *testing.T) {
	testConcurrentGeneratedIDs(t, false, true)
}
func TestConcurrentGeneratedIDsStartAboveDeletedCounterAfterRestart(t *testing.T) {
	testConcurrentGeneratedIDs(t, false, true, true)
}
func testConcurrentGeneratedIDs(t *testing.T, volatile, durable bool, deletedSeed ...bool) {
	dir := t.TempDir()
	opts := storage.DefaultOptions()
	opts.DisableWALSync = !durable
	opts.DisableAutoIncrementPersistence = volatile
	d, err := Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = d.Close() }()
	const tables = 4
	const writers = 200
	const perWriter = 16
	statements := make([]*Stmt, tables)
	for table := 0; table < tables; table++ {
		objectExec(t, d, fmt.Sprintf("CREATE TABLE t%d(id INTEGER PRIMARY KEY AUTOINCREMENT,v INTEGER NOT NULL); INSERT INTO t%d VALUES(%d,-1)", table, table, (table+1)*1000000))
		if len(deletedSeed) > 0 && deletedSeed[0] {
			objectExec(t, d, fmt.Sprintf("DELETE FROM t%d; INSERT INTO t%d VALUES(1,-1)", table, table))
		}
		statements[table], err = d.Prepare(fmt.Sprintf("INSERT INTO t%d(v) VALUES(?)", table))
		if err != nil {
			t.Fatal(err)
		}
	}
	if len(deletedSeed) > 0 && deletedSeed[0] {
		for _, stmt := range statements {
			_ = stmt.Close()
		}
		if err = d.Close(); err != nil {
			t.Fatal(err)
		}
		d, err = Open(dir, opts)
		if err != nil {
			t.Fatal(err)
		}
		for table := 0; table < tables; table++ {
			statements[table], err = d.Prepare(fmt.Sprintf("INSERT INTO t%d(v) VALUES(?)", table))
			if err != nil {
				t.Fatal(err)
			}
		}
	}
	start := make(chan struct{})
	errs := make(chan error, writers)
	var wg sync.WaitGroup
	for writer := 0; writer < writers; writer++ {
		wg.Add(1)
		go func(writer int) {
			defer wg.Done()
			s, e := d.NewSession()
			if e != nil {
				errs <- e
				return
			}
			defer s.Close()
			<-start
			var result ExecResult
			for n := 0; n < perWriter; n++ {
				_, e = s.ExecPreparedInto(&result, statements[writer%tables], int64(writer*perWriter+n))
				if e != nil {
					errs <- e
					return
				}
				if result.Changes != 1 || result.LastID <= int64((writer%tables+1)*1000000) {
					errs <- fmt.Errorf("bad generated result: %+v", result)
					return
				}
			}
		}(writer)
	}
	close(start)
	wg.Wait()
	close(errs)
	for e := range errs {
		t.Error(e)
	}
	if t.Failed() {
		return
	}
	maxima := make([]int64, tables)
	for table := 0; table < tables; table++ {
		mustCount := int64(1 + writers/tables*perWriter)
		r := objectExec(t, d, fmt.Sprintf("SELECT COUNT(*) AS n,MAX(id) AS max_id FROM t%d", table))
		if r.Rows[0]["n"].Int != mustCount {
			t.Fatal("missing commits", r)
		}
		value, ok := r.Rows[0]["max_id"]
		if !ok || value.Int < int64((table+1)*1000000) {
			t.Fatalf("bad maximum: %+v", r.Rows)
		}
		maxima[table] = value.Int
		objectExec(t, d, fmt.Sprintf("DELETE FROM t%d WHERE id=%d", table, maxima[table]))
		_ = statements[table].Close()
	}
	if err = d.Close(); err != nil {
		t.Fatal(err)
	}
	d, err = Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	for table := 0; table < tables; table++ {
		r := objectExec(t, d, fmt.Sprintf("INSERT INTO t%d(v) VALUES(999999)", table))
		want := maxima[table] + 1
		if volatile {
			want = maxima[table]
		}
		if r.LastID != want {
			t.Fatalf("table %d reused deleted high water: got %d want %d", table, r.LastID, want)
		}
	}
}

func TestQueuedGeneratedIDsPreserveOtherCountersAndResetDroppedTable(t *testing.T) {
	dir := t.TempDir()
	opts := storage.DefaultOptions()
	d, err := Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = d.Close() }()
	objectExec(t, d, "CREATE TABLE a(id INTEGER PRIMARY KEY AUTOINCREMENT,v INTEGER); CREATE TABLE b(id INTEGER PRIMARY KEY AUTOINCREMENT,v INTEGER); INSERT INTO b VALUES(5000,1); DELETE FROM b WHERE id=5000; INSERT INTO a(v) VALUES(1)")
	if err = d.Close(); err != nil {
		t.Fatal(err)
	}
	d, err = Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	if r := objectExec(t, d, "INSERT INTO b(v) VALUES(1)"); r.LastID != 5001 {
		t.Fatalf("lost inactive table counter: got %d", r.LastID)
	}
	objectExec(t, d, "DROP TABLE b; CREATE TABLE b(id INTEGER PRIMARY KEY AUTOINCREMENT,v INTEGER)")
	if r := objectExec(t, d, "INSERT INTO b(v) VALUES(1)"); r.LastID != 1 {
		t.Fatalf("dropped table inherited queued counter: got %d", r.LastID)
	}
	if err = d.Close(); err != nil {
		t.Fatal(err)
	}
	d, err = Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	if r := objectExec(t, d, "INSERT INTO b(v) VALUES(1)"); r.LastID != 2 {
		t.Fatalf("wrong counter after reopen: got %d", r.LastID)
	}
}

func TestAutoIncrementPersistenceModeControlsCounterWrites(t *testing.T) {
	for _, volatile := range []bool{false, true} {
		t.Run(fmt.Sprintf("volatile=%v", volatile), func(t *testing.T) {
			opts := storage.DefaultOptions()
			opts.DisableWALSync = true
			opts.DisableAutoIncrementPersistence = volatile
			d, err := Open(t.TempDir(), opts)
			if err != nil {
				t.Fatal(err)
			}
			defer d.Close()
			objectExec(t, d, "CREATE TABLE t(id INTEGER PRIMARY KEY AUTOINCREMENT,v INTEGER)")
			before := d.Stats().SeqNum
			r := objectExec(t, d, "INSERT INTO t(v) VALUES(1)")
			if r.LastID != 1 {
				t.Fatal(r)
			}
			// A row and its exact cardinality shard always publish together;
			// automatic-ID persistence controls only the additional ID metadata.
			want := uint64(3)
			if volatile {
				want = 2
			}
			if delta := d.Stats().SeqNum - before; delta != want {
				t.Fatalf("row/cardinality/automatic-ID mutations: got %d want %d", delta, want)
			}
			objectExec(t, d, "DELETE FROM t WHERE id=1")
			if r = objectExec(t, d, "INSERT INTO t(v) VALUES(2)"); r.LastID != 2 {
				t.Fatalf("ID reused before restart: %d", r.LastID)
			}
		})
	}
}

func TestAutoIncrementExhaustionInBothCounterModes(t *testing.T) {
	for _, volatile := range []bool{false, true} {
		t.Run(fmt.Sprintf("volatile=%v", volatile), func(t *testing.T) {
			dir := t.TempDir()
			opts := storage.DefaultOptions()
			opts.DisableWALSync = true
			opts.DisableAutoIncrementPersistence = volatile
			d, err := Open(dir, opts)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = d.Close() }()
			objectExec(t, d, "CREATE TABLE t(id INTEGER PRIMARY KEY AUTOINCREMENT,v INTEGER); INSERT INTO t(v) VALUES(1); INSERT INTO t VALUES(9223372036854775807,2)")
			if _, err = d.Exec("INSERT INTO t(v) VALUES(3)"); err == nil {
				t.Fatal("allocated an ID after maximum integer")
			}
			objectExec(t, d, "DELETE FROM t WHERE id=9223372036854775807")
			if _, err = d.Exec("INSERT INTO t(v) VALUES(3)"); err == nil {
				t.Fatal("reused exhausted IDs before restart")
			}
			if err = d.Close(); err != nil {
				t.Fatal(err)
			}
			d, err = Open(dir, opts)
			if err != nil {
				t.Fatal(err)
			}
			r, err := d.Exec("INSERT INTO t(v) VALUES(3)")
			if volatile {
				if err != nil || r.LastID != 2 {
					t.Fatalf("restart did not reset deleted maximum: result=%+v err=%v", r, err)
				}
			} else if err == nil {
				t.Fatal("lost persistent exhaustion after restart")
			}
		})
	}
}
