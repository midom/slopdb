// Adapted from /tmp/godb/standard_indexed_mvcc_review.go; see TESTING.md.
package regression

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"slopdb/storage"
)

func indexedMVCCMust(testState *regressionEnv, e error) {
	testState.t.Helper()
	if e != nil {
		testState.t.Fatalf("%v", e)
	}
}
func indexedMVCCAssert(testState *regressionEnv, b bool, s string) {
	testState.t.Helper()
	if !b {
		testState.t.Fatalf("%v", s)
	}
}
func TestIndexedMVCC(testT *testing.T) {
	testState := newRegressionEnv(testT)

	dir := testState.path("godb_indexed_mvcc_review")
	indexedMVCCMust(testState, os.RemoveAll(dir))
	o := storage.DefaultOptions()
	o.DisableWALSync = true
	o.WriteBufferSize = 32768
	o.BlockSize = 512
	o.CompactionL0Trigger = 3
	o.MaxOpenFiles = 4
	d, e := testState.openDatabase(dir, o)
	indexedMVCCMust(testState, e)
	_, e = d.Exec("CREATE TABLE t(id INTEGER PRIMARY KEY,v INTEGER NOT NULL,w INTEGER NOT NULL, UNIQUE(w,id), INDEX iv(v),INDEX iw(w)); CREATE TABLE u(id INTEGER PRIMARY KEY,x INTEGER UNIQUE); CREATE TABLE ids(id INTEGER PRIMARY KEY AUTOINCREMENT,v INTEGER); CREATE TABLE log(id INTEGER PRIMARY KEY,n INTEGER); INSERT INTO log VALUES(1,0)")
	indexedMVCCMust(testState, e)
	for i := 1; i <= 64; i++ {
		_, e = d.Exec(fmt.Sprintf("INSERT INTO t VALUES(%d,0,0)", i))
		indexedMVCCMust(testState, e)
	}
	var wg sync.WaitGroup
	errs := make(chan error, 16)
	stop := make(chan struct{})
	reader, e := d.NewSession()
	indexedMVCCMust(testState, e)
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer reader.Close()
		for {
			select {
			case <-stop:
				return
			default:
			}
			r, e := reader.Exec("SELECT COUNT(*) AS n FROM t WHERE v!=w")
			if e != nil {
				errs <- e
				return
			}
			indexed, e := reader.Exec("SELECT v,w FROM t WHERE v=1")
			if e != nil {
				errs <- e
				return
			}
			for _, row := range indexed.Rows {
				if row["v"].Int != 1 || row["w"].Int != 1 {
					errs <- fmt.Errorf("inconsistent secondary index snapshot")
					return
				}
			}
			if r.Rows[0]["n"].Int != 0 {
				errs <- fmt.Errorf("torn row/index statement view")
				return
			}
		}
	}()
	var writers sync.WaitGroup
	for w := 0; w < 8; w++ {
		writers.Add(1)
		go func(w int) {
			defer writers.Done()
			s, e := d.NewSession()
			if e != nil {
				errs <- e
				return
			}
			defer s.Close()
			for j := 0; j < 500; j++ {
				if j%3 == 0 {
					_, e = s.Exec("BEGIN")
					if e != nil {
						errs <- e
						return
					}
				}
				_, e = s.Exec(fmt.Sprintf("UPDATE t SET v=v+1,w=w+1 WHERE id=%d", (j*7+w)%64+1))
				if e != nil {
					errs <- e
					return
				}
				if j%3 == 0 {
					_, e = s.Exec("COMMIT")
					if e != nil {
						errs <- e
						return
					}
				}
				_, e = s.Exec(fmt.Sprintf("INSERT INTO ids(v) VALUES(%d)", w))
				if e != nil {
					errs <- e
					return
				}
			}
		}(w)
	}
	writers.Wait()
	close(stop)
	wg.Wait()
	close(errs)
	for e := range errs {
		indexedMVCCMust(testState, e)
	}
	r, e := d.Exec("SELECT SUM(v) AS n FROM t")
	indexedMVCCMust(testState, e)
	indexedMVCCAssert(testState, r.Rows[0]["n"].Int == 4000, "lost indexed updates")
	r, e = d.Exec("SELECT COUNT(*) AS n FROM ids")
	indexedMVCCMust(testState, e)
	indexedMVCCAssert(testState, r.Rows[0]["n"].Int == 4000, "lost reserved row IDs")
	// Unique-value contention: exactly one transaction may insert this predicate.
	var wins int
	var mu sync.Mutex
	wg = sync.WaitGroup{}
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			s, e := d.NewSession()
			indexedMVCCMust(testState, e)
			defer s.Close()
			_, e = s.Exec(fmt.Sprintf("INSERT INTO u VALUES(%d,99)", w))
			if e == nil {
				mu.Lock()
				wins++
				mu.Unlock()
			}
		}(w)
	}
	wg.Wait()
	indexedMVCCAssert(testState, wins == 1, "duplicate concurrent unique value")
	// READ COMMITTED sees a later commit, while preserving own transaction writes.
	a, e := d.NewSession()
	indexedMVCCMust(testState, e)
	b, e := d.NewSession()
	indexedMVCCMust(testState, e)
	_, e = a.Exec("BEGIN; SELECT v FROM t WHERE id=1")
	indexedMVCCMust(testState, e)
	_, e = b.Exec("UPDATE t SET v=v+1,w=w+1 WHERE id=1")
	indexedMVCCMust(testState, e)
	r, e = a.Exec("SELECT v,w FROM t WHERE id=1")
	indexedMVCCMust(testState, e)
	indexedMVCCAssert(testState, r.Rows[0]["v"].Int == r.Rows[0]["w"].Int, "READ COMMITTED stale index/row")
	_, e = a.Exec("ROLLBACK")
	indexedMVCCMust(testState, e)
	// Session.Close interrupts an active waiter; database Close interrupts waiters too.
	_, e = a.Exec("BEGIN; UPDATE t SET v=v+1,w=w+1 WHERE id=1")
	indexedMVCCMust(testState, e)
	done := make(chan error, 1)
	go func() { _, e := b.Exec("UPDATE t SET v=v+1,w=w+1 WHERE id=1"); done <- e }()
	time.Sleep(20 * time.Millisecond)
	indexedMVCCMust(testState, b.Close())
	indexedMVCCAssert(testState, errors.Is(<-done, context.Canceled), "Close failed to cancel waiter")
	indexedMVCCMust(testState, a.Close())
	indexedMVCCMust(testState, d.Close())
	d, e = testState.openDatabase(dir, o)
	indexedMVCCMust(testState, e)
	r, e = d.Exec("SELECT id,v,w FROM t")
	indexedMVCCMust(testState, e)
	for _, row := range r.Rows {
		v := row["v"].Int
		r, e := d.Exec(fmt.Sprintf("SELECT id FROM t WHERE v=%d", v))
		indexedMVCCMust(testState, e)
		found := false
		for _, x := range r.Rows {
			if x["id"].Int == row["id"].Int {
				found = true
			}
		}
		indexedMVCCAssert(testState, found, "missing secondary index entry")
	}
	_, e = d.Exec("INSERT INTO u VALUES(100,99)")
	indexedMVCCAssert(testState, e != nil, "unique persistence")
	indexedMVCCMust(testState, d.Close())
	testState.t.Log(fmt.Sprint("PASS 4,000 concurrent indexed updates and explicit transactions, snapshot consistency, 4,000 reserved IDs, unique contention, READ COMMITTED, waiter cancellation, index persistence"))
}
