// Adapted from /tmp/godb/foreign_concurrency_review.go; see TESTING.md.
package regression

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"slopdb/storage"
)

func TestForeignKeyConcurrency(testT *testing.T) {
	testState := newRegressionEnv(testT)
	dir := testState.path("slopdb_fk_concurrent")
	os.RemoveAll(dir)
	o := storage.DefaultOptions()
	o.DisableWALSync = true
	o.DeadlockDetection = true
	o.LockTimeout = 50 * time.Millisecond
	d, e := testState.openDatabase(dir, o)
	if e != nil {
		testState.t.Fatalf("%v", e)
	}
	defer d.Close()
	exec := func(q string) {
		if _, e := d.Exec(q); e != nil {
			testState.t.Fatalf("%v", q+": "+e.Error())
		}
	}
	exec("CREATE TABLE p(id INTEGER PRIMARY KEY,k INTEGER UNIQUE)")
	exec("CREATE TABLE c(id INTEGER PRIMARY KEY,pid INTEGER REFERENCES p(id))")
	exec("CREATE TABLE u(id INTEGER PRIMARY KEY,k INTEGER REFERENCES p(k))")
	for i := 1; i <= 80; i++ {
		exec(fmt.Sprintf("INSERT INTO p VALUES(%d,%d)", i, i))
		start := make(chan struct{})
		var wg sync.WaitGroup
		wg.Add(2)
		errs := make([]error, 2)
		go func() {
			defer wg.Done()
			<-start
			tx, e := d.Begin()
			if e != nil {
				errs[0] = e
				return
			}
			defer tx.Rollback()
			_, e = tx.Exec(fmt.Sprintf("INSERT INTO c VALUES(%d,%d)", i, i))
			if e == nil {
				e = tx.Commit()
			}
			errs[0] = e
		}()
		go func() {
			defer wg.Done()
			<-start
			tx, e := d.Begin()
			if e != nil {
				errs[1] = e
				return
			}
			defer tx.Rollback()
			_, e = tx.Exec(fmt.Sprintf("DELETE FROM p WHERE id=%d", i))
			if e == nil {
				e = tx.Commit()
			}
			errs[1] = e
		}()
		close(start)
		wg.Wait()
		if errs[0] == nil && errs[1] == nil {
			testState.t.Fatalf("%v", "both committed")
		}
	}
	r, e := d.Exec("SELECT COUNT(*) AS n FROM c LEFT JOIN p ON c.pid=p.id WHERE p.id IS NULL")
	if e != nil || r.Rows[0]["n"].Int != 0 {
		testState.t.Fatalf("%v", fmt.Sprint("orphans ", r, e))
	}
	exec("INSERT INTO p VALUES(100,100)")
	tx, e := d.Begin()
	if e != nil {
		testState.t.Fatalf("%v", e)
	}
	if _, e = tx.Exec("INSERT INTO u VALUES(100,100)"); e != nil {
		testState.t.Fatalf("%v", e)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	other, e := d.Begin()
	if e != nil {
		testState.t.Fatalf("%v", e)
	}
	_, e = other.ExecContext(ctx, "UPDATE p SET k=101 WHERE id=100")
	cancel()
	if e == nil {
		testState.t.Fatalf("%v", "concurrent parent update accepted")
	}
	other.Rollback()
	if e = tx.Commit(); e != nil {
		testState.t.Fatalf("%v", e)
	}
	exec("CREATE TABLE queue(w INTEGER,d INTEGER,o INTEGER,PRIMARY KEY(w,d,o))")
	exec("INSERT INTO queue VALUES(1,1,1),(1,1,2),(1,1,3)")
	a, _ := d.Begin()
	b, _ := d.Begin()
	defer a.Rollback()
	defer b.Rollback()
	_, e = a.Exec("SELECT o FROM queue WHERE w=1 AND d=1 ORDER BY o LIMIT 1 FOR UPDATE")
	if e != nil {
		testState.t.Fatalf("%v", e)
	}
	r, e = b.Exec("SELECT o FROM queue WHERE w=1 AND d=1 ORDER BY o LIMIT 1 FOR UPDATE SKIP LOCKED")
	if e != nil || r.Rows[0]["o"].Int != 2 {
		testState.t.Fatalf("%v", fmt.Sprint("skip locked ", r, e))
	}
	a.Rollback()
	b.Rollback()
	testState.t.Log(fmt.Sprint("PASS 80 concurrent FK races, UNIQUE parent locking, composite queue SKIP LOCKED"))
}
