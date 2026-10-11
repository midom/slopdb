// Adapted from /tmp/godb/join_state_review.go; see TESTING.md.
package regression

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"

	"slopdb/db"
	"slopdb/storage"
)

func TestJoinState(testT *testing.T) {
	testState := newRegressionEnv(testT)
	dir := testState.path("slopdb_join_state_review")
	os.RemoveAll(dir)
	o := storage.DefaultOptions()
	o.DisableWALSync = true
	d, e := testState.openDatabase(dir, o)
	if e != nil {
		testState.t.Fatalf("%v", e)
	}
	defer d.Close()
	n := 0
	exec := func(q string) *db.ExecResult {
		r, e := d.Exec(q)
		if e != nil {
			testState.t.Fatalf("%v", q+": "+e.Error())
		}
		return r
	}
	check := func(b bool, msg string) {
		if !b {
			testState.t.Fatalf("%v", msg)
		}
		n++
	}
	exec("CREATE TABLE a(id INTEGER PRIMARY KEY,k INTEGER,v INTEGER);CREATE TABLE b(id INTEGER PRIMARY KEY,k INTEGER,v INTEGER);INSERT INTO a VALUES(1,1,10),(2,2,20);INSERT INTO b VALUES(1,1,3),(2,2,4);CREATE INDEX bk ON b(k);ANALYZE")
	p, e := d.Prepare("SELECT SUM(a.v*b.v) AS s FROM a JOIN b ON a.k=b.k WHERE a.id>=?")
	if e != nil {
		testState.t.Fatalf("%v", e)
	}
	defer p.Close()
	sum := func(w int64) {
		r, e := p.Exec(1)
		if e != nil {
			testState.t.Fatalf("%v", e)
		}
		check(r.Rows[0]["s"].Int == w, "stale joined value")
	}
	sum(110)
	exec("BEGIN")
	exec("UPDATE b SET v=10 WHERE id=1")
	sum(180)
	exec("SAVEPOINT s")
	exec("UPDATE b SET k=1 WHERE id=2")
	sum(140)
	exec("ROLLBACK TO s")
	sum(180)
	exec("ROLLBACK")
	sum(110)
	exec("UPDATE b SET v=5 WHERE id=1")
	sum(130)
	exec("CREATE TABLE probe(fk INTEGER);INSERT INTO probe VALUES(1)")
	exec("UPDATE a SET v=v+1 WHERE EXISTS(SELECT 1 FROM probe WHERE fk=k)")
	sum(135)
	exec("DELETE FROM b WHERE id=2")
	sum(55)
	exec("INSERT INTO b VALUES(3,2,7)")
	sum(195)
	exec("CREATE TABLE vars(pk TEXT PRIMARY KEY,t TEXT UNIQUE,n INTEGER);INSERT INTO vars VALUES('x:a','a',1),('x:b','a:',2),('x:c','a::',3),('x:d','\x01vars:x:a',4),('x:e','',5)")
	for _, tc := range []struct {
		q    string
		want int64
	}{{"SELECT SUM(n) AS s FROM vars WHERE t='a'", 1}, {"SELECT SUM(n) AS s FROM vars WHERE t='a:'", 2}, {"SELECT SUM(n) AS s FROM vars WHERE t='\x01vars:x:a'", 4}, {"SELECT SUM(n) AS s FROM vars WHERE t>='a' AND t<='a::'", 6}} {
		r := exec(tc.q)
		check(r.Rows[0]["s"].Int == tc.want, "variable key boundary")
	}
	r := exec("EXPLAIN SELECT a.id,b.id FROM a JOIN b ON a.k=b.k WHERE a.id=1")
	seen := false
	for _, row := range r.Rows {
		if row["opcode"].Text == "JoinPlan" {
			seen = true
		}
	}
	check(seen, "missing adaptive join explanation")
	for _, q := range []string{"SELECT EXISTS(SELECT unknown_func(1) FROM b)", "SELECT EXISTS(SELECT LENGTH() FROM b)", "SELECT EXISTS(SELECT 1 FROM b WHERE COUNT(*)>0)"} {
		_, e := d.Exec(q)
		check(e != nil, "EXISTS suppressed validation error")
	}
	check(exec("SELECT EXISTS(SELECT 1/0 FROM b) AS yes").Rows[0]["yes"].Int == 1, "EXISTS evaluated unused projection")
	var wg sync.WaitGroup
	errch := make(chan error, 10)
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 30; j++ {
				r, e := p.Exec(1)
				if e != nil {
					errch <- e
					return
				}
				if r.Rows[0]["s"].Int != 195 {
					errch <- fmt.Errorf("concurrent prepared join result")
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errch)
	for e := range errch {
		testState.t.Fatalf("%v", e)
	}
	n += 300
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, e = p.ExecContext(ctx, 1)
	check(e != nil, "canceled query executed")
	testState.t.Logf("PASS %d joined prepared execution, transaction/savepoint/index mutation, variable key and concurrent checks\n", n)
}
