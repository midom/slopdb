// Adapted from /tmp/godb/performance_state_review.go; see TESTING.md.
package regression

import (
	"os"
	"testing"

	"slopdb/db"
	"slopdb/sql"
	"slopdb/storage"
)

func TestExecutionState(testT *testing.T) {
	testState := newRegressionEnv(testT)
	dir := testState.path("slopdb_perf_state")
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
	check := func(ok bool, msg string) {
		if !ok {
			testState.t.Fatalf("%v", msg)
		}
		n++
	}
	exec("CREATE TABLE t(id INTEGER PRIMARY KEY,k INTEGER,v REAL);INSERT INTO t VALUES(1,1,1.0),(2,1,2.0),(3,2,3.0);CREATE INDEX tk ON t(k);CREATE VIEW sums AS SELECT k,SUM(v) AS total FROM t GROUP BY k")
	p, e := d.Prepare("SELECT id,(SELECT SUM(v) FROM t q WHERE q.k=t.k) AS total FROM t ORDER BY id")
	if e != nil {
		testState.t.Fatalf("%v", e)
	}
	v, e := d.Prepare("SELECT id,(SELECT MAX(total) FROM sums) AS m FROM t ORDER BY id")
	if e != nil {
		testState.t.Fatalf("%v", e)
	}
	run := func(p *db.Stmt, col string, want float64) {
		r, e := p.Exec()
		if e != nil {
			testState.t.Fatalf("%v", e)
		}
		check(r.Rows[0][col].Real == want, "stale subquery: "+col)
	}
	run(p, "total", 3)
	run(v, "m", 3)
	exec("UPDATE t SET v=10 WHERE id=1")
	run(p, "total", 12)
	run(v, "m", 12)
	exec("BEGIN")
	exec("UPDATE t SET v=20 WHERE id=1")
	run(p, "total", 22)
	run(v, "m", 22)
	exec("ROLLBACK")
	run(p, "total", 12)
	run(v, "m", 12)
	r := exec("SELECT id,(SELECT RANDOM()) AS r FROM t ORDER BY id")
	check(r.Rows[0]["r"].Int != r.Rows[1]["r"].Int, "volatile subquery cached")
	r = exec("SELECT GROUP_CONCAT(id,CAST(k AS TEXT)) AS c FROM t")
	check(r.Rows[0]["c"].Text == "11213", "changed first-row separator semantics")
	_, e = d.Exec("SELECT (SELECT id FROM t) FROM t")
	check(e != nil, "scalar cardinality error suppressed")
	exec("CREATE TABLE big(id INTEGER PRIMARY KEY,v INTEGER);INSERT INTO big VALUES(1,9223372036854775807),(2,1),(3,-1)")
	_, e = d.Exec("SELECT SUM(v) FROM big")
	check(e != nil, "integer intermediate overflow suppressed")
	r = exec("SELECT SUM(CASE WHEN id=3 THEN 0.5 ELSE v END) AS s FROM big")
	check(!r.Rows[0]["s"].Null && r.Rows[0]["s"].Type == sql.ColReal, "mixed SUM spuriously overflowed")
	exec("BEGIN")
	r = exec("SELECT k,SUM(v) AS s FROM t GROUP BY k ORDER BY k")
	check(len(r.Rows) == 2, "transaction streaming scan")
	exec("COMMIT")
	exec("CREATE TABLE realkeys(id INTEGER PRIMARY KEY,v REAL);INSERT INTO realkeys VALUES(1,0.0),(2,1.0),(3,2.0),(4,3.0),(5,9007199254740992.0)")
	exec("CREATE TABLE intkeys(id INTEGER PRIMARY KEY,v INTEGER);INSERT INTO intkeys VALUES(1,0),(2,1),(3,2),(4,3),(5,9007199254740993)")
	r = exec("SELECT COUNT(*) AS n FROM realkeys a JOIN intkeys b ON a.v=b.v")
	check(r.Rows[0]["n"].Int == 4, "hash join rounded large integer")
	r = exec("SELECT id,v IN(SELECT v FROM realkeys) AS yes FROM intkeys ORDER BY id")
	check(r.Rows[4]["yes"].Int == 0, "hashed IN rounded large integer")
	testState.t.Logf("PASS %d execution state, volatility, overflow and exact numeric checks\n", n)
}
