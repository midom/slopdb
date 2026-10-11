package db_test

import (
	"fmt"
	"sync"
	"testing"

	"slopdb/db"
)

func planDatabase(t *testing.T) *db.Database {
	t.Helper()
	d, err := db.Open(t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = d.Close() })
	planExec(t, d, "CREATE TABLE t(id INTEGER PRIMARY KEY,k INTEGER,v INTEGER);CREATE INDEX tk ON t(k);INSERT INTO t VALUES(1,10,100),(2,20,200),(3,30,300)")
	return d
}
func planExec(t *testing.T, d *db.Database, q string) *db.ExecResult {
	t.Helper()
	r, err := d.Exec(q)
	if err != nil {
		t.Fatalf("%s: %v", q, err)
	}
	return r
}
func planPrepare(t *testing.T, d *db.Database, q string) *db.Stmt {
	t.Helper()
	s, err := d.Prepare(q)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func TestPreparedPlanCatalogAndRollback(t *testing.T) {
	d := planDatabase(t)
	read := planPrepare(t, d, "SELECT * FROM t WHERE id=?")
	update := planPrepare(t, d, "UPDATE t SET k=k+?,v=v+? WHERE id=?")
	for _, id := range []int{1, 2, 3, 1} {
		r, err := read.Exec(id)
		if err != nil || len(r.Rows) != 1 || r.Rows[0]["id"].Int != int64(id) {
			t.Fatalf("point binding: %v %v", r, err)
		}
	}
	if _, err := update.Exec(1, 5, 1); err != nil {
		t.Fatal(err)
	}
	planExec(t, d, "DROP INDEX tk;CREATE INDEX tk2 ON t(k);ANALYZE")
	if _, err := update.Exec(1, 5, 2); err != nil {
		t.Fatal(err)
	}
	if r := planExec(t, d, "SELECT id FROM t WHERE k=21"); len(r.Rows) != 1 || r.Rows[0]["id"].Int != 2 {
		t.Fatal("stale index maintenance")
	}
	planExec(t, d, "ALTER TABLE t ADD COLUMN extra TEXT DEFAULT 'ok'")
	if r, err := read.Exec(1); err != nil || len(r.Columns) != 4 || r.Rows[0]["extra"].Text != "ok" {
		t.Fatalf("ALTER plan invalidation: %v %v", r, err)
	}
	tx, err := d.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if _, err = tx.Exec("ALTER TABLE t RENAME COLUMN v TO value"); err != nil {
		t.Fatal(err)
	}
	if _, err = tx.ExecPrepared(update, 1, 1, 1); err == nil {
		t.Fatal("local schema rename must invalidate target bindings")
	}
	if err = tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	if _, err = update.Exec(1, 1, 1); err != nil {
		t.Fatal("rollback retained renamed target bindings:", err)
	}
	// A cached point update must fall back when a newly installed trigger requires
	// the full executor, then regain eligibility when that trigger is removed.
	planExec(t, d, "CREATE TABLE audit(id INTEGER);CREATE TRIGGER audit_t AFTER UPDATE ON t BEGIN INSERT INTO audit VALUES(NEW.id); END")
	if _, err = update.Exec(1, 1, 1); err != nil {
		t.Fatal(err)
	}
	if r := planExec(t, d, "SELECT id FROM audit"); len(r.Rows) != 1 || r.Rows[0]["id"].Int != 1 {
		t.Fatal("cached plan bypassed new trigger")
	}
	planExec(t, d, "DROP TRIGGER audit_t")
	if _, err = update.Exec(1, 1, 1); err != nil {
		t.Fatal(err)
	}
	if read.PlanStats().Builds == 0 || update.PlanStats().Builds == 0 {
		t.Fatal("prepared executions did not compile plans")
	}
}

func TestPreparedPlanSharedParametersAndSnapshots(t *testing.T) {
	d := planDatabase(t)
	point := planPrepare(t, d, "SELECT v FROM t WHERE id=?")
	rangeStmt := planPrepare(t, d, "SELECT SUM(v+?) AS total FROM t WHERE id BETWEEN ? AND ?")
	update := planPrepare(t, d, "UPDATE t SET v=v+? WHERE id=?")
	var wg sync.WaitGroup
	failures := make(chan error, 8)
	for worker := 0; worker < 8; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			s, err := d.NewSession()
			if err != nil {
				failures <- err
				return
			}
			defer s.Close()
			for n := 0; n < 30; n++ {
				id := int64(n%3 + 1)
				r, err := s.ExecPrepared(point, id)
				if err != nil || len(r.Rows) != 1 || r.Rows[0]["v"].Int != id*100 {
					failures <- fmt.Errorf("point %d: %v %v", id, r, err)
					return
				}
				low := int64(worker%3 + 1)
				r, err = s.ExecPrepared(rangeStmt, int64(worker), low, int64(3))
				want := int64(0)
				for id := low; id <= 3; id++ {
					want += id*100 + int64(worker)
				}
				if err != nil || r.Rows[0]["total"].Int != want {
					failures <- fmt.Errorf("aggregate %d: %v %v", worker, r, err)
					return
				}
			}
		}(worker)
	}
	wg.Wait()
	close(failures)
	for err := range failures {
		t.Error(err)
	}
	tx, err := d.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if _, err = tx.ExecPrepared(update, 7, 1); err != nil {
		t.Fatal(err)
	}
	if r, err := tx.ExecPrepared(point, 1); err != nil || r.Rows[0]["v"].Int != 107 {
		t.Fatalf("private writes: %v %v", r, err)
	}
	independent, err := d.NewSession()
	if err != nil {
		t.Fatal(err)
	}
	defer independent.Close()
	if r, err := independent.ExecPrepared(point, 1); err != nil || r.Rows[0]["v"].Int != 100 {
		t.Fatalf("uncommitted data leaked: %v %v", r, err)
	}
	if err = tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	if r, err := point.Exec(1); err != nil || r.Rows[0]["v"].Int != 100 {
		t.Fatalf("stale rollback result: %v %v", r, err)
	}
	// NULL and wrong-affinity arguments must preserve SQL fallback semantics.
	if r, err := point.Exec(nil); err != nil || len(r.Rows) != 0 {
		t.Fatalf("NULL parameter: %v %v", r, err)
	}
	if r, err := point.Exec("2"); err != nil || len(r.Rows) != 1 || r.Rows[0]["v"].Int != 200 {
		t.Fatalf("affinity parameter: %v %v", r, err)
	}
	if r, err := point.Exec(3); err != nil || len(r.Rows) != 1 || r.Rows[0]["v"].Int != 300 {
		t.Fatalf("binding after fallback: %v %v", r, err)
	}
}

func TestPreparedUpdateNewConstraintAndCatalogReplacement(t *testing.T) {
	d := planDatabase(t)
	update := planPrepare(t, d, "UPDATE t SET k=? WHERE id=?")
	if _, err := update.Exec(11, 1); err != nil {
		t.Fatal(err)
	}
	planExec(t, d, "CREATE UNIQUE INDEX unique_k ON t(k)")
	if _, err := update.Exec(20, 1); err == nil {
		t.Fatal("cached update bypassed new UNIQUE index")
	}
	if r := planExec(t, d, "SELECT k FROM t WHERE id=1"); r.Rows[0]["k"].Int != 11 {
		t.Fatal("failed update changed row")
	}
	planExec(t, d, "DROP INDEX unique_k")
	if _, err := update.Exec(20, 1); err != nil {
		t.Fatal(err)
	}
	read := planPrepare(t, d, "SELECT v FROM t WHERE id=?")
	if _, err := read.Exec(1); err != nil {
		t.Fatal(err)
	}
	planExec(t, d, "DROP TABLE t;CREATE TABLE t(id INTEGER PRIMARY KEY,v TEXT);INSERT INTO t VALUES(1,'replacement')")
	if r, err := read.Exec(1); err != nil || r.Rows[0]["v"].Text != "replacement" {
		t.Fatalf("recreated table stale binding: %v %v", r, err)
	}
	if _, err := update.Exec(1, 1); err == nil {
		t.Fatal("cached update used removed column on replacement table")
	}
}
