package db_test

import (
	"runtime"
	"testing"

	"slopdb/db"
)

func TestSessionPreparedBindingSurvivesGCAndCatalogChanges(t *testing.T) {
	d := planDatabase(t)
	s, err := d.NewSession()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	stmt, err := s.Prepare("SELECT id,v FROM t WHERE id=?")
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Close()
	var result db.ExecResult
	for _, id := range []int64{1, 2, 1} {
		got, err := s.ExecPreparedInto(&result, stmt, id)
		if err != nil || len(got.Rows) != 1 || got.Rows[0]["id"].Int != id {
			t.Fatalf("prepared answer: %+v %v", got, err)
		}
		runtime.GC()
		runtime.GC()
	}
	if stats := stmt.PlanStats(); stats.Builds != 1 || stats.Hits < 2 {
		t.Fatalf("session discarded its private plan across GC: %+v", stats)
	}
	planExec(t, d, "ALTER TABLE t ADD COLUMN extra TEXT; UPDATE t SET v=123 WHERE id=1")
	got, err := s.ExecPreparedInto(&result, stmt, 1)
	if err != nil || len(got.Rows) != 1 || got.Rows[0]["v"].Int != 123 {
		t.Fatalf("retained plan missed catalog/data changes: %+v %v", got, err)
	}
	if stats := stmt.PlanStats(); stats.Builds < 2 {
		t.Fatalf("catalog change did not invalidate the private plan: %+v", stats)
	}
}
