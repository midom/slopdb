package db

import (
	"fmt"
	"sync"
	"testing"

	"slopdb/sql"
	"slopdb/storage"
)

type rowCountExecutor interface {
	Exec(string) (*ExecResult, error)
}

func rowCountSQL(t *testing.T, e rowCountExecutor, q string) *ExecResult {
	t.Helper()
	r, err := e.Exec(q)
	if err != nil {
		t.Fatalf("%s: %v", q, err)
	}
	return r
}
func assertRowCount(t *testing.T, e rowCountExecutor, want int64) {
	t.Helper()
	r := rowCountSQL(t, e, "SELECT COUNT(*) AS n,COUNT(1) AS c FROM counted")
	if len(r.Rows) != 1 || r.Rows[0]["n"].Int != want || r.Rows[0]["c"].Int != want {
		t.Fatalf("count want%d: %+v", want, r.Rows)
	}
	scan := rowCountSQL(t, e, "SELECT COUNT(*) AS n FROM counted WHERE id>=0")
	if scan.Rows[0]["n"].Int != want {
		t.Fatalf("scan count differs: %+v", scan.Rows)
	}
}
func TestExactRowCountTransactionsAndReopen(t *testing.T) {
	path := t.TempDir()
	opt := storage.DefaultOptions()
	opt.DisableWALSync = true
	d, err := Open(path, opt)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { d.Close() })
	rowCountSQL(t, d, "CREATE TABLE counted(id INTEGER PRIMARY KEY,n INTEGER,UNIQUE(n))")
	assertRowCount(t, d, 0)
	rowCountSQL(t, d, "INSERT INTO counted VALUES(1,10),(2,20),(3,30)")
	tx, err := d.Begin()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { tx.Rollback() })
	rowCountSQL(t, tx, "UPDATE counted SET n=11 WHERE id=1;UPDATE counted SET n=12 WHERE id=1;SAVEPOINT saved;DELETE FROM counted WHERE id=2;INSERT INTO counted VALUES(4,40)")
	assertRowCount(t, tx, 3)
	assertRowCount(t, d, 3)
	rowCountSQL(t, tx, "ROLLBACK TO saved;DELETE FROM counted WHERE id=3;INSERT INTO counted VALUES(5,50);DELETE FROM counted WHERE id=5")
	assertRowCount(t, tx, 2)
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	assertRowCount(t, d, 2)
	rowCountSQL(t, d, "UPDATE counted SET id=6 WHERE id=2;REPLACE INTO counted VALUES(6,60)")
	assertRowCount(t, d, 2)
	if _, err = d.Exec("INSERT INTO counted VALUES(7,70),(8,12)"); err == nil {
		t.Fatal("expected UNIQUE failure")
	}
	assertRowCount(t, d, 2)
	if err = d.Close(); err != nil {
		t.Fatal(err)
	}
	d, err = Open(path, opt)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { d.Close() })
	assertRowCount(t, d, 2)
	rowCountSQL(t, d, "DROP TABLE counted;CREATE TABLE counted(id INTEGER PRIMARY KEY,n INTEGER)")
	assertRowCount(t, d, 0)
	rowCountSQL(t, d, "INSERT INTO counted VALUES(1,1)")
	assertRowCount(t, d, 1)
}

func TestExactRowCountRepeatableReadAndPrivateOverlay(t *testing.T) {
	d, err := Open(t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { d.Close() })
	rowCountSQL(t, d, "CREATE TABLE counted(id INTEGER PRIMARY KEY,n INTEGER);INSERT INTO counted VALUES(1,1),(2,2)")
	session, err := d.NewSession()
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	if err = session.SetDialect(sql.DialectMySQL); err != nil {
		t.Fatal(err)
	}
	rowCountSQL(t, session, "BEGIN")
	assertRowCount(t, session, 2)
	rowCountSQL(t, d, "INSERT INTO counted VALUES(3,3);DELETE FROM counted WHERE id=1")
	rowCountSQL(t, session, "INSERT INTO counted VALUES(4,4)")
	assertRowCount(t, session, 3)
	if err = d.ReplicationStorage().CompactRange(); err != nil {
		t.Fatal(err)
	}
	assertRowCount(t, session, 3)
	rowCountSQL(t, session, "COMMIT")
	assertRowCount(t, d, 3)
}

func TestExactRowCountConcurrentWriters(t *testing.T) {
	opt := storage.DefaultOptions()
	opt.DisableWALSync = true
	d, err := Open(t.TempDir(), opt)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { d.Close() })
	rowCountSQL(t, d, "CREATE TABLE counted(id INTEGER PRIMARY KEY,n INTEGER)")
	const workers = 12
	const commits = 12
	var wg sync.WaitGroup
	errors := make(chan error, workers)
	for w := 0; w < workers; w++ {
		session, e := d.NewSession()
		if e != nil {
			t.Fatal(e)
		}
		wg.Add(1)
		go func(w int, s *Session) {
			defer wg.Done()
			defer s.Close()
			for j := 0; j < commits; j++ {
				id := w*commits + j + 1
				if _, e := s.Exec(fmt.Sprintf("INSERT INTO counted VALUES(%d,%d)", id, id)); e != nil {
					errors <- e
					return
				}
				if _, e := s.Exec(fmt.Sprintf("UPDATE counted SET n=n+1 WHERE id=%d", id)); e != nil {
					errors <- e
					return
				}
			}
		}(w, session)
	}
	wg.Wait()
	close(errors)
	for e := range errors {
		t.Error(e)
	}
	assertRowCount(t, d, workers*commits)
	r := rowCountSQL(t, d, "SELECT (SELECT COUNT(*) FROM counted) AS a,(SELECT COUNT(*) FROM counted WHERE id>=0) AS b")
	if r.Rows[0]["a"].Int != r.Rows[0]["b"].Int {
		t.Fatalf("count/snapshot scan disagree: %+v", r.Rows)
	}
}

// COUNT executes against the statement snapshot. Ordinary SQL operations and
// ANALYZE must never create materialized cardinality records as a side effect.
func TestCountExecutionNeverMaintainsRowMetadata(t *testing.T) {
	path := t.TempDir()
	opt := storage.DefaultOptions()
	opt.DisableWALSync = true
	d, err := Open(path, opt)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { d.Close() })
	assertNoMetadata := func(stage string) {
		t.Helper()
		prefix := []byte("\x00__row_counts")
		upper := append(append([]byte(nil), prefix...), 255)
		it := d.ReplicationStorage().NewIteratorRange(prefix, upper, false)
		defer it.Close()
		if it.Valid() {
			t.Fatalf("%s wrote maintained row metadata: %q", stage, it.Key().UserKey)
		}
		if e := storage.IteratorError(it); e != nil {
			t.Fatal(e)
		}
	}
	assertNoMetadata("open")
	rowCountSQL(t, d, "CREATE TABLE counted(id INTEGER PRIMARY KEY,n INTEGER)")
	assertNoMetadata("CREATE")
	rowCountSQL(t, d, "INSERT INTO counted VALUES(1,1),(2,2)")
	assertRowCount(t, d, 2)
	assertNoMetadata("INSERT/COUNT")
	s, err := d.NewSession()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	prepared, err := s.Prepare("INSERT INTO counted VALUES(?,?)")
	if err != nil {
		t.Fatal(err)
	}
	defer prepared.Close()
	rowCountSQL(t, s, "BEGIN")
	if _, err = s.ExecPrepared(prepared, 3, 3); err != nil {
		t.Fatal(err)
	}
	assertRowCount(t, s, 3)
	assertRowCount(t, d, 2)
	assertNoMetadata("private prepared INSERT")
	rowCountSQL(t, s, "COMMIT")
	assertRowCount(t, d, 3)
	assertNoMetadata("prepared INSERT commit")
	update, err := s.Prepare("UPDATE counted SET n=n+? WHERE id=?")
	if err != nil {
		t.Fatal(err)
	}
	defer update.Close()
	rowCountSQL(t, s, "BEGIN")
	if _, err = s.ExecPrepared(update, 4, 2); err != nil {
		t.Fatal(err)
	}
	rowCountSQL(t, s, "SAVEPOINT saved;DELETE FROM counted WHERE id=1;INSERT INTO counted VALUES(4,4);ROLLBACK TO saved;COMMIT")
	assertRowCount(t, d, 3)
	assertNoMetadata("UPDATE/savepoint rollback")
	rowCountSQL(t, d, "DELETE FROM counted WHERE id=1")
	assertRowCount(t, d, 2)
	assertNoMetadata("DELETE")
	for repeat := 0; repeat < 2; repeat++ {
		rowCountSQL(t, d, "ANALYZE")
		assertRowCount(t, d, 2)
		assertNoMetadata("ANALYZE/COUNT")
	}
	if _, err = d.CheckIntegrity(); err != nil {
		t.Fatal(err)
	}
	assertNoMetadata("integrity check")
	if err = d.ReplicationStorage().CompactRange(); err != nil {
		t.Fatal(err)
	}
	assertNoMetadata("compaction")
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	if err = d.Close(); err != nil {
		t.Fatal(err)
	}
	d, err = Open(path, opt)
	if err != nil {
		t.Fatal(err)
	}
	assertRowCount(t, d, 2)
	assertNoMetadata("reopen/COUNT")
	rowCountSQL(t, d, "ANALYZE")
	assertNoMetadata("reopened ANALYZE")
	if _, err = d.CheckIntegrity(); err != nil {
		t.Fatal(err)
	}
}
