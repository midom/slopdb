package db

import (
	"fmt"
	"strings"
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
		t.Fatalf("metadata/snapshot scan disagree: %+v", r.Rows)
	}
}

func TestExactRowCountLegacyAnalyze(t *testing.T) {
	opt := storage.DefaultOptions()
	opt.DisableWALSync = true
	d, err := Open(t.TempDir(), opt)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { d.Close() })
	rowCountSQL(t, d, "CREATE TABLE counted(id INTEGER PRIMARY KEY,n INTEGER);INSERT INTO counted VALUES(1,1),(2,2)")
	// Remove only the optional maintained metadata to model a pre-upgrade table.
	l := d.ReplicationStorage()
	it := l.NewIterator()
	var batch []storage.Mutation
	for it.Valid() {
		k := it.Key().UserKey
		if strings.HasPrefix(string(k), "\x00__row_counts_v1__") {
			batch = append(batch, storage.Mutation{Key: append([]byte(nil), k...), Delete: true})
		}
		it.Next()
	}
	if e := storage.IteratorError(it); e != nil {
		t.Fatal(e)
	}
	it.Close()
	if len(batch) != 65 {
		t.Fatalf("expected65 counter records, got%d", len(batch))
	}
	if err = l.WriteBatch(batch); err != nil {
		t.Fatal(err)
	}
	assertRowCount(t, d, 2)
	rowCountSQL(t, d, "INSERT INTO counted VALUES(3,3);ANALYZE")
	assertRowCount(t, d, 3)
	rowCountSQL(t, d, "DELETE FROM counted WHERE id=1;UPDATE counted SET n=5 WHERE id=2")
	assertRowCount(t, d, 2)
}
