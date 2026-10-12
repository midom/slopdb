package db

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"testing"
	"time"
)

func waitCatalogRequest(t *testing.T, d *Database, exclusive bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		d.locks.graphMu.Lock()
		found := false
		for _, q := range d.locks.waiting {
			found = found || q.request.Resource == "catalog" && q.request.Exclusive == exclusive
		}
		d.locks.graphMu.Unlock()
		if found {
			return
		}
		runtime.Gosched()
	}
	t.Fatal("catalog waiter never registered")
}

func TestCatalogRenameWaitsForWriterCommit(t *testing.T) {
	dir := t.TempDir()
	d, e := Open(dir, nil)
	if e != nil {
		t.Fatal(e)
	}
	defer d.Close()
	objectExec(t, d, "CREATE TABLE original(id INTEGER PRIMARY KEY,v INTEGER UNIQUE); INSERT INTO original VALUES(1,10)")
	tx, e := d.Begin()
	if e != nil {
		t.Fatal(e)
	}
	defer tx.Rollback()
	if _, e = tx.Exec("INSERT INTO original VALUES(2,20)"); e != nil {
		t.Fatal(e)
	}
	renamed, e := d.NewSession()
	if e != nil {
		t.Fatal(e)
	}
	defer renamed.Close()
	done := make(chan error, 1)
	go func() { _, err := renamed.Exec("ALTER TABLE original RENAME TO renamed"); done <- err }()
	waitCatalogRequest(t, d, true)
	// An existing catalog owner must be able to finish despite a queued rename.
	if _, e = tx.Exec("UPDATE original SET v=21 WHERE id=2"); e != nil {
		t.Fatal(e)
	}
	if e = tx.Commit(); e != nil {
		t.Fatal(e)
	}
	if e = <-done; e != nil {
		t.Fatal(e)
	}
	r := objectExec(t, d, "SELECT v FROM renamed ORDER BY id")
	if len(r.Rows) != 2 || r.Rows[1]["v"].Int != 21 {
		t.Fatal("committed write lost during rename", r)
	}
	if _, e = d.CheckIntegrity(); e != nil {
		t.Fatal(e)
	}
	if e = d.Close(); e != nil {
		t.Fatal(e)
	}
	d, e = Open(dir, nil)
	if e != nil {
		t.Fatal(e)
	}
	defer d.Close()
	r = objectExec(t, d, "SELECT v FROM renamed ORDER BY id")
	if len(r.Rows) != 2 || r.Rows[1]["v"].Int != 21 {
		t.Fatal("write lost on reopen", r)
	}
}

func TestCatalogRenameQueuedPreparedWriter(t *testing.T) {
	for _, commit := range []bool{false, true} {
		t.Run(fmt.Sprintf("commit=%v", commit), func(t *testing.T) {
			d, e := Open(t.TempDir(), nil)
			if e != nil {
				t.Fatal(e)
			}
			defer d.Close()
			objectExec(t, d, "CREATE TABLE original(id INTEGER PRIMARY KEY,v INTEGER UNIQUE); INSERT INTO original VALUES(1,10)")
			prepared, e := d.Prepare("INSERT INTO original VALUES(?,?)")
			if e != nil {
				t.Fatal(e)
			}
			defer prepared.Close()
			if _, e = prepared.Exec(0, 0); e != nil {
				t.Fatal(e)
			} // warm the physical plan
			tx, e := d.Begin()
			if e != nil {
				t.Fatal(e)
			}
			defer tx.Rollback()
			if _, e = tx.Exec("ALTER TABLE original RENAME TO renamed"); e != nil {
				t.Fatal(e)
			}
			writer, e := d.NewSession()
			if e != nil {
				t.Fatal(e)
			}
			defer writer.Close()
			done := make(chan error, 1)
			go func() { _, err := writer.ExecPrepared(prepared, 2, 20); done <- err }()
			waitCatalogRequest(t, d, false)
			if commit {
				e = tx.Commit()
			} else {
				e = tx.Rollback()
			}
			if e != nil {
				t.Fatal(e)
			}
			e = <-done
			table := "original"
			expected := 3
			if commit {
				table = "renamed"
				expected = 2
				if e == nil {
					t.Fatal("stale prepared insert silently succeeded")
				}
			} else if e != nil {
				t.Fatal(e)
			}
			r := objectExec(t, d, "SELECT * FROM "+table)
			if len(r.Rows) != expected {
				t.Fatal("wrong committed row count", r)
			}
			if commit {
				// Reusing the name must invalidate the old physical plan.
				objectExec(t, d, "CREATE TABLE original(id INTEGER PRIMARY KEY,v INTEGER UNIQUE)")
				if _, e = writer.ExecPrepared(prepared, 3, 30); e != nil {
					t.Fatal(e)
				}
				if len(objectExec(t, d, "SELECT * FROM original").Rows) != 1 || len(objectExec(t, d, "SELECT * FROM renamed").Rows) != 2 {
					t.Fatal("prepared plan crossed storage IDs")
				}
			}
			if _, e = d.CheckIntegrity(); e != nil {
				t.Fatal(e)
			}
		})
	}
}

func TestCatalogConcurrentRenameNoLostUpdates(t *testing.T) {
	for _, hot := range []bool{false, true} {
		t.Run(fmt.Sprintf("hotspot=%v", hot), func(t *testing.T) { testCatalogRenameUpdates(t, hot) })
	}
}
func testCatalogRenameUpdates(t *testing.T, hot bool) {
	dir := t.TempDir()
	d, e := Open(dir, nil)
	if e != nil {
		t.Fatal(e)
	}
	defer d.Close()
	objectExec(t, d, "CREATE TABLE original(id INTEGER PRIMARY KEY,v INTEGER); CREATE INDEX iv ON original(v); CREATE VIEW live(id,v) AS SELECT id,v FROM original; CREATE TRIGGER live_update INSTEAD OF UPDATE ON live BEGIN UPDATE original SET v=NEW.v WHERE id=OLD.id; END")
	for i := 1; i <= 10; i++ {
		objectExec(t, d, fmt.Sprintf("INSERT INTO original VALUES(%d,0)", i))
	}
	p, e := d.Prepare("UPDATE live SET v=v+1 WHERE id=?")
	if e != nil {
		t.Fatal(e)
	}
	defer p.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	start := make(chan struct{})
	errs := make(chan error, 11)
	var wg sync.WaitGroup
	for i := 1; i <= 10; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			s, err := d.NewSession()
			if err != nil {
				errs <- err
				return
			}
			defer s.Close()
			<-start
			if hot {
				id = 1
			}
			for n := 0; n < 50; n++ {
				if _, err = s.ExecPreparedContext(ctx, p, id); err != nil {
					errs <- err
					return
				}
			}
		}(i)
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		s, err := d.NewSession()
		if err != nil {
			errs <- err
			return
		}
		defer s.Close()
		<-start
		for n := 0; n < 20; n++ {
			if _, err = s.ExecContext(ctx, "ALTER TABLE original RENAME TO renamed; ALTER TABLE renamed RENAME TO original"); err != nil {
				errs <- err
				return
			}
		}
	}()
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
	r := objectExec(t, d, "SELECT id,v FROM original ORDER BY id")
	if len(r.Rows) != 10 {
		t.Fatal("rows lost during rename", r)
	}
	for _, row := range r.Rows {
		expected := int64(50)
		if hot {
			expected = 0
			if row["id"].Int == 1 {
				expected = 500
			}
		}
		if row["v"].Int != expected {
			t.Fatalf("lost updates: id=%d v=%d", row["id"].Int, row["v"].Int)
		}
	}
	if _, e = d.CheckIntegrity(); e != nil {
		t.Fatal(e)
	}
	if e = d.Close(); e != nil {
		t.Fatal(e)
	}
	d, e = Open(dir, nil)
	if e != nil {
		t.Fatal(e)
	}
	defer d.Close()
	if objectExec(t, d, "SELECT SUM(v) AS n FROM original").Rows[0]["n"].Int != 500 {
		t.Fatal("lost writes on reopen")
	}
	if _, e = d.CheckIntegrity(); e != nil {
		t.Fatal(e)
	}
}

func TestCatalogPointReadSnapshotsAcrossRename(t *testing.T) {
	d, e := Open(t.TempDir(), nil)
	if e != nil {
		t.Fatal(e)
	}
	defer d.Close()
	objectExec(t, d, "CREATE TABLE original(id INTEGER PRIMARY KEY,v INTEGER); INSERT INTO original VALUES(1,17),(2,34)")
	reader, e := d.NewSession()
	if e != nil {
		t.Fatal(e)
	}
	defer reader.Close()
	p, e := d.Prepare("SELECT v FROM original WHERE id=?")
	if e != nil {
		t.Fatal(e)
	}
	defer p.Close()
	if _, e = reader.ExecPrepared(p, 1); e != nil {
		t.Fatal(e)
	}
	rows, e := reader.Query("SELECT v FROM original WHERE id=1")
	if e != nil {
		t.Fatal(e)
	}
	defer rows.Close()
	objectExec(t, d, "ALTER TABLE original RENAME TO renamed; UPDATE renamed SET v=18 WHERE id=1; CREATE TABLE original(id INTEGER PRIMARY KEY,v INTEGER); INSERT INTO original VALUES(1,99)")
	if !rows.Next() {
		t.Fatal(rows.Err())
	}
	values, e := rows.Values()
	if e != nil {
		t.Fatal(e)
	}
	if values[0].Int != 17 {
		t.Fatal("pinned read crossed catalog versions", values)
	}
	if rows.Next() || rows.Err() != nil {
		t.Fatal(rows.Err())
	}
	rows.Close()
	r, e := reader.ExecPrepared(p, 1)
	if e != nil {
		t.Fatal(e)
	}
	if r.Rows[0]["v"].Int != 99 {
		t.Fatal("next statement retained renamed storage", r)
	}
	if _, e = d.CheckIntegrity(); e != nil {
		t.Fatal(e)
	}
}
