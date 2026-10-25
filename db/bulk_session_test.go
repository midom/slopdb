package db

import (
	"context"
	"errors"
	"io"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"slopdb/sql"
	"slopdb/storage"
)

func bulkDatabase(t *testing.T) (*Database, string) {
	t.Helper()
	tmp := t.TempDir()
	opt := storage.DefaultOptions()
	opt.QueryTempDir = tmp
	opt.DisableWALSync = true
	d, e := Open(t.TempDir(), opt)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() {
		if e := d.Close(); e != nil {
			t.Error(e)
		}
	})
	return d, tmp
}
func bulkExec(t *testing.T, s *Session, q string) *ExecResult {
	t.Helper()
	r, e := s.Exec(q)
	if e != nil {
		t.Fatalf("%s: %v", q, e)
	}
	return r
}
func noBulkSpools(t *testing.T, path string) {
	t.Helper()
	entries, e := os.ReadDir(path)
	if e != nil {
		t.Fatal(e)
	}
	if len(entries) != 0 {
		t.Fatalf("bulk leaked temporary files: %v", entries)
	}
}
func TestSessionBulkInsertStatementForeignKeysAndPrivateState(t *testing.T) {
	d, tmp := bulkDatabase(t)
	s, e := d.NewSession()
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	bulkExec(t, s, "CREATE SCHEMA tenant;USE tenant;CREATE TABLE nodes(id INTEGER PRIMARY KEY,parent INTEGER REFERENCES nodes(id),v BLOB);BEGIN")
	i := 0
	buffer := []byte{0}
	n, e := s.BulkInsert(context.Background(), "nodes", []string{"id", "parent", "v"}, func(context.Context) ([]any, error) {
		i++
		if i > 300 {
			return nil, io.EOF
		}
		buffer[0] = byte(i)
		var parent any
		if i == 1 {
			parent = int64(300)
		}
		return []any{int64(i), parent, buffer}, nil
	})
	if e != nil || n != 300 {
		t.Fatalf("rows %d: %v", n, e)
	}
	r := bulkExec(t, s, "SELECT id,v FROM nodes WHERE id IN(1,2,300) ORDER BY id")
	if len(r.Rows) != 3 || r.Rows[0]["v"].Blob[0] != 1 || r.Rows[1]["v"].Blob[0] != 2 || r.Rows[2]["v"].Blob[0] != 44 {
		t.Fatalf("reused input %+v", r.Rows)
	}
	bulkExec(t, s, "ROLLBACK")
	if n := bulkExec(t, s, "SELECT count(*) AS n FROM nodes").Rows[0]["n"].Int; n != 0 {
		t.Fatalf("escaped transaction: %d", n)
	}
	bulkExec(t, s, "CREATE TEMPORARY TABLE private_bulk(id INTEGER PRIMARY KEY)")
	i = 0
	n, e = s.BulkInsert(context.Background(), "private_bulk", []string{"id"}, func(context.Context) ([]any, error) {
		i++
		if i > 2 {
			return nil, io.EOF
		}
		return []any{i}, nil
	})
	if e != nil || n != 2 {
		t.Fatal(n, e)
	}
	noBulkSpools(t, tmp)
}
func TestTransactionBulkInsertFailurePreservesEarlierWrites(t *testing.T) {
	d, tmp := bulkDatabase(t)
	if _, e := d.Exec("CREATE TABLE p(id INTEGER PRIMARY KEY,parent INTEGER REFERENCES p(id));INSERT INTO p VALUES(1,NULL)"); e != nil {
		t.Fatal(e)
	}
	tx, e := d.Begin()
	if e != nil {
		t.Fatal(e)
	}
	defer tx.Rollback()
	if _, e = tx.Exec("INSERT INTO p VALUES(2,1)"); e != nil {
		t.Fatal(e)
	}
	i := 0
	_, e = tx.BulkInsert(context.Background(), "p", []string{"id", "parent"}, func(context.Context) ([]any, error) {
		i++
		if i > 2 {
			return nil, io.EOF
		}
		if i == 1 {
			return []any{3, 1}, nil
		}
		return []any{4, 999}, nil
	})
	if e == nil {
		t.Fatal("missing foreign key accepted")
	}
	r, e := tx.Exec("SELECT count(*) AS n FROM p")
	if e != nil || r.Rows[0]["n"].Int != 2 {
		t.Fatalf("earlier writes lost: %+v %v", r, e)
	}
	if e = tx.Commit(); e != nil {
		t.Fatal(e)
	}
	noBulkSpools(t, tmp)
}
func TestSessionBulkInsertRetriesWithoutRepeatingSource(t *testing.T) {
	d, tmp := bulkDatabase(t)
	s, e := d.NewSession()
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	bulkExec(t, s, "CREATE TABLE retries(id INTEGER PRIMARY KEY,v BLOB)")
	holder, e := d.Begin()
	if e != nil {
		t.Fatal(e)
	}
	defer holder.Rollback()
	if _, e = holder.Exec("INSERT INTO retries VALUES(2,X'FF')"); e != nil {
		t.Fatal(e)
	}
	var calls atomic.Int64
	done := make(chan error, 1)
	go func() {
		buffer := []byte{0}
		n, e := s.BulkInsert(context.Background(), "retries", []string{"id", "v"}, func(context.Context) ([]any, error) {
			i := calls.Add(1)
			if i > 3 {
				return nil, io.EOF
			}
			buffer[0] = byte(i)
			return []any{i, buffer}, nil
		})
		if e == nil && n != 3 {
			e = errors.New("wrong row count")
		}
		done <- e
	}()
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	for len(d.LockWaits()) == 0 {
		select {
		case e := <-done:
			t.Fatalf("bulk did not wait: %v", e)
		case <-deadline.C:
			t.Fatal("no lock waiter")
		case <-tick.C:
		}
	}
	if e = holder.Rollback(); e != nil {
		t.Fatal(e)
	}
	if e = <-done; e != nil {
		t.Fatal(e)
	}
	if calls.Load() != 4 {
		t.Fatalf("source replayed: %d calls", calls.Load())
	}
	r := bulkExec(t, s, "SELECT id,v FROM retries ORDER BY id")
	for i, row := range r.Rows {
		if row["v"].Blob[0] != byte(i+1) {
			t.Fatalf("replay corruption: %+v", r.Rows)
		}
	}
	noBulkSpools(t, tmp)
}
func TestBulkInsertOneStatementClock(t *testing.T) {
	d, tmp := bulkDatabase(t)
	s, e := d.NewSession()
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	if e = s.SetDialect(sql.DialectPostgreSQL); e != nil {
		t.Fatal(e)
	}
	bulkExec(t, s, "CREATE TABLE clocked(id BIGINT PRIMARY KEY, stamp TIMESTAMPTZ DEFAULT statement_timestamp())")
	i := 0
	n, e := s.BulkInsert(context.Background(), "clocked", []string{"id"}, func(context.Context) ([]any, error) {
		i++
		if i > 4 {
			return nil, io.EOF
		}
		time.Sleep(time.Millisecond)
		return []any{i}, nil
	})
	if e != nil || n != 4 {
		t.Fatal(n, e)
	}
	r := bulkExec(t, s, "SELECT stamp FROM clocked ORDER BY id")
	for _, row := range r.Rows {
		if row["stamp"].Text != r.Rows[0]["stamp"].Text {
			t.Fatalf("statement time changed: %+v", r.Rows)
		}
	}
	noBulkSpools(t, tmp)
}
func TestBulkInsertSessionCloseCancelsSourceAndCleansSpool(t *testing.T) {
	d, tmp := bulkDatabase(t)
	s, e := d.NewSession()
	if e != nil {
		t.Fatal(e)
	}
	bulkExec(t, s, "CREATE TABLE cancelled(id INTEGER PRIMARY KEY)")
	entered := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		i := 0
		_, e := s.BulkInsert(context.Background(), "cancelled", []string{"id"}, func(ctx context.Context) ([]any, error) {
			i++
			if i == 1 {
				return []any{1}, nil
			}
			close(entered)
			<-ctx.Done()
			return nil, ctx.Err()
		})
		done <- e
	}()
	<-entered
	if e = s.Close(); e != nil {
		t.Fatal(e)
	}
	if e = <-done; !errors.Is(e, context.Canceled) {
		t.Fatal(e)
	}
	r, e := d.Exec("SELECT count(*) AS n FROM cancelled")
	if e != nil || r.Rows[0]["n"].Int != 0 {
		t.Fatal(r, e)
	}
	noBulkSpools(t, tmp)
}

func TestBulkInsertTriggersShareSnapshot(t *testing.T) {
	d, _ := bulkDatabase(t)
	s, e := d.NewSession()
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	other, e := d.NewSession()
	if e != nil {
		t.Fatal(e)
	}
	defer other.Close()
	bulkExec(t, s, "CREATE TABLE cfg(id INTEGER PRIMARY KEY,v INTEGER);INSERT INTO cfg VALUES(1,10);CREATE TABLE snap(id INTEGER PRIMARY KEY);CREATE TABLE audit(id INTEGER PRIMARY KEY,v INTEGER);CREATE TRIGGER capture AFTER INSERT ON snap BEGIN INSERT INTO audit VALUES(NEW.id,(SELECT v FROM cfg WHERE id=1)); END")
	i := 0
	n, e := s.BulkInsert(context.Background(), "snap", []string{"id"}, func(context.Context) ([]any, error) {
		i++
		if i > 3 {
			return nil, io.EOF
		}
		if i == 2 {
			if _, e := other.Exec("UPDATE cfg SET v=20 WHERE id=1"); e != nil {
				return nil, e
			}
		}
		return []any{i}, nil
	})
	if e != nil || n != 3 {
		t.Fatal(n, e)
	}
	r := bulkExec(t, s, "SELECT v FROM audit ORDER BY id")
	for _, row := range r.Rows {
		if row["v"].Int != 10 {
			t.Fatalf("snapshot changed within bulk statement: %+v", r.Rows)
		}
	}
}

func TestBulkInsertInputCannotRequestInternalRetry(t *testing.T) {
	d, tmp := bulkDatabase(t)
	if _, e := d.Exec("CREATE TABLE retry_input(id INTEGER)"); e != nil {
		t.Fatal(e)
	}
	calls := 0
	_, e := d.BulkInsert(context.Background(), "retry_input", []string{"id"}, func(context.Context) ([]any, error) { calls++; return nil, sql.ErrStatementRetry })
	if !errors.Is(e, sql.ErrStatementRetry) || calls != 1 {
		t.Fatalf("input callback error %v, calls %d", e, calls)
	}
	noBulkSpools(t, tmp)
}
