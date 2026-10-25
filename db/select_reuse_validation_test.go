package db_test

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"slopdb/db"
	"slopdb/sql"
	"slopdb/storage"
)

func selectReuseDatabase(t *testing.T) (*db.Database, string, *storage.Options) {
	t.Helper()
	path := t.TempDir()
	options := storage.DefaultOptions()
	options.DisableWALSync = true
	d, err := db.Open(path, options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = d.Close() })
	return d, path, options
}
func selectReuseSession(t *testing.T, d *db.Database) *db.Session {
	t.Helper()
	s, err := d.NewSession()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}
func selectReusePrepare(t *testing.T, d *db.Database, query string) *db.Stmt {
	t.Helper()
	p, err := d.Prepare(query)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Close() })
	return p
}
func selectReuseExec(t *testing.T, s *db.Session, query string) {
	t.Helper()
	if _, err := s.Exec(query); err != nil {
		t.Fatalf("%s: %v", query, err)
	}
}
func selectReusePoint(ctx context.Context, s *db.Session, p *db.Stmt, dst *db.ExecResult, namespace byte, epoch int) error {
	r, err := s.ExecPreparedContextInto(ctx, dst, p, int64(1))
	if err != nil {
		return err
	}
	if r != dst || len(r.Columns) != 3 || len(r.Rows) != 1 || len(r.Rows[0]) != 3 {
		return fmt.Errorf("stale destination shape: %+v", r)
	}
	for i, name := range []string{"id", "label", "payload"} {
		if r.Columns[i].Name != name {
			return fmt.Errorf("column%d: %+v", i, r.Columns)
		}
	}
	row := r.Rows[0]
	blob := row["payload"].Blob
	if row["id"].Type != sql.ColInteger || row["label"].Type != sql.ColText || row["payload"].Type != sql.ColBlob || row["id"].Int != 1 || len(blob) != 4 || blob[0] != namespace || blob[2] != 0xff || blob[3] != 0 || row["label"].Text != fmt.Sprintf("%c-%d", namespace, blob[1]) || epoch >= 0 && int(blob[1]) != epoch {
		return fmt.Errorf("namespace%c epoch%d: %+v", namespace, epoch, row)
	}
	return nil
}

func TestSelectReuseConcurrentNamespaceAndCatalogReplacement(t *testing.T) {
	d, path, options := selectReuseDatabase(t)
	writer := selectReuseSession(t, d)
	selectReuseExec(t, writer, "CREATE SCHEMA alpha;CREATE SCHEMA beta;CREATE TABLE alpha.items(id INTEGER PRIMARY KEY,label TEXT,payload BLOB);CREATE TABLE beta.items(id INTEGER PRIMARY KEY,label TEXT,payload BLOB);INSERT INTO alpha.items VALUES(1,'a-0',X'6100ff00');INSERT INTO beta.items VALUES(1,'b-0',X'6200ff00')")
	selectReuseExec(t, writer, "CREATE TABLE items(id INTEGER PRIMARY KEY,payload BLOB,label TEXT);INSERT INTO items VALUES(1,X'7200ff00','r-0')")
	statement := selectReusePrepare(t, d, "SELECT id,label,payload FROM items WHERE id=?")
	const workers, epochs = 4, 6
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	failures := make(chan error, workers*2)
	start := make(chan struct{})
	var ready, done sync.WaitGroup
	var finished atomic.Bool
	for worker := 0; worker < workers; worker++ {
		s := selectReuseSession(t, d)
		selectReuseExec(t, s, "USE alpha")
		ready.Add(1)
		done.Add(1)
		go func(s *db.Session) {
			defer done.Done()
			owned, err := s.ExecPreparedContext(ctx, statement, int64(1))
			ready.Done()
			if err != nil {
				failures <- err
				return
			}
			if len(owned.Rows) != 1 || !bytes.Equal(owned.Rows[0]["payload"].Blob, []byte{'a', 0, 0xff, 0}) {
				failures <- fmt.Errorf("bad retained result: %+v", owned)
				return
			}
			// Ordinary results are caller-owned even while this statement's reusable
			// destinations and table identities change in other sessions.
			retainedMap, retainedBlob, retainedColumns := owned.Rows[0], owned.Rows[0]["payload"].Blob, owned.Columns
			retainedMap["caller_only"] = sql.NewTextValue("retained")
			retainedBlob[0] = 0xee
			retainedColumns[0].Name = "caller_id"
			var dst db.ExecResult
			<-start
			for n := 0; n < 30 || !finished.Load(); n++ {
				namespace := byte('a')
				query := "USE alpha"
				callCtx := ctx
				switch n % 4 {
				case 1:
					query = "SET search_path TO beta,alpha"
					namespace = 'b'
				case 2:
					callCtx = sql.WithNamespaceSearchPath(ctx, []string{"beta"})
					namespace = 'b'
				case 3:
					query, namespace = "USE main", 'r'
				}
				if _, err = s.ExecContext(ctx, query); err != nil {
					failures <- err
					return
				}
				if err = selectReusePoint(callCtx, s, statement, &dst, namespace, -1); err != nil {
					failures <- err
					return
				}
				if n%4 == 2 {
					if err = selectReusePoint(ctx, s, statement, &dst, 'a', -1); err != nil {
						failures <- fmt.Errorf("context override leaked: %w", err)
						return
					}
				}
			}
			for _, ns := range []struct {
				schema string
				tag    byte
			}{{"alpha", 'a'}, {"beta", 'b'}} {
				if err = selectReusePoint(sql.WithNamespaceSearchPath(ctx, []string{ns.schema}), s, statement, &dst, ns.tag, epochs); err != nil {
					failures <- err
					return
				}
			}
			if retainedMap["caller_only"].Text != "retained" || retainedColumns[0].Name != "caller_id" || !bytes.Equal(retainedBlob, []byte{0xee, 0, 0xff, 0}) {
				failures <- fmt.Errorf("retained owned storage changed: %+v", owned)
			}
		}(s)
	}
	ready.Wait()
	close(start)
	var writerErr error
	for epoch := 1; epoch <= epochs; epoch++ {
		// Rename/name reuse and DROP/recreate are atomically published together.
		// Reordered physical columns catch a cached decoder using an old recipe.
		q := fmt.Sprintf("BEGIN;ALTER TABLE alpha.items RENAME TO previous_items;CREATE TABLE alpha.items(payload BLOB,label TEXT,id INTEGER PRIMARY KEY);INSERT INTO alpha.items(id,label,payload) VALUES(1,'a-%d',X'61%02xff00');USE alpha;DROP TABLE previous_items;USE beta;DROP TABLE items;CREATE TABLE beta.items(label TEXT,id INTEGER PRIMARY KEY,payload BLOB);INSERT INTO beta.items(id,label,payload) VALUES(1,'b-%d',X'62%02xff00');COMMIT", epoch, epoch, epoch, epoch)
		if _, writerErr = writer.ExecContext(ctx, q); writerErr != nil {
			// A failed assertion must not leave the writer's catalog locks
			// blocking the reader goroutines until the test deadline.
			_, _ = writer.Exec("ROLLBACK")
			break
		}
	}
	finished.Store(true)
	done.Wait()
	close(failures)
	if writerErr != nil {
		t.Error(writerErr)
	}
	for err := range failures {
		t.Error(err)
	}
	if t.Failed() {
		return
	}
	if err := statement.Close(); err != nil {
		t.Fatal(err)
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := db.Open(path, options)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	p := selectReusePrepare(t, reopened, "SELECT id,label,payload FROM items WHERE id=?")
	s := selectReuseSession(t, reopened)
	var dst db.ExecResult
	for _, ns := range []struct {
		schema string
		tag    byte
	}{{"alpha", 'a'}, {"beta", 'b'}} {
		if err = selectReusePoint(sql.WithNamespaceSearchPath(ctx, []string{ns.schema}), s, p, &dst, ns.tag, epochs); err != nil {
			t.Fatal(err)
		}
	}
}

func TestSelectReuseTemporaryDialectAndDestinationTransitions(t *testing.T) {
	d, _, _ := selectReuseDatabase(t)
	s, other := selectReuseSession(t, d), selectReuseSession(t, d)
	selectReuseExec(t, s, "CREATE TABLE items(id INTEGER PRIMARY KEY,label TEXT,payload BLOB,user TEXT,current_schema TEXT);INSERT INTO items VALUES(1,'a-0',X'6100ff00','stored-user','stored-schema')")
	point := selectReusePrepare(t, d, "SELECT id,label,payload FROM items WHERE id=?")
	bare := selectReusePrepare(t, d, "SELECT user AS u,current_schema AS s,payload FROM items WHERE id=?")
	alternate := selectReusePrepare(t, d, "SELECT label AS alternate FROM items WHERE id=?")
	owned, err := s.ExecPrepared(point, int64(1))
	if err != nil {
		t.Fatal(err)
	}
	var dst db.ExecResult
	ctx := sql.WithPostgresSession(context.Background(), sql.PostgresSessionOptions{User: "session-user"})
	if err = selectReusePoint(ctx, s, point, &dst, 'a', 0); err != nil {
		t.Fatal(err)
	}
	if err = s.SetDialect(sql.DialectMySQL); err != nil {
		t.Fatal(err)
	}
	selectReuseExec(t, s, "CREATE TEMPORARY TABLE items(payload BLOB,current_schema TEXT,id INTEGER PRIMARY KEY,user TEXT,label TEXT);INSERT INTO items(id,label,payload,user,current_schema) VALUES(1,'t-1',X'7401ff00','temp-user','temp-schema')")
	if err = selectReusePoint(ctx, s, point, &dst, 't', 1); err != nil {
		t.Fatal(err)
	}
	retainedTemp, err := s.ExecPrepared(point, int64(1))
	if err != nil {
		t.Fatal(err)
	}
	for _, dialect := range []sql.Dialect{sql.DialectNative, sql.DialectPostgreSQL, sql.DialectMySQL, sql.DialectNative} {
		if err = s.SetDialect(dialect); err != nil {
			t.Fatal(err)
		}
		r, e := s.ExecPreparedContextInto(ctx, &dst, bare, int64(1))
		if e != nil {
			t.Fatal(e)
		}
		u, schema := "temp-user", "temp-schema"
		if dialect == sql.DialectPostgreSQL {
			u, schema = "session-user", "public"
		}
		if len(r.Rows) != 1 || len(r.Rows[0]) != 3 || r.Rows[0]["u"].Text != u || r.Rows[0]["s"].Text != schema || !bytes.Equal(r.Rows[0]["payload"].Blob, []byte{'t', 1, 0xff, 0}) {
			t.Fatalf("dialect%v: %+v", dialect, r)
		}
	}
	r, err := s.ExecPreparedInto(&dst, point, int64(99))
	if err != nil || r != &dst || len(dst.Rows) != 0 || len(dst.Columns) != 3 || dst.Changes != 0 {
		t.Fatal(r, err)
	}
	r, err = s.ExecPreparedInto(&dst, alternate, int64(1))
	if err != nil || len(dst.Columns) != 1 || len(dst.Rows) != 1 || len(dst.Rows[0]) != 1 || dst.Rows[0]["alternate"].Text != "t-1" {
		t.Fatal(r, err)
	}
	var otherDst db.ExecResult
	if err = selectReusePoint(ctx, other, point, &otherDst, 'a', 0); err != nil {
		t.Fatal(err)
	}
	if err = s.SetDialect(sql.DialectMySQL); err != nil {
		t.Fatal(err)
	}
	selectReuseExec(t, s, "DROP TEMPORARY TABLE items")
	if err = selectReusePoint(ctx, s, point, &dst, 'a', 0); err != nil {
		t.Fatal(err)
	}
	if len(owned.Rows[0]) != 3 || owned.Rows[0]["label"].Text != "a-0" || !bytes.Equal(owned.Rows[0]["payload"].Blob, []byte{'a', 0, 0xff, 0}) || retainedTemp.Rows[0]["label"].Text != "t-1" || !bytes.Equal(retainedTemp.Rows[0]["payload"].Blob, []byte{'t', 1, 0xff, 0}) {
		t.Fatal("retained results changed across temporary/dialect reuse", owned, retainedTemp)
	}
}
