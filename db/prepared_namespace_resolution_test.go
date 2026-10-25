package db

import (
	"context"
	"fmt"
	"testing"

	"slopdb/sql"
	"slopdb/storage"
)

func namespaceTestDatabase(t *testing.T) *Database {
	t.Helper()
	options := storage.DefaultOptions()
	options.DisableWALSync = true
	database, e := Open(t.TempDir(), options)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() {
		if e := database.Close(); e != nil {
			t.Error(e)
		}
	})
	return database
}
func namespacePreparedValue(t *testing.T, session *Session, statement *Stmt, ctx context.Context, want int64) {
	t.Helper()
	r, e := session.ExecPreparedContext(ctx, statement, int64(1))
	if e != nil || len(r.Rows) != 1 || r.Rows[0]["v"].Int != want {
		t.Fatalf("prepared value want%d got%+v error%v", want, r, e)
	}
}
func TestPreparedNamespaceResolutionCatalogAndTemporaryLifecycle(t *testing.T) {
	d := namespaceTestDatabase(t)
	s := isolationSession(t, d)
	ctx := context.Background()
	isolationExec(t, s, "CREATE TABLE records(id INTEGER PRIMARY KEY,v INTEGER);INSERT INTO records VALUES(1,10)")
	statement, e := d.Prepare("SELECT v FROM records WHERE id=?")
	if e != nil {
		t.Fatal(e)
	}
	defer statement.Close()
	for i := 0; i < 3; i++ {
		namespacePreparedValue(t, s, statement, ctx, 10)
	}
	isolationExec(t, s, "CREATE SCHEMA tenant;CREATE TABLE tenant.records(id INTEGER PRIMARY KEY,v INTEGER);INSERT INTO tenant.records VALUES(1,20)")
	namespacePreparedValue(t, s, statement, ctx, 10)
	isolationExec(t, s, "USE tenant")
	namespacePreparedValue(t, s, statement, ctx, 20)
	namespacePreparedValue(t, s, statement, sql.WithNamespaceSearchPath(ctx, []string{"main"}), 10)
	namespacePreparedValue(t, s, statement, ctx, 20)
	isolationExec(t, s, "USE main;ALTER TABLE records RENAME TO old_records;CREATE TABLE records(id INTEGER PRIMARY KEY,v INTEGER);INSERT INTO records VALUES(1,30)")
	namespacePreparedValue(t, s, statement, ctx, 30)
	isolationExec(t, s, "ANALYZE records")
	namespacePreparedValue(t, s, statement, ctx, 30)
	isolationExec(t, s, "CREATE TEMPORARY TABLE records(id INTEGER PRIMARY KEY,v INTEGER);INSERT INTO records VALUES(1,40)")
	namespacePreparedValue(t, s, statement, ctx, 40)
	other := isolationSession(t, d)
	namespacePreparedValue(t, other, statement, ctx, 30)
	isolationExec(t, s, "DROP TEMPORARY TABLE records")
	namespacePreparedValue(t, s, statement, ctx, 30)
	isolationExec(t, s, "BEGIN;ALTER TABLE records RENAME TO rolled_records;CREATE TABLE records(id INTEGER PRIMARY KEY,v INTEGER);INSERT INTO records VALUES(1,50)")
	namespacePreparedValue(t, s, statement, ctx, 50)
	isolationExec(t, s, "ROLLBACK")
	namespacePreparedValue(t, s, statement, ctx, 30)
}
func TestPreparedNamespaceResolutionDialectAliasesAndBareKeywords(t *testing.T) {
	d := namespaceTestDatabase(t)
	s := isolationSession(t, d)
	ctx := context.Background()
	isolationExec(t, s, "CREATE TABLE records(id INTEGER PRIMARY KEY,v INTEGER,user TEXT,current_schema TEXT);INSERT INTO records VALUES(1,10,'stored-user','stored-schema')")
	statement, e := d.Prepare("SELECT v FROM public.records WHERE id=?")
	if e != nil {
		t.Fatal(e)
	}
	defer statement.Close()
	namespacePreparedValue(t, s, statement, ctx, 10)
	if e = s.SetDialect(sql.DialectMySQL); e != nil {
		t.Fatal(e)
	}
	isolationExec(t, s, "CREATE DATABASE public;CREATE TABLE public.records(id INTEGER PRIMARY KEY,v INTEGER);INSERT INTO public.records VALUES(1,20)")
	namespacePreparedValue(t, s, statement, ctx, 20)
	if e = s.SetDialect(sql.DialectPostgreSQL); e != nil {
		t.Fatal(e)
	}
	namespacePreparedValue(t, s, statement, ctx, 10)
	if e = s.SetDialect(sql.DialectMySQL); e != nil {
		t.Fatal(e)
	}
	namespacePreparedValue(t, s, statement, ctx, 20)
	if e = s.RegisterRootSchemaAlias(ctx, "public"); e != nil {
		t.Fatal(e)
	}
	namespacePreparedValue(t, s, statement, ctx, 10)
	bare, e := d.Prepare("SELECT user AS u,current_schema AS s FROM records WHERE id=?")
	if e != nil {
		t.Fatal(e)
	}
	defer bare.Close()
	for _, dialect := range []sql.Dialect{sql.DialectNative, sql.DialectPostgreSQL, sql.DialectMySQL, sql.DialectPostgreSQL} {
		if e = s.SetDialect(dialect); e != nil {
			t.Fatal(e)
		}
		sessionContext := sql.WithPostgresSession(ctx, sql.PostgresSessionOptions{User: "session-user"})
		r, err := s.ExecPreparedContext(sessionContext, bare, int64(1))
		if err != nil || len(r.Rows) != 1 {
			t.Fatal(r, err)
		}
		wantUser, wantSchema := "stored-user", "stored-schema"
		if dialect == sql.DialectPostgreSQL {
			wantUser, wantSchema = "session-user", "public"
		}
		if r.Rows[0]["u"].Text != wantUser || r.Rows[0]["s"].Text != wantSchema {
			t.Fatalf("dialect%v result%+v", dialect, r)
		}
	}
}
func TestPreparedNamespaceResolutionSharedIndependentSessions(t *testing.T) {
	d := namespaceTestDatabase(t)
	root := isolationSession(t, d)
	isolationExec(t, root, "CREATE TABLE records(id INTEGER PRIMARY KEY,v INTEGER);INSERT INTO records VALUES(1,10);CREATE SCHEMA tenant;CREATE TABLE tenant.records(id INTEGER PRIMARY KEY,v INTEGER);INSERT INTO tenant.records VALUES(1,20)")
	statement, e := d.Prepare("SELECT v FROM records WHERE id=?")
	if e != nil {
		t.Fatal(e)
	}
	defer statement.Close()
	a, b := isolationSession(t, d), isolationSession(t, d)
	isolationExec(t, b, "USE tenant")
	done := make(chan error, 2)
	for i, session := range []*Session{a, b} {
		go func(i int, session *Session) {
			want := int64(10 + 10*i)
			for n := 0; n < 50; n++ {
				result, err := session.ExecPrepared(statement, int64(1))
				if err != nil {
					done <- err
					return
				}
				if len(result.Rows) != 1 || result.Rows[0]["v"].Int != want {
					done <- fmt.Errorf("session%d value%+v want%d", i, result, want)
					return
				}
			}
			done <- nil
		}(i, session)
	}
	for i := 0; i < 2; i++ {
		if e = <-done; e != nil {
			t.Fatal(e)
		}
	}
}
