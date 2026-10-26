package db

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"slopdb/sql"
	"slopdb/storage"
)

func mutationPayload(id int) []byte {
	if id%5 == 0 {
		return nil
	}
	if id%7 == 0 {
		return []byte{}
	}
	size := 500 + id
	if id == 3 {
		size = 100000
	}
	return bytes.Repeat([]byte{byte(id), 0, 255}, size)
}
func TestPreparedMutationPrivateBuffersConstraintsAndReopen(t *testing.T) {
	for _, dialect := range []sql.Dialect{sql.DialectNative, sql.DialectMySQL, sql.DialectPostgreSQL} {
		t.Run(dialect.String(), func(t *testing.T) {
			path := t.TempDir()
			options := storage.DefaultOptions()
			options.DisableWALSync = true
			d, e := Open(path, options)
			if e != nil {
				t.Fatal(e)
			}
			defer d.Close()
			s := isolationSession(t, d)
			if e = s.SetDialect(dialect); e != nil {
				t.Fatal(e)
			}
			isolationExec(t, s, "CREATE TABLE pm(a INTEGER,b INTEGER,v INTEGER UNIQUE,n INTEGER NOT NULL,payload BLOB,label TEXT,PRIMARY KEY(a,b));CREATE INDEX pm_label ON pm(label)")
			insert, e := s.Prepare("INSERT INTO pm VALUES(?,?,?,?,?,?)")
			if e != nil {
				t.Fatal(e)
			}
			defer insert.Close()
			permuted, e := s.Prepare("INSERT INTO pm(label,payload,n,v,b,a) VALUES(?,?,?,?,?,?)")
			if e != nil {
				t.Fatal(e)
			}
			defer permuted.Close()
			update, e := s.Prepare("UPDATE pm SET n=n+?,label=? WHERE a=? AND b=?")
			if e != nil {
				t.Fatal(e)
			}
			defer update.Close()
			isolationExec(t, s, "BEGIN")
			var dst ExecResult
			for id := 1; id <= 32; id++ {
				if id == 17 {
					isolationExec(t, s, "SAVEPOINT middle")
				}
				payload := mutationPayload(id)
				var blob any = payload
				if payload == nil {
					blob = nil
				}
				r, e := s.ExecPreparedInto(&dst, insert, id%4+1, id, id*10, 0, blob, "old")
				if e != nil || r.Changes != 1 {
					t.Fatalf("insert%d result%+v err%v", id, r, e)
				}
				if len(payload) > 0 {
					payload[0] = 255
				}
			}
			isolationExec(t, s, "ROLLBACK TO middle")
			if _, e = s.ExecPrepared(insert, 2, 1, 999, 0, nil, "duplicate"); e == nil {
				t.Fatal("duplicate PK accepted")
			}
			if _, e = s.ExecPrepared(insert, 4, 99, 10, 0, nil, "duplicate"); e == nil {
				t.Fatal("duplicate UNIQUE accepted")
			}
			if _, e = s.ExecPrepared(insert, 4, 99, 999, nil, nil, "invalid"); e == nil {
				t.Fatal("NULL in required column accepted")
			}
			for id := 17; id <= 32; id++ {
				var blob any = mutationPayload(id)
				if id%5 == 0 {
					blob = nil
				}
				if _, e = s.ExecPrepared(permuted, "old", blob, 0, id*10, id, id%4+1); e != nil {
					t.Fatal(e)
				}
			}
			for id := 1; id <= 32; id++ {
				r, e := s.ExecPrepared(update, id, "new", id%4+1, id)
				if e != nil || r.Changes != 1 {
					t.Fatalf("update%d result%+v err%v", id, r, e)
				}
			}
			isolationExec(t, s, "COMMIT")
			held := isolationExec(t, s, "SELECT b,payload FROM pm WHERE a IN(1,2,3,4) AND b>=1 ORDER BY b")
			if len(held.Rows) != 32 {
				t.Fatal("committed count", len(held.Rows))
			}
			for i, row := range held.Rows {
				want := mutationPayload(i + 1)
				if row["payload"].Null != (want == nil) || !bytes.Equal(row["payload"].Blob, want) {
					t.Fatalf("payload %d changed", i+1)
				}
			}
			r := isolationExec(t, s, "SELECT COUNT(*) AS n FROM pm WHERE label='new'")
			if r.Rows[0]["n"].Int != 32 {
				t.Fatal("index maintenance", r)
			}
			// Catalog changes must invalidate a warmed INSERT recipe before triggers.
			isolationExec(t, s, "CREATE TABLE pm_audit(id INTEGER PRIMARY KEY);CREATE TRIGGER pm_insert AFTER INSERT ON pm BEGIN INSERT INTO pm_audit VALUES(NEW.b);END;BEGIN")
			if _, e = s.ExecPrepared(insert, 2, 33, 330, 0, []byte{3, 3}, "triggered"); e != nil {
				t.Fatal(e)
			}
			isolationExec(t, s, "COMMIT")
			if r := isolationExec(t, s, "SELECT COUNT(*) AS n FROM pm_audit"); r.Rows[0]["n"].Int != 1 {
				t.Fatal("trigger was bypassed")
			}
			isolationExec(t, s, "DROP TRIGGER pm_insert;CREATE TABLE checked(id INTEGER PRIMARY KEY,n INTEGER CHECK(n>0));CREATE TABLE parent(id INTEGER PRIMARY KEY);CREATE TABLE child(id INTEGER PRIMARY KEY,p INTEGER REFERENCES parent(id));BEGIN")
			for _, query := range []string{"INSERT INTO checked VALUES(?,?)", "INSERT INTO child VALUES(?,?)"} {
				p, e := s.Prepare(query)
				if e != nil {
					t.Fatal(e)
				}
				_, e = s.ExecPrepared(p, 1, -1)
				p.Close()
				if e == nil {
					t.Fatalf("constraint bypass: %s", query)
				}
			}
			returning, e := s.Prepare("INSERT INTO checked VALUES(?,?) RETURNING n")
			if e != nil {
				t.Fatal(e)
			}
			r, e = s.ExecPrepared(returning, 2, 5)
			returning.Close()
			if e != nil || len(r.Rows) != 1 || r.Rows[0]["n"].Int != 5 {
				t.Fatalf("RETURNING %+v %v", r, e)
			}
			isolationExec(t, s, "ROLLBACK")
			// Results retained before later writes/session closure keep their bytes.
			isolationExec(t, s, "BEGIN")
			if _, e = s.ExecPrepared(update, 1, "later", 4, 3); e != nil {
				t.Fatal(e)
			}
			isolationExec(t, s, "ROLLBACK")
			s.Close()
			for i, row := range held.Rows {
				if !bytes.Equal(row["payload"].Blob, mutationPayload(i+1)) {
					t.Fatal("retained result overwritten")
				}
			}
			if _, e = d.CheckIntegrity(); e != nil {
				t.Fatal(e)
			}
			if e = d.Close(); e != nil {
				t.Fatal(e)
			}
			d, e = Open(path, options)
			if e != nil {
				t.Fatal(e)
			}
			defer d.Close()
			r, e = d.Exec("SELECT COUNT(*) AS n,SUM(n) AS total FROM pm")
			if e != nil || r.Rows[0]["n"].Int != 33 || r.Rows[0]["total"].Int != 528 {
				t.Fatalf("reopen result%+v err%v", r, e)
			}
			if _, e = d.CheckIntegrity(); e != nil {
				t.Fatal(e)
			}
		})
	}
}

func TestPreparedPointLockingProjectionOwnedAndContention(t *testing.T) {
	d := namespaceTestDatabase(t)
	a, b := isolationSession(t, d), isolationSession(t, d)
	isolationExec(t, a, "CREATE TABLE lock_projection(a INTEGER,b INTEGER,v INTEGER,payload BLOB,PRIMARY KEY(a,b));INSERT INTO lock_projection VALUES(1,2,10,X'010203'),(1,3,30,X'')")
	prepare := func(s *Session, q string) *Stmt {
		t.Helper()
		p, e := s.Prepare(q)
		if e != nil {
			t.Fatal(e)
		}
		t.Cleanup(func() { p.Close() })
		return p
	}
	locked := prepare(a, "SELECT v,payload FROM lock_projection WHERE a=? AND b=? FOR UPDATE")
	skip := prepare(b, "SELECT v FROM lock_projection WHERE a=? AND b=? FOR UPDATE SKIP LOCKED")
	nowait := prepare(b, "SELECT v FROM lock_projection WHERE a=? AND b=? FOR UPDATE NOWAIT")
	isolationExec(t, a, "BEGIN")
	owned, e := a.ExecPrepared(locked, 1, 2)
	if e != nil {
		t.Fatal(e)
	}
	isolationExec(t, b, "BEGIN")
	if r, e := b.ExecPrepared(skip, 1, 2); e != nil || len(r.Rows) != 0 {
		t.Fatalf("SKIP LOCKED %+v %v", r, e)
	}
	if _, e = b.ExecPrepared(nowait, 1, 2); !errors.Is(e, ErrLockNotAvailable) {
		t.Fatalf("NOWAIT %v", e)
	}
	wait := prepare(b, "SELECT v FROM lock_projection WHERE a=? AND b=? FOR UPDATE")
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, e = b.ExecPreparedContext(ctx, wait, 1, 2); !errors.Is(e, context.DeadlineExceeded) && !errors.Is(e, ErrLockTimeout) {
		t.Fatalf("cancelled wait %v", e)
	}
	isolationExec(t, b, "ROLLBACK")
	isolationExec(t, a, "COMMIT")
	isolationExec(t, b, "UPDATE lock_projection SET v=20,payload=X'090909' WHERE a=1 AND b=2")
	if owned.Rows[0]["v"].Int != 10 || !bytes.Equal(owned.Rows[0]["payload"].Blob, []byte{1, 2, 3}) {
		t.Fatal("owned lock result overwritten")
	}
	isolationExec(t, a, "BEGIN")
	var dst ExecResult
	for _, key := range []int{2, 3, 99, 2} {
		r, e := a.ExecPreparedInto(&dst, locked, 1, key)
		if e != nil {
			t.Fatal(e)
		}
		if key == 99 {
			if len(r.Rows) != 0 {
				t.Fatal("missing row")
			}
		} else if len(r.Rows) != 1 {
			t.Fatal("missing locked row")
		}
	}
	isolationExec(t, a, "ROLLBACK")
	// LIMIT 0 must not acquire a row lock. OFFSET remains on the general path.
	zero := prepare(a, "SELECT v FROM lock_projection WHERE a=? AND b=? LIMIT 0 FOR UPDATE")
	isolationExec(t, a, "BEGIN")
	if r, e := a.ExecPrepared(zero, 1, 2); e != nil || len(r.Rows) != 0 {
		t.Fatal(r, e)
	}
	isolationExec(t, b, "BEGIN")
	if r, e := b.ExecPrepared(nowait, 1, 2); e != nil || len(r.Rows) != 1 {
		t.Fatal(r, e)
	}
	isolationExec(t, b, "ROLLBACK")
	isolationExec(t, a, "ROLLBACK")
}

func TestTupleProjectionResidualAliasesAndRetainedValues(t *testing.T) {
	d := namespaceTestDatabase(t)
	s := isolationSession(t, d)
	isolationExec(t, s, "CREATE TABLE tuple_projection(a INTEGER,b INTEGER,selected BLOB,hidden TEXT,n INTEGER,PRIMARY KEY(a,b));CREATE INDEX tuple_projection_n ON tuple_projection(n)")
	var rows []string
	for i := 0; i < 100; i++ {
		rows = append(rows, fmt.Sprintf("(%d,%d,X'%02x00ff','%s',%d)", i%4, i, byte(i), strings.Repeat("wide", 300), i%7))
	}
	isolationExec(t, s, "INSERT INTO tuple_projection VALUES "+strings.Join(rows, ","))
	queries := []string{"SELECT DISTINCT n FROM tuple_projection WHERE a IN(0,1,2,3) AND b>=0 ORDER BY n", "SELECT b,selected FROM tuple_projection WHERE a=1 AND b>=1 AND LENGTH(hidden)=1200 ORDER BY b DESC LIMIT 5 OFFSET 2", "SELECT p.b AS id,p.selected AS payload FROM tuple_projection p WHERE p.a=1 AND p.b>=1 ORDER BY id DESC LIMIT 5", "SELECT n,COUNT(*) AS c FROM tuple_projection WHERE a IN(0,1,2,3) GROUP BY n ORDER BY n", "SELECT * FROM tuple_projection WHERE a=1 AND b>=1 ORDER BY b DESC LIMIT 2"}
	var held []*ExecResult
	for _, query := range queries {
		r, e := s.Exec(query)
		if e != nil {
			t.Fatal(e)
		}
		held = append(held, r)
	}
	if len(held[0].Rows) != 7 {
		t.Fatal("DISTINCT projection")
	}
	for i, row := range held[0].Rows {
		if row["n"].Int != int64(i) {
			t.Fatal("numeric ordering")
		}
	}
	for i, row := range held[1].Rows {
		want := 89 - i*4
		if row["b"].Int != int64(want) || !bytes.Equal(row["selected"].Blob, []byte{byte(want), 0, 255}) {
			t.Fatal("residual/window projection")
		}
	}
	if len(held[4].Rows) != 2 || len(held[4].Rows[0]) != 5 {
		t.Fatal("star fallback projection")
	}
	isolationExec(t, s, "BEGIN;UPDATE tuple_projection SET selected=X'09' WHERE a=1 AND b=89;SELECT selected FROM tuple_projection WHERE a IN(0,1,2,3) AND b>=0;ROLLBACK")
	for i, row := range held[1].Rows {
		want := 89 - i*4
		if !bytes.Equal(row["selected"].Blob, []byte{byte(want), 0, 255}) {
			t.Fatal("retained projection bytes changed")
		}
	}
}
