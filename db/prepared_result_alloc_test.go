package db_test

import (
	"bytes"
	"testing"

	"slopdb/db"
	"slopdb/storage"
)

// The fixed fixture and checked results isolate result allocation from loading.
// Compare B/op and allocs/op between owned and explicitly reusable results;
// throughput still needs the separate multi-session test-machine benchmark.
func BenchmarkPreparedPointResultStorage(b *testing.B) {
	for _, reuse := range []bool{false, true} {
		name := "owned"
		if reuse {
			name = "reuse"
		}
		b.Run(name, func(b *testing.B) {
			options := storage.DefaultOptions()
			options.DisableWALSync = true
			database, e := db.Open(b.TempDir(), options)
			if e != nil {
				b.Fatal(e)
			}
			defer database.Close()
			if _, e = database.Exec(`CREATE TABLE point_result(id INTEGER PRIMARY KEY,v INTEGER,payload BLOB)`); e != nil {
				b.Fatal(e)
			}
			insert, e := database.Prepare(`INSERT INTO point_result VALUES(?,?,?)`)
			if e != nil {
				b.Fatal(e)
			}
			payload := bytes.Repeat([]byte{0x5a}, 240)
			if _, e = insert.Exec(int64(1), int64(17), payload); e != nil {
				b.Fatal(e)
			}
			insert.Close()
			session, e := database.NewSession()
			if e != nil {
				b.Fatal(e)
			}
			defer session.Close()
			stmt, e := session.Prepare(`SELECT id,v,payload FROM point_result WHERE id=?`)
			if e != nil {
				b.Fatal(e)
			}
			defer stmt.Close()
			var destination db.ExecResult
			run := func() (*db.ExecResult, error) {
				if reuse {
					return session.ExecPreparedInto(&destination, stmt, int64(1))
				}
				return session.ExecPrepared(stmt, int64(1))
			}
			for i := 0; i < 16; i++ {
				if _, e = run(); e != nil {
					b.Fatal(e)
				}
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				r, e := run()
				if e != nil {
					b.Fatal(e)
				}
				if len(r.Rows) != 1 || r.Rows[0]["id"].Int != 1 || r.Rows[0]["v"].Int != 17 || !bytes.Equal(r.Rows[0]["payload"].Blob, payload) {
					b.Fatal("incorrect point result")
				}
			}
			b.StopTimer()
		})
	}
}

func TestPreparedResultPrivatePredicateStorageAndEmptyReuse(t *testing.T) {
	database := planDatabase(t)
	planExec(t, database, `ALTER TABLE t ADD COLUMN payload BLOB; ALTER TABLE t ADD COLUMN label TEXT DEFAULT 'repeated'; UPDATE t SET payload=X'01020304' WHERE id=1; UPDATE t SET payload=X'0506' WHERE id=2`)
	session, e := database.NewSession()
	if e != nil {
		t.Fatal(e)
	}
	defer session.Close()
	statement := planPrepare(t, database, `SELECT payload,label FROM t WHERE id=? AND v=?`)
	var first, second db.ExecResult
	if _, e = session.ExecPreparedInto(&first, statement, 1, 999); e != nil || len(first.Rows) != 0 || cap(first.Rows) != 1 {
		t.Fatalf("filtered destination lost reusable row: %+v %v", first, e)
	}
	initialBlob := first.Rows[:1][0]["payload"].Blob
	if _, e = session.ExecPreparedInto(&first, statement, 1, 100); e != nil || len(first.Rows) != 1 || len(first.Rows[0]) != 2 {
		t.Fatal(first, e)
	}
	if &initialBlob[0] != &first.Rows[0]["payload"].Blob[0] {
		t.Fatal("filtered destination replaced BLOB buffer")
	}
	if _, e = session.ExecPreparedInto(&second, statement, 2, 200); e != nil {
		t.Fatal(e)
	}
	if !bytes.Equal(first.Rows[0]["payload"].Blob, []byte{1, 2, 3, 4}) || !bytes.Equal(second.Rows[0]["payload"].Blob, []byte{5, 6}) {
		t.Fatal("private predicate scratch aliased independent destinations")
	}
	first.Rows[0]["payload"].Blob[0] = 99
	owned, e := session.ExecPrepared(statement, 1, 100)
	if e != nil || !bytes.Equal(owned.Rows[0]["payload"].Blob, []byte{1, 2, 3, 4}) {
		t.Fatal(owned, e)
	}
	if _, e = session.ExecPreparedInto(&second, statement, 1, 100); e != nil {
		t.Fatal(e)
	}
	if !bytes.Equal(owned.Rows[0]["payload"].Blob, []byte{1, 2, 3, 4}) || owned.Rows[0]["label"].Text != "repeated" {
		t.Fatal("later Into mutated an owned result")
	}
	if _, e = session.ExecPreparedInto(&first, statement, 99, 100); e != nil || len(first.Rows) != 0 {
		t.Fatal(first, e)
	}
	planExec(t, database, `UPDATE t SET label='changed',payload=X'0908' WHERE id=1`)
	if _, e = session.ExecPreparedInto(&first, statement, 1, 100); e != nil || first.Rows[0]["label"].Text != "changed" || !bytes.Equal(first.Rows[0]["payload"].Blob, []byte{9, 8}) {
		t.Fatal(first, e)
	}
	if owned.Rows[0]["label"].Text != "repeated" || !bytes.Equal(owned.Rows[0]["payload"].Blob, []byte{1, 2, 3, 4}) {
		t.Fatal("owned text/BLOB lifetime changed")
	}
}
