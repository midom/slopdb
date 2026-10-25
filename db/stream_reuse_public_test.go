package db_test

import (
	"bytes"
	"context"
	stdsql "database/sql"
	"fmt"
	"io"
	"testing"

	"slopdb/db"
	"slopdb/driver"
	"slopdb/sql"
	"slopdb/storage"
)

func streamReuseBlob(id int64) []byte {
	if id%17 == 0 {
		return nil
	}
	if id%19 == 0 {
		return []byte{}
	}
	n := 160 + int(id%97)
	if id == 257 || id == 509 {
		n = 6000
	}
	b := make([]byte, n)
	for j := range b {
		b[j] = byte(id*37 + int64(j)*29)
	}
	return b
}
func streamReuseDatabase(t *testing.T) *db.Database {
	t.Helper()
	o := storage.DefaultOptions()
	o.DisableWALSync = true
	o.PageSize = 4096
	o.BlockCacheSize = 64 << 10
	d, e := db.Open(t.TempDir(), o)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() {
		if e := d.Close(); e != nil {
			t.Error(e)
		}
	})
	if _, e = d.Exec("CREATE TABLE reuse_rows(id BIGINT PRIMARY KEY,visible BIGINT,b BLOB,hidden BIGINT,label TEXT)"); e != nil {
		t.Fatal(e)
	}
	id := int64(0)
	_, e = d.BulkInsert(context.Background(), "reuse_rows", []string{"id", "visible", "b", "hidden", "label"}, func(context.Context) ([]any, error) {
		id++
		if id > 1000 {
			return nil, io.EOF
		}
		var visible any = id * 17
		if id%23 == 0 {
			visible = nil
		}
		var blob any = streamReuseBlob(id)
		if id%17 == 0 {
			blob = nil
		}
		return []any{id, visible, blob, id % 5, fmt.Sprintf("row-%04d-🪐", id)}, nil
	})
	if e != nil {
		t.Fatal(e)
	}
	if _, e = d.Exec("VACUUM"); e != nil {
		t.Fatal(e)
	}
	return d
}
func streamReuseSession(t *testing.T, d *db.Database, dialect sql.Dialect) *db.Session {
	t.Helper()
	s, e := d.NewSession()
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { s.Close() })
	if e = s.SetDialect(dialect); e != nil {
		t.Fatal(e)
	}
	return s
}
func streamReuseCell(t *testing.T, got, want sql.Value) {
	t.Helper()
	if got.Null != want.Null {
		t.Fatalf("NULL: got %#v want %#v", got, want)
	}
	if want.Null {
		return
	}
	if got.Type != want.Type || got.Int != want.Int || got.Text != want.Text || !bytes.Equal(got.Blob, want.Blob) || (want.Type == sql.ColBlob && (got.Blob == nil) != (want.Blob == nil)) {
		t.Fatalf("value got %#v want %#v", got, want)
	}
}
func streamReuseExpected(id int64, shape string) []sql.Value {
	v := sql.NewIntValue(id * 17)
	if id%23 == 0 {
		v = sql.NewNullValue()
	}
	b := sql.NewBlobValue(streamReuseBlob(id))
	if id%17 == 0 {
		b = sql.NewNullValue()
	}
	switch shape {
	case "physical":
		return []sql.Value{sql.NewIntValue(id), v, b}
	case "condition_blob":
		return []sql.Value{sql.NewIntValue(id), v}
	case "star":
		return []sql.Value{sql.NewIntValue(id), v, b, sql.NewIntValue(id % 5), sql.NewTextValue(fmt.Sprintf("row-%04d-🪐", id))}
	case "functions":
		fill := b
		if b.Null {
			fill = sql.NewBlobValue([]byte{127})
		}
		return []sql.Value{sql.NewIntValue(id), b, b, fill, v}
	case "aliases":
		generated := b
		if b.Null {
			generated = sql.NewBlobValue([]byte{127})
		}
		return []sql.Value{sql.NewIntValue(id), sql.NewTextValue(fmt.Sprintf("row-%04d-🪐", id)), b, generated}
	case "duplicates":
		return []sql.Value{sql.NewIntValue(id), b, b}
	}
	panic(shape)
}
func TestStreamingPublicValuesOwnership(t *testing.T) {
	d := streamReuseDatabase(t)
	cases := []struct {
		name, query     string
		reverse, filter bool
	}{
		{"physical", "SELECT id,visible,b FROM reuse_rows WHERE id>=? AND hidden IN(1,3) ORDER BY id", false, true},
		{"condition_blob", "SELECT id,visible FROM reuse_rows WHERE id>=? AND b IS NOT NULL ORDER BY id", false, false},
		{"star", "SELECT * FROM reuse_rows WHERE id>=? ORDER BY id DESC", true, false},
		{"functions", "SELECT id,b AS hidden,CAST(b AS BLOB) AS cast_blob,COALESCE(b,X'7f') AS fill_blob,visible AS id_alias FROM reuse_rows WHERE id>=? AND hidden IN(1,3) ORDER BY id DESC", true, true},
		{"aliases", "SELECT id,label AS b,b AS label,CASE WHEN b IS NULL THEN X'7f' ELSE b END AS generated FROM reuse_rows WHERE id>=? AND hidden IN(1,3) ORDER BY id", false, true},
		{"duplicates", "SELECT r.id,r.b AS duplicate,r.b AS duplicate FROM reuse_rows AS r WHERE r.id>=? AND r.hidden IN(1,3) ORDER BY r.id DESC", true, true},
	}
	for _, dialect := range []sql.Dialect{sql.DialectNative, sql.DialectMySQL, sql.DialectPostgreSQL} {
		for _, tc := range cases {
			for _, prepared := range []bool{false, true} {
				t.Run(fmt.Sprintf("%v/%s/prepared=%v", dialect, tc.name, prepared), func(t *testing.T) {
					s := streamReuseSession(t, d, dialect)
					var rows *db.Rows
					var stmt *db.Stmt
					var e error
					if prepared {
						stmt, e = s.Prepare(tc.query)
						if e == nil {
							rows, e = stmt.Query(int64(1))
						}
					} else {
						query := tc.query
						for i := range query {
							if query[i] == '?' {
								query = query[:i] + "1" + query[i+1:]
								break
							}
						}
						rows, e = s.Query(query)
					}
					if e != nil {
						t.Fatal(e)
					}
					defer rows.Close()
					var ids []int64
					for id := int64(1); id <= 1000; id++ {
						if tc.filter && id%5 != 1 && id%5 != 3 {
							continue
						}
						if tc.name == "condition_blob" && id%17 == 0 {
							continue
						}
						ids = append(ids, id)
					}
					if tc.reverse {
						for l, r := 0, len(ids)-1; l < r; l, r = l+1, r-1 {
							ids[l], ids[r] = ids[r], ids[l]
						}
					}
					var retained, into []([]sql.Value)
					var scratch []sql.Value
					for _, id := range ids {
						if !rows.Next() {
							t.Fatalf("missing id %d: %v", id, rows.Err())
						}
						a, e := rows.Values()
						if e != nil {
							t.Fatal(e)
						}
						b, e := rows.ValuesInto(nil)
						if e != nil {
							t.Fatal(e)
						}
						retained = append(retained, a)
						into = append(into, b)
						scratch, e = rows.ValuesInto(scratch)
						if e != nil {
							t.Fatal(e)
						}
						for j := range scratch {
							if scratch[j].Type == sql.ColBlob && len(scratch[j].Blob) > 0 {
								scratch[j].Blob[0] ^= 255
							}
						}
						current, e := rows.Values()
						if e != nil {
							t.Fatal(e)
						}
						want := streamReuseExpected(id, tc.name)
						if len(current) != len(want) {
							t.Fatal("column count")
						}
						for j := range want {
							streamReuseCell(t, current[j], want[j])
						}
					}
					if rows.Next() || rows.Err() != nil {
						t.Fatalf("extra row/error: %v", rows.Err())
					}
					if e = rows.Close(); e != nil {
						t.Fatal(e)
					}
					if stmt != nil {
						stmt.Close()
					}
					if e = s.Close(); e != nil {
						t.Fatal(e)
					}
					for i, id := range ids {
						want := streamReuseExpected(id, tc.name)
						for j := range want {
							streamReuseCell(t, retained[i][j], want[j])
							streamReuseCell(t, into[i][j], want[j])
						}
					}
				})
			}
		}
	}
}

func TestStreamingDatabaseSQLScannedOwnership(t *testing.T) {
	d := streamReuseDatabase(t)
	pool, e := driver.OpenDB(d)
	if e != nil {
		t.Fatal(e)
	}
	defer pool.Close()
	pool.SetMaxOpenConns(1)
	for _, prepared := range []bool{false, true} {
		t.Run(fmt.Sprint(prepared), func(t *testing.T) {
			q := "SELECT id,visible,b,CAST(b AS BLOB),b FROM reuse_rows WHERE id>=? ORDER BY id DESC"
			var rows *stdsql.Rows
			var stmt *stdsql.Stmt
			var e error
			if prepared {
				stmt, e = pool.Prepare(q)
				if e == nil {
					rows, e = stmt.Query(1)
				}
			} else {
				rows, e = pool.Query(q, 1)
			}
			if e != nil {
				t.Fatal(e)
			}
			defer rows.Close()
			type saved struct {
				id      int64
				v       stdsql.NullInt64
				b, cast []byte
				anyBlob any
			}
			var all []saved
			for rows.Next() {
				var r saved
				if e = rows.Scan(&r.id, &r.v, &r.b, &r.cast, &r.anyBlob); e != nil {
					t.Fatal(e)
				}
				all = append(all, r)
			}
			if e = rows.Err(); e != nil {
				t.Fatal(e)
			}
			rows.Close()
			if stmt != nil {
				stmt.Close()
			}
			if len(all) != 1000 {
				t.Fatalf("rows=%d", len(all))
			}
			for i, r := range all {
				id := int64(1000 - i)
				if r.id != id || r.v.Valid != (id%23 != 0) || (r.v.Valid && r.v.Int64 != id*17) {
					t.Fatalf("scalar row %d: %#v", i, r)
				}
				want := streamReuseBlob(id)
				for _, b := range [][]byte{r.b, r.cast} {
					if !bytes.Equal(b, want) || (b == nil) != (want == nil) {
						t.Fatalf("retained BLOB id %d", id)
					}
				}
				if want == nil {
					if r.anyBlob != nil {
						t.Fatalf("NULL any id%d=%#v", id, r.anyBlob)
					}
				} else {
					b, ok := r.anyBlob.([]byte)
					if !ok || !bytes.Equal(b, want) || (b == nil) != (want == nil) {
						t.Fatalf("any BLOB id%d", id)
					}
				}
			}
		})
	}
}

func TestStreamingSnapshotPrivateOverlayOwnership(t *testing.T) {
	for _, dialect := range []sql.Dialect{sql.DialectNative, sql.DialectMySQL, sql.DialectPostgreSQL} {
		t.Run(fmt.Sprint(dialect), func(t *testing.T) {
			d := streamReuseDatabase(t)
			a := streamReuseSession(t, d, dialect)
			b := streamReuseSession(t, d, dialect)
			if dialect != sql.DialectNative {
				if _, e := a.Exec("SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ"); e != nil {
					t.Fatal(e)
				}
			}
			if _, e := a.Exec("BEGIN"); e != nil {
				t.Fatal(e)
			}
			rows, e := a.Query("SELECT id,visible,b FROM reuse_rows WHERE id>=1 ORDER BY id DESC")
			if e != nil {
				t.Fatal(e)
			}
			defer rows.Close()
			if !rows.Next() {
				t.Fatal(rows.Err())
			}
			first, e := rows.Values()
			if e != nil {
				t.Fatal(e)
			}
			if _, e = b.Exec("UPDATE reuse_rows SET b=X'010203' WHERE id=2"); e != nil {
				t.Fatal(e)
			}
			retained := [][]sql.Value{first}
			for rows.Next() {
				v, e := rows.Values()
				if e != nil {
					t.Fatal(e)
				}
				retained = append(retained, v)
			}
			if e = rows.Err(); e != nil {
				t.Fatal(e)
			}
			rows.Close()
			if len(retained) != 1000 {
				t.Fatalf("rows=%d", len(retained))
			}
			for i, v := range retained {
				want := streamReuseExpected(int64(1000-i), "physical")
				for j := range want {
					streamReuseCell(t, v[j], want[j])
				}
			}
			if _, e = a.Exec("UPDATE reuse_rows SET b=X'040506' WHERE id=100;DELETE FROM reuse_rows WHERE id=101;INSERT INTO reuse_rows VALUES(1001,17017,X'',1,'private')"); e != nil {
				t.Fatal(e)
			}
			rows, e = a.Query("SELECT id,b FROM reuse_rows WHERE id>=1 ORDER BY id DESC")
			if e != nil {
				t.Fatal(e)
			}
			var last int64 = 1002
			count := 0
			for rows.Next() {
				v, e := rows.Values()
				if e != nil {
					t.Fatal(e)
				}
				id := v[0].Int
				if id >= last || id == 101 {
					t.Fatalf("overlay order id=%d last=%d", id, last)
				}
				last = id
				count++
				want := streamReuseBlob(id)
				if id == 100 {
					want = []byte{4, 5, 6}
				}
				if id == 1001 {
					want = []byte{}
				}
				if id == 2 && dialect == sql.DialectNative {
					want = []byte{1, 2, 3}
				}
				streamReuseCell(t, v[1], func() sql.Value {
					if want == nil {
						return sql.NewNullValue()
					}
					return sql.NewBlobValue(want)
				}())
			}
			if rows.Err() != nil || count != 1000 {
				t.Fatalf("overlay rows=%d err=%v", count, rows.Err())
			}
			rows.Close()
			a.Close()
			for i, v := range retained {
				want := streamReuseExpected(int64(1000-i), "physical")
				for j := range want {
					streamReuseCell(t, v[j], want[j])
				}
			}
			b.Close()
		})
	}
}
