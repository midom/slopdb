package db_test

import (
	"path/filepath"
	"sync"
	"testing"

	"slopdb/db"
	"slopdb/sql"
)

func TestPlainColumnProjectionOwnershipAndStages(t *testing.T) {
	path := filepath.Join(t.TempDir(), "database")
	d, err := db.Open(path, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = d.Close() })
	if _, err = d.Exec("CREATE TABLE t(id INTEGER PRIMARY KEY,v INTEGER,b BLOB,hidden INTEGER); INSERT INTO t VALUES(1,7,X'0001',9),(2,8,X'0002',3),(3,9,X'0003',5)"); err != nil {
		t.Fatal(err)
	}
	if err = d.Close(); err != nil {
		t.Fatal(err)
	}
	d, err = db.Open(path, nil)
	if err != nil {
		t.Fatal(err)
	}
	stmt, err := d.Prepare("SELECT id,v,b FROM t WHERE id=? AND v>6")
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Close()
	var wg sync.WaitGroup
	for worker := 0; worker < 100; worker++ {
		s, e := d.NewSession()
		if e != nil {
			t.Fatal(e)
		}
		wg.Add(1)
		go func(s *db.Session) {
			defer wg.Done()
			defer s.Close()
			for n := 0; n < 16; n++ {
				id := int64(n%3 + 1)
				r, e := s.ExecPrepared(stmt, id)
				if e != nil {
					t.Error(e)
					return
				}
				if len(r.Rows) != 1 || len(r.Rows[0]) != 3 || r.Rows[0]["id"].Int != id || r.Rows[0]["v"].Int != id+6 || string(r.Rows[0]["b"].Blob) != string([]byte{0, byte(id)}) {
					t.Error("wrong projected row")
					return
				}
				r.Rows[0]["b"].Blob[0] = 255
				delete(r.Rows[0], "id")
				r.Columns[0].Name = "mutated"
			}
		}(s)
	}
	wg.Wait()
	cases := []struct {
		query  string
		ids    []int64
		column string
	}{
		{"SELECT id FROM t WHERE v>=7 ORDER BY hidden LIMIT 2 OFFSET 1", []int64{3, 1}, "id"},
		{"SELECT DISTINCT v FROM t ORDER BY v DESC", []int64{9, 8, 7}, "v"},
		{"SELECT v AS id,id AS v FROM t WHERE id=1", []int64{7}, "id"},
		{"SELECT ID FROM t WHERE id=1", []int64{1}, "id"},
		{"SELECT t.id FROM t WHERE id=1", []int64{1}, "t.id"},
	}
	for _, c := range cases {
		t.Run(c.query, func(t *testing.T) {
			r, e := d.Exec(c.query)
			if e != nil {
				t.Fatal(e)
			}
			if len(r.Rows) != len(c.ids) {
				t.Fatalf("got %d rows", len(r.Rows))
			}
			for i, id := range c.ids {
				if r.Rows[i][c.column].Int != id {
					t.Fatalf("row %d: %+v", i, r.Rows[i])
				}
			}
			if c.query == cases[0].query && len(r.Rows[0]) != 1 {
				t.Fatal("hidden sort/filter fields leaked")
			}
		})
	}
	for _, query := range []string{"SELECT missing FROM t WHERE id=1", "SELECT id,SUM(v) FROM t"} {
		if _, err = d.Exec(query); err == nil {
			t.Fatalf("accepted invalid query: %s", query)
		}
	}
	s, err := d.NewSession()
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	for _, query := range []string{"BEGIN", "UPDATE t SET v=70 WHERE id=1"} {
		if _, err = s.Exec(query); err != nil {
			t.Fatal(err)
		}
	}
	r, err := s.ExecPrepared(stmt, int64(1))
	if err != nil {
		t.Fatal(err)
	}
	if r.Rows[0]["v"].Int != 70 {
		t.Fatal("private transaction write not projected")
	}
	if _, err = s.Exec("ROLLBACK"); err != nil {
		t.Fatal(err)
	}
	r, err = s.ExecPrepared(stmt, int64(1))
	if err != nil {
		t.Fatal(err)
	}
	if v := r.Rows[0]["v"]; v.Type != sql.ColInteger || v.Int != 7 {
		t.Fatalf("stale value after rollback: %+v", v)
	}
	if err = d.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestPreparedPointProjectionBindingsAndCatalogChanges(t *testing.T) {
	d, err := db.Open(filepath.Join(t.TempDir(), "database"), nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = d.Close() })
	if _, err = d.Exec("CREATE TABLE t(id INTEGER PRIMARY KEY,v INTEGER,b BLOB); INSERT INTO t VALUES(1,7,X'0001'),(2,8,X'0002')"); err != nil {
		t.Fatal(err)
	}
	stmt, err := d.Prepare("SELECT id,v,b FROM t WHERE id=? AND v>? LIMIT ? OFFSET ?")
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Close()
	cases := []struct {
		id            any
		threshold     int64
		limit, offset int64
		rows          int
	}{
		{int64(1), 6, -1, 0, 1}, {int64(1), 7, -1, 0, 0}, {int64(99), 0, -1, 0, 0}, {nil, 0, -1, 0, 0},
		{"1", 6, -1, 0, 1}, {float64(1), 6, -1, 0, 1}, {int64(1), 6, 0, 0, 0}, {int64(1), 6, -1, 1, 0}, {int64(1), 6, -1, -1, 1},
	}
	for round := 0; round < 3; round++ {
		for _, c := range cases {
			r, e := stmt.Exec(c.id, c.threshold, c.limit, c.offset)
			if e != nil {
				t.Fatal(e)
			}
			if len(r.Rows) != c.rows || len(r.Columns) != 3 || r.Columns[0].Name != "id" {
				t.Fatalf("binding %+v: %+v", c, r)
			}
			if c.rows != 0 {
				if r.Rows[0]["id"].Int != 1 || r.Rows[0]["v"].Int != 7 || string(r.Rows[0]["b"].Blob) != "\x00\x01" {
					t.Fatal("wrong projected point row")
				}
				r.Rows[0]["b"].Blob[0] = 255
				delete(r.Rows[0], "id")
			}
			r.Columns[0].Name = "mutated"
		}
	}
	invalid, err := d.Prepare("SELECT id FROM t WHERE id=? AND SUM(v)>0")
	if err != nil {
		t.Fatal(err)
	}
	defer invalid.Close()
	for _, id := range []int64{1, 99} {
		if _, err = invalid.Exec(id); err == nil {
			t.Fatal("aggregate in WHERE accepted")
		}
	}
	star, err := d.Prepare("SELECT * FROM t WHERE id=?")
	if err != nil {
		t.Fatal(err)
	}
	defer star.Close()
	r, err := star.Exec(int64(1))
	if err != nil || len(r.Columns) != 3 {
		t.Fatalf("initial star: %+v %v", r, err)
	}
	if _, err = d.Exec("ALTER TABLE t ADD COLUMN added INTEGER DEFAULT 9"); err != nil {
		t.Fatal(err)
	}
	r, err = star.Exec(int64(1))
	if err != nil || len(r.Columns) != 4 || r.Rows[0]["added"].Int != 9 {
		t.Fatalf("stale projection after ALTER: %+v %v", r, err)
	}
	if _, err = d.Exec("ALTER TABLE t RENAME TO old_t; CREATE TABLE t(id INTEGER PRIMARY KEY,v INTEGER,b BLOB); INSERT INTO t VALUES(1,70,X'0304')"); err != nil {
		t.Fatal(err)
	}
	r, err = stmt.Exec(int64(1), int64(6), int64(-1), int64(0))
	if err != nil || len(r.Rows) != 1 || r.Rows[0]["v"].Int != 70 || string(r.Rows[0]["b"].Blob) != "\x03\x04" {
		t.Fatalf("stale projection after name reuse: %+v %v", r, err)
	}
	if err = d.Close(); err != nil {
		t.Fatal(err)
	}
}
