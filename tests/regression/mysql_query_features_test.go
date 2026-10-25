package regression

import (
	"fmt"
	"testing"

	"slopdb/db"
	"slopdb/storage"
)

func TestMySQLGroupedJoinsPreserveOuterRowsAndReopen(t *testing.T) {
	e := newRegressionEnv(t)
	d, err := e.openDatabase(e.path("mysql-query"), storage.DefaultOptions())
	if err != nil {
		t.Fatal(err)
	}
	exec := func(text string) *db.ExecResult {
		t.Helper()
		r, err := d.Exec(text)
		if err != nil {
			t.Fatalf("%s: %v", text, err)
		}
		return r
	}
	exec("CREATE TABLE a(id INTEGER PRIMARY KEY);CREATE TABLE b(id INTEGER PRIMARY KEY,aid INTEGER);CREATE TABLE c(id INTEGER PRIMARY KEY,bid INTEGER,v TEXT)")
	exec("INSERT INTO a VALUES(1),(2),(3);INSERT INTO b VALUES(10,1),(11,2);INSERT INTO c VALUES(100,10,'x'),(101,10,'y')")
	query := "SELECT a.id AS id,c.v AS v FROM a LEFT JOIN (b JOIN c ON b.id=c.bid) ON a.id=b.aid ORDER BY a.id,c.v"
	exec("CREATE VIEW grouped AS " + query)
	check := func() {
		for _, q := range []string{query, "SELECT * FROM grouped"} {
			r := exec(q)
			if len(r.Rows) != 4 {
				t.Fatalf("%s got %+v", q, r.Rows)
			}
			for i, want := range []int64{1, 1, 2, 3} {
				if r.Rows[i]["id"].Int != want {
					t.Fatalf("row %d: %v", i, r.Rows[i])
				}
				if i >= 2 && !r.Rows[i]["v"].Null {
					t.Fatal("outer row lost NULL extension")
				}
			}
		}
		r := exec("SELECT a.id AS id FROM a LEFT JOIN (b JOIN c ON b.id=c.bid) ON a.id=b.aid WHERE c.v IS NULL ORDER BY a.id")
		if len(r.Rows) != 2 || r.Rows[0]["id"].Int != 2 || r.Rows[1]["id"].Int != 3 {
			t.Fatal(r.Rows)
		}
		r = exec("SELECT b.aid AS id,c.v AS v FROM (b JOIN c ON b.id=c.bid) ORDER BY c.id")
		if len(r.Rows) != 2 {
			t.Fatal(r.Rows)
		}
		stmt, err := d.Prepare(query + " LIMIT ?")
		if err != nil {
			t.Fatal(err)
		}
		defer stmt.Close()
		r, err = stmt.Exec(3)
		if err != nil || len(r.Rows) != 3 {
			t.Fatal(r, err)
		}
	}
	check()
	if err = d.Close(); err != nil {
		t.Fatal(err)
	}
	d, err = e.openDatabase(e.path("mysql-query"), storage.DefaultOptions())
	if err != nil {
		t.Fatal(err)
	}
	check()
	exec("DELETE FROM c")
	r := exec(query)
	if len(r.Rows) != 3 {
		t.Fatal(r.Rows)
	}
	for _, row := range r.Rows {
		if !row["v"].Null {
			t.Fatal(row)
		}
	}
	if err = d.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestMySQLGroupConcatPreparedAndPersisted(t *testing.T) {
	e := newRegressionEnv(t)
	d, err := e.openDatabase(e.path("concat"), storage.DefaultOptions())
	if err != nil {
		t.Fatal(err)
	}
	for _, q := range []string{"CREATE TABLE words(id INTEGER PRIMARY KEY,v TEXT)", "INSERT INTO words VALUES(1,'b'),(2,'a'),(3,'b'),(4,NULL)", "CREATE VIEW concatenated AS SELECT GROUP_CONCAT(DISTINCT v ORDER BY v DESC SEPARATOR '|') AS v FROM words"} {
		if _, err = d.Exec(q); err != nil {
			t.Fatal(err)
		}
	}
	for pass := 0; pass < 2; pass++ {
		r, err := d.Exec("SELECT * FROM concatenated")
		if err != nil || r.Rows[0]["v"].Text != "b|a" {
			t.Fatal(r, err)
		}
		p, err := d.Prepare("SELECT GROUP_CONCAT(v ORDER BY id SEPARATOR ?) AS v FROM words")
		if err != nil {
			t.Fatal(err)
		}
		for _, sep := range []string{"/", "-"} {
			r, err = p.Exec(sep)
			if err != nil || r.Rows[0]["v"].Text != fmt.Sprintf("b%sa%sb", sep, sep) {
				t.Fatal(r, err)
			}
		}
		p.Close()
		if err = d.Close(); err != nil {
			t.Fatal(err)
		}
		if pass == 0 {
			d, err = e.openDatabase(e.path("concat"), storage.DefaultOptions())
			if err != nil {
				t.Fatal(err)
			}
		}
	}
}
