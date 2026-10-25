package regression

import (
	"fmt"
	"strings"
	"testing"

	"slopdb/db"
	"slopdb/storage"
)

func TestMySQLJoinedMutations(t *testing.T) {
	e := newRegressionEnv(t)
	d, err := e.openDatabase(e.path("joined-mutations"), storage.DefaultOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	exec := func(text string) *db.ExecResult {
		t.Helper()
		r, err := d.Exec(text)
		if err != nil {
			t.Fatalf("%s: %v", text, err)
		}
		return r
	}
	exec("CREATE TABLE a(id INTEGER PRIMARY KEY,v INTEGER CHECK(v>=0)); CREATE TABLE b(id INTEGER PRIMARY KEY,aid INTEGER,v INTEGER); CREATE INDEX bv ON b(v)")
	exec("INSERT INTO a VALUES(1,10),(2,20),(3,30); INSERT INTO b VALUES(11,1,100),(12,1,100),(13,2,200)")
	r := exec("UPDATE a JOIN b ON a.id=b.aid SET a.v=a.v+1,b.v=b.v+1 WHERE a.id=1")
	if r.Changes != 3 {
		t.Fatalf("updated duplicate matches: %+v", r)
	}
	if got := exec("SELECT v FROM a WHERE id=1").Rows[0]["v"].Int; got != 11 {
		t.Fatal(got)
	}
	if got := len(exec("SELECT id FROM b WHERE v=101").Rows); got != 2 {
		t.Fatal(got)
	}
	prepared, err := d.Prepare("UPDATE a AS x,b AS y SET x.v=y.v WHERE x.id=y.aid AND y.id=?")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = prepared.Exec(13); err != nil {
		t.Fatal(err)
	}
	if _, err = prepared.Exec(11); err != nil {
		t.Fatal(err)
	}
	if err = prepared.Close(); err != nil {
		t.Fatal(err)
	}
	if got := exec("SELECT v FROM a WHERE id=2").Rows[0]["v"].Int; got != 200 {
		t.Fatal(got)
	}
	// Both targets are statement-atomic when a later candidate fails a CHECK.
	if _, err := d.Exec("UPDATE a AS x JOIN b AS y ON x.id=y.aid SET x.v=-1,y.v=9"); err == nil {
		t.Fatal("accepted failing update")
	}
	if got := exec("SELECT COUNT(*) AS n FROM b WHERE v=9").Rows[0]["n"].Int; got != 0 {
		t.Fatal("partial joined update", got)
	}
	exec("BEGIN")
	r = exec("DELETE x,y FROM a AS x JOIN b AS y ON x.id=y.aid WHERE x.id=1")
	if r.Changes != 3 {
		t.Fatalf("deleted duplicate matches: %+v", r)
	}
	exec("ROLLBACK")
	if got := len(exec("SELECT id FROM a").Rows); got != 3 {
		t.Fatal(got)
	}
	r = exec("DELETE FROM y USING a AS x JOIN b AS y ON x.id=y.aid WHERE x.id=2")
	if r.Changes != 1 {
		t.Fatal(r)
	}
	// A NULL-extended right side contributes no physical deletion candidate.
	r = exec("DELETE y FROM a AS x LEFT JOIN b AS y ON x.id=y.aid WHERE y.id IS NULL")
	if r.Changes != 0 {
		t.Fatal(r)
	}
	exec("CREATE TABLE hidden(v INTEGER);INSERT INTO hidden VALUES(1),(1),(2)")
	r = exec("DELETE x FROM hidden AS x JOIN hidden AS y ON x.v=y.v WHERE y.v=1")
	if r.Changes != 2 || len(exec("SELECT v FROM hidden").Rows) != 1 {
		t.Fatal(r)
	}
	for _, text := range []string{
		"UPDATE a JOIN b ON a.id=b.aid SET v=1",
		"UPDATE a AS x JOIN b AS x ON x.id=x.aid SET x.v=1",
		"UPDATE a AS x JOIN b AS y ON x.id=y.aid SET missing.v=1",
		"UPDATE a AS x JOIN b AS y ON x.id=y.aid SET x.v=1 LIMIT 1",
		"DELETE missing FROM a AS x JOIN b AS y ON x.id=y.aid",
		"DELETE x FROM a AS x JOIN b AS y ON missing.id=y.aid",
	} {
		if _, err := d.Exec(text); err == nil {
			t.Errorf("accepted %s", text)
		}
	}
}

func TestMySQLCreateTableLikePreservesDefinitionAndReopens(t *testing.T) {
	e := newRegressionEnv(t)
	d, err := e.openDatabase(e.path("table-like"), storage.DefaultOptions())
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
	exec("CREATE TABLE parent(id INTEGER PRIMARY KEY)")
	exec("CREATE TABLE original(id INTEGER PRIMARY KEY AUTOINCREMENT, parent_id INTEGER REFERENCES parent(id), label VARBINARY(20) NOT NULL DEFAULT X'78', state ENUM('open','closed') NOT NULL DEFAULT 'open', title VARCHAR(30), CHECK(length(label)>0), UNIQUE KEY label_key(label), KEY label_prefix(label(5)), FULLTEXT KEY title_key(title)) DEFAULT CHARSET=utf8mb4")
	exec("INSERT INTO original(title) VALUES('original row')")
	exec("CREATE TABLE copied LIKE original")
	if n := exec("SELECT COUNT(*) AS n FROM copied").Rows[0]["n"].Int; n != 0 {
		t.Fatal("copied data", n)
	}
	exec("INSERT INTO copied(parent_id,title) VALUES(999,'searchable title')") // foreign keys are not copied
	row := exec("SELECT id,label,state FROM copied").Rows[0]
	if row["id"].Int != 1 || string(row["label"].Blob) != "x" || row["state"].Text != "open" {
		t.Fatal(row)
	}
	for _, text := range []string{"INSERT INTO copied(title) VALUES('duplicate')", "INSERT INTO copied(label) VALUES(X'')", "INSERT INTO copied(label,state) VALUES(X'79','invalid')"} {
		if _, err := d.Exec(text); err == nil {
			t.Errorf("accepted %s", text)
		}
	}
	check := func() {
		r := exec("SHOW INDEX FROM copied")
		var names []string
		for _, row := range r.Rows {
			names = append(names, row["Key_name"].Text)
		}
		text := strings.Join(names, ",")
		if !strings.Contains(text, "label_key") || !strings.Contains(text, "title_key") {
			t.Fatal(names)
		}
		if n := len(exec("SELECT title FROM copied WHERE MATCH(title) AGAINST('searchable')").Rows); n != 1 {
			t.Fatal(n)
		}
	}
	check()
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	d, err = e.openDatabase(e.path("table-like"), storage.DefaultOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	check()
	exec("DROP INDEX label_key ON copied")
	exec("INSERT INTO copied(title) VALUES('now permitted')")
	exec("ALTER TABLE copied DROP INDEX title_key")
	if len(exec("SHOW INDEX FROM original").Rows) < 3 {
		t.Fatal("source indexes changed")
	}
	for _, name := range []string{"missing", "parent"} {
		text := fmt.Sprintf("CREATE TABLE copied LIKE %s", name)
		if _, err := d.Exec(text); err == nil {
			t.Errorf("accepted duplicate: %s", text)
		}
	}
	exec("CREATE TABLE IF NOT EXISTS copied LIKE missing")
}
