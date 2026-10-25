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
	// A physical row can be an UPDATE target through two aliases. MariaDB's
	// independent reference applies both target assignments and reports two.
	exec("CREATE TABLE self_update(id INTEGER PRIMARY KEY,v INTEGER,w INTEGER); INSERT INTO self_update VALUES(1,0,0)")
	r = exec("UPDATE self_update AS x JOIN self_update AS y ON x.id=y.id SET x.v=1,y.w=2")
	row := exec("SELECT v,w FROM self_update").Rows[0]
	if r.Changes != 2 || row["v"].Int != 1 || row["w"].Int != 2 {
		t.Fatal(r, row)
	}
	if _, err := d.Exec("UPDATE self_update AS x JOIN self_update AS y ON x.id=y.id SET x.id=2,y.w=3"); err == nil {
		t.Fatal("accepted primary key mutation through multiple aliases")
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

func TestMySQLAlterUnicodeCharset(t *testing.T) {
	e := newRegressionEnv(t)
	d, err := e.openDatabase(e.path("charset"), storage.DefaultOptions())
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
	exec("CREATE TABLE docs(id INTEGER PRIMARY KEY,title TEXT,data BLOB); CREATE INDEX title_idx ON docs(title)")
	exec("INSERT INTO docs VALUES(1,'café',X'ff'),(2,'😀',X'ff')")
	exec("ALTER TABLE docs CONVERT TO CHARACTER SET utf8mb4")
	if _, err := d.Exec("ALTER TABLE docs CONVERT TO CHARACTER SET utf8mb3"); err == nil {
		t.Fatal("accepted supplementary character in utf8mb3")
	}
	if _, err := d.Exec("ALTER TABLE docs CONVERT TO CHARACTER SET utf8mb3, MODIFY COLUMN title VARCHAR(1)"); err == nil {
		t.Fatal("accepted failing multi-action conversion")
	}
	create := exec("SHOW CREATE TABLE docs").Rows[0]["Create table"].Text
	if !strings.Contains(create, "CHARSET=utf8mb4") {
		t.Fatal(create)
	}
	if got := exec("SELECT title,HEX(data) AS data FROM docs WHERE title='café'").Rows; len(got) != 1 || got[0]["data"].Text != "FF" {
		t.Fatal(got)
	}
	exec("DELETE FROM docs WHERE id=2")
	exec("ALTER TABLE docs CONVERT TO CHARACTER SET utf8mb3")
	if _, err := d.Exec("INSERT INTO docs VALUES(3,'😀',X'ff')"); err == nil {
		t.Fatal("new insert ignored converted repertoire")
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	d, err = e.openDatabase(e.path("charset"), storage.DefaultOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	create = exec("SHOW CREATE TABLE docs").Rows[0]["Create table"].Text
	if !strings.Contains(create, "CHARSET=utf8mb3") {
		t.Fatal(create)
	}
	if got := len(exec("SELECT id FROM docs WHERE title='café'").Rows); got != 1 {
		t.Fatal(got)
	}
	for _, text := range []string{"ALTER TABLE docs CONVERT CHARACTER SET utf8mb4", "ALTER TABLE docs CONVERT TO CHARACTER SET missing"} {
		if _, err := d.Exec(text); err == nil {
			t.Errorf("accepted %s", text)
		}
	}
}

func TestMySQLAlterPrefixIndexes(t *testing.T) {
	e := newRegressionEnv(t)
	d, err := e.openDatabase(e.path("prefix-alter"), storage.DefaultOptions())
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
	exec("CREATE TABLE prefixes(id INTEGER PRIMARY KEY,a VARCHAR(20),b VARBINARY(20)); INSERT INTO prefixes VALUES(1,'same prefix one',X'616263'),(2,'same prefix two',X'616264')")
	exec("ALTER TABLE prefixes ADD INDEX online_prefix(a(5),b(2))")
	exec("BEGIN; ALTER TABLE prefixes ADD KEY transactional_prefix(b(3)); COMMIT")
	check := func() {
		r := exec("SHOW INDEX FROM prefixes")
		got := make(map[string]int64)
		for _, row := range r.Rows {
			if !row["Sub_part"].Null {
				got[row["Key_name"].Text+":"+row["Column_name"].Text] = row["Sub_part"].Int
			}
		}
		for key, want := range map[string]int64{"online_prefix:a": 5, "online_prefix:b": 2, "transactional_prefix:b": 3} {
			if got[key] != want {
				t.Fatalf("%s: %v", key, got)
			}
		}
		if n := len(exec("SELECT id FROM prefixes WHERE a='same prefix two' AND b=X'616264'").Rows); n != 1 {
			t.Fatal(n)
		}
		create := exec("SHOW CREATE TABLE prefixes").Rows[0]["Create table"].Text
		if !strings.Contains(create, `"a"(5)`) || !strings.Contains(create, `"b"(2)`) {
			t.Fatal(create)
		}
	}
	check()
	exec("EXPLAIN ALTER TABLE prefixes ADD UNIQUE KEY valid_explain(a(2))")
	for _, text := range []string{"ALTER TABLE prefixes ADD UNIQUE INDEX invalid_unique(a(5))", "ALTER TABLE prefixes ADD CONSTRAINT invalid_constraint UNIQUE KEY (a(5))", "ALTER TABLE prefixes ADD INDEX invalid_number(id(2))", "ALTER TABLE prefixes ADD INDEX invalid_length(a(21))"} {
		if _, err := d.Exec(text); err == nil {
			t.Errorf("accepted %s", text)
		}
	}
	exec("BEGIN")
	if _, err := d.Exec("ALTER TABLE prefixes ADD UNIQUE KEY invalid_tx(a(2))"); err == nil {
		t.Fatal("accepted unique prefix in transaction")
	}
	exec("ROLLBACK")
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	d, err = e.openDatabase(e.path("prefix-alter"), storage.DefaultOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	check()
}

func TestMySQLJoinedTemporaryAndPermanentMutation(t *testing.T) {
	e := newRegressionEnv(t)
	d, err := e.openDatabase(e.path("joined-temporary"), storage.DefaultOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	s, err := d.NewSession()
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	exec := func(text string) *db.ExecResult {
		t.Helper()
		r, err := s.Exec(text)
		if err != nil {
			t.Fatalf("%s: %v", text, err)
		}
		return r
	}
	exec("CREATE TABLE durable(id INTEGER PRIMARY KEY,v INTEGER CHECK(v>0)); INSERT INTO durable VALUES(1,1)")
	exec("CREATE TEMPORARY TABLE scratch(id INTEGER PRIMARY KEY,v INTEGER); INSERT INTO scratch VALUES(1,2)")
	exec("BEGIN; UPDATE durable AS d JOIN scratch AS s ON d.id=s.id SET d.v=3,s.v=4; ROLLBACK")
	if v := exec("SELECT v FROM durable").Rows[0]["v"].Int; v != 1 {
		t.Fatal(v)
	}
	if v := exec("SELECT v FROM scratch").Rows[0]["v"].Int; v != 2 {
		t.Fatal(v)
	}
	exec("UPDATE durable AS d JOIN scratch AS s ON d.id=s.id SET d.v=s.v+1,s.v=d.v+3")
	if v := exec("SELECT v FROM durable").Rows[0]["v"].Int; v != 3 {
		t.Fatal(v)
	}
	if v := exec("SELECT v FROM scratch").Rows[0]["v"].Int; v != 4 {
		t.Fatal(v)
	}
	if _, err := s.Exec("UPDATE scratch AS s JOIN durable AS d ON s.id=d.id SET s.v=99,d.v=-1"); err == nil {
		t.Fatal("accepted failing mixed update")
	}
	if v := exec("SELECT v FROM scratch").Rows[0]["v"].Int; v != 4 {
		t.Fatal("private rows survived failed statement", v)
	}
	if r := exec("DELETE s FROM scratch AS s JOIN durable AS d ON s.id=d.id"); r.Changes != 1 {
		t.Fatal(r)
	}
	if r, err := d.Exec("SELECT v FROM durable"); err != nil || r.Rows[0]["v"].Int != 3 {
		t.Fatal(r, err)
	}
	if _, err := d.Exec("SELECT * FROM scratch"); err == nil {
		t.Fatal("temporary definition leaked")
	}
	if _, err := d.CheckIntegrity(); err != nil {
		t.Fatal(err)
	}
}
