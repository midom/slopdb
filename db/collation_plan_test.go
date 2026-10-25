package db

import (
	"strings"
	"testing"

	"slopdb/sql"
)

func TestExplicitColumnCollationsInJoinsAndAggregatesReopen(t *testing.T) {
	dir := t.TempDir()
	d, e := Open(dir, nil)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { _ = d.Close() })
	for _, q := range []string{
		"CREATE TABLE left_ci(id INTEGER PRIMARY KEY,word TEXT COLLATE utf8mb4_unicode_520_ci)",
		"CREATE TABLE right_plain(id INTEGER PRIMARY KEY,word TEXT)",
		"CREATE TABLE exact_docs(id INTEGER PRIMARY KEY,word TEXT COLLATE BINARY)",
		"CREATE TABLE plain_docs(id INTEGER PRIMARY KEY,word TEXT)",
		"CREATE TABLE inherited_ci(id INTEGER PRIMARY KEY,word TEXT) COLLATE=utf8mb4_unicode_520_ci",
		"CREATE TABLE ordered_ci(word TEXT COLLATE utf8mb4_unicode_520_ci,id INTEGER,PRIMARY KEY(word,id))",
		"CREATE TABLE extrema(id INTEGER PRIMARY KEY,word TEXT COLLATE utf8mb4_unicode_520_ci)",
		"INSERT INTO left_ci VALUES(1,'A'),(2,'B'),(3,'Straße'),(4,'missing')",
		"INSERT INTO right_plain VALUES(10,'a'),(11,'b'),(12,'STRASSE'),(13,'A')",
		"INSERT INTO exact_docs VALUES(1,'A'),(2,'a'),(3,'B'),(4,'b')",
		"INSERT INTO plain_docs SELECT * FROM exact_docs",
		"INSERT INTO inherited_ci SELECT * FROM exact_docs",
		"INSERT INTO ordered_ci VALUES('A',1),('a',2),('B',3),('b',4)",
		"INSERT INTO extrema VALUES(1,'Zulu'),(2,'apple')",
	} {
		objectExec(t, d, q)
	}
	check := func(nonbinary bool) {
		t.Helper()
		for _, dialect := range []sql.Dialect{sql.DialectNative, sql.DialectMySQL, sql.DialectPostgreSQL} {
			s, e := d.NewSession()
			if e != nil {
				t.Fatal(e)
			}
			if e = s.SetDialect(dialect); e != nil {
				t.Fatal(e)
			}
			exec := func(q string, prepared bool) *ExecResult {
				t.Helper()
				var r *ExecResult
				var e error
				if prepared {
					p, err := s.Prepare(q)
					if err != nil {
						t.Fatal(q, err)
					}
					r, e = p.Exec()
					p.Close()
				} else {
					r, e = s.Exec(q)
				}
				if e != nil {
					t.Fatalf("%s %s: %v", dialect, q, e)
				}
				return r
			}
			for _, prepared := range []bool{false, true} {
				for _, q := range []string{
					"SELECT l.id AS lid,r.id AS rid FROM main.left_ci l JOIN main.right_plain r ON l.word=r.word ORDER BY l.id,r.id",
					"SELECT l.id AS lid,r.id AS rid FROM main.right_plain r JOIN main.left_ci l ON r.word=l.word ORDER BY l.id,r.id",
					"SELECT l.id AS lid,r.id AS rid FROM (SELECT id,word FROM main.left_ci) l JOIN main.right_plain r ON l.word=r.word ORDER BY l.id,r.id",
				} {
					r := exec(q, prepared)
					if len(r.Rows) != 4 {
						t.Fatalf("%s join prepared=%v: %s => %v", dialect, prepared, q, r.Rows)
					}
				}
				r := exec("SELECT l.id AS lid,r.id AS rid FROM main.left_ci l LEFT JOIN main.right_plain r ON l.word=r.word ORDER BY l.id,r.id", prepared)
				if len(r.Rows) != 5 || !r.Rows[4]["rid"].Null {
					t.Fatalf("outer join lost match/unmatched row: %v", r.Rows)
				}
				r = exec("SELECT COUNT(*) AS n FROM main.left_ci l JOIN main.right_plain r ON l.word=r.word", prepared)
				if r.Rows[0]["n"].Int != 4 {
					t.Fatal("aggregate join lost collated match", r.Rows)
				}
				for _, table := range []string{"inherited_ci", "ordered_ci", "exact_docs", "plain_docs"} {
					want := 4
					if table == "inherited_ci" || table == "ordered_ci" || table == "plain_docs" && nonbinary {
						want = 2
					}
					r := exec("SELECT word,COUNT(*) AS n FROM main."+table+" GROUP BY word", prepared)
					if len(r.Rows) != want {
						t.Fatalf("%s %s groups prepared=%v: %v", dialect, table, prepared, r.Rows)
					}
					r = exec("SELECT COUNT(DISTINCT word) AS n FROM main."+table, prepared)
					if r.Rows[0]["n"].Int != int64(want) {
						t.Fatalf("%s %s distinct: %v", dialect, table, r.Rows)
					}
				}
				r = exec("SELECT MIN(word) AS lo,MAX(word) AS hi FROM main.extrema", prepared)
				if r.Rows[0]["lo"].Text != "apple" || r.Rows[0]["hi"].Text != "Zulu" {
					t.Fatal("bytewise extrema escaped collation evaluation", dialect, r.Rows)
				}
				r = exec("SELECT COUNT(*) AS n FROM main.inherited_ci WHERE word='a'", prepared)
				if r.Rows[0]["n"].Int != 2 {
					t.Fatal("aggregate filter used bytewise comparison", dialect, r.Rows)
				}
			}
			if e = s.Close(); e != nil {
				t.Fatal(e)
			}
		}
		if _, e = d.CheckIntegrity(); e != nil {
			t.Fatal(e)
		}
	}
	check(false)
	objectExec(t, d, "CREATE INDEX right_word ON right_plain(word);CREATE INDEX left_word ON left_ci(word);CREATE INDEX inherited_word ON inherited_ci(word);CREATE INDEX exact_word ON exact_docs(word);CREATE INDEX plain_word ON plain_docs(word)")
	check(false)
	if e = d.Close(); e != nil {
		t.Fatal(e)
	}
	d, e = Open(dir, nil)
	if e != nil {
		t.Fatal(e)
	}
	check(false)
	objectExec(t, d, "PRAGMA collation=NFKC_CI")
	check(true)
	if e = d.Close(); e != nil {
		t.Fatal(e)
	}
	d, e = Open(dir, nil)
	if e != nil {
		t.Fatal(e)
	}
	check(true)
	// Durable table/column semantics remain visible in DDL after all plans run.
	if text := d.GetCreateTableSQL("inherited_ci"); !strings.Contains(strings.ToUpper(text), "UTF8MB4_UNICODE_520_CI") {
		t.Fatal(text)
	}
}
