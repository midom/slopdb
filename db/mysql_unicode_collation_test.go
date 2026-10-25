package db

import (
	"strings"
	"testing"

	"slopdb/sql"
)

func TestUnicodeTableAndColumnCollationsPersistWithIndexes(t *testing.T) {
	path := t.TempDir()
	d, e := Open(path, nil)
	if e != nil {
		t.Fatal(e)
	}
	defer func() { _ = d.Close() }()
	s := isolationSession(t, d)
	if e = s.SetDialect(sql.DialectMySQL); e != nil {
		t.Fatal(e)
	}
	for _, q := range []string{
		"CREATE TABLE collated(id VARCHAR(40) PRIMARY KEY, word VARCHAR(40), exact VARCHAR(40) COLLATE BINARY, KEY word_idx(word(20))) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_520_ci",
		"INSERT INTO collated VALUES('École','Straße','École'),('😀','😀','😀'),('😁','😁','😁')",
		"CREATE TABLE uniq(name VARCHAR(40), UNIQUE(name)) COLLATE=utf8mb4_unicode_520_ci",
		"INSERT INTO uniq VALUES('Straße')",
		"CREATE TABLE child(name VARCHAR(40) REFERENCES uniq(name)) COLLATE=utf8mb4_unicode_520_ci",
		"INSERT INTO child VALUES('STRASSE')",
		"ALTER TABLE collated ADD COLUMN inherited VARCHAR(40) DEFAULT 'É'",
		"CREATE TABLE copied LIKE collated",
	} {
		isolationExec(t, s, q)
	}
	if _, e = s.Exec("INSERT INTO uniq VALUES('STRASSE')"); e == nil {
		t.Fatal("Unicode unique expansion not enforced")
	}
	check := func() {
		t.Helper()
		for _, q := range []string{
			"SELECT count(*) AS v FROM collated WHERE id='ecole'",
			"SELECT count(*) AS v FROM collated WHERE word='STRASSE'",
			"SELECT count(*) AS v FROM collated WHERE word LIKE 'str%'",
			"SELECT count(*) AS v FROM collated WHERE id='😀'",
			"SELECT count(*) AS v FROM collated WHERE exact='École'",
			"SELECT count(*) AS v FROM collated WHERE inherited='e' AND id='ecole'",
		} {
			isolationValue(t, s, q, 1)
		}
		isolationValue(t, s, "SELECT count(*) AS v FROM collated WHERE exact='ecole'", 0)
		r := isolationExec(t, s, "SHOW CREATE TABLE copied")
		if !strings.Contains(r.Rows[0]["Create table"].Text, "UTF8MB4_UNICODE_520_CI") {
			t.Fatal("LIKE lost collation", r)
		}
	}
	check()
	if _, e = s.Exec("PRAGMA collation=NFKC_CI"); e != nil {
		t.Fatal(e)
	}
	check()
	if e = s.Close(); e != nil {
		t.Fatal(e)
	}
	if e = d.Close(); e != nil {
		t.Fatal(e)
	}
	d, e = Open(path, nil)
	if e != nil {
		t.Fatal(e)
	}
	s = isolationSession(t, d)
	if e = s.SetDialect(sql.DialectMySQL); e != nil {
		t.Fatal(e)
	}
	check()
}
