package regression

import (
	"errors"
	"path/filepath"
	"testing"
	"time"

	"slopdb/db"
)

func TestUnicodeCollationRebuildAndReopen(t *testing.T) {
	env := newRegressionEnv(t)
	dir := filepath.Join(t.TempDir(), "database")
	d, err := env.openDatabase(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	requireExec(t, d, "CREATE TABLE items(k TEXT PRIMARY KEY, v TEXT UNIQUE)")
	requireExec(t, d, "CREATE INDEX items_v ON items(v)")
	requireExec(t, d, "INSERT INTO items VALUES('É', 'Ａ')")

	checkMode := func(database *db.Database, mode string) {
		t.Helper()
		result := requireExec(t, database, "PRAGMA collation")
		if len(result.Rows) != 1 || result.Rows[0]["collation"].Text != mode {
			t.Fatalf("collation = %v, want %s", result.Rows, mode)
		}
	}
	checkCount := func(database *db.Database, query string, want int64) {
		t.Helper()
		result := requireExec(t, database, query)
		if len(result.Rows) != 1 || result.Rows[0]["n"].Int != want {
			t.Fatalf("%s: got %v, want %d", query, result.Rows, want)
		}
	}
	checkMode(d, "BINARY")
	checkCount(d, "SELECT COUNT(*) AS n FROM items WHERE k='é'", 0)
	requireExec(t, d, "PRAGMA collation='NFKC_CI'")
	checkMode(d, "NFKC_CI")
	checkCount(d, "SELECT COUNT(*) AS n FROM items WHERE k='é'", 1)
	checkCount(d, "SELECT COUNT(*) AS n FROM items WHERE v='a'", 1)
	checkCount(d, "SELECT COUNT(*) AS n FROM items WHERE BINARY k='é'", 0)
	checkCount(d, "SELECT COUNT(*) AS n FROM items WHERE k COLLATE BINARY='é'", 0)
	if _, err := d.Exec("INSERT INTO items VALUES('é','other')"); err == nil {
		t.Fatal("canonical-equivalent primary key accepted")
	}
	if _, err := d.Exec("INSERT INTO items VALUES('other','a')"); err == nil {
		t.Fatal("compatibility-equivalent UNIQUE value accepted")
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	d, err = env.openDatabase(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	checkMode(d, "NFKC_CI")
	checkCount(d, "SELECT COUNT(*) AS n FROM items WHERE k='é'", 1)
	checkCount(d, "SELECT COUNT(*) AS n FROM items WHERE v='a'", 1)
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestUnicodeCollationChangeRejectsCollisions(t *testing.T) {
	env := newRegressionEnv(t)
	d, err := env.openDatabase(env.path("collision"), nil)
	if err != nil {
		t.Fatal(err)
	}
	requireExec(t, d, "CREATE TABLE names(k TEXT PRIMARY KEY)")
	requireExec(t, d, "INSERT INTO names VALUES('é'),('é')")
	if _, err := d.Exec("PRAGMA collation='NFC'"); err == nil {
		t.Fatal("colliding primary keys accepted during rebuild")
	}
	result := requireExec(t, d, "PRAGMA collation")
	if result.Rows[0]["collation"].Text != "BINARY" {
		t.Fatalf("failed change altered collation: %v", result.Rows)
	}
	result = requireExec(t, d, "SELECT COUNT(*) AS n FROM names")
	if result.Rows[0]["n"].Int != 2 {
		t.Fatalf("failed change altered rows: %v", result.Rows)
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestUnicodeCollationModesAndBinaryColumn(t *testing.T) {
	env := newRegressionEnv(t)
	d, err := env.openDatabase(env.path("modes"), nil)
	if err != nil {
		t.Fatal(err)
	}
	requireExec(t, d, "CREATE TABLE words(id INTEGER PRIMARY KEY, ordinary TEXT, exact TEXT COLLATE BINARY)")
	requireExec(t, d, "CREATE INDEX words_ordinary ON words(ordinary)")
	requireExec(t, d, "CREATE INDEX words_exact ON words(exact)")
	requireExec(t, d, "INSERT INTO words VALUES(1,'Ａ','Ａ'),(2,'é','é')")
	count := func(query string, want int64) {
		t.Helper()
		result := requireExec(t, d, query)
		if len(result.Rows) != 1 || result.Rows[0]["n"].Int != want {
			t.Fatalf("%s: got %v, want %d", query, result.Rows, want)
		}
	}
	requireExec(t, d, "PRAGMA collation=NFC")
	count("SELECT COUNT(*) AS n FROM words WHERE ordinary='A'", 0)
	count("SELECT COUNT(*) AS n FROM words WHERE ordinary='é'", 1)
	requireExec(t, d, "PRAGMA collation=NFKC_CI")
	count("SELECT COUNT(*) AS n FROM words WHERE ordinary='a'", 1)
	count("SELECT COUNT(*) AS n FROM words WHERE exact='a'", 0)
	count("SELECT COUNT(*) AS n FROM words WHERE BINARY ordinary='a'", 0)
	count("SELECT COUNT(*) AS n FROM words WHERE 'a'=BINARY ordinary", 0)
	count("SELECT COUNT(*) AS n FROM words WHERE 'a'='Ａ'", 2)
	count("SELECT COUNT(*) AS n FROM words WHERE BINARY 'a'='Ａ'", 0)
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestUnicodeCollationTransactionRollback(t *testing.T) {
	env := newRegressionEnv(t)
	dir := env.path("transaction")
	d, err := env.openDatabase(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	requireExec(t, d, "CREATE TABLE keys(k TEXT PRIMARY KEY)")
	requireExec(t, d, "INSERT INTO keys VALUES('é')")
	requireExec(t, d, "BEGIN")
	requireExec(t, d, "PRAGMA collation='NFC_CI'")
	result := requireExec(t, d, "SELECT COUNT(*) AS n FROM keys WHERE k='É'")
	if result.Rows[0]["n"].Int != 1 {
		t.Fatalf("transaction did not use new collation: %v", result.Rows)
	}
	requireExec(t, d, "ROLLBACK")
	result = requireExec(t, d, "PRAGMA collation")
	if result.Rows[0]["collation"].Text != "BINARY" {
		t.Fatalf("rollback retained mode: %v", result.Rows)
	}
	result = requireExec(t, d, "SELECT COUNT(*) AS n FROM keys WHERE k='É'")
	if result.Rows[0]["n"].Int != 0 {
		t.Fatalf("rollback retained rebuilt key: %v", result.Rows)
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	d, err = env.openDatabase(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	result = requireExec(t, d, "PRAGMA collation")
	if result.Rows[0]["collation"].Text != "BINARY" {
		t.Fatalf("reopened database retained rolled back mode: %v", result.Rows)
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestUnicodeCollationUniqueLockEquivalence(t *testing.T) {
	env := newRegressionEnv(t)
	d, err := env.openDatabase(env.path("locks"), nil)
	if err != nil {
		t.Fatal(err)
	}
	requireExec(t, d, "CREATE TABLE names(id INTEGER PRIMARY KEY, name TEXT UNIQUE)")
	requireExec(t, d, "PRAGMA collation='NFC_CI'")
	first, err := d.NewSession()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = first.Close() })
	second, err := d.NewSession()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = second.Close() })
	if _, err := first.Exec("BEGIN; INSERT INTO names VALUES(1,'É')"); err != nil {
		t.Fatal(err)
	}
	zero := time.Duration(0)
	_, err = second.ExecWithOptions("INSERT INTO names VALUES(2,'é')", db.ExecOptions{LockTimeout: &zero})
	if !errors.Is(err, db.ErrLockTimeout) {
		t.Fatalf("canonically equivalent UNIQUE value did not contend: %v", err)
	}
	if _, err := first.Exec("ROLLBACK"); err != nil {
		t.Fatal(err)
	}
	if _, err := second.Exec("INSERT INTO names VALUES(2,'é')"); err != nil {
		t.Fatalf("lock remained after rollback: %v", err)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	if err := second.Close(); err != nil {
		t.Fatal(err)
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
}
