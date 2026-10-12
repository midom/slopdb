package regression

import (
	"strings"
	"testing"

	"slopdb/db"
)

func TestVaryingTypesLengthsCollationAndReopen(t *testing.T) {
	env := newRegressionEnv(t)
	dir := env.path("varying")
	d, err := env.openDatabase(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	requireExec(t, d, "CREATE TABLE items(name VARCHAR(3) PRIMARY KEY, exact VARCHAR(3) COLLATE BINARY UNIQUE, payload VARBINARY(4))")
	requireExec(t, d, "CREATE INDEX items_exact ON items(exact)")
	requireExec(t, d, "INSERT INTO items VALUES('É','É',X'00FF10')")
	requireExec(t, d, "PRAGMA collation='NFC_CI'")
	checkCount := func(database *db.Database, query string, want int64) {
		t.Helper()
		result := requireExec(t, database, query)
		if len(result.Rows) != 1 || result.Rows[0]["n"].Int != want {
			t.Fatalf("%s: got %v, want %d", query, result.Rows, want)
		}
	}
	checkCount(d, "SELECT COUNT(*) AS n FROM items WHERE name='é'", 1)
	checkCount(d, "SELECT COUNT(*) AS n FROM items WHERE exact='é'", 0)
	if _, err := d.Exec("INSERT INTO items VALUES('é','other',X'01')"); err == nil {
		t.Fatal("VARCHAR primary key ignored collation equivalence")
	}
	for _, query := range []string{
		"INSERT INTO items VALUES('abcd','ok',X'01')",
		"INSERT INTO items VALUES('ok','abcd',X'01')",
		"INSERT INTO items VALUES('ok','ok',X'0102030405')",
		"INSERT INTO items SELECT 'abcd','ok',X'01'",
		"UPDATE items SET exact='abcd' WHERE name='É'",
		"UPDATE items SET payload=X'0102030405' WHERE name='É'",
	} {
		if _, err := d.Exec(query); err == nil {
			t.Fatalf("accepted overlength value: %s", query)
		}
	}
	requireExec(t, d, "INSERT INTO items VALUES('猫犬鳥','ok',X'01020304')")
	requireExec(t, d, "CREATE TABLE single_character(v VARCHAR(1))")
	requireExec(t, d, "INSERT INTO single_character VALUES('é')")
	if _, err := d.Exec("INSERT INTO single_character VALUES('é')"); err == nil {
		t.Fatal("VARCHAR length measured normalized key rather than stored characters")
	}
	checkCount(d, "SELECT COUNT(*) AS n FROM items", 2)
	result := requireExec(t, d, "PRAGMA table_info(items)")
	for i, want := range []string{"VARCHAR(3)", "VARCHAR(3)", "VARBINARY(4)"} {
		if got := result.Rows[i]["type"].Text; got != want {
			t.Fatalf("PRAGMA type %d = %q, want %q", i, got, want)
		}
	}
	result = requireExec(t, d, "SHOW CREATE TABLE items")
	found := false
	for _, value := range result.Rows[0] {
		if strings.Contains(value.Text, "VARCHAR(3)") && strings.Contains(value.Text, "VARBINARY(4)") {
			found = true
		}
	}
	if !found {
		t.Fatalf("SHOW CREATE TABLE lost lengths: %v", result.Rows)
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	d, err = env.openDatabase(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	checkCount(d, "SELECT COUNT(*) AS n FROM items WHERE name='é'", 1)
	checkCount(d, "SELECT COUNT(*) AS n FROM items WHERE exact='é'", 0)
	if _, err := d.Exec("UPDATE items SET name='abcd' WHERE name='É'"); err == nil {
		t.Fatal("reopened VARCHAR lost length limit")
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestVaryingTypesAlterValidation(t *testing.T) {
	env := newRegressionEnv(t)
	d, err := env.openDatabase(env.path("alter"), nil)
	if err != nil {
		t.Fatal(err)
	}
	requireExec(t, d, "CREATE TABLE t(id INTEGER PRIMARY KEY, name VARCHAR(4), data VARBINARY(4))")
	requireExec(t, d, "INSERT INTO t VALUES(1,'abcd',X'01020304')")
	if _, err := d.Exec("ALTER TABLE t ALTER COLUMN name TYPE VARCHAR(3)"); err == nil {
		t.Fatal("ALTER accepted existing overlength VARCHAR")
	}
	if _, err := d.Exec("ALTER TABLE t ALTER COLUMN data TYPE VARBINARY(3)"); err == nil {
		t.Fatal("ALTER accepted existing overlength VARBINARY")
	}
	result := requireExec(t, d, "PRAGMA table_info(t)")
	if result.Rows[1]["type"].Text != "VARCHAR(4)" || result.Rows[2]["type"].Text != "VARBINARY(4)" {
		t.Fatalf("failed ALTER changed metadata: %v", result.Rows)
	}
	requireExec(t, d, "ALTER TABLE t ALTER COLUMN name TYPE VARCHAR(5)")
	requireExec(t, d, "ALTER TABLE t MODIFY COLUMN data VARBINARY(5)")
	requireExec(t, d, "ALTER TABLE t ADD COLUMN tag VARCHAR(2) DEFAULT 'ok'")
	if _, err := d.Exec("UPDATE t SET tag='long' WHERE id=1"); err == nil {
		t.Fatal("added VARCHAR lost length limit")
	}
	requireExec(t, d, "INSERT INTO t(id,name,data) VALUES(2,'abcde',X'0102030405')")
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestVaryingTypesOnlineLengthChanges(t *testing.T) {
	env := newRegressionEnv(t)
	path := env.path("online_lengths")
	d, err := env.openDatabase(path, nil)
	if err != nil {
		t.Fatal(err)
	}
	requireExec(t, d, "CREATE TABLE t(id INTEGER PRIMARY KEY, name VARCHAR(4), data VARBINARY(4), note TEXT)")
	requireExec(t, d, "CREATE INDEX t_name ON t(name)")
	requireExec(t, d, "CREATE INDEX t_data ON t(data)")
	requireExec(t, d, "INSERT INTO t VALUES(1,'abcd',X'01020304','old')")
	requireExec(t, d, "ALTER TABLE t ALTER COLUMN name TYPE VARCHAR(6)")
	requireExec(t, d, "ALTER TABLE t ALTER COLUMN data TYPE VARBINARY(6)")
	requireExec(t, d, "INSERT INTO t VALUES(2,'abcdef',X'010203040506','new')")
	for _, query := range []string{
		"ALTER TABLE t ALTER COLUMN name TYPE VARCHAR(3)",
		"ALTER TABLE t ALTER COLUMN data TYPE VARBINARY(3)",
		"ALTER TABLE t ALTER COLUMN name TYPE VARCHAR(7) ALLOW EXISTING",
		"ALTER TABLE t ALTER COLUMN name TYPE TEXT ALLOW EXISTING",
		"ALTER TABLE t ALTER COLUMN name TYPE VARCHAR(3) ALLOW EXISTING USING name",
	} {
		if _, err := d.Exec(query); err == nil {
			t.Fatalf("accepted invalid ALTER: %s", query)
		}
	}
	requireExec(t, d, "ALTER TABLE t ALTER COLUMN name TYPE VARCHAR(3) ALLOW EXISTING")
	requireExec(t, d, "ALTER TABLE t ALTER COLUMN data TYPE VARBINARY(3) ALLOW EXISTING")
	for _, query := range []string{
		"INSERT INTO t VALUES(3,'abcd',X'01','x')",
		"INSERT INTO t VALUES(3,'ok',X'01020304','x')",
		"UPDATE t SET name='abcd' WHERE id=1",
		"UPDATE t SET data=X'01020304' WHERE id=1",
		"UPDATE t SET name=name WHERE id=1 RETURNING id",
	} {
		if _, err := d.Exec(query); err == nil {
			t.Fatalf("accepted new overlength value: %s", query)
		}
	}
	requireExec(t, d, "UPDATE t SET note='updated' WHERE id=1 RETURNING id")
	requireExec(t, d, "UPDATE t SET note='also updated' WHERE id=2")
	requireExec(t, d, "ALTER TABLE t ADD COLUMN extra INTEGER DEFAULT 1")
	requireExec(t, d, "INSERT INTO t(id,name,data,note) VALUES(1,'ok',X'01','upsert') ON CONFLICT(id) DO UPDATE SET note=excluded.note")
	for _, query := range []string{
		"SELECT COUNT(*) AS n FROM t WHERE name='abcd'",
		"SELECT COUNT(*) AS n FROM t WHERE data=X'01020304'",
	} {
		result := requireExec(t, d, query)
		if len(result.Rows) != 1 || result.Rows[0]["n"].Int != 1 {
			t.Fatalf("grandfathered indexed value missing for %s: %v", query, result.Rows)
		}
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	d, err = env.openDatabase(path, nil)
	if err != nil {
		t.Fatal(err)
	}
	result := requireExec(t, d, "SELECT name, data, note FROM t WHERE id=1")
	if len(result.Rows) != 1 || result.Rows[0]["name"].Text != "abcd" || len(result.Rows[0]["data"].Blob) != 4 || result.Rows[0]["note"].Text != "upsert" {
		t.Fatalf("grandfathered values changed on reopen: %v", result.Rows)
	}
	if _, err := d.Exec("UPDATE t SET name='abcd' WHERE id=1"); err == nil {
		t.Fatal("reopened database accepted new overlength value")
	}
	requireExec(t, d, "UPDATE t SET note='reopened' WHERE id=1")
	if _, err := d.CheckIntegrity(); err != nil {
		t.Fatalf("integrity rejected preserved oversized values: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
}
