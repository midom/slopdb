package db_test

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"slopdb/db"
	"slopdb/sql"
	"slopdb/storage"
)

func mysqlTypeSession(t *testing.T, d *db.Database) *db.Session {
	t.Helper()
	s, err := d.NewSession()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	if err = s.SetMySQLMode(true); err != nil {
		t.Fatal(err)
	}
	return s
}
func mysqlTypeExec(t *testing.T, s *db.Session, q string) *db.ExecResult {
	t.Helper()
	r, err := s.Exec(q)
	if err != nil {
		t.Fatalf("%s: %v", q, err)
	}
	return r
}

func TestMySQLDeclaredTypesBinaryAffinityAndReopen(t *testing.T) {
	path := t.TempDir()
	opt := storage.DefaultOptions()
	opt.DisableWALSync = true
	d, err := db.Open(path, opt)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = d.Close() })
	s := mysqlTypeSession(t, d)
	mysqlTypeExec(t, s, "CREATE TABLE typed (id BIGINT UNSIGNED AUTO_INCREMENT NOT NULL, name VARBINARY(20) DEFAULT '' NOT NULL, token BINARY(14), tiny TINYINT UNSIGNED, small SMALLINT, size FLOAT, stamp TIMESTAMP, kind ENUM('page','subcat','file') DEFAULT 'page', payload MEDIUMBLOB, INDEX typed_name(name(8)), PRIMARY KEY(id)) ENGINE=InnoDB, DEFAULT CHARSET=binary")
	mysqlTypeExec(t, s, "INSERT INTO typed(name,token,tiny,small,size,stamp,kind,payload) VALUES('alice','abc',255,-32768,1.5,'2026-10-25 01:02:03','subcat','payload')")
	mysqlTypeExec(t, s, "INSERT INTO typed(id,token,tiny) VALUES(NULL,'x',0),(0,'y',1)")
	for _, q := range []string{"SELECT id FROM typed WHERE name='alice'", "SELECT id FROM typed WHERE name IN ('alice')", "SELECT id FROM typed WHERE name IN (SELECT 'alice')", "SELECT id FROM typed WHERE name='alice' OR tiny=99", "SELECT id FROM typed WHERE name>='alice' AND name<='alice'"} {
		r := mysqlTypeExec(t, s, q)
		if len(r.Rows) != 1 || r.Rows[0]["id"].Int != 1 {
			t.Fatalf("bad binary predicate %s: %+v", q, r.Rows)
		}
	}
	for _, q := range []string{"INSERT INTO typed(tiny) VALUES(256)", "INSERT INTO typed(small) VALUES(-32769)", "INSERT INTO typed(kind) VALUES('unknown')", "INSERT INTO typed(token) VALUES('this string is more than fourteen bytes')", "INSERT INTO typed(stamp) VALUES('yesterday')"} {
		if _, err = s.Exec(q); err == nil {
			t.Fatalf("accepted invalid value: %s", q)
		}
	}
	r := mysqlTypeExec(t, s, "SELECT id,token,kind,kind+0 AS ordinal,payload FROM typed WHERE id=1")
	if len(r.Rows) != 1 || r.Rows[0]["ordinal"].Int != 2 || !bytes.Equal(r.Rows[0]["token"].Blob, append([]byte("abc"), make([]byte, 11)...)) || string(r.Rows[0]["payload"].Blob) != "payload" {
		t.Fatalf("stored types: %+v", r.Rows)
	}
	create := mysqlTypeExec(t, s, "SHOW CREATE TABLE typed")
	if !strings.Contains(create.Rows[0]["Create table"].Text, "BINARY(14)") || !strings.Contains(create.Rows[0]["Create table"].Text, "name\"(8)") {
		t.Fatalf("lost metadata: %+v", create.Rows)
	}
	idx := mysqlTypeExec(t, s, "SHOW INDEX FROM typed")
	found := false
	for _, row := range idx.Rows {
		if row["Key_name"].Text == "typed_name" {
			found = row["Sub_part"].Int == 8
		}
	}
	if !found {
		t.Fatal("prefix metadata missing")
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	if err = d.Close(); err != nil {
		t.Fatal(err)
	}
	d, err = db.Open(path, opt)
	if err != nil {
		t.Fatal(err)
	}
	s = mysqlTypeSession(t, d)
	r = mysqlTypeExec(t, s, "SELECT id,token,kind+0 AS ordinal FROM typed WHERE name='alice'")
	if len(r.Rows) != 1 || r.Rows[0]["ordinal"].Int != 2 || len(r.Rows[0]["token"].Blob) != 14 {
		t.Fatalf("reopen: %+v", r.Rows)
	}
	mysqlTypeExec(t, s, "ALTER TABLE typed RENAME TO renamed")
	r = mysqlTypeExec(t, s, "DESCRIBE renamed")
	types := map[string]string{}
	for _, row := range r.Rows {
		types[row["Field"].Text] = row["Type"].Text
	}
	if types["token"] != "BINARY(14)" || types["kind"] != "ENUM('page','subcat','file')" || types["tiny"] != "TINYINT UNSIGNED" {
		t.Fatalf("renamed metadata: %+v", types)
	}
	if _, err = d.CheckIntegrity(); err != nil {
		t.Fatal(err)
	}
	if err = d.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestMySQLModeIsIndependentAndAutoZeroOption(t *testing.T) {
	d, err := db.Open(t.TempDir(), storage.DefaultOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	s := mysqlTypeSession(t, d)
	mysqlTypeExec(t, s, "CREATE TABLE auto(id INTEGER PRIMARY KEY AUTO_INCREMENT,v INTEGER)")
	ctx := sql.WithMySQLSession(context.Background(), sql.MySQLSessionOptions{SQLMode: "NO_AUTO_VALUE_ON_ZERO"})
	if _, err = s.ExecContext(ctx, "INSERT INTO auto VALUES(0,7)"); err != nil {
		t.Fatal(err)
	}
	r := mysqlTypeExec(t, s, "SELECT id FROM auto")
	if len(r.Rows) != 1 || r.Rows[0]["id"].Int != 0 {
		t.Fatalf("NO_AUTO_VALUE_ON_ZERO: %+v", r.Rows)
	}
	if _, err = d.Exec("CREATE TABLE native(id INTEGER PRIMARY KEY AUTOINCREMENT,b BLOB)"); err != nil {
		t.Fatal(err)
	}
	for _, q := range []string{"INSERT INTO native(id,b) VALUES(NULL,X'01')", "INSERT INTO native(b) VALUES('text')"} {
		if _, err = d.Exec(q); err == nil {
			t.Fatalf("native semantics changed: %s", q)
		}
	}
	if _, err = d.Exec("INSERT INTO native(b) VALUES(X'01')"); err != nil {
		t.Fatal(err)
	}
}
