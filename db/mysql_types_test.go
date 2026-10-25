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

func TestMySQLEngineOptionIsIgnored(t *testing.T) {
	d, err := db.Open(t.TempDir(), storage.DefaultOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	s := mysqlTypeSession(t, d)
	for _, engine := range []string{"InnoDB", "MyISAM", "AnythingElse", "'unrecognized-engine'"} {
		mysqlTypeExec(t, s, "CREATE TABLE engine_test(id INTEGER PRIMARY KEY) ENGINE="+engine)
		mysqlTypeExec(t, s, "INSERT INTO engine_test VALUES(1)")
		mysqlTypeExec(t, s, "ALTER TABLE engine_test ENGINE="+engine)
		mysqlTypeExec(t, s, "EXPLAIN ALTER TABLE engine_test ENGINE="+engine)
		if rows := mysqlTypeExec(t, s, "SELECT id FROM engine_test").Rows; len(rows) != 1 || rows[0]["id"].Int != 1 {
			t.Fatalf("ENGINE changed data: %+v", rows)
		}
		mysqlTypeExec(t, s, "DROP TABLE engine_test")
	}
}

func TestMySQLNonStrictAssignmentsWarningsAndPrecision(t *testing.T) {
	path := t.TempDir()
	d, err := db.Open(path, storage.DefaultOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { d.Close() }()
	s := mysqlTypeSession(t, d)
	mysqlTypeExec(t, s, "CREATE TABLE values_test(id INTEGER PRIMARY KEY,tiny TINYINT UNSIGNED,n INTEGER,b BINARY(3),v VARCHAR(2),e ENUM('page','subcat'),stamp TIMESTAMP(6),short_text TINYTEXT)")
	warnings := []uint16{}
	ctx := sql.WithMySQLSession(context.Background(), sql.MySQLSessionOptions{SQLMode: "", Warning: func(code uint16, _ string) { warnings = append(warnings, code) }})
	if _, err = s.ExecContext(ctx, "INSERT INTO values_test VALUES(1,999,'12tail','abcd','long','invalid','bad date','ok')"); err != nil {
		t.Fatal(err)
	}
	r := mysqlTypeExec(t, s, "SELECT tiny,n,b,v,e,e+0 AS ordinal,stamp FROM values_test WHERE id=1")
	row := r.Rows[0]
	if row["tiny"].Int != 255 || row["n"].Int != 12 || string(row["b"].Blob) != "abc" || row["v"].Text != "lo" || row["e"].Text != "" || row["ordinal"].Int != 0 || row["stamp"].Text != "0000-00-00 00:00:00" || len(warnings) < 6 {
		t.Fatalf("nonstrict normalization: %+v, warnings %+v", row, warnings)
	}
	mysqlTypeExec(t, s, "INSERT INTO values_test(id,stamp) VALUES(2,'2026-10-25 01:02:03.123456')")
	r = mysqlTypeExec(t, s, "SELECT stamp FROM values_test WHERE id=2")
	if r.Rows[0]["stamp"].Text != "2026-10-25 01:02:03.123456" {
		t.Fatalf("lost precision: %+v", r.Rows)
	}
	strict := sql.WithMySQLSession(context.Background(), sql.MySQLSessionOptions{SQLMode: "STRICT_TRANS_TABLES"})
	if _, err = s.ExecContext(strict, "INSERT INTO values_test(id,tiny) VALUES(3,999)"); err == nil {
		t.Fatal("strict mode truncated a value")
	}
	if _, err = s.ExecContext(strict, "INSERT INTO values_test(id,n) VALUES(3,'12tail')"); err == nil {
		t.Fatal("strict mode accepted invalid numeric input")
	}
	if _, err = s.Exec("INSERT INTO values_test(id,short_text) VALUES(3,'" + strings.Repeat("é", 128) + "')"); err == nil {
		t.Fatal("TINYTEXT length used character count instead of bytes")
	}
	if _, err = d.CheckIntegrity(); err != nil {
		t.Fatal(err)
	}
	if err = d.Close(); err != nil {
		t.Fatal(err)
	}
	d, err = db.Open(path, storage.DefaultOptions())
	if err != nil {
		t.Fatal(err)
	}
	s = mysqlTypeSession(t, d)
	mysqlTypeExec(t, s, "UPDATE values_test SET n=n WHERE id=1")
	r = mysqlTypeExec(t, s, "SELECT e+0 AS ordinal,stamp FROM values_test ORDER BY id")
	if r.Rows[0]["ordinal"].Int != 0 || r.Rows[1]["stamp"].Text != "2026-10-25 01:02:03.123456" {
		t.Fatalf("reopened ENUM or precision: %+v", r.Rows)
	}
	if _, err = d.CheckIntegrity(); err != nil {
		t.Fatal(err)
	}
}

func TestMySQLIgnoreAssignmentsAndDuplicateWarnings(t *testing.T) {
	d, err := db.Open(t.TempDir(), storage.DefaultOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	s := mysqlTypeSession(t, d)
	mysqlTypeExec(t, s, "CREATE TABLE ignore_values(id INTEGER PRIMARY KEY AUTO_INCREMENT,n TINYINT UNSIGNED NOT NULL,b BINARY(2) NOT NULL,e ENUM('a','b') NOT NULL)")
	warnings := []uint16{}
	ctx := sql.WithMySQLSession(context.Background(), sql.MySQLSessionOptions{SQLMode: "STRICT_TRANS_TABLES,NO_AUTO_VALUE_ON_ZERO", Warning: func(code uint16, _ string) { warnings = append(warnings, code) }})
	r, err := s.ExecContext(ctx, "INSERT IGNORE INTO ignore_values VALUES(0,999,'long','bad'),(0,5,'xx','a'),(1,NULL,NULL,NULL)")
	if err != nil {
		t.Fatal(err)
	}
	if r.Changes != 2 || len(warnings) < 7 {
		t.Fatalf("IGNORE changes or warnings: %+v, %+v", r, warnings)
	}
	r = mysqlTypeExec(t, s, "SELECT id,n,b,e FROM ignore_values ORDER BY id")
	if len(r.Rows) != 2 || r.Rows[0]["id"].Int != 0 || r.Rows[0]["n"].Int != 255 || string(r.Rows[0]["b"].Blob) != "lo" || r.Rows[1]["n"].Int != 0 || !bytes.Equal(r.Rows[1]["b"].Blob, []byte{0, 0}) || r.Rows[1]["e"].Text != "a" {
		t.Fatalf("IGNORE normalized rows: %+v", r.Rows)
	}
	if _, err = s.ExecContext(ctx, "INSERT INTO ignore_values VALUES(2,999,'xx','a')"); err == nil {
		t.Fatal("IGNORE leaked nonstrict mode to next statement")
	}
}

func TestMySQLWriteCountsAndInsertIDs(t *testing.T) {
	d, err := db.Open(t.TempDir(), storage.DefaultOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	s := mysqlTypeSession(t, d)
	mysqlTypeExec(t, s, "CREATE TABLE counts(id INTEGER PRIMARY KEY AUTO_INCREMENT,v INTEGER UNIQUE)")
	r := mysqlTypeExec(t, s, "INSERT INTO counts(v) VALUES(10),(20)")
	if r.LastID != 1 || r.Changes != 2 {
		t.Fatalf("multi insert: %+v", r)
	}
	r = mysqlTypeExec(t, s, "INSERT IGNORE INTO counts(v) VALUES(10)")
	if r.Changes != 0 {
		t.Fatalf("ignore: %+v", r)
	}
	r = mysqlTypeExec(t, s, "INSERT INTO counts(id,v) VALUES(1,11) ON DUPLICATE KEY UPDATE v=VALUES(v)")
	if r.Changes != 2 || r.LastID != 1 {
		t.Fatalf("upsert: %+v", r)
	}
	r = mysqlTypeExec(t, s, "REPLACE INTO counts VALUES(1,12)")
	if r.Changes != 2 {
		t.Fatalf("replace: %+v", r)
	}
	for _, found := range []bool{false, true} {
		ctx := sql.WithMySQLSession(context.Background(), sql.MySQLSessionOptions{FoundRows: found, SQLMode: "STRICT_TRANS_TABLES"})
		r, err = s.ExecContext(ctx, "UPDATE counts SET v=v WHERE id=1")
		if err != nil {
			t.Fatal(err)
		}
		want := int64(0)
		if found {
			want = 1
		}
		if r.Changes != want {
			t.Fatalf("unchanged update found=%t: %+v", found, r)
		}
		r, err = s.ExecContext(ctx, "INSERT INTO counts(id,v) VALUES(1,12) ON DUPLICATE KEY UPDATE v=VALUES(v)")
		if err != nil {
			t.Fatal(err)
		}
		if r.Changes != want {
			t.Fatalf("unchanged upsert found=%t: %+v", found, r)
		}
	}
}

func TestMySQLEnumNumericContextsAndOrder(t *testing.T) {
	d, err := db.Open(t.TempDir(), storage.DefaultOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	s := mysqlTypeSession(t, d)
	mysqlTypeExec(t, s, "CREATE TABLE enums(id INTEGER PRIMARY KEY,e ENUM('z','a'), KEY e_idx(e))")
	mysqlTypeExec(t, s, "INSERT INTO enums VALUES(1,'z'),(2,'2'),(3,1)")
	for _, q := range []string{
		"SELECT id,e+0 AS ordinal FROM enums ORDER BY e,id",
		"SELECT id,+e AS ordinal FROM enums ORDER BY e,id",
		"SELECT id,e+0 AS ordinal FROM enums WHERE e IN ('z','a') ORDER BY e,id",
		"SELECT x.id AS id,x.e+0 AS ordinal FROM enums x JOIN enums y ON x.id=y.id ORDER BY x.e,x.id",
	} {
		r := mysqlTypeExec(t, s, q)
		if len(r.Rows) != 3 || r.Rows[0]["id"].Int != 1 || r.Rows[1]["id"].Int != 3 || r.Rows[2]["id"].Int != 2 || r.Rows[2]["ordinal"].Int != 2 {
			t.Fatalf("%s: %+v", q, r.Rows)
		}
	}
	r := mysqlTypeExec(t, s, "SELECT SUM(e) AS total,AVG(e) AS mean_value FROM enums")
	if r.Rows[0]["total"].Int != 4 || r.Rows[0]["mean_value"].Real != 4.0/3 {
		t.Fatalf("enum aggregates: %+v", r.Rows)
	}
	r = mysqlTypeExec(t, s, "SELECT id FROM enums WHERE e=2")
	if len(r.Rows) != 1 || r.Rows[0]["id"].Int != 2 {
		t.Fatalf("numeric enum comparison: %+v", r.Rows)
	}
	mysqlTypeExec(t, s, "CREATE TABLE enum_keys(e ENUM('z','a') PRIMARY KEY)")
	mysqlTypeExec(t, s, "INSERT INTO enum_keys VALUES('a'),('z')")
	r = mysqlTypeExec(t, s, "SELECT e FROM enum_keys ORDER BY e LIMIT 1")
	if len(r.Rows) != 1 || r.Rows[0]["e"].Text != "z" {
		t.Fatalf("clustered enum order: %+v", r.Rows)
	}
}

func TestMySQLDeleteIntegerPrimaryKeyInNumericStrings(t *testing.T) {
	d, err := db.Open(t.TempDir(), storage.DefaultOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	s := mysqlTypeSession(t, d)
	mysqlTypeExec(t, s, "CREATE TABLE revision(rev_id INTEGER PRIMARY KEY,rev_page INTEGER,KEY page_idx(rev_page))")
	mysqlTypeExec(t, s, "INSERT INTO revision VALUES(1,1),(2,2),(3,3),(4,4)")
	r := mysqlTypeExec(t, s, "DELETE FROM revision WHERE rev_id IN ('2','3')")
	if r.Changes != 2 {
		t.Fatalf("quoted integer IN deleted %d rows", r.Changes)
	}
	r = mysqlTypeExec(t, s, "SELECT rev_id FROM revision ORDER BY rev_id")
	if len(r.Rows) != 2 || r.Rows[0]["rev_id"].Int != 1 || r.Rows[1]["rev_id"].Int != 4 {
		t.Fatalf("remaining revisions: %+v", r.Rows)
	}
	mysqlTypeExec(t, s, "INSERT INTO revision VALUES(2,2),(3,3)")
	r = mysqlTypeExec(t, s, "DELETE FROM revision WHERE rev_page IN ('2','3')")
	if r.Changes != 2 {
		t.Fatalf("secondary quoted integer IN deleted %d rows", r.Changes)
	}
}
