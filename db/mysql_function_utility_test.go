package db_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"slopdb/db"
	"slopdb/storage"
)

func TestMySQLUtilityFunctionsPreparedAndBinary(t *testing.T) {
	d, err := db.Open(t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	s := mysqlTypeSession(t, d)
	// Digest, AES, UUID/IP and compression answers were checked independently
	// against Oracle MySQL 8.0.46, not generated from SlopDB.
	for _, tc := range []struct{ expr, want string }{
		{"MD5('abc')", "900150983cd24fb0d6963f7d28e17f72"},
		{"SHA1('abc')", "a9993e364706816aba3e25717850c26c9cd0d89d"},
		{"SHA('abc')", "a9993e364706816aba3e25717850c26c9cd0d89d"},
		{"SHA2('abc',256)", "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"},
		{"LENGTH(SHA2('abc',224))", "56"}, {"LENGTH(SHA2('abc',384))", "96"}, {"LENGTH(SHA2('abc',512))", "128"},
		{"SHA2('abc',0)=SHA2('abc',256)", "1"},
		{"HEX(AES_ENCRYPT('hello','key'))", "5E27050B59C889FEE9F953137259BF6B"},
		{"HEX(AES_ENCRYPT('hello','key',NULL))", "5E27050B59C889FEE9F953137259BF6B"},
		{"HEX(AES_DECRYPT(AES_ENCRYPT(X'0001FF','secret'),'secret'))", "0001FF"},
		{"IS_UUID('6ccd780c-baba-1026-9564-5b8c656024db')", "1"},
		{"IS_UUID('{6ccd780c-baba-1026-9564-5b8c656024db}')", "1"},
		{"IS_UUID('6ccd780cbaba102695645b8c656024db')", "1"},
		{"IS_UUID('6ccd780c-baba1026-9564-5b8c656024db')", "0"},
		{"HEX(UUID_TO_BIN('6ccd780c-baba-1026-9564-5b8c656024db',1))", "1026BABA6CCD780C95645B8C656024DB"},
		{"BIN_TO_UUID(UUID_TO_BIN('6ccd780c-baba-1026-9564-5b8c656024db',1),1)", "6ccd780c-baba-1026-9564-5b8c656024db"},
		{"IS_UUID(UUID())", "1"}, {"SUBSTRING(UUID(),15,1)", "1"}, {"UUID()=UUID()", "0"},
		{"INET_ATON('127.1')", "2130706433"}, {"INET_ATON('127.2.3')", "2130837507"},
		{"INET_NTOA(INET_ATON('192.168.1.20'))", "192.168.1.20"}, {"INET_NTOA(1.5)", "0.0.0.2"},
		{"HEX(INET6_ATON('10.0.5.9'))", "0A000509"},
		{"HEX(INET6_ATON('fdfe::5a55:caff:fefa:9089'))", "FDFE0000000000005A55CAFFFEFA9089"},
		{"INET6_NTOA(INET6_ATON('::ffff:10.0.5.9'))", "::ffff:10.0.5.9"},
		{"INET6_NTOA(INET6_ATON('::10.0.5.9'))", "::10.0.5.9"},
		{"IS_IPV4('127.1')", "0"}, {"IS_IPV4('127.0.0.1')", "1"}, {"IS_IPV6('::ffff:127.0.0.1')", "1"},
		{"IS_IPV4_COMPAT(INET6_ATON('::10.0.5.9'))", "1"}, {"IS_IPV4_COMPAT(INET6_ATON('::1'))", "0"},
		{"IS_IPV4_MAPPED(INET6_ATON('::ffff:10.0.5.9'))", "1"}, {"IS_IPV4_MAPPED(INET6_ATON('::10.0.5.9'))", "0"},
		{"HEX(UNCOMPRESS(COMPRESS(X'0001FF20')))", "0001FF20"},
		{"HEX(UNCOMPRESS(X'02000000789C4B54000000E40082'))", "6120"},
		{"UNCOMPRESSED_LENGTH(COMPRESS('hello'))", "5"}, {"UNCOMPRESSED_LENGTH('abcde')", "610493025"},
		{"HEX(COMPRESS(''))", ""}, {"HEX(UNCOMPRESS(''))", ""},
		{"ISNULL(NULL)", "1"}, {"ISNULL('')", "0"}, {"CHARSET(NULL)", "binary"}, {"CHARSET(1)", "binary"},
		{"CHARSET('🪐')", "utf8mb4"}, {"COLLATION('🪐')", "utf8mb4_bin"},
		{"LENGTH('café🪐')", "9"}, {"CONVERT('🪐' USING utf8mb3)", "🪐"}, {"CONVERT('🪐' USING utf8)", "🪐"},
		{"SLEEP(0)", "0"},
	} {
		q := "SELECT " + tc.expr + " AS v"
		r := mysqlTypeExec(t, s, q)
		if len(r.Rows) != 1 || r.Rows[0]["v"].String() != tc.want {
			t.Fatalf("%s = %v, want %q", tc.expr, r.Rows, tc.want)
		}
		p, err := s.Prepare(q)
		if err != nil {
			t.Fatal(q, err)
		}
		r, err = p.Exec()
		p.Close()
		if err != nil || r.Rows[0]["v"].String() != tc.want {
			t.Fatalf("prepared %s: %v %v", q, r, err)
		}
	}
	for _, expr := range []string{"MD5(NULL)", "SHA2('abc',123)", "SHA2(NULL,256)", "AES_DECRYPT(X'00','key')", "UUID_TO_BIN(NULL)", "BIN_TO_UUID(NULL)", "IS_UUID(NULL)", "INET_ATON('1.2.3.999')", "INET_NTOA(-1)", "INET6_ATON('fe80::1%eth0')", "INET6_NTOA(X'00')", "COMPRESS(NULL)", "UNCOMPRESS('invalid')"} {
		r := mysqlTypeExec(t, s, "SELECT "+expr+" AS v")
		if !r.Rows[0]["v"].Null {
			t.Fatalf("%s: %v", expr, r.Rows)
		}
	}
	for _, q := range []string{"SELECT MD5()", "SELECT SHA2('x')", "SELECT RANDOM_BYTES(0)", "SELECT UUID_TO_BIN('bad')", "SELECT BIN_TO_UUID('bad')"} {
		if _, err := s.Exec(q); err == nil {
			t.Fatal("accepted invalid utility call", q)
		}
	}
	r := mysqlTypeExec(t, s, "SELECT LENGTH(RANDOM_BYTES(1024)) AS v")
	if r.Rows[0]["v"].Int != 1024 {
		t.Fatal(r.Rows)
	}
	// Native LENGTH retains its existing character-count semantics.
	r, err = d.Exec("SELECT LENGTH('café🪐') AS v")
	if err != nil || r.Rows[0]["v"].Int != 5 {
		t.Fatal(r, err)
	}
}

func TestMySQLUtilityCancellationAndExpansionBudget(t *testing.T) {
	opt := storage.DefaultOptions()
	opt.QueryMemoryBytes = 1024
	d, err := db.Open(t.TempDir(), opt)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	s := mysqlTypeSession(t, d)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if _, err = s.ExecContext(ctx, "SELECT SLEEP(10)"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("sleep cancellation", err)
	}
	if _, err = s.Exec("SELECT UNCOMPRESS(X'00001000789C030000000001')"); err == nil || !strings.Contains(err.Error(), "budget") {
		t.Fatal("unbounded decompression", err)
	}
	mysqlTypeExec(t, s, "SELECT ISNULL(NULL)")
}

func TestMySQLScientificLiteralsRemainApproximate(t *testing.T) {
	d, err := db.Open(t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	s := mysqlTypeSession(t, d)
	for _, tc := range []struct{ expr, typ string }{{"1.25", "DECIMAL"}, {"1.25e0", "REAL"}, {"1.25E0", "REAL"}, {"1+1.25e0", "REAL"}} {
		r := mysqlTypeExec(t, s, "SELECT "+tc.expr+" AS v")
		if r.Rows[0]["v"].Type.String() != tc.typ {
			t.Fatalf("%s: %v", tc.expr, r.Rows)
		}
	}
	mysqlTypeExec(t, s, "CREATE TABLE literal_types(v INTEGER);INSERT INTO literal_types VALUES(1),(2)")
	r := mysqlTypeExec(t, s, "SELECT SUM(v+1e0) AS v FROM literal_types")
	if r.Rows[0]["v"].Type.String() != "REAL" {
		t.Fatal(r.Rows)
	}
}
