package db_test

import (
	"strings"
	"testing"

	"slopdb/db"
	"slopdb/sql"
	"slopdb/storage"
)

func TestJSONFunctionsAccessorsAndNumbers(t *testing.T) {
	d, err := db.Open(t.TempDir(), storage.DefaultOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	s := mysqlTypeSession(t, d)
	for _, test := range []struct{ query, want string }{
		{`SELECT JSON_EXTRACT('{"a":{"b":[1,18446744073709551615,null]}}','$.a.b[1]') AS v`, `18446744073709551615`},
		{`SELECT JSON_EXTRACT('{"a":[1,2,3]}','$.a[*]') AS v`, `[1,2,3]`},
		{`SELECT JSON_EXTRACT('{"a":1,"b":2}','$.a','$.b') AS v`, `[1,2]`},
		{`SELECT JSON_EXTRACT('{"a":{"x":1},"b":{"x":2}}','$**.x') AS v`, `[1,2]`},
		{`SELECT JSON_EXTRACT('[1,2,3]','$[last-1]') AS v`, `2`},
		{`SELECT JSON_SET('{"a":[1,2],"b":1}','$.a[1]',7,'$.c',JSON_ARRAY(3,4)) AS v`, `{"a":[1,7],"b":1,"c":[3,4]}`},
		{`SELECT JSON_INSERT('{"a":1}','$.a',2,'$.b',3) AS v`, `{"a":1,"b":3}`},
		{`SELECT JSON_REPLACE('{"a":1}','$.a',2,'$.b',3) AS v`, `{"a":2}`},
		{`SELECT JSON_REMOVE('{"a":[1,2,3],"b":1}','$.a[1]','$.b') AS v`, `{"a":[1,3]}`},
		{`SELECT JSON_ARRAY_APPEND('{"a":1}','$.a',2) AS v`, `{"a":[1,2]}`},
		{`SELECT JSON_ARRAY_INSERT('[1,2,3]','$[1]',9) AS v`, `[1,9,2,3]`},
		{`SELECT JSON_OBJECT('n',18446744073709551615,'d',CAST('0.12345678901234567890' AS DECIMAL(30,20))) AS v`, `{"d":0.12345678901234567890,"n":18446744073709551615}`},
		{`SELECT JSON_CONTAINS('{"a":[1,2,3]}','[2,1]','$.a') AS v`, `1`},
		{`SELECT JSON_CONTAINS('[18446744073709551615]','18446744073709551614') AS v`, `0`},
		{`SELECT JSON_CONTAINS_PATH('{"a":null}','all','$.a') AS v`, `1`},
		{`SELECT JSON_OVERLAPS('[1,2]','[3,2]') AS v`, `1`},
		{`SELECT JSON_MERGE_PATCH('{"a":1,"b":{"x":1}}','{"a":null,"b":{"y":2}}') AS v`, `{"b":{"x":1,"y":2}}`},
		{`SELECT JSON_MERGE_PRESERVE('{"a":1}','{"a":2}') AS v`, `{"a":[1,2]}`},
		{`SELECT JSON_KEYS('{"b":2,"a":1}') AS v`, `["a","b"]`},
		{`SELECT JSON_LENGTH('{"a":[1,2]}','$.a') AS v`, `2`},
		{`SELECT JSON_DEPTH('{"a":[1,2]}') AS v`, `3`},
		{`SELECT JSON_TYPE('{"a":1}') AS v`, `OBJECT`},
		{`SELECT JSON_UNQUOTE('"a\nb"') AS v`, "a\nb"},
		{`SELECT JSON_QUOTE('a"b') AS v`, `"a\"b"`},
		{`SELECT JSON_VALID('{"a":') AS v`, `0`},
		{`SELECT JSON_VALUE('{"a":18446744073709551615}','$.a') AS v`, `18446744073709551615`},
		{`SELECT CAST('{"a":{"b":[1,"two"]}}' AS JSON)->'$.a.b[1]' AS v`, `"two"`},
		{`SELECT CAST('{"a":{"b":[1,"two"]}}' AS JSON)->'a'->'b'->>1 AS v`, `two`},
		{`SELECT CAST('{"a":{"b":[1,"two"]}}' AS JSON)#>>'{a,b,1}' AS v`, `two`},
	} {
		r := mysqlTypeExec(t, s, test.query)
		if got := r.Rows[0]["v"].String(); got != test.want {
			t.Fatalf("%s: got %q want %q", test.query, got, test.want)
		}
	}
	for _, query := range []string{`SELECT JSON_EXTRACT('{}','$..a')`, `SELECT JSON_REMOVE('{}','$')`, `SELECT JSON_SET('{}','$.*',1)`, `SELECT JSON_CONTAINS_PATH('{}','bad','$.a')`, `SELECT JSON_OBJECT('odd')`} {
		if _, err = s.Exec(query); err == nil {
			t.Fatalf("accepted malformed JSON operation: %s", query)
		}
	}
	for _, query := range []string{`SELECT JSON_EXTRACT('{"a":1}','$.missing') AS v`, `SELECT JSON_VALUE('{"a":null}','$.a') AS v`, `SELECT JSON_EXTRACT(NULL,'$.a') AS v`} {
		r := mysqlTypeExec(t, s, query)
		if !r.Rows[0]["v"].Null {
			t.Fatalf("expected SQL NULL: %+v", r.Rows)
		}
	}
}

func TestJSONPreparedMutationIndexAndReopen(t *testing.T) {
	path := t.TempDir()
	d, err := db.Open(path, storage.DefaultOptions())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = d.Close() })
	s := mysqlTypeSession(t, d)
	mysqlTypeExec(t, s, `CREATE TABLE documents(id INTEGER PRIMARY KEY,body JSON NOT NULL,UNIQUE KEY document_body(body));INSERT INTO documents VALUES(1,'{"z":1,"a":[1,2]}')`)
	if _, err = s.Exec(`INSERT INTO documents VALUES(2,'{"a":[1,2],"z":1}')`); err == nil {
		t.Fatal("canonical JSON unique key accepted duplicate")
	}
	p, err := s.Prepare("UPDATE documents SET body=JSON_SET(body,?,CAST(? AS JSON)) WHERE id=?")
	if err != nil {
		t.Fatal(err)
	}
	mysqlTypeExec(t, s, "BEGIN")
	if _, err = p.Exec("$.a[1]", "18446744073709551615", 1); err != nil {
		t.Fatal(err)
	}
	r := mysqlTypeExec(t, s, "SELECT body->>'$.a[1]' AS n FROM documents")
	if r.Rows[0]["n"].Text != "18446744073709551615" {
		t.Fatal(r.Rows)
	}
	mysqlTypeExec(t, s, "ROLLBACK")
	if _, err = p.Exec("$.x", `{"unicode":"🌍"}`, 1); err != nil {
		t.Fatal(err)
	}
	_ = p.Close()
	if err = s.Close(); err != nil {
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
	r = mysqlTypeExec(t, s, "SELECT body,body->>'$.x.unicode' AS unicode_text FROM documents")
	if r.Rows[0]["body"].Type != sql.ColJSON || r.Rows[0]["unicode_text"].Text != "🌍" || !strings.Contains(r.Rows[0]["body"].Text, `"a":[1,2]`) {
		t.Fatal(r.Rows)
	}
	if _, err = d.CheckIntegrity(); err != nil {
		t.Fatal(err)
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	if err = d.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestJSONNumericEqualityAndIndexOrder(t *testing.T) {
	d, err := db.Open(t.TempDir(), storage.DefaultOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	s := mysqlTypeSession(t, d)
	for _, query := range []string{`SELECT CAST('1' AS JSON)=CAST('1.0' AS JSON) AS equal_value`, `SELECT CAST('{"a":1}' AS JSON)=CAST('{"a":1.00}' AS JSON) AS equal_value`} {
		r := mysqlTypeExec(t, s, query)
		if r.Rows[0]["equal_value"].Int != 1 {
			t.Fatal(r.Rows)
		}
	}
	mysqlTypeExec(t, s, `CREATE TABLE json_keys(id INTEGER PRIMARY KEY,v JSON UNIQUE);INSERT INTO json_keys VALUES(1,'1'),(2,'2'),(3,'10'),(4,'-5'),(5,'0.01')`)
	if _, err = s.Exec(`INSERT INTO json_keys VALUES(6,'1.00')`); err == nil {
		t.Fatal("numerically equal JSON values bypassed uniqueness")
	}
	r := mysqlTypeExec(t, s, "SELECT id FROM json_keys ORDER BY v")
	for i, want := range []int64{4, 5, 1, 2, 3} {
		if r.Rows[i]["id"].Int != want {
			t.Fatal(r.Rows)
		}
	}
	if _, err = d.CheckIntegrity(); err != nil {
		t.Fatal(err)
	}
}
