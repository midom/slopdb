package db_test

import (
	"context"
	"strings"
	"testing"

	"slopdb/db"
	"slopdb/sql"
	"slopdb/storage"
)

func TestMySQLRegexpFunctionFamilies(t *testing.T) {
	d, err := db.Open(t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	s := mysqlTypeSession(t, d)
	for _, tc := range []struct{ expression, want string }{
		{"REGEXP_LIKE('abc','a.c')", "1"}, {"REGEXP_LIKE('abc','ABC')", "0"}, {"REGEXP_LIKE('abc','ABC','i')", "1"}, {"REGEXP_LIKE('abc','ABC','ic')", "0"}, {"REGEXP_LIKE('abc','ABC','ci')", "1"},
		{"REGEXP_LIKE('abc' COLLATE utf8mb4_general_ci,'ABC')", "1"}, {"REGEXP_LIKE('é🪐','^..$')", "1"},
		{"REGEXP_LIKE('a\nb','a.b')", "0"}, {"REGEXP_LIKE('a\nb','a.b','n')", "1"}, {"REGEXP_LIKE('a\nb','^b','m')", "1"},
		{"REGEXP_INSTR('foo12bar34','[0-9]+')", "4"}, {"REGEXP_INSTR('foo12bar34','[0-9]+',1,2)", "9"}, {"REGEXP_INSTR('foo12bar34','[0-9]+',1,2,1)", "11"},
		{"REGEXP_INSTR('éaé','é',2,1,1)", "4"}, {"REGEXP_INSTR('abc','x')", "0"}, {"REGEXP_INSTR('abc','.',1,0)", "1"}, {"REGEXP_INSTR('abc','.',1,-1)", "1"}, {"REGEXP_INSTR('abc','^b',2)", "2"},
		{"REGEXP_SUBSTR('foo12bar34','[0-9]+',1,2)", "34"}, {"REGEXP_SUBSTR('éaé','é',2)", "é"},
		{"REGEXP_REPLACE('ab12cd34','[0-9]+','X')", "abXcdX"}, {"REGEXP_REPLACE('foo12bar34','[0-9]+','X',1,2)", "foo12barX"}, {"REGEXP_REPLACE('abc','.','X',1,-1)", "Xbc"},
		{"REGEXP_REPLACE('abc','(.)','$1$1')", "aabbcc"}, {"REGEXP_REPLACE('abc','x','Z',2)", "abc"}, {"REGEXP_REPLACE('aaaa','a+','X',2)", "aX"},
		{"REGEXP_REPLACE('abc','a*','x')", "xxbxcx"}, {"REGEXP_INSTR('abc','a*',1,2)", "2"}, {"REGEXP_INSTR('abc','a*',1,4)", "4"}, {"REGEXP_SUBSTR('abc','a*',1,2)", ""},
		{"REGEXP_LIKE('','a*')", "1"}, {"REGEXP_INSTR('','a*')", "1"}, {"REGEXP_SUBSTR('','a*')", ""}, {"REGEXP_REPLACE('','a*','x')", ""},
	} {
		r := mysqlTypeExec(t, s, "SELECT "+tc.expression+" AS v")
		if value := r.Rows[0]["v"]; value.Null || value.String() != tc.want {
			t.Errorf("%s = %v, want %q", tc.expression, value, tc.want)
		}
	}
	for _, expression := range []string{"REGEXP_LIKE(NULL,'a')", "REGEXP_LIKE('a','a',NULL)", "REGEXP_SUBSTR('abc','x')", "REGEXP_REPLACE('abc','a',NULL)"} {
		r := mysqlTypeExec(t, s, "SELECT "+expression+" AS v")
		if !r.Rows[0]["v"].Null {
			t.Errorf("%s must return NULL: %v", expression, r.Rows)
		}
	}
	for _, expression := range []string{"REGEXP_LIKE('a','')", "REGEXP_LIKE('a','a','x')", "REGEXP_LIKE(X'61','a')", "REGEXP_LIKE('a','(?<=x)a')", "REGEXP_INSTR('abc','.',0)", "REGEXP_SUBSTR('abc','.',4)", "REGEXP_INSTR('abc','.',1,1,2)", "REGEXP_REPLACE('a','a','$2')", "REGEXP_REPLACE('abc','^','x',2)"} {
		if _, err := s.Exec("SELECT " + expression); err == nil {
			t.Errorf("unsupported/invalid regexp accepted: %s", expression)
		}
	}
}

func TestMySQLRegexpPreparedParametersBudgetAndCancellation(t *testing.T) {
	options := storage.DefaultOptions()
	options.QueryMemoryBytes = 1024
	d, err := db.Open(t.TempDir(), options)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	s := mysqlTypeSession(t, d)
	mysqlTypeExec(t, s, "CREATE TABLE regexp_input(id INTEGER PRIMARY KEY,v TEXT);INSERT INTO regexp_input VALUES(1,'abc'),(2,'xyz')")
	p, err := s.Prepare("SELECT id,REGEXP_LIKE(v,?) AS yes FROM regexp_input ORDER BY id")
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	for _, tc := range []struct {
		pattern string
		first   int64
	}{{"^a", 1}, {"^x", 0}, {"^a", 1}} {
		r, err := p.Exec(tc.pattern)
		if err != nil || r.Rows[0]["yes"].Int != tc.first || r.Rows[1]["yes"].Int != 1-tc.first {
			t.Fatal(r, err)
		}
	}
	if _, err := s.Exec("SELECT REGEXP_REPLACE(REPEAT('a',100),'.',REPEAT('x',100))"); err == nil || !strings.Contains(err.Error(), "memory budget") {
		t.Fatalf("unbounded regex result: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := s.ExecContext(ctx, "SELECT REGEXP_LIKE(v,'.') FROM regexp_input"); err == nil {
		t.Fatal("cancelled regexp query succeeded")
	}
	r := mysqlTypeExec(t, s, "SELECT REGEXP_REPLACE('abc','b','x') AS v")
	if r.Rows[0]["v"].Type != sql.ColText || r.Rows[0]["v"].Text != "axc" {
		t.Fatal(r.Rows)
	}
}
