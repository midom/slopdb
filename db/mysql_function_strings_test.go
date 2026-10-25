package db_test

import (
	"strings"
	"testing"

	"slopdb/db"
	"slopdb/sql"
	"slopdb/storage"
)

// Expected answers include Oracle MySQL 8.0.46 reference queries. BINARY
// comparison remains SlopDB's configured default; explicit MySQL collations
// exercise the named comparison modes independently.
func TestMySQLStringFunctionFamilies(t *testing.T) {
	d, err := db.Open(t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	s := mysqlTypeSession(t, d)
	for _, tc := range []struct{ expression, want string }{
		{"CONCAT_WS(',', 'a', NULL, '', 'b')", "a,,b"}, {"CONCAT_WS(',',NULL,NULL)", ""},
		{"LEFT('café🪐',4)", "café"}, {"RIGHT('café🪐',2)", "é🪐"}, {"LEFT('abc',-1)", ""}, {"LEFT('abc',1.5)", "ab"}, {"LEFT('abc','1.9')", "a"},
		{"RIGHT('abc',18446744073709551615)", "abc"}, {"LEFT('abc','18446744073709551615')", ""},
		{"LEFT('abcd',CAST(2.5 AS DOUBLE))", "ab"}, {"LEFT('abcd',CAST(2.5 AS DECIMAL(5,1)))", "abc"},
		{"LPAD('é',4,'xy')", "xyxé"}, {"RPAD('é',4,'xy')", "éxyx"}, {"LPAD('abcdef',3,'x')", "abc"}, {"LPAD('x',0,'')", ""},
		{"LPAD('a',3,'')", ""}, {"RPAD('a',3,'')", ""},
		{"REPEAT('ab',1.5)", "abab"}, {"REPEAT('a','1.9')", "a"}, {"REVERSE('café🪐')", "🪐éfac"}, {"SPACE(3)", "   "},
		{"SUBSTRING_INDEX('a.b.c','.',2)", "a.b"}, {"SUBSTRING_INDEX('a.b.c','.',-2)", "b.c"}, {"SUBSTRING_INDEX('abc','.',-2)", "abc"},
		{"SUBSTRING_INDEX('a...b','..',-1)", ".b"}, {"SUBSTRING_INDEX('a...b','..',1)", "a"},
		{"LPAD('é',4,X'7879')", "xyxé"}, {"INSERT('éé',2,1,X'78')", "éx"}, {"HEX(SOUNDEX(X'C3A961'))", "41303030"},
		{"LOCATE('é','aéx')", "2"}, {"LOCATE('x',X'C3A978')", "3"}, {"POSITION('é' IN 'aéx')", "2"}, {"LOCATE('a','baab',3)", "3"},
		{"LOCATE('a ','a' COLLATE utf8mb4_general_ci)", "0"}, {"LOCATE('A','a' COLLATE utf8mb4_general_ci)", "1"},
		{"STRCMP('a','b')", "-1"}, {"STRCMP('a','a')", "0"}, {"STRCMP('a','a ')", "-1"}, {"STRCMP('a','a ' COLLATE utf8mb4_bin)", "0"},
		{"FIELD('b','a','b','c')", "2"}, {"FIELD(NULL,NULL,'')", "0"}, {"FIELD(2,'2x','3')", "1"}, {"FIND_IN_SET('b','a,b,c')", "2"}, {"FIND_IN_SET('','a,')", "2"},
		{"ELT(1.5,'a','b')", "b"}, {"ELT('1.9','a','b')", "a"}, {"INSERT('éé',2,1,'x')", "éx"}, {"INSERT('abcd',2,-1,'x')", "ax"},
		{"ASCII('é')", "195"}, {"ORD('é')", "50089"}, {"ORD(CAST('é' AS BINARY))", "195"}, {"CHAR_LENGTH('é🪐')", "2"}, {"CHARACTER_LENGTH('é🪐')", "2"}, {"OCTET_LENGTH('é🪐')", "6"}, {"BIT_LENGTH('é🪐')", "48"},
		{"LCASE('ABCé')", "abcé"}, {"UCASE('abcé')", "ABCÉ"}, {"MID('café',-2,1)", "f"}, {"MID('café' FROM 2 FOR 2)", "af"}, {"SUBSTRING('café' FROM 2 FOR 2)", "af"},
		{"HEX(CHAR(77,121,83,81,'76'))", "4D7953514C"}, {"HEX(CHAR(77.9,'77.9'))", "4E4D"}, {"HEX(CHAR(9223372036854775808))", "00"}, {"HEX(CHAR(18446744073709551615))", "FFFFFFFF"}, {"HEX(CHAR(256,65536))", "0100010000"},
		{"CHAR(240,159,170,144 USING utf8mb3)", "🪐"}, {"HEX(UNHEX('abc'))", "0ABC"}, {"TO_BASE64('abc')", "YWJj"}, {"HEX(FROM_BASE64(' YW\nJj '))", "616263"},
		{"MAKE_SET(5,'a','b','c')", "a,c"}, {"MAKE_SET(5,'a',NULL,NULL)", "a"}, {"MAKE_SET(1.9,'a','b')", "b"}, {"MAKE_SET('1.9','a','b')", "a"},
		{"EXPORT_SET(5,'Y','N',',',4)", "Y,N,Y,N"}, {"EXPORT_SET(0,'Y','N','',0)", ""},
		{"SOUNDEX('Ashcraft')", "A2613"}, {"SOUNDEX('hello')", "H400"}, {"SOUNDEX('Pfister')", "P236"}, {"SOUNDEX('éàb')", "é100"}, {"SOUNDEX('123')", ""},
		{"SOUNDEX('bob-bob')", "B000"}, {"SOUNDEX('Tymczak')", "T520"},
		{"TRIM(BOTH 'xy' FROM 'xyxyabxy')", "ab"}, {"TRIM(LEADING 'xy' FROM 'xyabxy')", "abxy"}, {"TRIM(TRAILING FROM ' a  ')", " a"}, {"TRIM(FROM ' a ')", "a"},
	} {
		r := mysqlTypeExec(t, s, "SELECT "+tc.expression+" AS v")
		v := r.Rows[0]["v"]
		got := v.String()
		if v.Type == sql.ColBlob {
			got = string(v.Blob)
		}
		if v.Null || got != tc.want {
			t.Errorf("%s = %q (%v), want %q", tc.expression, got, v, tc.want)
		}
	}
	for _, expression := range []string{"CONCAT_WS(NULL,'a')", "LEFT(NULL,2)", "ELT(0,'a')", "LOCATE(NULL,'a')", "UNHEX('zz')", "FROM_BASE64('YQ==Yg==')", "CHAR(255 USING utf8mb4)", "MAKE_SET(NULL,'a')", "EXPORT_SET(1,'a',NULL)", "TRIM(NULL FROM 'abc')"} {
		r := mysqlTypeExec(t, s, "SELECT "+expression+" AS v")
		if !r.Rows[0]["v"].Null {
			t.Errorf("%s must return NULL, got %v", expression, r.Rows)
		}
	}
	for _, expression := range []string{"LEFT('a')", "CHAR()", "LOCATE('a','b',1,2)", "TRIM(LEADING 'x')", "POSITION('x' 'y')"} {
		if _, err := s.Exec("SELECT " + expression + " FROM (SELECT 1) z WHERE 0"); err == nil {
			t.Errorf("invalid function accepted on empty input: %s", expression)
		}
	}
}

func TestMySQLStringFunctionsPreparedPersistedAndBinary(t *testing.T) {
	path := t.TempDir()
	d, err := db.Open(path, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = d.Close() }()
	s := mysqlTypeSession(t, d)
	mysqlTypeExec(t, s, "CREATE TABLE strings_input(id INTEGER PRIMARY KEY,v TEXT); INSERT INTO strings_input VALUES(1,'café'),(2,'abcd'); CREATE VIEW strings_view AS SELECT id,LPAD(LEFT(v,2),4,'x') AS padded,POSITION('a' IN v) AS pos FROM strings_input")
	p, err := s.Prepare("SELECT CONCAT_WS('-',LEFT(v,?),RIGHT(v,?)) AS v FROM strings_input WHERE id=?")
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	for _, tc := range []struct {
		id   int64
		want string
	}{{1, "ca-é"}, {2, "ab-d"}} {
		r, err := p.Exec(int64(2), int64(1), tc.id)
		if err != nil || r.Rows[0]["v"].Text != tc.want {
			t.Fatal(r, err)
		}
	}
	for _, expression := range []string{"LEFT(X'00FF',1)", "REVERSE(X'00FF')", "CONCAT_WS(X'00','a','b')", "UNHEX('ff')", "FROM_BASE64('/w==')", "CHAR(255)"} {
		r := mysqlTypeExec(t, s, "SELECT "+expression+" AS v")
		if r.Rows[0]["v"].Type != sql.ColBlob {
			t.Errorf("%s lost binary type: %v", expression, r.Rows)
		}
	}
	if err = d.Close(); err != nil {
		t.Fatal(err)
	}
	d, err = db.Open(path, nil)
	if err != nil {
		t.Fatal(err)
	}
	r, err := d.Exec("SELECT padded,pos FROM strings_view ORDER BY id")
	if err != nil || len(r.Rows) != 2 || r.Rows[0]["padded"].Text != "xxca" || r.Rows[0]["pos"].Int != 2 {
		t.Fatal(r, err)
	}
	if err = d.SetCollation("NFKC_CI"); err != nil {
		t.Fatal(err)
	}
	r, err = d.Exec("SELECT LOCATE('x','ﬁx') AS a,LOCATE('x','éx') AS b")
	if err != nil || r.Rows[0]["a"].Int != 2 || r.Rows[0]["b"].Int != 3 {
		t.Fatal(r, err)
	}
}

func TestMySQLStringFunctionMemoryBudget(t *testing.T) {
	options := storage.DefaultOptions()
	options.QueryMemoryBytes = 1024
	d, err := db.Open(t.TempDir(), options)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	s := mysqlTypeSession(t, d)
	for _, expression := range []string{"REPEAT('ab',18446744073709551615)", "LPAD('x',100000,'abc')", "SPACE(100000)", "TO_BASE64(REPEAT('a',900))"} {
		if _, err := s.Exec("SELECT " + expression); err == nil || !strings.Contains(err.Error(), "memory budget") {
			t.Errorf("%s: %v", expression, err)
		}
	}
}
