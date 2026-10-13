package sql

import (
	"strings"
	"testing"
)

func TestMySQLQuotedTokens(t *testing.T) {
	options := ParseOptions{MySQLQuoting: true}
	for _, tc := range []struct {
		query string
		typ   TokenType
		value string
	}{
		{`"hello"`, TokString, "hello"},
		{`'hello'`, TokString, "hello"},
		{`"a\"b;?"`, TokString, `a"b;?`},
		{`'a\'b;?'`, TokString, `a'b;?`},
		{`'\0\b\n\r\t\Z\\\"\'\%\_\q\B'`, TokString, "\x00\b\n\r\t\x1a\\\"'\\%\\_qB"},
		{`"a""b"`, TokString, `a"b`},
		{`'a''b'`, TokString, `a'b`},
		{"`a``b\\n`", TokIdent, "a`b\\n"},
		{`[a\n]`, TokIdent, `a\n`},
		{`X'001AFF'`, TokBlob, "001AFF"},
	} {
		t.Run(tc.query, func(t *testing.T) {
			tokenizer := NewTokenizerWithOptions("  "+tc.query+";", options)
			peek := tokenizer.Peek()
			got := tokenizer.Next()
			if got != peek || got.Type != tc.typ || got.Value != tc.value || got.Start != 2 {
				t.Fatalf("token: %+v, want type=%v value=%q", got, tc.typ, tc.value)
			}
			if got := tokenizer.Next(); got.Type != TokSemicolon {
				t.Fatalf("semicolon swallowed: %+v", got)
			}
		})
	}
	// Default quoting keeps double-quoted identifiers and literal backslashes.
	for _, tc := range []struct {
		query string
		typ   TokenType
		value string
	}{
		{`"a\nb"`, TokIdent, `a\nb`},
		{`'a\nb'`, TokString, `a\nb`},
	} {
		got := NewTokenizer(tc.query).Next()
		if got.Type != tc.typ || got.Value != tc.value {
			t.Fatalf("default mode changed: %+v", got)
		}
	}
	for _, query := range []string{`'unterminated`, `"unterminated`, `'trailing\`, `"trailing\`, "`unterminated"} {
		if got := NewTokenizerWithOptions(query, options).Next(); got.Type != TokInvalid {
			t.Fatalf("accepted unterminated quote %q: %+v", query, got)
		}
	}
}

func TestMySQLQuotingParseAndPrepare(t *testing.T) {
	options := ParseOptions{MySQLQuoting: true}
	parsed, err := ParseWithOptions(`SELECT "a\n" 'b' AS "label", 1 "number"; SHOW TABLE STATUS LIKE "a\_b"`, options)
	if err != nil {
		t.Fatal(err)
	}
	selectStmt := parsed[0].(*SelectStmt)
	if selectStmt.Columns[0].Expr.(*LiteralExpr).Value.Text != "a\nb" || selectStmt.Columns[0].Alias != "label" || selectStmt.Columns[1].Alias != "number" {
		t.Fatalf("select: %+v", selectStmt.Columns)
	}
	if pattern := parsed[1].(*ShowStmt).Like; pattern == nil || *pattern != `a\_b` {
		t.Fatalf("LIKE pattern: %+v", pattern)
	}
	prepared, err := PrepareWithOptions(`SELECT "quoted\"?;text" AS literal, ? AS bound`, options)
	if err != nil || prepared.NumInput() != 1 {
		t.Fatalf("prepare: %+v, %v", prepared, err)
	}
	bound, err := prepared.Bind([]Value{NewTextValue("binding\\n")})
	if err != nil {
		t.Fatal(err)
	}
	defer bound.Release()
	stmt := bound.Statements()[0].(*SelectStmt)
	if stmt.Columns[0].Expr.(*LiteralExpr).Value.Text != `quoted"?;text` || stmt.Columns[1].Expr.(*LiteralExpr).Value.Text != `binding\n` {
		t.Fatalf("bound AST: %+v", stmt)
	}
	for _, query := range []string{`CREATE TABLE "t" (id INTEGER)`, `SELECT * FROM "t"`, `SELECT "trailing\`, `SELECT 'a\'`, `SELECT "x" LIKE "x" ESCAPE ""`, `SELECT "x" NOT LIKE "x" ESCAPE "long"`} {
		if _, err := ParseWithOptions(query, options); err == nil {
			t.Fatalf("accepted invalid MySQL quotes: %s", query)
		}
	}
	// Trigger bodies persist decoded, canonical SQL, including aliases and
	// adjacent strings. They must parse with the default catalog parser.
	parsed, err = ParseWithOptions(`CREATE TRIGGER tr AFTER INSERT ON t BEGIN INSERT INTO logs VALUES ("a\n" 'b', NEW.id); SELECT "c\\d" AS "label"; END`, options)
	if err != nil {
		t.Fatal(err)
	}
	trigger := parsed[0].(*CreateTriggerStmt)
	for _, text := range trigger.BodySQL {
		if _, err := Parse(text); err != nil {
			t.Fatalf("canonical trigger %q: %v", text, err)
		}
		if strings.Contains(text, `"a\n"`) {
			t.Fatal("raw MySQL trigger SQL persisted")
		}
	}
}

func FuzzMySQLQuoting(f *testing.F) {
	for _, query := range []string{`SELECT "a\";?"`, `SELECT '\0\n\\'`, `CREATE TABLE ` + "`a``b`" + `(v TEXT DEFAULT "a\n")`, `CREATE TRIGGER tr AFTER INSERT ON t BEGIN INSERT INTO t VALUES("x"); END`, `SELECT "trailing\`} {
		f.Add(query)
	}
	f.Fuzz(func(t *testing.T, query string) { _, _ = ParseWithOptions(query, ParseOptions{MySQLQuoting: true}) })
}
