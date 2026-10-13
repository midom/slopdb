package cli

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"slopdb/db"
	"slopdb/sql"
	"slopdb/storage"
)

func TestMySQLQuotingStatementInput(t *testing.T) {
	options := sql.ParseOptions{MySQLQuoting: true}
	input := "SELECT \"a\\\";b\"; SELECT 'c\\';d'; SELECT `semi;colon`;"
	statements := splitSQLWithOptions(input, options)
	if len(statements) != 3 {
		t.Fatalf("MySQL splitting: %q", statements)
	}
	for _, statement := range statements {
		if _, err := sql.ParseWithOptions(statement, options); err != nil {
			t.Fatalf("split SQL %q: %v", statement, err)
		}
	}
	for _, tc := range []struct {
		input    string
		complete bool
	}{
		{`SELECT "a\";`, false},
		{`SELECT "a\";b";`, true},
		{`SELECT 'a\';`, false},
		{`SELECT 'a\';b';`, true},
		{`SELECT "semicolon;"`, false},
		{`SELECT 1; -- trailing comment`, true},
		{`SELECT '\\';`, true},
	} {
		if got := hasStatementTerminator(tc.input, options); got != tc.complete {
			t.Fatalf("completion for %q: %t", tc.input, got)
		}
	}
	// Quoting inside identifiers retains semicolons and doubled backticks.
	statements = splitSQLWithOptions("SELECT `a``;b`; SELECT 'next';", options)
	if len(statements) != 2 {
		t.Fatalf("identifier splitting: %q", statements)
	}
	explanation, err := queryExplanationSQLWithOptions(`SELECT "a\";b" AS label`, options)
	if err != nil || !strings.HasPrefix(explanation, "EXPLAIN ") {
		t.Fatalf("MySQL explain: %q %v", explanation, err)
	}
}

func TestMySQLQuotingImportAndPlanning(t *testing.T) {
	options := storage.DefaultOptions()
	options.MySQLQuoting = true
	database, err := db.Open(t.TempDir(), options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	previousDB, previousOptions, previousCfg := currentDB, currentDBOptions, cliCfg
	cfg := *cliCfg
	cfg.allowDotCommands = true
	currentDB, currentDBOptions, cliCfg = database, options, &cfg
	t.Cleanup(func() { currentDB, currentDBOptions, cliCfg = previousDB, previousOptions, previousCfg })
	file := filepath.Join(t.TempDir(), "import.csv")
	if err := os.WriteFile(file, []byte("name,value\none,quoted\n"), 0600); err != nil {
		t.Fatal(err)
	}
	handleImport(file, "new`table")
	result, err := database.Exec("SELECT value FROM `new``table`")
	if err != nil || len(result.Rows) != 1 || result.Rows[0]["value"].Text != "quoted" {
		t.Fatalf("MySQL CSV import: %+v %v", result, err)
	}
	plan, err := planSchemaChange(database, "ALTER TABLE `new``table` ADD COLUMN note TEXT DEFAULT \"a\\\";b\"")
	if err != nil || len(plan.Rows) != 1 {
		t.Fatalf("MySQL plan: %+v %v", plan, err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
}
