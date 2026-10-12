package main

import (
	"encoding/json"
	"flag"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"slopdb/db"
	"slopdb/sql"
	"slopdb/storage"
)

func TestOneMiBLoadOnlySkipsAllTPCQueries(t *testing.T) {
	root := t.TempDir()
	input := filepath.Join(root, "inputs")
	if err := os.Mkdir(input, 0755); err != nil {
		t.Fatal(err)
	}
	schema, _ := assets.ReadFile("schema.sql")
	statements, err := sql.Parse(string(schema))
	if err != nil {
		t.Fatal(err)
	}
	for _, statement := range statements {
		table := statement.(*sql.CreateTableStmt)
		values := make([]string, len(table.Columns))
		for i, column := range table.Columns {
			switch column.ColType {
			case sql.ColInteger:
				values[i] = "1"
			case sql.ColReal:
				values[i] = "1.25"
			default:
				values[i] = "x"
			}
		}
		if err := os.WriteFile(filepath.Join(input, table.Table+".tbl"), []byte(strings.Join(values, "|")+"|\n"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	directory, output := filepath.Join(root, "database"), filepath.Join(root, "report.json")
	oldArgs, oldFlags := os.Args, flag.CommandLine
	defer func() { os.Args, flag.CommandLine = oldArgs, oldFlags }()
	os.Args = []string{"tpch", "-data", input, "-dir", directory, "-json", output,
		"-page-size", "1048576", "-load-only", "-load-batch-rows", "2", "-runs", "3", "-warmups", "1", "-timeout", "1ns"}
	flag.CommandLine = flag.NewFlagSet("tpch", flag.ContinueOnError)
	if err := run(); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(output)
	if err != nil {
		t.Fatal(err)
	}
	var result report
	if err := json.Unmarshal(raw, &result); err != nil {
		t.Fatal(err)
	}
	if !result.Config.LoadOnly || result.QueryBenchmarked || len(result.Queries) != 0 ||
		result.Successful != 0 || result.Failed != 0 || result.TimedOut != 0 || !result.CountsVerified ||
		result.PageSize != 1048576 || result.LoadMode != "bulk_insert" || result.LoadTransactions != 8 {
		t.Fatalf("load-only executed queries or missed persisted loading: %+v", result)
	}
	database, err := db.Open(directory, storage.DefaultOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	if definition := database.GetCreateViewSQL("tpch_revenue"); definition == "" {
		t.Fatalf("load-only must retain the shared Q15 schema; views=%v", database.GetViewList())
	}
}
