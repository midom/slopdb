package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"slopdb/db"
	"slopdb/sql"
	"slopdb/storage"
)

func TestLoadTableBulkChunksTypedValuesAndSourceFailure(t *testing.T) {
	for _, bad := range []bool{false, true} {
		t.Run(fmt.Sprint(bad), func(t *testing.T) {
			root := t.TempDir()
			database, err := db.Open(filepath.Join(root, "database"), storage.DefaultOptions())
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = database.Close() })
			const ddl = "CREATE TABLE probe(id INTEGER PRIMARY KEY, amount REAL, note TEXT)"
			if _, err = database.Exec(ddl + "; CREATE INDEX by_note ON probe(note)"); err != nil {
				t.Fatal(err)
			}
			parsed, err := sql.Parse(ddl)
			if err != nil {
				t.Fatal(err)
			}
			var input strings.Builder
			const base int64 = 9007199254740993
			for i := int64(0); i < 601; i++ {
				amount := "1.25"
				if bad && i == 570 {
					amount = "NaN"
				}
				fmt.Fprintf(&input, "%d|%s|note%d|\r\n", base+i, amount, i)
			}
			path := filepath.Join(root, "probe.tbl")
			if err = os.WriteFile(path, []byte(input.String()), 0600); err != nil {
				t.Fatal(err)
			}
			stats, err := loadTable(context.Background(), database, path, parsed[0].(*sql.CreateTableStmt), 300)
			wantRows, wantTransactions := int64(601), int64(3)
			if bad {
				wantRows, wantTransactions = 300, 1
				if err == nil || !strings.Contains(err.Error(), "row 571 column amount") {
					t.Fatalf("expected source diagnostic: %v", err)
				}
			} else if err != nil {
				t.Fatal(err)
			}
			if stats.Rows != wantRows || stats.Transactions != wantTransactions {
				t.Fatalf("unexpected bulk load stats: %+v", stats)
			}
			if err = database.Close(); err != nil {
				t.Fatal(err)
			}
			database, err = db.Open(filepath.Join(root, "database"), storage.DefaultOptions())
			if err != nil {
				t.Fatal(err)
			}
			count, err := database.Exec("SELECT COUNT(*) AS n FROM probe")
			if err != nil || count.Rows[0]["n"].Int != wantRows {
				t.Fatalf("count mismatch after reopen: %v %v", count, err)
			}
			row, err := database.Exec("SELECT id,amount FROM probe WHERE note = 'note299'")
			if err != nil || len(row.Rows) != 1 || row.Rows[0]["id"].Int != base+299 || row.Rows[0]["amount"].Real != 1.25 {
				t.Fatalf("typed indexed row mismatch after reopen: %v %v", row, err)
			}
			if err = database.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}
