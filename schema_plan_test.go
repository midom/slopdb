package main

import (
	"testing"

	"slopdb/db"
)

func TestPlanSchemaChange(t *testing.T) {
	database, err := db.Open(t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	if _, err := database.Exec("CREATE TABLE items(id INTEGER PRIMARY KEY, label VARCHAR(8), payload VARBINARY(8))"); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		sql  string
		work string
	}{
		{"ALTER TABLE items RENAME COLUMN label TO title", "catalog-only"},
		{"ALTER TABLE items ALTER COLUMN label TYPE VARCHAR(16)", "catalog-only"},
		{"ALTER TABLE items ALTER COLUMN payload TYPE VARBINARY(4) ALLOW EXISTING", "catalog-only"},
		{"ALTER TABLE items ALTER COLUMN label TYPE VARCHAR(4)", "table rewrite"},
		{"ALTER TABLE items DROP COLUMN label", "table rewrite"},
		{"ALTER TABLE items ALTER COLUMN label COLLATE BINARY", "database rewrite"},
		{"CREATE INDEX items_label ON items(label)", "online index build"},
	} {
		plan, err := planSchemaChange(database, tc.sql)
		if err != nil {
			t.Fatalf("%s: %v", tc.sql, err)
		}
		if len(plan.Rows) != 1 || plan.Rows[0]["work"].Text != tc.work || plan.Rows[0]["locking"].Text == "" {
			t.Fatalf("%s: got %+v, want work %s", tc.sql, plan, tc.work)
		}
	}
	if _, err := planSchemaChange(database, "DELETE FROM items"); err == nil {
		t.Fatal("accepted non-ALTER statement")
	}
	if _, err := planSchemaChange(database, "ALTER TABLE missing DROP COLUMN x"); err == nil {
		t.Fatal("accepted nonexistent table")
	}
}
