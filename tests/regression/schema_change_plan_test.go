package regression

import (
	"context"
	"os"
	"os/exec"
	"slices"
	"strings"
	"testing"
	"time"
)

func TestExplainSchemaChangesPreservesCatalogAndRows(t *testing.T) {
	env := newRegressionEnv(t)
	d, err := env.openDatabase(env.path("schema_plans"), nil)
	if err != nil {
		t.Fatal(err)
	}
	requireExec(t, d, "CREATE TABLE t(id INTEGER PRIMARY KEY, label VARCHAR(8), exact TEXT COLLATE BINARY)")
	requireExec(t, d, "INSERT INTO t VALUES(1, 'hello', 'world')")
	requireExec(t, d, "CREATE INDEX existing ON t(label)")
	before := d.GetCreateTableSQL("t")
	beforeIndexes := d.GetIndexList()
	for _, tc := range []struct {
		query string
		work  string
	}{
		{"ALTER TABLE t ALTER COLUMN label TYPE VARCHAR(16)", "catalog-only"},
		{"ALTER TABLE t ALTER COLUMN label TYPE VARCHAR(4) ALLOW EXISTING", "catalog-only"},
		{"ALTER TABLE t ALTER COLUMN label TYPE VARCHAR(4)", "table rewrite"},
		{"ALTER TABLE t ALTER COLUMN exact COLLATE BINARY", "catalog-only"},
		{"ALTER TABLE t ALTER COLUMN label COLLATE BINARY", "database rewrite"},
		{"CREATE UNIQUE INDEX planned ON t(label)", "online index build"},
		{"DROP INDEX existing", "catalog-only"},
		{"ALTER TABLE t ADD INDEX planned(label)", "online index build"},
		{"ALTER TABLE t DROP INDEX existing", "catalog-only"},
		{"ALTER TABLE t ADD COLUMN IF NOT EXISTS label TEXT", "no-op"},
		{"ALTER TABLE t DROP COLUMN IF EXISTS missing", "no-op"},
		{"CREATE INDEX IF NOT EXISTS existing ON t(label)", "no-op"},
	} {
		result := requireExec(t, d, "EXPLAIN "+tc.query)
		if len(result.Rows) != 1 || result.Rows[0]["work"].Text != tc.work || result.Rows[0]["locking"].Text == "" {
			t.Fatalf("%s: unexpected plan %v", tc.query, result.Rows)
		}
	}
	for _, query := range []string{
		"EXPLAIN ALTER TABLE t ALTER COLUMN label TYPE VARCHAR(16), ALTER COLUMN label TYPE VARCHAR(12) ALLOW EXISTING",
		"EXPLAIN ALTER TABLE t RENAME COLUMN label TO title, ALTER COLUMN title TYPE VARCHAR(16)",
		"EXPLAIN ALTER TABLE t RENAME TO renamed, DROP INDEX existing",
		"EXPLAIN ALTER TABLE t RENAME INDEX existing TO renamed_index, DROP INDEX renamed_index",
	} {
		result := requireExec(t, d, query)
		if len(result.Rows) != 2 {
			t.Fatalf("%s: missing action plan: %v", query, result.Rows)
		}
		for _, row := range result.Rows {
			if row["work"].Text != "catalog-only" {
				t.Fatalf("%s: planning ignored earlier metadata action: %v", query, result.Rows)
			}
		}
	}
	if _, err := d.Exec("EXPLAIN ALTER TABLE t ALTER COLUMN missing SET DEFAULT 1"); err == nil {
		t.Fatal("metadata plan accepted a nonexistent column")
	}
	requireExec(t, d, "BEGIN")
	result := requireExec(t, d, "EXPLAIN CREATE INDEX planned ON t(label)")
	if result.Rows[0]["work"].Text != "index build" {
		t.Fatalf("transactional index plan reported an online build: %v", result.Rows)
	}
	requireExec(t, d, "ROLLBACK")
	if _, err := d.Exec("EXPLAIN ANALYZE CREATE INDEX planned ON t(label)"); err == nil {
		t.Fatal("EXPLAIN ANALYZE executed schema-changing input")
	}
	if d.GetCreateTableSQL("t") != before || !slices.Equal(d.GetIndexList(), beforeIndexes) || d.GetIndexSQL("existing") == "" {
		t.Fatalf("schema explanations changed the catalog: schema=%s indexes=%v (before=%v)", d.GetCreateTableSQL("t"), d.GetIndexList(), beforeIndexes)
	}
	result = requireExec(t, d, "SELECT label, exact FROM t WHERE id=1")
	if len(result.Rows) != 1 || result.Rows[0]["label"].Text != "hello" || result.Rows[0]["exact"].Text != "world" {
		t.Fatalf("schema explanations changed data: %v", result.Rows)
	}
}

func TestCLIPlanPreservesSchemaAndHonorsOutputAndBail(t *testing.T) {
	env := newRegressionEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	path, outputPath := env.path("cli_plan"), env.path("plan.txt")
	command := exec.CommandContext(ctx, env.cli(), "--allow-dot-commands", path)
	command.Stdin = strings.NewReader("CREATE TABLE t(id INTEGER PRIMARY KEY, label VARCHAR(8)); CREATE INDEX existing ON t(label);\n" +
		".output " + outputPath + "\n.plan CREATE INDEX planned ON t(label)\n.plan DROP INDEX existing\n" +
		".plan ALTER TABLE t ALTER COLUMN label TYPE VARCHAR(16)\n.output\n")
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("CLI plan failed: %v: %s", err, output)
	}
	contents, err := os.ReadFile(outputPath)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"online index build", "catalog-only", "exclusive catalog lock"} {
		if !strings.Contains(string(contents), want) {
			t.Fatalf("missing plan detail %q in redirected output: %s", want, contents)
		}
	}
	database, err := env.openDatabase(path, nil)
	if err != nil {
		t.Fatal(err)
	}
	if database.GetIndexSQL("planned") != "" || database.GetIndexSQL("existing") == "" || !strings.Contains(database.GetCreateTableSQL("t"), "VARCHAR(8)") {
		t.Fatal("CLI plan executed DDL")
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	bail := exec.CommandContext(ctx, env.cli(), "--allow-dot-commands", path)
	bail.Stdin = strings.NewReader(".bail on\n.plan DELETE FROM t\nDROP TABLE t;\n")
	if output, err := bail.CombinedOutput(); err == nil || !strings.Contains(string(output), "Plan error:") {
		t.Fatalf("CLI plan ignored .bail: %v: %s", err, output)
	}
}
