package regression

import (
	"context"
	"os"
	"os/exec"
	"regexp"
	"strings"
	"testing"
	"time"
)

func TestCLIStatusShowsEffectiveDurabilityAndStorage(t *testing.T) {
	env := newRegressionEnv(t)
	for _, tc := range []struct {
		name string
		flag string
		want string
	}{
		{"default", "", "Durability: WAL enabled with fsync"},
		{"no-fsync", "--no-wal-fsync", "Durability: WAL enabled without fsync"},
		{"no-wal", "--no-wal", "Durability: WAL disabled"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			args := []string{"--allow-dot-commands", "--page-size=16384", "--query-timeout=5s", "--lock-timeout=2s", "--deadlock-detection", "--l0-compression=snappy"}
			if tc.flag != "" {
				args = append(args, tc.flag)
			}
			args = append(args, env.path(tc.name))
			command := exec.CommandContext(ctx, env.cli(), args...)
			command.Stdin = strings.NewReader("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT); INSERT INTO t VALUES(1, 'one');\n.status\n")
			output, err := command.CombinedOutput()
			if err != nil {
				t.Fatalf("CLI failed: %v: %s", err, output)
			}
			for _, want := range []string{tc.want, "SSTable page target: 16384 bytes", "Memtables:", "SSTables:", "Database:", "Timeouts: query 5s, lock wait 2s", "Deadlock detection: true", "Compression for new blocks: L0 snappy, L1-L6 zstd"} {
				if !strings.Contains(string(output), want) {
					t.Fatalf("missing %q in status:\n%s", want, output)
				}
			}
			// Closing the first process flushes its rows into an SSTable.
			status := exec.CommandContext(ctx, env.cli(), "--allow-dot-commands", env.path(tc.name))
			status.Stdin = strings.NewReader(".status\n")
			persisted, err := status.CombinedOutput()
			if err != nil || !regexp.MustCompile(`L[0-6]: [1-9][0-9]* files`).Match(persisted) {
				t.Fatalf("missing persisted level summary: %v: %s", err, persisted)
			}
			// A clean close flushes rows even when WAL is disabled.
			database, err := env.openDatabase(env.path(tc.name), nil)
			if err != nil {
				t.Fatal(err)
			}
			result, err := database.Exec("SELECT v FROM t WHERE id=1")
			if err != nil || len(result.Rows) != 1 || result.Rows[0]["v"].Text != "one" {
				t.Fatal("status changed database state", err, result)
			}
			if err := database.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestCLIExplainShowsPlanWithoutExecutingQuery(t *testing.T) {
	env := newRegressionEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, env.cli(), "--allow-dot-commands", env.path("explain"))
	command.Stdin = strings.NewReader("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT); INSERT INTO t VALUES(1, 'one');\n" +
		".explain SELECT v FROM t WHERE id=1\n.explain SELECT v FROM t ORDER BY v\n.explain SELECT 1/0\n" +
		".explain DELETE FROM t\n.explain SELECT * FROM t; DELETE FROM t\n.explain EXPLAIN ANALYZE SELECT * FROM t\n" +
		"SELECT COUNT(*) AS retained FROM t;\n.locks\n")
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("CLI failed: %v: %s", err, output)
	}
	for _, want := range []string{"Estimated query plan (query not executed)", "PointLookup", "Sorting: rows need a separate sort", "Explain error: .explain requires one SELECT statement", "run EXPLAIN ANALYZE SELECT explicitly", "No active lock waits."} {
		if !strings.Contains(string(output), want) {
			t.Fatalf("missing %q in explanation:\n%s", want, output)
		}
	}
	if strings.Contains(string(output), "division by zero") {
		t.Fatalf(".explain evaluated the query:\n%s", output)
	}
	database, err := env.openDatabase(env.path("explain"), nil)
	if err != nil {
		t.Fatal(err)
	}
	result, err := database.Exec("SELECT COUNT(*) AS n FROM t")
	if err != nil || len(result.Rows) != 1 || result.Rows[0]["n"].Int != 1 {
		t.Fatalf(".explain changed data: %v, %v", err, result)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestCLIDiagnosticsRespectOutputAndBail(t *testing.T) {
	env := newRegressionEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	outputPath := env.path("status-output.txt")
	command := exec.CommandContext(ctx, env.cli(), "--allow-dot-commands", env.path("diagnostics-output"))
	command.Stdin = strings.NewReader(".output " + outputPath + "\n.status\n.explain SELECT 1\n.locks\n.output\n")
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("CLI failed: %v: %s", err, output)
	}
	contents, err := os.ReadFile(outputPath)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"Durability:", "Timeouts: query unlimited, lock wait 30s", "query not executed", "No active lock waits."} {
		if !strings.Contains(string(contents), want) {
			t.Fatalf("missing redirected %q: %s", want, contents)
		}
	}
	if strings.Contains(string(output), "Durability:") {
		t.Fatalf("status ignored .output: %s", output)
	}
	bail := exec.CommandContext(ctx, env.cli(), "--allow-dot-commands", env.path("diagnostics-bail"))
	bail.Stdin = strings.NewReader(".bail on\n.explain DELETE FROM missing\nCREATE TABLE after_error(id INTEGER);\n")
	if output, err := bail.CombinedOutput(); err == nil || !strings.Contains(string(output), "Explain error:") {
		t.Fatalf("invalid explain did not honor .bail: %v: %s", err, output)
	}
}
