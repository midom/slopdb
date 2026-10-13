package regression

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"slopdb/db"
	"slopdb/sql"
	"slopdb/storage"
)

func TestMySQLQuotingOptionsAndPersistence(t *testing.T) {
	env := newRegressionEnv(t)
	options := storage.DefaultOptions()
	options.MySQLQuoting = true
	options.DisableWALSync = true
	dir := env.path("mysql")
	d, err := env.openDatabase(dir, options)
	if err != nil {
		t.Fatal(err)
	}
	// Open owns its options; later caller changes must not change parsing.
	options.MySQLQuoting = false
	if !d.ParseOptions().MySQLQuoting {
		t.Fatal("Open did not copy quoting option")
	}
	requireExec(t, d, "CREATE TABLE `a``b` (id INTEGER PRIMARY KEY, `select` TEXT DEFAULT \"default\\n\\\\path\" CHECK (`select` <> \"blocked\")); CREATE INDEX av ON `a``b` (`select`); CREATE TABLE logs (v TEXT)")
	requireExec(t, d, "CREATE VIEW words AS SELECT id, `select`, \"view\\n\" AS marker FROM `a``b`; CREATE TRIGGER record AFTER INSERT ON `a``b` BEGIN INSERT INTO logs VALUES (NEW.`select` || \"\\\\suffix\\n\"); END")
	requireExec(t, d, "INSERT INTO `a``b`(id) VALUES(1); INSERT INTO `a``b` VALUES(2, \"a\\\";b\\\\c\")")
	expected := []string{"default\n\\path", "a\";b\\c"}
	check := func(database *db.Database, table string) {
		t.Helper()
		result := requireExec(t, database, "SELECT `select` FROM "+table+" ORDER BY id")
		var values []string
		for _, row := range result.Rows {
			values = append(values, row["select"].Text)
		}
		if !reflect.DeepEqual(values, expected) {
			t.Fatalf("stored values: %q, expected %q", values, expected)
		}
		view := requireExec(t, database, "SELECT * FROM words ORDER BY id")
		if len(view.Rows) != 2 || view.Rows[0]["marker"].Text != "view\n" {
			t.Fatalf("stored view: %+v", view.Rows)
		}
		logs := requireExec(t, database, "SELECT v FROM logs")
		if len(logs.Rows) != 2 || logs.Rows[0]["v"].Text != expected[0]+"\\suffix\n" || logs.Rows[1]["v"].Text != expected[1]+"\\suffix\n" {
			t.Fatalf("stored trigger: %+v", logs.Rows)
		}
	}
	check(d, "`a``b`")
	requireExec(t, d, "CREATE TABLE controls(v TEXT)")
	controlValue := "\x00\b\n\r\t\x1a\\'\";\\%\\_"
	controlInsert, err := d.Prepare("INSERT INTO controls VALUES(?)")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := controlInsert.Exec(controlValue); err != nil {
		t.Fatal(err)
	}
	if err := controlInsert.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := d.Exec("INSERT INTO `a``b` VALUES(3, \"blocked\")"); err == nil {
		t.Fatal("MySQL-quoted CHECK not enforced")
	}
	if got := requireExec(t, d, `SELECT "select" AS literal FROM `+"`a``b`"+` WHERE id = 1`).Rows[0]["literal"].Text; got != "select" {
		t.Fatalf("double quotes are not strings: %q", got)
	}
	if got := requireExec(t, d, `SELECT 'a\_b' LIKE "a\_b" AS matched`).Rows[0]["matched"].Int; got != 0 {
		t.Fatalf("LIKE escapes not preserved: %d", got)
	}
	if got := requireExec(t, d, `SELECT "a_b" LIKE "a\_b" AS matched`).Rows[0]["matched"].Int; got != 1 {
		t.Fatalf("LIKE literal underscore: %d", got)
	}
	prepared, err := d.Prepare(`SELECT "quoted\"?;text" AS literal, ? AS bound`)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = prepared.Close() })
	result, err := prepared.Exec(`binding\n`)
	if err != nil || prepared.NumInput() != 1 || result.Rows[0]["literal"].Text != `quoted"?;text` || result.Rows[0]["bound"].Text != `binding\n` {
		t.Fatalf("prepared values: %+v %v", result, err)
	}
	session, err := d.NewSession()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = session.Close() })
	result, err = session.Exec(`SELECT "session\n" AS value`)
	if err != nil || result.Rows[0]["value"].Text != "session\n" {
		t.Fatalf("session quoting: %+v %v", result, err)
	}
	p, err := session.Prepare(`SELECT "prepared\n" AS value`)
	if err != nil {
		t.Fatal(err)
	}
	result, err = p.Exec()
	if err != nil || result.Rows[0]["value"].Text != "prepared\n" {
		t.Fatalf("session prepare: %+v %v", result, err)
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	checkRows := func(rows *db.Rows, err error, want string) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = rows.Close() })
		var value string
		if !rows.Next() {
			t.Fatalf("missing streamed row: %v", rows.Err())
		}
		if err := rows.Scan(&value); err != nil || value != want {
			t.Fatalf("streamed value %q, want %q (%v)", value, want, err)
		}
		if rows.Next() || rows.Err() != nil {
			t.Fatalf("stream completion: %v", rows.Err())
		}
		if err := rows.Close(); err != nil {
			t.Fatal(err)
		}
	}
	rows, err := d.Query(`SELECT "database\n" AS value`)
	checkRows(rows, err, "database\n")
	rows, err = session.Query(`SELECT "stream\n" AS value`)
	checkRows(rows, err, "stream\n")
	rows, err = prepared.Query(`query\n`)
	if err != nil {
		t.Fatal(err)
	}
	if !rows.Next() {
		t.Fatal("prepared Query missing row")
	}
	var literal, bound string
	if err := rows.Scan(&literal, &bound); err != nil || literal != `quoted"?;text` || bound != `query\n` {
		t.Fatalf("prepared stream: %q %q %v", literal, bound, err)
	}
	if err := rows.Close(); err != nil {
		t.Fatal(err)
	}
	tx, err := d.Begin()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = tx.Rollback() })
	result, err = tx.Exec(`SELECT "transaction\n" AS value`)
	if err != nil || result.Rows[0]["value"].Text != "transaction\n" {
		t.Fatalf("transaction quoting: %+v %v", result, err)
	}
	rows, err = tx.Query(`SELECT "tx-stream\n" AS value`)
	checkRows(rows, err, "tx-stream\n")
	p, err = tx.Prepare(`SELECT "tx-prepared\n" AS value`)
	if err != nil {
		t.Fatal(err)
	}
	result, err = p.Exec()
	if err != nil || result.Rows[0]["value"].Text != "tx-prepared\n" {
		t.Fatalf("transaction prepare: %+v %v", result, err)
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	for _, query := range []string{"SHOW CREATE TABLE `a``b`", "SHOW CREATE VIEW words", "SHOW CREATE TRIGGER record"} {
		result := requireExec(t, d, query)
		for _, name := range []string{"Create table", "Create view", "SQL Original Statement"} {
			if text, ok := result.Rows[0][name]; ok {
				if _, err := sql.ParseWithOptions(text.Text, d.ParseOptions()); err != nil {
					t.Fatalf("SHOW CREATE roundtrip: %q %v", text.Text, err)
				}
			}
		}
	}
	restored, err := env.openDatabase(env.path("restored"), &storage.Options{MySQLQuoting: true, CreateIfNotExist: true, DisableWALSync: true})
	if err != nil {
		t.Fatal(err)
	}
	requireExec(t, restored, d.Dump())
	check(restored, "`a``b`")
	if value := requireExec(t, restored, "SELECT v FROM controls").Rows[0]["v"].Text; value != controlValue {
		t.Fatalf("control characters in dump: %q, want %q", value, controlValue)
	}
	if err := restored.Close(); err != nil {
		t.Fatal(err)
	}
	requireExec(t, d, "ALTER TABLE `a``b` RENAME TO renamed")
	check(d, "renamed")
	if err := session.Close(); err != nil {
		t.Fatal(err)
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	// Input dialect is an open-time option. Stored definitions work in both
	// quoting modes, including backslashes in persisted trigger bodies.
	for _, mysql := range []bool{false, true} {
		options.MySQLQuoting = mysql
		d, err = env.openDatabase(dir, options)
		if err != nil {
			t.Fatal(err)
		}
		check(d, "renamed")
		if !mysql {
			result := requireExec(t, d, `SELECT "select" FROM "renamed" WHERE id=1`)
			if result.Rows[0]["select"].Text != expected[0] {
				t.Fatal("default identifier quoting changed")
			}
			if value := requireExec(t, d, `SELECT 'raw\n' AS value`).Rows[0]["value"].Text; value != `raw\n` {
				t.Fatalf("default escapes changed: %q", value)
			}
		}
		requireExec(t, d, "BEGIN; INSERT INTO renamed(id) VALUES(9)")
		if requireExec(t, d, "SELECT COUNT(*) AS n FROM logs").Rows[0]["n"].Int != 3 {
			t.Fatal("reopened trigger did not execute")
		}
		requireExec(t, d, "ROLLBACK")
		if err := d.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestCLIMySQLQuoting(t *testing.T) {
	env := newRegressionEnv(t)
	binary := env.cli()
	first, second := env.path("first"), env.path("second")
	run := func(args []string, input string) string {
		t.Helper()
		command := exec.Command(binary, args...)
		command.Stdin = strings.NewReader(input)
		output, err := command.CombinedOutput()
		if err != nil {
			t.Fatalf("CLI %v: %v\n%s", args, err, output)
		}
		return string(output)
	}
	// A semicolon after an escaped quote belongs to the string, including in
	// trigger bodies. The CLI splitter must not turn it into another command.
	input := "CREATE TABLE t(v TEXT); CREATE TABLE logs(v TEXT); CREATE TRIGGER tr AFTER INSERT ON t BEGIN INSERT INTO logs VALUES (NEW.v || \"suffix\\n\"); END; INSERT INTO t VALUES (\"a\\\";b\"); SELECT v FROM logs;\n"
	output := run([]string{"--mysql-quoting", first}, input)
	if !strings.Contains(output, "a\";bsuffix") || strings.Contains(output, "Error") {
		t.Fatal(output)
	}
	script := filepath.Join(t.TempDir(), "quotes.sql")
	if err := os.WriteFile(script, []byte("INSERT INTO t VALUES('read\\';value'); SELECT v FROM t;\n"), 0600); err != nil {
		t.Fatal(err)
	}
	output = run([]string{"--mysql-quoting", "--allow-dot-commands", first}, ".read "+script+"\n.status\n.open "+second+"\nSELECT \"open\\n\" AS value;\n")
	if !strings.Contains(output, "read';value") || !strings.Contains(output, "SQL quoting: MySQL") || !strings.Contains(output, "open") || strings.Contains(output, "Error") {
		t.Fatal(output)
	}
	output = run([]string{first}, "SELECT v FROM t;\nSELECT 'raw\\n' AS value;\n")
	if !strings.Contains(output, `raw\n`) || strings.Contains(output, "Error") {
		t.Fatal(output)
	}
	// Preparation checks cancellation before parsing under the selected mode.
	options := storage.DefaultOptions()
	options.MySQLQuoting = true
	d, err := env.openDatabase(second, options)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := d.PrepareContext(ctx, `SELECT "cancel\n"`); err != context.Canceled {
		t.Fatalf("prepare cancellation: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
}
