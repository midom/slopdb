package regression

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"slopdb/sql"
	"slopdb/storage"
)

func TestShowTableStatus(t *testing.T) {
	env := newRegressionEnv(t)
	dir := env.path("status")
	options := storage.DefaultOptions()
	options.DisableWALSync = true
	d, err := env.openDatabase(dir, options)
	if err != nil {
		t.Fatal(err)
	}
	empty := requireExec(t, d, "SHOW TABLE STATUS")
	wantColumns := []string{"Name", "Engine", "Version", "Row_format", "Rows", "Avg_row_length", "Data_length", "Max_data_length", "Index_length", "Data_free", "Auto_increment", "Create_time", "Update_time", "Check_time", "Collation", "Checksum", "Create_options", "Comment"}
	var names []string
	for _, col := range empty.Columns {
		names = append(names, col.Name)
	}
	if !reflect.DeepEqual(names, wantColumns) || len(empty.Rows) != 0 {
		t.Fatalf("empty status: %+v", empty)
	}
	requireExec(t, d, `CREATE TABLE a_b (id INTEGER PRIMARY KEY AUTOINCREMENT, v TEXT); CREATE INDEX a_v ON a_b(v); CREATE TABLE bare (v BLOB); CREATE VIEW z_view AS SELECT * FROM a_b; INSERT INTO a_b(v) VALUES ('one'),('two'),('three'); INSERT INTO bare VALUES (X'010203')`)
	check := func() {
		t.Helper()
		all := requireExec(t, d, "SHOW TABLE STATUS FROM main")
		if len(all.Rows) != 3 || all.Rows[0]["Name"].Text != "a_b" || all.Rows[1]["Name"].Text != "bare" || all.Rows[2]["Name"].Text != "z_view" {
			t.Fatalf("status order: %+v", all.Rows)
		}
		for _, row := range all.Rows {
			if len(row) != len(wantColumns) {
				t.Fatalf("row shape: %+v", row)
			}
		}
		row := all.Rows[0]
		if row["Engine"].Text != "LSM" || row["Row_format"].Text != "LSM" || row["Rows"].Int != 3 || row["Auto_increment"].Int < 4 || row["Data_length"].Int <= 0 || row["Index_length"].Int <= 0 || row["Avg_row_length"].Int != row["Data_length"].Int/3 {
			t.Fatalf("table status: %+v", row)
		}
		for _, col := range []string{"Version", "Max_data_length", "Data_free", "Create_time", "Update_time", "Check_time", "Checksum"} {
			if !row[col].Null {
				t.Fatalf("unsupported %s should be NULL", col)
			}
		}
		if !all.Rows[1]["Auto_increment"].Null || all.Rows[1]["Index_length"].Int != 0 {
			t.Fatalf("unindexed table: %+v", all.Rows[1])
		}
		if all.Rows[2]["Comment"].Text != "VIEW" || !all.Rows[2]["Rows"].Null || !all.Rows[2]["Engine"].Null {
			t.Fatalf("view: %+v", all.Rows[2])
		}
		for _, query := range []string{`SHOW TABLE STATUS IN MAIN LIKE 'a\_b'`, `SHOW TABLE STATUS WHERE Name = 'a_b' AND Rows > 0 AND Index_length > 0`} {
			r := requireExec(t, d, query)
			if len(r.Rows) != 1 || r.Rows[0]["Name"].Text != "a_b" {
				t.Fatalf("filter %s: %+v", query, r.Rows)
			}
		}
	}
	check()
	for _, query := range []string{"SHOW TABLE", "SHOW TABLE OTHER", "SHOW FULL TABLE STATUS", "SHOW TABLE STATUS FROM other", "SHOW TABLE STATUS LIKE 1", "SHOW TABLE STATUS WHERE missing = 1", "SHOW TABLE STATUS LIKE '%' WHERE Rows > 0"} {
		if _, err := d.Exec(query); err == nil {
			t.Fatalf("accepted invalid status: %s", query)
		}
	}
	prepared, err := d.Prepare("SHOW TABLE STATUS WHERE Name = ? AND Rows >= ?")
	if err != nil {
		t.Fatal(err)
	}
	r, err := prepared.Exec("a_b", int64(3))
	if err != nil || len(r.Rows) != 1 {
		t.Fatalf("prepared status: %+v, %v", r, err)
	}
	if err := prepared.Close(); err != nil {
		t.Fatal(err)
	}
	requireExec(t, d, "BEGIN; INSERT INTO a_b(v) VALUES ('private'); DELETE FROM a_b WHERE id = 1; CREATE TABLE pending (v INTEGER); INSERT INTO pending VALUES (1)")
	r = requireExec(t, d, "SHOW TABLE STATUS LIKE 'a_b'")
	if r.Rows[0]["Rows"].Int != 3 || r.Rows[0]["Auto_increment"].Int != 5 {
		t.Fatalf("private rows: %+v", r.Rows)
	}
	if requireExec(t, d, "SHOW TABLE STATUS LIKE 'pending'").Rows[0]["Rows"].Int != 1 {
		t.Fatal("private table missing")
	}
	session, err := d.NewSession()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = session.Close() })
	outsider, err := session.Exec("SHOW TABLE STATUS LIKE 'pending'")
	if err != nil || len(outsider.Rows) != 0 {
		t.Fatalf("private catalog leaked: %+v %v", outsider, err)
	}
	outsider, err = session.Exec("SHOW TABLE STATUS LIKE 'a_b'")
	if err != nil || outsider.Rows[0]["Rows"].Int != 3 {
		t.Fatalf("private writes leaked: %+v %v", outsider, err)
	}
	requireExec(t, d, "ROLLBACK")
	check()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := d.ExecContext(ctx, "SHOW TABLE STATUS"); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation: %v", err)
	}
	requireExec(t, d, "VACUUM")
	check()
	requireExec(t, d, "ALTER TABLE a_b RENAME TO renamed; DROP INDEX a_v; PRAGMA collation = 'NFC_CI'")
	row := requireExec(t, d, "SHOW TABLE STATUS LIKE 'renamed'").Rows[0]
	if row["Rows"].Int != 3 || row["Index_length"].Int != 0 || row["Collation"].Text != "NFC_CI" {
		t.Fatalf("renamed table: %+v", row)
	}
	if err := session.Close(); err != nil {
		t.Fatal(err)
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	d, err = env.openDatabase(dir, options)
	if err != nil {
		t.Fatal(err)
	}
	row = requireExec(t, d, "SHOW TABLE STATUS LIKE 'renamed'").Rows[0]
	if row["Rows"].Int != 3 || row["Data_length"].Int <= 0 || row["Collation"].Text != "NFC_CI" {
		t.Fatalf("reopened status: %+v", row)
	}
	// Numeric output columns remain typed for WHERE and callers.
	for _, col := range requireExec(t, d, "SHOW TABLE STATUS").Columns {
		if col.Name == "Rows" && col.ColType != sql.ColInteger {
			t.Fatal("Rows is not integer metadata")
		}
	}
	requireExec(t, d, "CREATE TABLE empty (id INTEGER PRIMARY KEY AUTOINCREMENT, v TEXT)")
	row = requireExec(t, d, "SHOW TABLE STATUS LIKE 'empty'").Rows[0]
	if row["Rows"].Int != 0 || row["Data_length"].Int != 0 || row["Index_length"].Int != 0 || row["Avg_row_length"].Int != 0 || row["Auto_increment"].Int != 1 {
		t.Fatalf("empty table: %+v", row)
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
}
