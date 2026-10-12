package main

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"slopdb/db"
	"slopdb/sql"
)

func savedAnswerDatabase(t *testing.T) (string, string) {
	t.Helper()
	directory := filepath.Join(t.TempDir(), "database")
	database, err := db.Open(directory, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = database.Exec("CREATE TABLE t(id INTEGER PRIMARY KEY,value REAL);INSERT INTO t VALUES(9223372036854775807,1.25),(9007199254740993,2.5),(-9223372036854775808,3.75)"); err != nil {
		t.Fatal(err)
	}
	if err = database.Close(); err != nil {
		t.Fatal(err)
	}
	report := filepath.Join(t.TempDir(), "original.json")
	text := `{"table_rows":{"t":3},"queries":[{"number":1,"sql":"SELECT id,value FROM t ORDER BY id","columns":["id","value"],"results":[[9223372036854775807,1.25],[9007199254740993,2.5],[-9223372036854775808,3.75]]},{"number":2,"sql":"SELECT SUM(value) AS total FROM t","columns":["total"],"results":[[7.50000000001]]},{"number":3,"sql":"SELECT id FROM t WHERE id=0","columns":["id"],"samples":[{"status":"ok","rows":0}]}]}`
	if err = os.WriteFile(report, []byte(text), 0600); err != nil {
		t.Fatal(err)
	}
	return directory, report
}
func TestCompactVerifiesAllSavedAnswersAfterReopen(t *testing.T) {
	directory, answers := savedAnswerDatabase(t)
	output := filepath.Join(t.TempDir(), "maintenance.json")
	if err := compact(directory, 65536, output, answers); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(output)
	if err != nil {
		t.Fatal(err)
	}
	var report struct {
		Counts  bool             `json:"counts_verified_after_reopen"`
		Answers int              `json:"query_answers_verified_after_maintenance"`
		Rows    map[string]int64 `json:"rows"`
	}
	if err = json.Unmarshal(raw, &report); err != nil {
		t.Fatal(err)
	}
	if !report.Counts || report.Answers != 3 || report.Rows["t"] != 3 {
		t.Fatal("maintenance did not record complete verification", string(raw))
	}
}
func TestCompactRejectsChangedAnswerIncludingAdjacentLargeInteger(t *testing.T) {
	for _, change := range []struct{ name, old, new string }{{"float", "7.50000000001", "7.6"}, {"integer above float precision", "9007199254740993,2.5", "9007199254740992,2.5"}, {"max integer", "9223372036854775807,1.25", "9223372036854775806,1.25"}, {"column", "\"total\"", "\"different\""}} {
		t.Run(change.name, func(t *testing.T) {
			directory, answers := savedAnswerDatabase(t)
			raw, err := os.ReadFile(answers)
			if err != nil {
				t.Fatal(err)
			}
			wrong := strings.Replace(string(raw), change.old, change.new, 1)
			if wrong == string(raw) {
				t.Fatal("test did not modify fixture")
			}
			if err = os.WriteFile(answers, []byte(wrong), 0600); err != nil {
				t.Fatal(err)
			}
			output := filepath.Join(t.TempDir(), "maintenance.json")
			if err = compact(directory, 16384, output, answers); err == nil {
				t.Fatal("changed answer accepted")
			}
			if _, err = os.Stat(output); !os.IsNotExist(err) {
				t.Fatal("failed verification published success report", err)
			}
		})
	}
}
func TestSavedAnswerComparisonDuplicatesNullAndNumericStrings(t *testing.T) {
	row := []sql.Value{sql.NewIntValue(9007199254740993), sql.NewRealValue(1.25), sql.NewNullValue(), sql.NewTextValue("1.0")}
	expected := []any{json.Number("9007199254740993"), json.Number("1.25000000001"), nil, "1"}
	if err := compareSavedRows(context.Background(), [][]sql.Value{row, row}, [][]any{expected, expected}); err != nil {
		t.Fatal(err)
	}
	other := append([]any(nil), expected...)
	other[1] = json.Number("1.5")
	if err := compareSavedRows(context.Background(), [][]sql.Value{row, row}, [][]any{expected, other}); err == nil {
		t.Fatal("duplicate multiplicity mismatch accepted")
	}
}
func TestSavedAnswerRejectsMissingResultsMutationsAndTimeout(t *testing.T) {
	directory, _ := savedAnswerDatabase(t)
	database, err := db.Open(directory, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	query := savedQuery{Number: 1, SQL: "SELECT id FROM t", Columns: []string{"id"}}
	if err = verifySavedQuery(database, query, time.Second); err == nil {
		t.Fatal("missing results accepted")
	}
	query.Results = json.RawMessage(`[]`)
	query.SQL = "DELETE FROM t"
	if err = verifySavedQuery(database, query, time.Second); err == nil {
		t.Fatal("mutating verification query allowed")
	}
	query.SQL = "SELECT id FROM t"
	query.Results = json.RawMessage(`[[9007199254740993],[9223372036854775807],[-9223372036854775808]]`)
	if err = verifySavedQuery(database, query, time.Nanosecond); err == nil {
		t.Fatal("expired verification returned success")
	}
	result, err := database.Exec("SELECT COUNT(*) AS n FROM t")
	if err != nil || result.Rows[0]["n"].Int != 3 {
		t.Fatal("bad verification mutated data", err, result)
	}
}
