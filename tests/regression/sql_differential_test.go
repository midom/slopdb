package regression

import (
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"slopdb/db"
	"slopdb/sql"
	"slopdb/storage"
)

type referenceQuery struct {
	Query string  `json:"query"`
	Rows  [][]any `json:"rows"`
}

// The checked-in answers were computed with SQLite. Decoding integers exactly
// makes this oracle safe for int64 values above the IEEE 754 integer boundary.
func loadReferenceQueries(t *testing.T, name string) []referenceQuery {
	t.Helper()
	f, err := os.Open(filepath.Join("testdata", name))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	decoder := json.NewDecoder(f)
	decoder.UseNumber()
	var cases []referenceQuery
	if err := decoder.Decode(&cases); err != nil {
		t.Fatal(err)
	}
	if len(cases) == 0 {
		t.Fatalf("empty reference fixture %s", name)
	}
	return cases
}

func compareReferenceResult(t *testing.T, query string, got *db.ExecResult, want [][]any) {
	t.Helper()
	if len(got.Rows) != len(want) {
		t.Fatalf("%s\nrow count: got %d, want %d", query, len(got.Rows), len(want))
	}
	for i, row := range want {
		if len(got.Columns) != len(row) {
			t.Fatalf("%s\ncolumn count: got %d, want %d", query, len(got.Columns), len(row))
		}
		for j, expected := range row {
			value, ok := got.Rows[i][got.Columns[j].Name]
			if !ok || !referenceValueEqual(value, expected) {
				t.Fatalf("%s\nrow %d, column %d: got %#v, want %#v", query, i, j, value, expected)
			}
		}
	}
}

func referenceValueEqual(value sql.Value, expected any) bool {
	if expected == nil {
		return value.Null
	}
	if value.Null {
		return false
	}
	switch expected := expected.(type) {
	case string:
		return value.Type == sql.ColText && value.Text == expected
	case json.Number:
		integer, err := strconv.ParseInt(string(expected), 10, 64)
		if err == nil && value.Type == sql.ColInteger {
			return value.Int == integer
		}
		if value.Type != sql.ColInteger && value.Type != sql.ColReal {
			return false
		}
		want, err := expected.Float64()
		if err != nil {
			return false
		}
		got := value.Real
		if value.Type == sql.ColInteger {
			got = float64(value.Int)
		}
		return !math.IsNaN(got) && math.Abs(got-want) < 1e-8
	}
	return false
}

func runReferenceQueries(t *testing.T, d *db.Database, cases []referenceQuery) {
	t.Helper()
	for i, c := range cases {
		t.Run(fmt.Sprintf("case_%04d", i+1), func(t *testing.T) {
			r, err := d.Exec(c.Query)
			if err != nil {
				t.Fatalf("%s\n%v", c.Query, err)
			}
			compareReferenceResult(t, c.Query, r, c.Rows)
		})
	}
}

func referenceDatabase(t *testing.T, setup string) (*regressionEnv, *db.Database, string, *storage.Options) {
	t.Helper()
	env := newRegressionEnv(t)
	dir := env.path("database")
	options := storage.DefaultOptions()
	options.DisableWALSync = true
	d, err := env.openDatabase(dir, options)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := d.Exec(setup); err != nil {
		t.Fatal(err)
	}
	return env, d, dir, options
}

// Adapted from sql_differential_review.go.
func TestSQLDifferential(t *testing.T) {
	setup, err := os.ReadFile(filepath.Join("testdata", "sql_differential_setup.sql"))
	if err != nil {
		t.Fatal(err)
	}
	_, d, _, _ := referenceDatabase(t, string(setup))
	runReferenceQueries(t, d, loadReferenceQueries(t, "sql_differential_cases.json"))
}

// Adapted from performance_execution_review.go.
func TestExecutionDifferential(t *testing.T) {
	setup, err := os.ReadFile(filepath.Join("testdata", "performance_setup.sql"))
	if err != nil {
		t.Fatal(err)
	}
	_, d, _, _ := referenceDatabase(t, string(setup))
	runReferenceQueries(t, d, loadReferenceQueries(t, "performance_cases.json"))
}

// Adapted from join_optimization_review.go; test both memtables and
// persisted SSTables against the same SQLite answers.
func TestJoinDifferential(t *testing.T) {
	setup, err := os.ReadFile(filepath.Join("testdata", "join_optimization_setup.sql"))
	if err != nil {
		t.Fatal(err)
	}
	env, d, dir, options := referenceDatabase(t, string(setup))
	cases := loadReferenceQueries(t, "join_optimization_cases.json")
	t.Run("fresh", func(t *testing.T) { runReferenceQueries(t, d, cases) })
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	d, err = env.openDatabase(dir, options)
	if err != nil {
		t.Fatal(err)
	}
	t.Run("reopened", func(t *testing.T) { runReferenceQueries(t, d, cases) })
}
