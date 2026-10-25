package regression

import (
	"math"
	"strings"
	"testing"

	"slopdb/sql"
	"slopdb/storage"
)

func TestBetweenPreparedReadPlans(t *testing.T) {
	env := newRegressionEnv(t)
	dir := env.path("between")
	options := storage.DefaultOptions()
	options.DisableWALSync = true
	database, err := env.openDatabase(dir, options)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := database.Exec("CREATE TABLE t(a INTEGER,b INTEGER,v TEXT,PRIMARY KEY(a,b))"); err != nil {
		t.Fatal(err)
	}
	values := [][2]int64{{math.MinInt64, 0}, {-2, 0}, {-2, 1}, {-1, 0}, {0, 0}, {1, 0}, {1, 1}, {2, 0}, {math.MaxInt64, 0}}
	rows := make([]map[string]sql.Value, len(values))
	insert, err := database.Prepare("INSERT INTO t VALUES(?,?,'payload')")
	if err != nil {
		t.Fatal(err)
	}
	for i, value := range values {
		if _, err := insert.Exec(value[0], value[1]); err != nil {
			t.Fatal(err)
		}
		rows[i] = map[string]sql.Value{"a": sql.NewIntValue(value[0]), "b": sql.NewIntValue(value[1]), "v": sql.NewTextValue("payload")}
	}
	if err := insert.Close(); err != nil {
		t.Fatal(err)
	}
	for _, state := range []string{"fresh", "reopened"} {
		t.Run(state, func(t *testing.T) {
			query, err := database.Prepare("SELECT * FROM t WHERE a BETWEEN ? AND ? ORDER BY a,b LIMIT ? OFFSET ?")
			if err != nil {
				t.Fatal(err)
			}
			defer query.Close()
			for _, tc := range []struct {
				name        string
				low, high   any
				limit, skip int64
				want        []map[string]sql.Value
			}{
				{"int64_range", int64(math.MinInt64), int64(math.MaxInt64), 4294967296, 0, rows},
				{"negative", int64(-2), int64(-1), 10, 0, rows[1:4]},
				{"equal_endpoints", int64(-2), int64(-2), 10, 0, rows[1:3]},
				{"limit_offset", int64(1), int64(2), 2, 1, rows[6:8]},
				{"reversed", int64(2), int64(-2), 10, 0, nil},
				{"null_low", nil, int64(2), 10, 0, nil},
				{"null_high", int64(-2), nil, 10, 0, nil},
				{"real_bounds", -1.5, 1.5, 10, 0, rows[3:7]},
				{"large_offset", int64(math.MinInt64), int64(math.MaxInt64), 10, 4294967296, nil},
			} {
				t.Run(tc.name, func(t *testing.T) {
					result, err := query.Exec(tc.low, tc.high, tc.limit, tc.skip)
					if err != nil {
						t.Fatal(err)
					}
					if !equalBetweenRows(result.Rows, tc.want) {
						t.Fatalf("got %v, want %v", result.Rows, tc.want)
					}
				})
			}
			// An unlimited SELECT * sorted by a payload column must still
			// bound reads rather than depend on ordered LIMIT optimization.
			result, err := database.Exec("EXPLAIN SELECT * FROM t WHERE a BETWEEN -2 AND -1 ORDER BY v,a,b")
			if err != nil {
				t.Fatal(err)
			}
			bounded := false
			for _, row := range result.Rows {
				bounded = bounded || strings.Contains(row["p4"].Text, "bounded ranges")
			}
			if !bounded {
				t.Fatal("EXPLAIN shows no bounded BETWEEN access")
			}
			if result, err := database.Exec("SELECT * FROM t WHERE a BETWEEN -2 AND -1 ORDER BY v,a,b"); err != nil {
				t.Fatal(err)
			} else if !equalBetweenRows(result.Rows, rows[1:4]) {
				t.Fatalf("unlimited read: got %v, want %v", result.Rows, rows[1:4])
			}
		})
		if err := database.Close(); err != nil {
			t.Fatal(err)
		}
		if state == "fresh" {
			database, err = env.openDatabase(dir, options)
			if err != nil {
				t.Fatal(err)
			}
		}
	}
}

// Compare SQL type and exact payload; declaration/protocol provenance is
// intentionally richer on decoded columns than on constructor-built fixtures.
func equalBetweenRows(got, want []map[string]sql.Value) bool {
	if len(got) != len(want) {
		return false
	}
	for i, row := range want {
		if len(got[i]) != len(row) {
			return false
		}
		for name, expected := range row {
			actual, ok := got[i][name]
			if !ok || actual.Type != expected.Type || actual.Null != expected.Null {
				return false
			}
			if actual.Null {
				continue
			}
			if actual.LogicalType().Kind != expected.LogicalType().Kind {
				return false
			}
			switch expected.Type {
			case sql.ColInteger:
				if actual.Int != expected.Int {
					return false
				}
			case sql.ColText:
				if actual.Text != expected.Text {
					return false
				}
			default:
				return false
			}
		}
	}
	return true
}
