package regression

import (
	"reflect"
	"slopdb/sql"
	"slopdb/storage"
	"strings"
	"testing"
)

func TestSharedScalarAccessPlans(t *testing.T) {
	env := newRegressionEnv(t)
	options := storage.DefaultOptions()
	options.DisableWALSync = true
	database, err := env.openDatabase(env.path("shared-access"), options)
	if err != nil {
		t.Fatal(err)
	}
	exec := func(query string) {
		t.Helper()
		if _, err := database.Exec(query); err != nil {
			t.Fatalf("%s: %v", query, err)
		}
	}
	exec("CREATE TABLE ints(id INTEGER PRIMARY KEY,v TEXT)")
	exec("INSERT INTO ints VALUES(-3,'e'),(-2,'d'),(-1,'c'),(0,'b'),(1,'a'),(2,'f'),(3,'g')")
	exec("CREATE TABLE texts(id INTEGER PRIMARY KEY,a TEXT,b INTEGER,INDEX ia(a),INDEX iab(a,b))")
	exec("INSERT INTO texts VALUES(1,'',10),(2,'a',20),(3,'a:',30),(4,'a::',40),(5,'aa',50),(6,'b',60),(7,'bb',70)")
	for _, tc := range []struct {
		query string
		want  []int64
	}{
		{"SELECT id FROM ints AS x WHERE x.id BETWEEN -2 AND +1 ORDER BY v", []int64{1, 0, -1, -2}},
		{"SELECT * FROM ints WHERE id >= -2 AND id <= 1 ORDER BY v", []int64{1, 0, -1, -2}},
		{"SELECT id FROM ints WHERE id BETWEEN 1+1 AND 2*2 ORDER BY v", []int64{2, 3}},
		{"SELECT id FROM texts WHERE a BETWEEN 'a' AND 'b' ORDER BY b DESC", []int64{6, 5, 4, 3, 2}},
		{"SELECT * FROM texts WHERE a IN('a','a:','a::') ORDER BY id", []int64{2, 3, 4}},
	} {
		result, err := database.Exec("EXPLAIN " + tc.query)
		if err != nil {
			t.Fatal(err)
		}
		bounded := false
		for _, row := range result.Rows {
			bounded = bounded || strings.Contains(row["p4"].Text, "bounded ranges")
		}
		if !bounded {
			t.Fatalf("no bounded scalar access: %s", tc.query)
		}
		result, err = database.Exec(tc.query)
		if err != nil {
			t.Fatal(err)
		}
		got := make([]int64, len(result.Rows))
		for i, row := range result.Rows {
			got[i] = row["id"].Int
		}
		if !reflect.DeepEqual(got, tc.want) {
			t.Fatalf("%s: got %v, want %v", tc.query, got, tc.want)
		}
	}
	prepared, err := database.Prepare("SELECT id FROM ints AS x WHERE x.id BETWEEN ? AND ? ORDER BY v")
	if err != nil {
		t.Fatal(err)
	}
	defer prepared.Close()
	for _, tc := range []struct {
		lo, hi any
		want   []int64
	}{{int64(-2), int64(1), []int64{1, 0, -1, -2}}, {int64(2), int64(3), []int64{2, 3}}, {nil, int64(3), nil}, {-1.5, 1.5, []int64{1, 0, -1}}} {
		result, err := prepared.Exec(tc.lo, tc.hi)
		if err != nil {
			t.Fatal(err)
		}
		var got []int64
		for _, row := range result.Rows {
			got = append(got, row["id"].Int)
		}
		if !reflect.DeepEqual(got, tc.want) {
			t.Fatalf("prepared range %v..%v: got %v, want %v", tc.lo, tc.hi, got, tc.want)
		}
	}
	// Secondary variable ranges must keep residuals and update each row once,
	// even when an IN prefix overlaps a longer value containing a delimiter.
	for _, query := range []string{
		"UPDATE texts SET b=b+1 WHERE a BETWEEN 'a:' AND 'aa'",
		"DELETE FROM texts WHERE a BETWEEN 'a:' AND 'aa'",
	} {
		result, err := database.Exec("EXPLAIN " + query)
		if err != nil {
			t.Fatal(err)
		}
		bounded := false
		for _, row := range result.Rows {
			bounded = bounded || strings.Contains(row["p4"].Text, "bounded ranges")
		}
		if !bounded {
			t.Fatalf("DML omitted bounded secondary range: %s", query)
		}
	}
	orResult, err := database.Exec("SELECT id FROM ints WHERE id BETWEEN -3 AND -2 OR id BETWEEN 1 AND 3 ORDER BY v LIMIT 2")
	if err != nil {
		t.Fatal(err)
	}
	if len(orResult.Rows) != 2 || orResult.Rows[0]["id"].Int != 1 || orResult.Rows[1]["id"].Int != -2 {
		t.Fatalf("OR range LIMIT ran before payload sorting: %v", orResult.Rows)
	}
	exec("BEGIN")
	exec("UPDATE texts SET b=b+1 WHERE a IN('a','a:','a::')")
	result, err := database.Exec("SELECT SUM(b) AS n FROM texts WHERE a BETWEEN 'a' AND 'b'")
	if err != nil {
		t.Fatal(err)
	}
	if result.Rows[0]["n"].Int != 203 {
		t.Fatalf("private secondary range updates: %v", result.Rows)
	}
	exec("ROLLBACK")
	exec("DELETE FROM texts WHERE a BETWEEN 'a:' AND 'aa'")
	result, err = database.Exec("SELECT COUNT(*) AS n FROM texts")
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(result.Rows[0]["n"], sql.NewIntValue(4)) {
		t.Fatalf("secondary range delete: %v", result.Rows)
	}
}
