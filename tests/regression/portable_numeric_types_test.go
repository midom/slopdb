package regression

import (
	"math"
	"testing"

	"slopdb/sql"
)

func TestPortableNumericTypesPersistExactValues(t *testing.T) {
	e := newRegressionEnv(t)
	dir := e.path("portable_types")
	database, err := e.openDatabase(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = database.Exec("CREATE TABLE numeric_data(id BIGINT NOT NULL PRIMARY KEY, value DOUBLE NOT NULL, precise DOUBLE PRECISION); INSERT INTO numeric_data VALUES (9007199254740993,0.10000000000000001,1.0000000000000002),(-9223372036854775808,-1.25,NULL),(9223372036854775807,2.5,3.0)"); err != nil {
		t.Fatal(err)
	}
	if _, err = database.Exec("ALTER TABLE numeric_data ADD COLUMN added BIGINT DEFAULT 9007199254740993"); err != nil {
		t.Fatal(err)
	}
	for pass := 0; pass < 2; pass++ {
		result, err := database.Exec("SELECT id,value,precise,added FROM numeric_data ORDER BY id")
		if err != nil || len(result.Rows) != 3 {
			t.Fatalf("pass %d: result=%v err=%v", pass, result, err)
		}
		want := []int64{math.MinInt64, 9007199254740993, math.MaxInt64}
		for i, row := range result.Rows {
			if row["id"].Type != sql.ColInteger || row["id"].Int != want[i] || row["added"].Int != 9007199254740993 || row["value"].Type != sql.ColReal {
				t.Fatalf("pass %d row %d lost types/precision: %v", pass, i, row)
			}
		}
		if math.Float64bits(result.Rows[1]["value"].Real) != math.Float64bits(0.1) || math.Float64bits(result.Rows[1]["precise"].Real) != math.Float64bits(math.Nextafter(1, 2)) || !result.Rows[0]["precise"].Null {
			t.Fatalf("lost DOUBLE values or NULL: %v", result.Rows)
		}
		cast, err := database.Exec("SELECT CAST('9007199254740993' AS BIGINT) AS i, CAST('0.1' AS DOUBLE) AS f, CAST('1.25' AS DOUBLE PRECISION) AS p")
		if err != nil || cast.Rows[0]["i"].Int != 9007199254740993 || cast.Rows[0]["f"].Type != sql.ColReal || cast.Rows[0]["p"].Real != 1.25 {
			t.Fatalf("alias casts: %v err=%v", cast, err)
		}
		if _, err := database.Exec("SELECT CAST('9223372036854775808' AS BIGINT)"); err == nil {
			t.Fatal("BIGINT cast accepted integer overflow")
		}
		if err := database.Close(); err != nil {
			t.Fatal(err)
		}
		if pass == 0 {
			database, err = e.openDatabase(dir, nil)
			if err != nil {
				t.Fatal(err)
			}
		}
	}
}
