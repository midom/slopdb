package regression

import "testing"

func TestCTEExistsFastPathRespectsScope(t *testing.T) {
	d := cteDatabase(t)
	cteExec(t, d, "CREATE TABLE x(n INTEGER PRIMARY KEY);INSERT INTO x VALUES(99)")
	for _, tc := range []struct {
		query string
		want  int64
	}{
		{"WITH x(n) AS (SELECT 7) SELECT EXISTS(SELECT 1 FROM x WHERE n=7) AS e", 1},
		{"WITH x(n) AS (SELECT 7) SELECT EXISTS(SELECT 1 FROM x WHERE n=99) AS e", 0},
		{"SELECT EXISTS(WITH x(n) AS (SELECT 7) SELECT 1 FROM x WHERE n=7) AS e", 1},
		{"SELECT EXISTS(SELECT n FROM x WHERE n=0 UNION ALL SELECT 1) AS e", 1},
	} {
		cteInts(t, cteExec(t, d, tc.query), "e", []int64{tc.want})
	}
	// DML must resolve the same scoped input, and a subsequent statement must
	// return to the persistent catalog rather than retain the CTE result.
	cteExec(t, d, "WITH x(n) AS (SELECT 7) UPDATE x SET n=100 WHERE EXISTS(SELECT 1 FROM x WHERE n=7)")
	cteInts(t, cteExec(t, d, "SELECT n FROM x"), "n", []int64{100})
}
