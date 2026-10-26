package sql

import (
	"fmt"
	"strings"
	"testing"
)

func TestScanCountPreservesLazyDecimalLiteralErrors(t *testing.T) {
	for _, dialect := range []Dialect{DialectMySQL, DialectPostgreSQL} {
		t.Run(fmt.Sprint(dialect), func(t *testing.T) {
			s, _ := statisticsSchema(t, t.TempDir())
			s.SetDialect(dialect)
			statisticsSQL(t, s, "CREATE TABLE t(id INTEGER PRIMARY KEY)")
			// The float lexer can represent this as zero, but exact dialect evaluation
			// must reject the excessive DECIMAL scale when an input row consumes it.
			literal := "0." + strings.Repeat("0", maxDecimalDigits+40) + "1"
			stmts, err := Parse("SELECT COUNT(" + literal + ") AS n FROM t")
			if err != nil {
				t.Fatal(err)
			}
			stmt := stmts[0].(*SelectStmt)
			if err = s.StartStatement(); err != nil {
				t.Fatal(err)
			}
			r, err := s.Select(stmt)
			s.EndStatement()
			if err != nil || r.Rows[0]["n"].Int != 0 {
				t.Fatalf("empty input must not evaluate argument: %+v %v", r, err)
			}
			statisticsSQL(t, s, "INSERT INTO t VALUES(1)")
			if err = s.StartStatement(); err != nil {
				t.Fatal(err)
			}
			_, err = s.Select(stmt)
			s.EndStatement()
			if err == nil {
				t.Fatal("COUNT skipped invalid exact numeric argument")
			}
		})
	}
}
