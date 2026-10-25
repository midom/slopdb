package sql

import "testing"

func TestDescribeDecimalCastDialect(t *testing.T) {
	for _, mysql := range []bool{false, true} {
		s := &Schema{}
		s.SetMySQLMode(mysql)
		statements, e := Parse("SELECT CAST(1.25 AS DECIMAL) AS d,CAST(1.25 AS DECIMAL(20,4)) AS fixed")
		if e != nil {
			t.Fatal(e)
		}
		cols, e := s.Describe(statements[0])
		if e != nil {
			t.Fatal(e)
		}
		precision := 0
		if mysql {
			precision = 10
		}
		if len(cols) != 2 || cols[0].ColType != ColDecimal || cols[0].Precision != precision || cols[0].Scale != 0 || cols[1].Precision != 20 || cols[1].Scale != 4 {
			t.Fatalf("mysql %v columns: %+v", mysql, cols)
		}
	}
}

func TestDescribeMySQLExactLiteral(t *testing.T) {
	for _, mysql := range []bool{false, true} {
		s := &Schema{}
		s.SetMySQLMode(mysql)
		stmts, e := Parse("SELECT 12345678901234567890.123456789 AS d,0.0001 AS tiny,1e10 AS approximate")
		if e != nil {
			t.Fatal(e)
		}
		cols, e := s.Describe(stmts[0])
		if e != nil {
			t.Fatal(e)
		}
		if mysql {
			if cols[0].ColType != ColDecimal || cols[0].Precision != 29 || cols[0].Scale != 9 || cols[1].ColType != ColDecimal || cols[1].Precision != 4 || cols[1].Scale != 4 {
				t.Fatalf("exact MySQL metadata: %+v", cols)
			}
		} else if cols[0].ColType != ColReal || cols[1].ColType != ColReal {
			t.Fatalf("native REAL metadata: %+v", cols)
		}
		if cols[2].ColType != ColReal {
			t.Fatalf("exponent literal metadata: %+v", cols)
		}
	}
}
