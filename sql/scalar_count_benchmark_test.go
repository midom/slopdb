package sql

import "testing"

// Every run scans the same flushed fixture on one query goroutine. Answers are
// defined by the fixture's integer formula, independently of SQL execution.
func BenchmarkScalarFilteredCount(b *testing.B) {
	const rows = 32768
	for _, dialect := range []Dialect{DialectNative, DialectMySQL, DialectPostgreSQL} {
		b.Run(dialect.String(), func(b *testing.B) {
			s := tpchReuseSchema(b, rows)
			if err := s.SetDialect(dialect); err != nil {
				b.Fatal(err)
			}
			if err := s.lsm.Flush(); err != nil {
				b.Fatal(err)
			}
			stmt := statisticsSelectForBenchmark(b, "SELECT COUNT(*) AS n FROM lineitem WHERE quantity%2=0")
			var expected int64
			for i := 0; i < rows; i++ {
				if (i%50+1)%2 == 0 {
					expected++
				}
			}
			tpchReuseSQL(b, s, "SELECT COUNT(*) AS n FROM lineitem WHERE quantity%2=0")
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := s.StartStatement(); err != nil {
					b.Fatal(err)
				}
				var result *ResultSet
				err := s.ExecuteStatement(func() error { var e error; result, e = s.Select(stmt); return e })
				s.EndStatement()
				if err != nil || len(result.Rows) != 1 || result.Rows[0]["n"].Int != expected {
					b.Fatalf("result %v error %v", result, err)
				}
			}
		})
	}
}
