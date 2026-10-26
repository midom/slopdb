package sql

import "testing"

// The fixture and checked answers are defined by independent integer formulas;
// this complements DBGEN qualification without requiring external generators.
func BenchmarkTypedDialectAggregates(b *testing.B) {
	const rows = 8192
	for _, dialect := range []Dialect{DialectNative, DialectMySQL, DialectPostgreSQL} {
		b.Run(dialect.String(), func(b *testing.B) {
			s := tpchReuseSchema(b, rows)
			if err := s.SetDialect(dialect); err != nil {
				b.Fatal(err)
			}
			var q6 int64
			var counts, quantities, revenues [3][2]int64
			for i := 0; i < rows; i++ {
				f, st := i%3, i%2
				counts[f][st]++
				quantities[f][st] += int64(i%50 + 1)
				revenues[f][st] += int64((i*7 + 11) * (3 - i%3))
				if i%2 == 0 && i%3 >= 1 && i%50+1 < 24 {
					q6 += int64((i*7 + 11) * (i % 3))
				}
			}
			for _, tc := range []struct{ name, query string }{
				{"Q1", "SELECT flag,status,COUNT(*) AS n,SUM(quantity) AS q,SUM(price*(3-discount)) AS revenue FROM lineitem GROUP BY flag,status"},
				{"Q6", "SELECT SUM(price*discount) AS revenue FROM lineitem WHERE shipdate>='1994-01-01' AND shipdate<'1995-01-01' AND discount BETWEEN 1 AND 2 AND quantity<24"},
			} {
				b.Run(tc.name, func(b *testing.B) {
					stmt := statisticsSelectForBenchmark(b, tc.query)
					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						if err := s.StartStatement(); err != nil {
							b.Fatal(err)
						}
						var result *ResultSet
						err := s.ExecuteStatement(func() error { var e error; result, e = s.Select(stmt); return e })
						s.EndStatement()
						if err != nil {
							b.Fatal(err)
						}
						if tc.name == "Q6" {
							if len(result.Rows) != 1 || result.Rows[0]["revenue"].Int != q6 {
								b.Fatal(result.Rows)
							}
							continue
						}
						if len(result.Rows) != 6 {
							b.Fatal(result.Rows)
						}
						for _, row := range result.Rows {
							flag, status := row["flag"].Text, row["status"].Text
							if len(flag) != 1 || len(status) != 1 || flag[0] < 'A' || flag[0] > 'C' || status[0] < 'F' || status[0] > 'G' {
								b.Fatal(row)
							}
							f, st := flag[0]-'A', status[0]-'F'
							if row["n"].Int != counts[f][st] || row["q"].Int != quantities[f][st] || row["revenue"].Int != revenues[f][st] {
								b.Fatal(row)
							}
						}
					}
				})
			}
		})
	}
}
