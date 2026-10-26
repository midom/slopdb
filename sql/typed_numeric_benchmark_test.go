package sql

import "testing"

func BenchmarkTypedNumericPredicates(b *testing.B) {
	for _, dialect := range []Dialect{DialectNative, DialectMySQL, DialectPostgreSQL} {
		b.Run(dialect.String(), func(b *testing.B) {
			for _, tc := range []struct {
				name, expr string
				accept     func(int64) bool
			}{
				{"modulo", "v%2=0", func(v int64) bool { return v%2 == 0 }},
				{"range", "v>=100 AND v<1000", func(v int64) bool { return v >= 100 && v < 1000 }},
				{"equality", "v=99", func(v int64) bool { return v == 99 }},
			} {
				b.Run(tc.name, func(b *testing.B) {
					program, ok := compileTypedProgram(typedTestExpr(b, tc.expr), map[string]int{"v": 0})
					if !ok {
						b.Fatal("compile")
					}
					program.setDialect(&Schema{dialect: dialect, collation: BinaryCollation})
					defer program.clearRegisters()
					row := []Value{NewIntValue(0)}
					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						v := int64(i%2001) - 1000
						row[0].Int = v
						value, err := program.eval(row)
						if err != nil || value.Null || (value.Int != 0) != tc.accept(v) {
							b.Fatalf("input %d: %#v/%v", v, value, err)
						}
					}
				})
			}
		})
	}
}
