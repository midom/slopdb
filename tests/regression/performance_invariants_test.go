package regression

import (
	"fmt"
	"path/filepath"
	"testing"

	"slopdb/db"
	"slopdb/storage"
)

func performanceFixture(tb testing.TB, count int) *db.Database {
	tb.Helper()
	options := storage.DefaultOptions()
	options.DisableWALSync = true
	d, err := db.Open(filepath.Join(tb.TempDir(), "database"), options)
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() { _ = d.Close() })
	if _, err := d.Exec("CREATE TABLE items(id INTEGER PRIMARY KEY,w INTEGER,k INTEGER,price REAL,quantity INTEGER,payload TEXT,INDEX wk(w,k))"); err != nil {
		tb.Fatal(err)
	}
	insert, err := d.Prepare("INSERT INTO items VALUES(?,?,?,?,?,?)")
	if err != nil {
		tb.Fatal(err)
	}
	defer insert.Close()
	for start := 0; start < count; start += 1000 {
		tx, err := d.Begin()
		if err != nil {
			tb.Fatal(err)
		}
		for i := start; i < min(count, start+1000); i++ {
			if _, err := tx.ExecPrepared(insert, int64(i), int64(i/100), int64(i%100), float64(i%100)+0.5, int64(i%20), fmt.Sprintf("%012d", count-i)); err != nil {
				_ = tx.Rollback()
				tb.Fatal(err)
			}
		}
		if err := tx.Commit(); err != nil {
			tb.Fatal(err)
		}
	}
	return d
}

func TestPerformanceBoundedAccessInvariant(t *testing.T) {
	d := performanceFixture(t, 1000)
	for _, query := range []string{
		"SELECT * FROM items WHERE id BETWEEN 901 AND 905 ORDER BY payload",
		"SELECT id FROM items WHERE id>=901 AND id<=905 ORDER BY payload",
		"SELECT id FROM items AS x WHERE x.id BETWEEN 901 AND 905 ORDER BY x.payload",
		"SELECT * FROM items WHERE w=9 AND k BETWEEN 1 AND 5 ORDER BY payload",
		"SELECT id FROM items WHERE w=9 AND k IN(1,2,3,4,5) ORDER BY payload",
	} {
		result, err := d.Exec(query)
		if err != nil {
			t.Fatal(err)
		}
		if len(result.Rows) != 5 {
			t.Fatalf("%s returned %d rows", query, len(result.Rows))
		}
		for i, row := range result.Rows {
			if row["id"].Int != int64(905-i) {
				t.Fatalf("%s returned unexpected row %v", query, row)
			}
		}
		analyzed, err := d.Exec("EXPLAIN ANALYZE " + query)
		if err != nil {
			t.Fatal(err)
		}
		p := analyzed.Profile
		if p == nil || p.RowsExamined+p.IndexEntriesExamined > 32 {
			t.Fatalf("%s lost bounded access: %+v", query, p)
		}
	}
}

func BenchmarkReadPlans(b *testing.B) {
	for _, count := range []int{10000, 100000} {
		b.Run(fmt.Sprint(count), func(b *testing.B) {
			d := performanceFixture(b, count)
			for _, tc := range []struct{ name, query string }{
				{"point", fmt.Sprintf("SELECT * FROM items WHERE id=%d", count-25)},
				{"scalar_range_payload_sort", fmt.Sprintf("SELECT * FROM items WHERE id BETWEEN %d AND %d ORDER BY payload", count-30, count-11)},
				{"composite_range", fmt.Sprintf("SELECT * FROM items WHERE w=%d AND k BETWEEN 70 AND 89 ORDER BY k", count/100-1)},
				{"aggregate", "SELECT SUM(price*0.06) FROM items WHERE quantity<12"},
			} {
				b.Run(tc.name, func(b *testing.B) {
					stmt, err := d.Prepare(tc.query)
					if err != nil {
						b.Fatal(err)
					}
					defer stmt.Close()
					for i := 0; i < 3; i++ {
						if _, err := stmt.Exec(); err != nil {
							b.Fatal(err)
						}
					}
					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						if _, err := stmt.Exec(); err != nil {
							b.Fatal(err)
						}
					}
				})
			}
		})
	}
}
