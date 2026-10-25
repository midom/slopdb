package db_test

import (
	"bytes"
	"context"
	"io"
	"testing"

	"slopdb/db"
	"slopdb/sql"
	"slopdb/storage"
)

// Setup and persistence are outside timing. Every scan validates all 1,000
// rows, including every BLOB byte, so allocation changes cannot hide lost rows
// or reused buffers containing another row's data.
func BenchmarkStreamingRangeResultStorage(b *testing.B) {
	for _, reuse := range []bool{false, true} {
		name := "Values"
		if reuse {
			name = "ValuesInto"
		}
		b.Run(name, func(b *testing.B) {
			path := b.TempDir()
			options := storage.DefaultOptions()
			options.DisableWALSync = true
			d, err := db.Open(path, options)
			if err != nil {
				b.Fatal(err)
			}
			b.Cleanup(func() { d.Close() })
			if _, err = d.Exec("CREATE TABLE stream_result(id BIGINT PRIMARY KEY,v BIGINT,payload BLOB)"); err != nil {
				b.Fatal(err)
			}
			var expected [1000][]byte
			for i := range expected {
				expected[i] = bytes.Repeat([]byte{byte(i), byte(i >> 8)}, 120)
			}
			position := 0
			_, err = d.BulkInsert(context.Background(), "stream_result", []string{"id", "v", "payload"}, func(context.Context) ([]any, error) {
				if position == len(expected) {
					return nil, io.EOF
				}
				position++
				return []any{int64(position), int64(position * 17), expected[position-1]}, nil
			})
			if err != nil {
				b.Fatal(err)
			}
			if err = d.Close(); err != nil {
				b.Fatal(err)
			}
			d, err = db.Open(path, options)
			if err != nil {
				b.Fatal(err)
			}
			stmt, err := d.Prepare("SELECT id,v,payload FROM stream_result WHERE id>=? ORDER BY id LIMIT 1000")
			if err != nil {
				b.Fatal(err)
			}
			defer stmt.Close()
			var values []sql.Value
			run := func() {
				rows, err := stmt.Query(int64(1))
				if err != nil {
					b.Fatal(err)
				}
				if rows.Materialized() {
					b.Fatal("range unexpectedly materialized")
				}
				count := 0
				for rows.Next() {
					if reuse {
						values, err = rows.ValuesInto(values)
					} else {
						values, err = rows.Values()
					}
					if err != nil || count >= len(expected) || len(values) != 3 || values[0].Int != int64(count+1) || values[1].Int != int64((count+1)*17) || !bytes.Equal(values[2].Blob, expected[count]) {
						b.Fatalf("incorrect row %d: %v %v", count, values, err)
					}
					count++
				}
				if err = rows.Err(); err != nil || count != len(expected) {
					b.Fatalf("scan count=%d: %v", count, err)
				}
				if err = rows.Close(); err != nil {
					b.Fatal(err)
				}
			}
			run()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				run()
			}
			b.StopTimer()
		})
	}
}
