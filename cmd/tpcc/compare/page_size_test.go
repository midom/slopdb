package main

import (
	"fmt"

	"slopdb/internal/tpcc"
	"testing"
	"time"
)

func TestPageSizeNativeLimits(t *testing.T) {
	for _, f := range []func(int64) error{validateSQLitePageSize, validateDuckPageSize} {
		for _, n := range []int64{0, 16384, 65536} {
			if err := f(n); err != nil {
				t.Fatal(err)
			}
		}
		for _, n := range []int64{-1, 12345, 1048576} {
			if err := f(n); err == nil {
				t.Fatalf("accepted invalid page size %d", n)
			}
		}
	}
}
func TestNativePageSizeCreationAndReopen(t *testing.T) {
	for _, engine := range []string{"sqlite", "duckdb"} {
		for _, size := range []int64{16384, 65536} {
			t.Run(engine+"/"+fmt.Sprint(size), func(t *testing.T) {
				c := tpcc.Config{Engine: engine, PageSize: size, WALFsync: true, Dir: t.TempDir(), Workers: 1, LockTimeout: time.Second}
				for attempt := 0; attempt < 2; attempt++ {
					database, err := openEngine(c)
					if err != nil {
						t.Fatal(err)
					}
					if got := database.Info()["page_size_bytes"]; got != size {
						t.Fatalf("actual=%v want=%d", got, size)
					}
					if attempt == 0 {
						_, err = database.Exec("CREATE TABLE page_probe(id INTEGER PRIMARY KEY, value INTEGER)")
						if err != nil {
							t.Fatal(err)
						}
						_, err = database.Exec("INSERT INTO page_probe VALUES (1,42)")
						if err != nil {
							t.Fatal(err)
						}
					}
					result, err := database.Exec("SELECT value FROM page_probe WHERE id=1")
					if err != nil {
						t.Fatal(err)
					}
					if len(result.Rows) != 1 || result.Rows[0]["value"].Int != 42 {
						t.Fatal(result)
					}
					if err = database.Close(); err != nil {
						t.Fatal(err)
					}
				}
			})
		}
	}
}
