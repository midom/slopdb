package main

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
)

func TestDuckPageSizeCreationAndReopen(t *testing.T) {
	t.Setenv("SLOPDB_TPCC_WAL_FILE", "")
	for _, size := range []int64{16384, 65536} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			c := config{Database: filepath.Join(t.TempDir(), "test.duckdb"), PageSize: size, Threads: 1, Memory: "1GB"}
			for attempt := 0; attempt < 2; attempt++ {
				database, err := open(context.Background(), c)
				if err != nil {
					t.Fatal(err)
				}
				var actual int64
				if err = database.QueryRow("SELECT block_size FROM pragma_database_size()").Scan(&actual); err != nil {
					t.Fatal(err)
				}
				if actual != size {
					t.Fatalf("actual=%d want=%d", actual, size)
				}
				if attempt == 0 {
					if _, err = database.Exec("CREATE TABLE probe(id BIGINT); INSERT INTO probe VALUES (42)"); err != nil {
						t.Fatal(err)
					}
				}
				var value int64
				if err = database.QueryRow("SELECT id FROM probe").Scan(&value); err != nil {
					t.Fatal(err)
				}
				if value != 42 {
					t.Fatal(value)
				}
				if err = database.Close(); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}
