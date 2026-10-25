package main

import (
	"path/filepath"
	"testing"
	"time"
)

func TestEmbeddedDatabaseSQLBenchmarkModes(t *testing.T) {
	c := config{Dir: filepath.Join(t.TempDir(), "db"), SeedBytes: 256 * 129, CacheBytes: 1 << 20, Workers: 4, Duration: 10 * time.Millisecond, Samples: 2, Prewarm: true, API: "embedded"}
	if err := seed(c); err != nil {
		t.Fatal(err)
	}
	c.SeedBytes = 0
	for _, mode := range []string{"text", "prepared"} {
		c.QueryMode = mode
		c.JSON = filepath.Join(t.TempDir(), mode+".json")
		result, err := run(c)
		if err != nil {
			t.Fatal(err)
		}
		if result.APIMode != "database/sql:embedded:"+mode || len(result.Samples) != 2 || result.PrewarmQueries != 17 {
			t.Fatal(result)
		}
		for _, sample := range result.Samples {
			if sample.Queries <= 0 || sample.Before.SeqNum != sample.After.SeqNum || sample.PoolAfter.InUse != 0 || sample.PoolBefore.OpenConnections != 4 || sample.PoolAfter.WaitCount != sample.PoolBefore.WaitCount {
				t.Fatal(sample)
			}
		}
		if err := verify(c, result.Seed); err != nil {
			t.Fatal(err)
		}
	}
}
