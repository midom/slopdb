package main

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestSeedReuseAndCheckedConcurrentReaders(t *testing.T) {
	c := config{Dir: filepath.Join(t.TempDir(), "database"), SeedBytes: 256 * 257, CacheBytes: 1 << 20, Workers: 4, Duration: 10 * time.Millisecond, Samples: 2, Prewarm: true, JSON: filepath.Join(t.TempDir(), "report.json")}
	if err := seed(c); err != nil {
		t.Fatal(err)
	}
	if err := seed(c); err == nil {
		t.Fatal("reseed overwrote existing database")
	}
	c.SeedBytes = 0
	for _, mode := range []struct{ policy, memory string }{{"lru", "mmap"}, {"clock", "mmap"}, {"lru", "heap"}, {"clock", "heap"}} {
		policy := mode.policy
		c.CachePolicy = policy
		c.CacheMemory = mode.memory
		r, err := run(c)
		if err != nil {
			t.Fatal(err)
		}
		if r.Seed.Rows != 257 || r.PrewarmQueries != 33 || len(r.Samples) != 2 {
			t.Fatalf("incomplete run: %+v", r)
		}
		for _, s := range r.Samples {
			if s.Queries <= 0 || s.QueriesPerSecond <= 0 || s.After.BlockCacheBytes > int64(c.CacheBytes) || s.After.BlockCacheHits <= s.Before.BlockCacheHits || string(s.After.BlockCachePolicy) != policy || string(s.After.BlockCacheMemory) != mode.memory {
				t.Fatalf("invalid sample: %+v", s)
			}
		}
		if err := verify(c, r.Seed); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := os.Stat(c.JSON); err != nil {
		t.Fatal(err)
	}
}

func TestReadRunDoesNotCreateMissingSeed(t *testing.T) {
	c := config{Dir: filepath.Join(t.TempDir(), "missing"), Workers: 1, CacheBytes: 1 << 20, Duration: time.Millisecond, Samples: 1}
	if _, err := run(c); err == nil {
		t.Fatal("missing seed accepted")
	}
	if _, err := os.Stat(c.Dir); !os.IsNotExist(err) {
		t.Fatalf("read run created seed: %v", err)
	}
}

func TestHistogramBounds(t *testing.T) {
	var h histogram
	h.add(1000)
	h.add(1000000)
	if p := h.percentile(.5); p < 1 || p > 1.1 {
		t.Fatalf("median %f us", p)
	}
	if p := h.percentile(.99); p < 1000 || p > 1100 {
		t.Fatalf("p99 %f us", p)
	}
}
