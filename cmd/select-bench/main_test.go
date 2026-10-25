package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"slopdb/db"
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
	for _, reuse := range []bool{false, true} {
		for _, mode := range []struct{ policy, memory string }{{"lru", "mmap"}, {"clock", "mmap"}, {"lru", "heap"}, {"clock", "heap"}} {
			t.Run(fmt.Sprintf("%s/%s/reuse=%t", mode.policy, mode.memory, reuse), func(t *testing.T) {
				policy := mode.policy
				c.CachePolicy = policy
				c.CacheMemory = mode.memory
				c.ReuseResults = reuse
				t.Setenv("GOGC", "123") // Report configuration; does not change runtime GC.
				t.Setenv("GOMEMLIMIT", "64MiB")
				r, err := run(c)
				if err != nil {
					t.Fatal(err)
				}
				if r.Seed.Rows != 257 || r.PrewarmQueries != 33 || len(r.Samples) != 2 {
					t.Fatalf("incomplete run: %+v", r)
				}
				wantAPI := "ExecPrepared"
				if reuse {
					wantAPI = "ExecPreparedInto"
				}
				if r.RuntimeEnv["GOGC"] != "123" || r.RuntimeEnv["GOMEMLIMIT"] != "64MiB" || r.Config.ReuseResults != reuse || r.APIMode != wantAPI {
					t.Fatalf("missing runtime/API configuration: %+v", r)
				}
				for _, s := range r.Samples {
					if s.Queries <= 0 || s.QueriesPerSecond <= 0 || s.After.BlockCacheBytes > int64(c.CacheBytes) || s.After.BlockCacheHits <= s.Before.BlockCacheHits || string(s.After.BlockCachePolicy) != policy || string(s.After.BlockCacheMemory) != mode.memory {
						t.Fatalf("invalid sample: %+v", s)
					}
					if s.CPUSeconds < 0 || s.GCPauseSeconds < 0 || s.VoluntaryContextSwitches < 0 || s.InvoluntaryContextSwitches < 0 || s.Before.SeqNum != s.After.SeqNum {
						t.Fatalf("invalid runtime evidence or fixture mutation: %+v", s)
					}
				}
				data, err := os.ReadFile(c.JSON)
				if err != nil {
					t.Fatal(err)
				}
				var saved report
				if err = json.Unmarshal(data, &saved); err != nil {
					t.Fatal(err)
				}
				if saved.APIMode != wantAPI || saved.Config.ReuseResults != reuse || len(saved.Samples) != c.Samples || saved.RuntimeEnv["GOGC"] != "123" {
					t.Fatalf("incomplete saved evidence: %+v", saved)
				}
				if err := verify(c, r.Seed); err != nil {
					t.Fatal(err)
				}
			})
		}
	}
	if _, err := os.Stat(c.JSON); err != nil {
		t.Fatal(err)
	}
}

func TestExecutionModeKeepsOwnedResultsIndependent(t *testing.T) {
	c := config{Dir: filepath.Join(t.TempDir(), "database"), SeedBytes: 256 * 17, CacheBytes: 1 << 20, CacheMemory: "heap"}
	if err := seed(c); err != nil {
		t.Fatal(err)
	}
	d, err := db.Open(c.Dir, options(c))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if d != nil {
			_ = d.Close()
		}
	})
	s, err := d.NewSession()
	if err != nil {
		t.Fatal(err)
	}
	stmt, err := d.Prepare(query)
	if err != nil {
		t.Fatal(err)
	}
	var dst db.ExecResult
	expected := make([]byte, payloadBytes)
	owned, err := execute(s, stmt, &dst, false, 1)
	if err != nil {
		t.Fatal(err)
	}
	if owned == &dst {
		t.Fatal("default mode reused the destination")
	}
	for _, id := range []int64{2, 17, 1, 3} {
		reused, err := execute(s, stmt, &dst, true, id)
		if err != nil {
			t.Fatal(err)
		}
		if reused != &dst {
			t.Fatal("reusable mode did not return the worker destination")
		}
		if err = check(reused, id, expected); err != nil {
			t.Fatal(err)
		}
		if err = check(owned, 1, expected); err != nil {
			t.Fatalf("retained owned result changed: %v", err)
		}
	}
	second, err := execute(s, stmt, &dst, false, 17)
	if err != nil {
		t.Fatal(err)
	}
	if second == &dst {
		t.Fatal("owned mode reused the worker destination")
	}
	if err = check(second, 17, expected); err != nil {
		t.Fatal(err)
	}
	if err = check(&dst, 3, expected); err != nil {
		t.Fatalf("owned execution changed reusable destination: %v", err)
	}
	if err = stmt.Close(); err != nil {
		t.Fatal(err)
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	if err = d.Close(); err != nil {
		t.Fatal(err)
	}
	d = nil
	if err = verify(c, seedInfo{Rows: 17}); err != nil {
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
