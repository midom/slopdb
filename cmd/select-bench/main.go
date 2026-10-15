// select-bench creates a reusable small-row fixture and measures checked,
// uniformly distributed prepared primary-key SELECTs with independent sessions.
package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math/bits"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"slopdb/db"
	"slopdb/internal/benchload"
	"slopdb/sql"
	"slopdb/storage"
)

const rowBytes = 256 // logical BIGINT + BIGINT + 240-byte BLOB; excludes encoding
const payloadBytes = rowBytes - 16
const query = "SELECT id,v,payload FROM small_rows WHERE id=?"

type seedInfo struct {
	Rows         int64
	LogicalBytes int64
	RowBytes     int
	Generator    string
}

type config struct {
	CachePolicy string
	Dir         string
	SeedBytes   int64
	CacheBytes  int
	Workers     int
	Duration    time.Duration
	Samples     int
	Prewarm     bool
	ProfileDir  string
	JSON        string
}

type sample struct {
	CPUSeconds, AverageCPUCores, CPUPercent     float64
	Queries                                     int64
	Seconds                                     float64
	QueriesPerSecond                            float64
	MeanUS, P50US, P95US, P99US                 float64
	AllocatedBytesPerQuery, AllocationsPerQuery float64
	GCs                                         uint32
	Before, After                               storage.LSMStats
}

type report struct {
	Config         config
	Seed           seedInfo
	GoVersion      string
	GOMAXPROCS     int
	Started        time.Time
	PrewarmQueries int64
	PrewarmSeconds float64
	Samples        []sample
	Error          string `json:",omitempty"`
}

// SplitMix64 is stateless, so fixture validation is independent of access order.
func mix(x uint64) uint64 {
	x += 0x9e3779b97f4a7c15
	x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9
	x = (x ^ (x >> 27)) * 0x94d049bb133111eb
	return x ^ (x >> 31)
}

func payload(id int64, dst []byte) {
	for i := 0; i < payloadBytes; i += 8 {
		binary.LittleEndian.PutUint64(dst[i:], mix(uint64(id)*31+uint64(i/8)))
	}
}

func check(r *db.ExecResult, id int64, expected []byte) error {
	if len(r.Rows) != 1 {
		return fmt.Errorf("id %d: got %d rows", id, len(r.Rows))
	}
	row := r.Rows[0]
	payload(id, expected)
	if row["id"].Type != sql.ColInteger || row["id"].Int != id || row["v"].Type != sql.ColInteger || row["v"].Int != id*17 || row["payload"].Type != sql.ColBlob || !bytes.Equal(row["payload"].Blob, expected) {
		return fmt.Errorf("id %d: incorrect result", id)
	}
	return nil
}

func options(c config) *storage.Options {
	o := storage.DefaultOptions()
	o.BlockCacheSize = c.CacheBytes
	if c.CachePolicy != "" {
		o.BlockCachePolicy = storage.BlockCachePolicy(c.CachePolicy)
	}
	o.CreateIfNotExist = false
	o.DisableWALSync = true
	return o
}

func seed(c config) (err error) {
	if c.SeedBytes < rowBytes || c.SeedBytes%rowBytes != 0 {
		return fmt.Errorf("seed bytes must be a positive multiple of %d", rowBytes)
	}
	o := options(c)
	o.CreateIfNotExist, o.ErrorIfExists = true, true
	// Bound load memory while avoiding tiny-file startup overhead. Read runs use
	// default options and the persisted 4 KiB physical page size.
	o.WriteBufferSize = 64 << 20
	d, err := db.Open(c.Dir, o)
	if err != nil {
		return err
	}
	defer func() {
		if d != nil {
			e := d.Close()
			if err == nil {
				err = e
			}
		}
	}()
	if _, err = d.Exec("CREATE TABLE small_rows(id BIGINT PRIMARY KEY,v BIGINT NOT NULL,payload BLOB NOT NULL)"); err != nil {
		return err
	}
	info := seedInfo{Rows: c.SeedBytes / rowBytes, LogicalBytes: c.SeedBytes, RowBytes: rowBytes, Generator: "splitmix64-v1"}
	var id int64
	started, last := time.Now(), time.Now()
	_, err = benchload.Load(context.Background(), d, "small_rows", []string{"id", "v", "payload"}, func(context.Context) ([]any, error) {
		if id == info.Rows {
			return nil, io.EOF
		}
		id++
		data := make([]byte, payloadBytes)
		payload(id, data)
		if time.Since(last) >= 10*time.Second {
			fmt.Fprintf(os.Stderr, "loaded %d/%d rows (%.0f rows/s)\n", id, info.Rows, float64(id)/time.Since(started).Seconds())
			last = time.Now()
		}
		return []any{id, id * 17, data}, nil
	}, benchload.DefaultBatchRows)
	if err != nil {
		return err
	}
	fmt.Fprintln(os.Stderr, "load complete; full bottom-level rewrite")
	if _, err = d.Exec("VACUUM"); err != nil {
		return err
	}
	if err = d.Close(); err != nil {
		d = nil
		return err
	}
	d = nil
	if err = verify(c, info); err != nil {
		return err
	}
	return writeJSON(filepath.Join(c.Dir, "seed.json"), info)
}

// Verify every typed value after reopening. This is deliberately outside the
// measurement process and does not fill its block cache.
func verify(c config, info seedInfo) (err error) {
	d, err := db.Open(c.Dir, options(c))
	if err != nil {
		return err
	}
	defer func() {
		e := d.Close()
		if err == nil {
			err = e
		}
	}()
	rows, err := d.Query("SELECT id,v,payload FROM small_rows ORDER BY id")
	if err != nil {
		return err
	}
	defer rows.Close()
	if rows.Materialized() {
		return fmt.Errorf("verification unexpectedly materialized")
	}
	var count int64
	expected := make([]byte, payloadBytes)
	for rows.Next() {
		var id, v int64
		var data []byte
		if err = rows.Scan(&id, &v, &data); err != nil {
			return err
		}
		count++
		payload(count, expected)
		if id != count || v != count*17 || !bytes.Equal(data, expected) {
			return fmt.Errorf("verification mismatch at row %d", count)
		}
	}
	if err = rows.Err(); err != nil {
		return err
	}
	if count != info.Rows {
		return fmt.Errorf("got %d rows, want %d", count, info.Rows)
	}
	stats := d.Stats()
	for i := 0; i < 6; i++ {
		if stats.Levels[i].FileCount != 0 {
			return fmt.Errorf("seed has upper-level SSTables")
		}
	}
	fmt.Fprintf(os.Stderr, "verified all %d rows after reopening; L6-only\n", count)
	return nil
}

// 16 logarithmic sub-buckets per power of two retain latency tails without
// sharing counters between workers or retaining one sample per query.
type histogram [1024]int64

func (h *histogram) add(ns int64) {
	u := uint64(ns)
	if u == 0 {
		u = 1
	}
	e := bits.Len64(u) - 1
	b := int((u - (uint64(1) << e)) * 16 / (uint64(1) << e))
	h[e*16+b]++
}
func (h *histogram) percentile(p float64) float64 {
	var total int64
	for _, n := range h {
		total += n
	}
	target := int64(float64(total)*p + 0.999999)
	var n int64
	for i, k := range h {
		n += k
		if n >= target {
			e := i / 16
			return float64(uint64(1)<<e) * (1 + float64(i%16+1)/16) / 1000
		}
	}
	return 0
}

type workerStats struct {
	n, ns int64
	h     histogram
	err   error
}

func run(c config) (r report, err error) {
	r = report{Config: c, GoVersion: runtime.Version(), GOMAXPROCS: runtime.GOMAXPROCS(0), Started: time.Now().UTC()}
	defer func() {
		if err != nil {
			r.Error = err.Error()
		}
		if c.JSON != "" {
			e := writeJSON(c.JSON, r)
			if err == nil {
				err = e
			}
		}
	}()
	data, err := os.ReadFile(filepath.Join(c.Dir, "seed.json"))
	if err != nil {
		return r, err
	}
	if err = json.Unmarshal(data, &r.Seed); err != nil {
		return r, err
	}
	if r.Seed.Rows <= 0 || r.Seed.RowBytes != rowBytes || r.Seed.Rows > (1<<63-1)/rowBytes || r.Seed.LogicalBytes != r.Seed.Rows*rowBytes || r.Seed.Generator != "splitmix64-v1" {
		return r, fmt.Errorf("unsupported seed")
	}
	d, err := db.Open(c.Dir, options(c))
	if err != nil {
		return r, err
	}
	defer func() {
		e := d.Close()
		if err == nil {
			err = e
		}
	}()
	if d.Stats().PageSize != 4096 {
		return r, fmt.Errorf("page-spaced warmup requires a 4096-byte seed page size")
	}
	stmt, err := d.Prepare(query)
	if err != nil {
		return r, err
	}
	defer stmt.Close()
	sessions := make([]*db.Session, c.Workers)
	for i := range sessions {
		sessions[i], err = d.NewSession()
		if err != nil {
			return r, err
		}
		defer sessions[i].Close()
	}
	if c.Prewarm {
		// At most eight rows between probes visits full 4 KiB data pages.
		// Short trailing pages at SSTable boundaries can remain cold; measured
		// hit rates and occupancy expose this small warmup remainder.
		start := time.Now()
		var wg sync.WaitGroup
		results := make([]workerStats, c.Workers)
		for i, s := range sessions {
			wg.Add(1)
			go func(i int, s *db.Session) {
				defer wg.Done()
				buf := make([]byte, payloadBytes)
				for id := int64(1 + i*8); id <= r.Seed.Rows; id += int64(c.Workers * 8) {
					out, e := s.ExecPrepared(stmt, id)
					if e == nil {
						e = check(out, id, buf)
					}
					if e != nil {
						results[i].err = e
						return
					}
					results[i].n++
				}
			}(i, s)
		}
		wg.Wait()
		for _, w := range results {
			if w.err != nil {
				return r, w.err
			}
			r.PrewarmQueries += w.n
		}
		r.PrewarmSeconds = time.Since(start).Seconds()
		fmt.Fprintf(os.Stderr, "prewarmed %d checked page-spaced SELECTs in %.1fs\n", r.PrewarmQueries, r.PrewarmSeconds)
	}
	if c.ProfileDir != "" {
		if err = os.MkdirAll(c.ProfileDir, 0755); err != nil {
			return r, err
		}
		runtime.SetMutexProfileFraction(10)
		runtime.SetBlockProfileRate(1000000)
		f, e := os.Create(filepath.Join(c.ProfileDir, "cpu.pprof"))
		if e != nil {
			return r, e
		}
		if e = pprof.StartCPUProfile(f); e != nil {
			f.Close()
			return r, e
		}
		defer func() {
			pprof.StopCPUProfile()
			f.Close()
			for _, kind := range []string{"mutex", "block", "heap"} {
				f, e := os.Create(filepath.Join(c.ProfileDir, kind+".pprof"))
				if e == nil {
					pprof.Lookup(kind).WriteTo(f, 0)
					f.Close()
				}
			}
		}()
	}
	for n := 0; n < c.Samples; n++ {
		var beforeMem, afterMem runtime.MemStats
		runtime.ReadMemStats(&beforeMem)
		sample := sample{Before: d.Stats()}
		results := make([]workerStats, c.Workers)
		var stop atomic.Bool
		startGate := make(chan struct{})
		var ready, wg sync.WaitGroup
		for i, s := range sessions {
			ready.Add(1)
			wg.Add(1)
			go func(i int, s *db.Session) {
				defer wg.Done()
				rng := rand.New(rand.NewSource(42 + int64(i) + int64(n)*1000))
				buf := make([]byte, payloadBytes)
				ready.Done()
				<-startGate
				for !stop.Load() {
					id := rng.Int63n(r.Seed.Rows) + 1
					started := time.Now()
					out, e := s.ExecPrepared(stmt, id)
					elapsed := time.Since(started).Nanoseconds()
					if e == nil {
						e = check(out, id, buf)
					}
					if e != nil {
						results[i].err = e
						stop.Store(true)
						return
					}
					results[i].n++
					results[i].ns += elapsed
					results[i].h.add(elapsed)
				}
			}(i, s)
		}
		ready.Wait()
		cpuStart := processCPUSeconds()
		start := time.Now()
		close(startGate)
		timer := time.AfterFunc(c.Duration, func() { stop.Store(true) })
		wg.Wait()
		timer.Stop()
		sample.Seconds = time.Since(start).Seconds()
		sample.CPUSeconds = processCPUSeconds() - cpuStart
		sample.AverageCPUCores = sample.CPUSeconds / sample.Seconds
		sample.CPUPercent = sample.AverageCPUCores * 100 / float64(runtime.NumCPU())
		var h histogram
		var ns int64
		for _, w := range results {
			if w.err != nil {
				return r, w.err
			}
			sample.Queries += w.n
			ns += w.ns
			for i, v := range w.h {
				h[i] += v
			}
		}
		if sample.Queries == 0 {
			return r, fmt.Errorf("no queries completed")
		}
		sample.QueriesPerSecond = float64(sample.Queries) / sample.Seconds
		sample.MeanUS = float64(ns) / float64(sample.Queries) / 1000
		sample.P50US = h.percentile(.5)
		sample.P95US = h.percentile(.95)
		sample.P99US = h.percentile(.99)
		runtime.ReadMemStats(&afterMem)
		sample.AllocatedBytesPerQuery = float64(afterMem.TotalAlloc-beforeMem.TotalAlloc) / float64(sample.Queries)
		sample.AllocationsPerQuery = float64(afterMem.Mallocs-beforeMem.Mallocs) / float64(sample.Queries)
		sample.GCs = afterMem.NumGC - beforeMem.NumGC
		sample.After = d.Stats()
		r.Samples = append(r.Samples, sample)
		fmt.Fprintf(os.Stderr, "sample %d: %.0f SELECT/s; mean %.1f us p99 <= %.1f us; cache %.3f GB; %.1f CPU cores (%.1f%% of host)\n", n+1, sample.QueriesPerSecond, sample.MeanUS, sample.P99US, float64(sample.After.BlockCacheBytes)/1e9, sample.AverageCPUCores, sample.CPUPercent)
	}
	return r, nil
}

func processCPUSeconds() float64 {
	var usage syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &usage); err != nil {
		panic(err)
	}
	return float64(usage.Utime.Sec+usage.Stime.Sec) + float64(usage.Utime.Usec+usage.Stime.Usec)/1e6
}

func writeJSON(path string, v any) error {
	data, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(data, '\n'), 0644)
}

func main() {
	var c config
	flag.StringVar(&c.Dir, "dir", "", "existing seed directory (required)")
	flag.Int64Var(&c.SeedBytes, "seed-bytes", 0, "create a fresh seed of this many logical bytes; never overwrite")
	flag.IntVar(&c.CacheBytes, "cache-bytes", 100000000, "decoded page-cache bytes (decimal)")
	flag.StringVar(&c.CachePolicy, "cache-policy", "lru", "decoded page-cache policy: lru or clock")
	flag.IntVar(&c.Workers, "workers", 100, "independent concurrent reader sessions")
	flag.DurationVar(&c.Duration, "duration", 30*time.Second, "measurement duration per sample")
	flag.IntVar(&c.Samples, "samples", 3, "consecutive samples in the same process")
	flag.BoolVar(&c.Prewarm, "prewarm", true, "checked page-spaced point SELECT warmup before timing (stride 8 rows)")
	flag.StringVar(&c.ProfileDir, "profile-dir", "", "optional diagnostic profiles (use separate runs)")
	flag.StringVar(&c.JSON, "json", "", "report destination")
	flag.Parse()
	if c.Dir == "" || c.Workers < 1 || c.Duration <= 0 || c.Samples < 1 || c.CacheBytes <= 0 {
		fmt.Fprintln(os.Stderr, "invalid benchmark configuration")
		os.Exit(1)
	}
	var err error
	if c.SeedBytes != 0 {
		err = seed(c)
	} else {
		_, err = run(c)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
