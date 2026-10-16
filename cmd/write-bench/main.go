package main

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"math/bits"
	"os"
	"runtime"
	"runtime/pprof"
	"slopdb/db"
	"slopdb/storage"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

var dir = flag.String("db", "", "database")
var workload = flag.String("workload", "insert-random", "workload")
var writers = flag.Int("writers", 96, "writers")
var procs = flag.Int("procs", 96, "Go processors")
var mem = flag.Int("mem-mib", 8, "memtable")
var duration = flag.Float64("seconds", 30, "sample seconds")
var warm = flag.Float64("warmup", 10, "warmup seconds")
var samples = flag.Int("samples", 3, "samples")
var hot = flag.Int64("hot", 1000000, "update key count")
var noWAL = flag.Bool("no-wal", false, "disable WAL entirely; unflushed writes are lost on crash")
var hashMemTable = flag.Bool("hash-memtable", false, "use hash memtable instead of default skip list")
var durable = flag.Bool("durable", false, "sync WAL")
var volatileIDs = flag.Bool("volatile-ids", false, "do not persist automatic ID counters")
var profile = flag.String("profile", "", "CPU profile")

type worker struct {
	count, retries uint64
	hist           [64]uint64
	latency        uint64
	rng            uint64
	err            error
	session        *db.Session
	padding        [72]byte // separate adjacent writers even without cache-line alignment
}

func next(x *uint64) uint64 { *x ^= *x << 13; *x ^= *x >> 7; *x ^= *x << 17; return *x }
func blob(n uint64) [64]byte {
	var b [64]byte
	x := n*0x9e3779b97f4a7c15 + 1
	for i := 0; i < 64; i += 8 {
		binary.LittleEndian.PutUint64(b[i:], next(&x))
	}
	binary.LittleEndian.PutUint64(b[:], n)
	return b
}
func usage() float64 {
	var r syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &r); err != nil {
		panic(err)
	}
	return float64(r.Utime.Sec+r.Stime.Sec) + float64(r.Utime.Usec+r.Stime.Usec)/1e6
}
func must(err error) {
	if err != nil {
		panic(err)
	}
}
func out(v any) { must(json.NewEncoder(os.Stdout).Encode(v)) }
func main() {
	flag.Parse()
	if *dir == "" || *writers < 1 || *procs < 1 || *mem < 1 || *duration <= 0 || *warm < 0 || *samples < 1 || *hot < 1 {
		panic("require --db, positive writers/mem/seconds/samples/hot, and nonnegative warmup")
	}
	switch *workload {
	case "insert-random", "insert-auto", "update-random", "update-sequential":
	default:
		panic("unknown workload")
	}
	if *volatileIDs && *workload != "insert-auto" {
		panic("--volatile-ids requires insert-auto")
	}
	if *noWAL && *durable {
		panic("--no-wal cannot be combined with --durable")
	}
	runtime.GOMAXPROCS(*procs)
	o := storage.DefaultOptions()
	o.BlockCacheSize = 20 << 30
	o.WriteBufferSize = *mem << 20
	o.MaxBackgroundJobs = 8
	o.DisableWAL = *noWAL
	o.HashMemTable = *hashMemTable
	o.DisableWALSync = !*durable
	o.DisableAutoIncrementPersistence = *volatileIDs
	o.LockTimeout = time.Second
	d, e := db.Open(*dir, o)
	must(e)
	insert := *workload == "insert-random" || *workload == "insert-auto"
	if insert {
		pk := "INTEGER PRIMARY KEY"
		if *workload == "insert-auto" {
			pk += " AUTOINCREMENT"
		}
		_, e = d.Exec("CREATE TABLE bench(id " + pk + ",v INTEGER NOT NULL,payload BLOB NOT NULL)")
		must(e)
	}
	query := "UPDATE bench SET v=v+1 WHERE id=?"
	if *workload == "insert-random" {
		query = "INSERT INTO bench(id,v,payload) VALUES(?,?,?)"
	}
	if *workload == "insert-auto" {
		query = "INSERT INTO bench(v,payload) VALUES(?,?)"
	}
	if !insert {
		r, e := d.Query(fmt.Sprintf("SELECT id FROM bench WHERE id>=1 AND id<=%d", *hot))
		must(e)
		var n int64
		for r.Next() {
			n++
		}
		must(r.Err())
		must(r.Close())
		if n != *hot {
			panic("warm cache count mismatch")
		}
	}
	stmt, e := d.Prepare(query)
	must(e)
	defer stmt.Close()
	ws := make([]worker, *writers)
	for i := range ws {
		s, e := d.NewSession()
		must(e)
		ws[i].session = s
		ws[i].rng = uint64(i+1) * 0x9e3779b97f4a7c15
	}
	run := func(sec float64, phase int) {
		var stop atomic.Bool
		var wg, ready sync.WaitGroup
		start := make(chan struct{})
		wg.Add(*writers)
		ready.Add(*writers)
		before := d.Stats()
		var m0 runtime.MemStats
		runtime.ReadMemStats(&m0)
		c0 := usage()
		var n0, r0 uint64
		counts0 := make([]uint64, len(ws))
		for i := range ws {
			counts0[i] = ws[i].count
			n0 += ws[i].count
			r0 += ws[i].retries
			ws[i].hist = [64]uint64{}
			ws[i].latency = 0
		}
		for id := range ws {
			go func(id int) {
				defer wg.Done()
				w := &ws[id]
				ready.Done()
				<-start
				var result db.ExecResult
				for !stop.Load() {
					ticket := w.count*uint64(*writers) + uint64(id) + 1
					var args []any
					if insert {
						b := blob(ticket)
						if *workload == "insert-auto" {
							args = []any{int64(ticket), b[:]}
						} else {
							k := int64(ticket * 6364136223846793005 & ((uint64(1) << 63) - 1))
							args = []any{k, int64(ticket), b[:]}
						}
					} else {
						k := int64(ticket-1)%*hot + 1
						if *workload == "update-random" {
							k = int64(next(&w.rng)%uint64(*hot)) + 1
						}
						args = []any{k}
					}
					sampled := w.count&63 == 0
					var t time.Time
					if sampled {
						t = time.Now()
					}
					for {
						_, err := w.session.ExecPreparedInto(&result, stmt, args...)
						if err == nil {
							if result.Changes != 1 {
								w.err = fmt.Errorf("changes=%d", result.Changes)
								stop.Store(true)
								return
							}
							w.count++
							break
						}
						if errors.Is(err, db.ErrLockTimeout) || errors.Is(err, db.ErrConflict) {
							w.retries++
							if stop.Load() {
								return
							}
							continue
						}
						w.err = err
						stop.Store(true)
						return
					}
					if sampled {
						ns := uint64(time.Since(t))
						w.hist[bits.Len64(ns)]++
						w.latency += ns
					}
				}
			}(id)
		}
		ready.Wait()
		t0 := time.Now()
		close(start)
		time.Sleep(time.Duration(sec * 1e9))
		stop.Store(true)
		wg.Wait()
		elapsed := time.Since(t0).Seconds()
		c1 := usage()
		var m1 runtime.MemStats
		runtime.ReadMemStats(&m1)
		after := d.Stats()
		var n, r, ls, ln uint64
		var hist [64]uint64
		for i := range ws {
			must(ws[i].err)
			n += ws[i].count
			r += ws[i].retries
			ls += ws[i].latency
			for j := range hist {
				hist[j] += ws[i].hist[j]
				ln += ws[i].hist[j]
			}
		}
		var min, max, active uint64
		min = ^uint64(0)
		for i := range ws {
			x := ws[i].count - counts0[i]
			if x < min {
				min = x
			}
			if x > max {
				max = x
			}
			if x > 0 {
				active++
			}
		}
		percentile := func(p float64) uint64 {
			target := uint64(float64(ln) * p)
			var c uint64
			for i, v := range hist {
				c += v
				if c >= target {
					return uint64(1) << i
				}
			}
			return 0
		}
		out(map[string]any{"type": "sample", "writer_min_commits": min, "writer_max_commits": max, "active_writers": active, "phase": phase, "engine": "slopdb", "workload": *workload, "writers": *writers, "durable": *durable, "wal_enabled": !*noWAL, "hash_memtable": *hashMemTable, "volatile_ids": *volatileIDs, "mem_mib": *mem, "seconds": elapsed, "commits": n - n0, "tps": float64(n-n0) / elapsed, "retries": r - r0, "cpu_cores": (c1 - c0) / elapsed, "latency_mean_us": float64(ls) / float64(ln) / 1000, "latency_p99_upper_us": float64(percentile(.99)) / 1000, "wal_active": after.WALActive, "wal_sync_disabled": after.WALSyncDisabled, "write_groups": after.WriteGroups - before.WriteGroups, "write_batches": after.WriteBatches - before.WriteBatches, "flushes": after.CompletedFlushes - before.CompletedFlushes, "compactions": after.CompletedCompactions - before.CompletedCompactions, "flush_bytes": after.FlushOutputBytes - before.FlushOutputBytes, "compaction_bytes": after.CompactionOutputBytes - before.CompactionOutputBytes, "alloc_bytes": m1.TotalAlloc - m0.TotalAlloc, "gc_cycles": m1.NumGC - m0.NumGC, "cache_bytes": after.BlockCacheBytes, "mem_bytes": after.MemSize})
	}
	out(map[string]any{"type": "config", "engine": "slopdb", "workload": *workload, "writers": *writers, "row_bytes": 80, "transactions_per_row": 1, "durable": *durable, "wal_enabled": !*noWAL, "hash_memtable": *hashMemTable, "volatile_ids": *volatileIDs, "mem_mib": *mem, "go": runtime.Version(), "gomaxprocs": runtime.GOMAXPROCS(0), "gogc": os.Getenv("GOGC"), "cache_bytes": 20 << 30, "background_jobs": 8})
	if *warm > 0 {
		run(*warm, -1)
	}
	if *profile != "" {
		f, e := os.Create(*profile)
		must(e)
		must(pprof.StartCPUProfile(f))
		runtime.SetMutexProfileFraction(10)
		runtime.SetBlockProfileRate(100000)
		defer f.Close()
	}
	for i := 0; i < *samples; i++ {
		run(*duration, i)
	}
	if *profile != "" {
		pprof.StopCPUProfile()
		runtime.SetMutexProfileFraction(0)
		runtime.SetBlockProfileRate(0)
		for _, name := range []string{"mutex", "block", "allocs"} {
			f, e := os.Create(*profile + "." + name)
			must(e)
			must(pprof.Lookup(name).WriteTo(f, 0))
			must(f.Close())
		}
	}
	for i := range ws {
		must(ws[i].session.Close())
	}
	must(stmt.Close())
	var total uint64
	for i := range ws {
		total += ws[i].count
	}
	// Close and reopen to check committed data after memtable flush and WAL replay.
	must(d.Close())
	d, e = db.Open(*dir, o)
	must(e)
	q := "SELECT id,v,payload FROM bench"
	if !insert {
		q = fmt.Sprintf("SELECT id,v,payload FROM bench WHERE id>=1 AND id<=%d", *hot)
	}
	rows, e := d.Query(q)
	must(e)
	var count, delta uint64
	seen := make([]byte, (maxTickets(ws, *writers)+7)/8)
	for rows.Next() {
		v, e := rows.Values()
		must(e)
		id := v[0].Int
		value := v[1].Int
		b := v[2].Blob
		stamp := uint64(id)
		if insert {
			stamp = uint64(value)
			if stamp == 0 {
				panic("bad stamp")
			}
			w := (stamp - 1) % uint64(*writers)
			seq := (stamp-1)/uint64(*writers) + 1
			if seq > ws[w].count {
				panic("unexpected transaction")
			}
			ix := stamp - 1
			if seen[ix/8]&(1<<(ix%8)) != 0 {
				panic("duplicate transaction")
			}
			seen[ix/8] |= 1 << (ix % 8)
			if *workload == "insert-random" && uint64(id) != stamp*6364136223846793005&((uint64(1)<<63)-1) {
				panic("random key mismatch")
			}
			if id <= 0 {
				panic("nonpositive key")
			}
		} else {
			if value < id*17 {
				panic("update value regressed")
			}
			delta += uint64(value - id*17)
		}
		want := blob(stamp)
		if !bytes.Equal(want[:], b) {
			panic("payload mismatch")
		}
		count++
	}
	must(rows.Err())
	must(rows.Close())
	if insert && count != total {
		panic("insert count mismatch")
	}
	if !insert && (count != uint64(*hot) || delta != total) {
		panic(fmt.Sprintf("update check count=%d delta=%d total=%d", count, delta, total))
	}
	must(d.Close())
	out(map[string]any{"type": "verify", "rows": count, "commits_including_warmup": total, "update_delta": delta, "ok": true})
}
func maxTickets(ws []worker, n int) uint64 {
	var max uint64
	for i := range ws {
		x := ws[i].count*uint64(n) + uint64(i) + 1
		if x > max {
			max = x
		}
	}
	return max
}
