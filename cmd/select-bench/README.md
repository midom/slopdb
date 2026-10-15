# Concurrent small-row SELECT benchmark

Build from the repository root and keep binaries, databases and raw reports
outside the checkout:

```sh
mkdir -p ~/slopdb-bench
go build -o ~/slopdb-bench/select-bench ./cmd/select-bench
~/slopdb-bench/select-bench -dir ~/slopdb-small-row-10gb -seed-bytes 10000000000
python3 scripts/select_cache_matrix.py \
  --binary ~/slopdb-bench/select-bench --dir ~/slopdb-small-row-10gb \
  --output ~/slopdb-bench/select-cache-results
```

Add `--policy clock` and use a separate output directory to compare second-chance
CLOCK with the default sampled LRU. Run each matrix serially with the same
binary and seed. Both policies use 128 shards and identical byte budgets. The
public storage option is `Options.BlockCachePolicy` (`BlockCacheLRU` or
`BlockCacheCLOCK`); it changes eviction only and is not persisted in the database.

The cache uses mmap arenas by default. Add `--memory heap` to the matrix script,
or `-cache-memory heap` to the benchmark binary, to use the Go heap cache.
`--memory mmap` and `-cache-memory mmap` select the default explicitly.
The public option is `Options.BlockCacheMemory`, with `BlockCacheMmap` and
`BlockCacheHeap`. This setting does not change the database files.

Seeding refuses to overwrite a database. The fixture contains 39,062,500 rows
of 256 logical bytes: two BIGINT values and a 240-byte deterministic BLOB.
This is 10 decimal GB of logical data, excluding row tags, keys, SSTable metadata
and compression. The persisted physical page target is 4 KiB. Loading uses the
public SQL BulkInsert API in 4,096-row transactions and 256-row prepared INSERTs,
with WAL enabled and WAL fsync disabled. A full VACUUM rewrites all levels into
L6. Every typed row is checked after reopening before the seed is marked ready.

The cache matrix reuses those SSTables, with decimal decoded-block budgets of
100 MB, 1 GB, 10 GB and 20 GB. Each configuration opens a fresh process with 100
independent sessions and a shared prepared statement:

```sql
SELECT id,v,payload FROM small_rows WHERE id=?
```

Before timing, each process probes every eighth row. This warms full data pages;
short trailing pages at file boundaries can remain cold. Small caches retain
only their working budget. Then each session draws uniformly distributed IDs
using its own deterministic PRNG. Every returned value is checked against the
fixture generator. Validation time contributes to throughput; request latency
covers the ExecPrepared call. Per-worker histograms use 16 subdivisions per
power of two and report percentile upper bounds. Three consecutive 30-second
samples share the warmed process cache; they are descriptive observations,
not three independent fixture replications.

Per-sample process user/system CPU seconds are measured with `getrusage`,
excluding warmup. `AverageCPUCores` is CPU seconds divided by elapsed seconds;
`CPUPercent` divides that by the host's logical CPU count. Background Go GC
work contributes to process CPU use. More CPU use alone is not a speedup.

To measure scaling at a fully fitting cache, run:

```sh
python3 scripts/select_scaling.py \
  --binary ~/slopdb-bench/select-bench --dir ~/slopdb-small-row-10gb \
  --output ~/slopdb-bench/select-scaling --policy clock \
  --workers 1 8 16 32 60 100 --cache-bytes 20000000000
```

Keep runtime settings (`GOGC`, `GOMEMLIMIT`, `GOMAXPROCS`) the same for code
comparisons. Record tuning experiments separately, including their memory cost.

For `GOGC=200` alone, explicitly unset any inherited memory limit:

```sh
env -u GOMEMLIMIT -u GOMAXPROCS GOGC=200 python3 scripts/select_cache_matrix.py \
  --binary ~/slopdb-bench/select-bench --dir ~/slopdb-small-row-10gb \
  --output ~/slopdb-bench/select-clock-gogc200 --policy clock
```

The matrix records cache occupancy and hit/miss deltas, allocation, latency,
throughput, process resource counters, exact commands, binary fingerprint and
the SHA256 of every SSTable before and after the suite. It rejects failures,
mutation of the seed, missing samples and cache-budget violations. Linux
filesystem caching stays enabled and is never reset; these are engine-cache
measurements and do not isolate physical device reads. A byte budget does not
include all process memory or cache metadata. A 10 GB cache need not hold a
10 GB logical fixture once row/storage encodings are included.

Raw sample statistics also expose `BlockCacheAdmissionSkips` (best-effort
admissions bypassed because a shard was busy) and `PointIndexBytes` (additional
predecoded SSTable index-array capacity in the current file view). Index arrays
are reader metadata outside the decoded-page budget. Older pinned views and
other process allocations can retain more memory; use peak RSS alongside cache
occupancy when comparing configurations. See the [latest optimization report](../../docs/small-row-select-final-optimizations.md).

For diagnostic profiles, use a separate invocation and retain its flags. Do not
mix its throughput into unprofiled results:

```sh
~/slopdb-bench/select-bench -dir ~/slopdb-small-row-10gb \
  -cache-bytes 100000000 -workers 100 -prewarm=false \
  -duration 15s -samples 1 -profile-dir ~/slopdb-bench/select-profile \
  -json ~/slopdb-bench/select-profile.json
go tool pprof -top ~/slopdb-bench/select-profile/cpu.pprof
go tool pprof -top ~/slopdb-bench/select-profile/mutex.pprof
```

Setup, page-spaced warmup, seed validation, compaction and close are outside the
timed samples. No loading or writes run alongside measurements. Benchmark
timings are not ordinary test pass/fail thresholds; functionality tests use
`t.TempDir()` and small fixtures.
