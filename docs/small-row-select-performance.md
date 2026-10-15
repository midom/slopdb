# Small-row SELECTs with 100 concurrent readers

This is the initial optimization comparison. See the
[scaling follow-up](small-row-select-scaling.md) for the subsequent reader-view,
lifecycle, allocation and 128-shard cache changes and their measurements.

Measured on `test` on 2026-10-15: Linux ARM64, 60 Neoverse-N1 cores, approximately
58 GiB RAM, Go 1.26.0 and `GOMAXPROCS=60`. The baseline is production source at
`940d63d9ff30f1b8001417f663f0d65e3c3d97f3`, plus the new benchmark harness.
The final build includes the optimizations described below. Exact source
fingerprints, binary hashes, commands and all samples are retained with the
[comparison](benchmarks/select/2026-10-15/summary.json).

## Fixture and method

The existing mounted filesystem on `test` contained no seed. A new seed was
created once at `/home/ubuntu/slopdb-small-row-10gb`, then reused throughout.
It contains 39,062,500 rows of 256 logical bytes: two BIGINT columns and a
240-byte deterministic pseudorandom BLOB. That is exactly 10,000,000,000 logical
bytes, excluding encodings, keys and storage metadata. A full VACUUM produced
1,226 L6 SSTables totaling 10,147,558,012 physical bytes. Every typed row was
verified after reopening. The physical page target remains 4 KiB.

The measured query is:

```sql
SELECT id,v,payload FROM small_rows WHERE id=?
```

Each of 100 independent sessions shares a prepared statement and draws uniform
random primary keys using a separate deterministic PRNG. Every returned ID,
value and BLOB is checked. Validation contributes to aggregate throughput;
request latency covers the ExecPrepared call. Percentiles are histogram upper
bounds with at most 6.25% bucket width.

Each cache size gets a fresh process, 4,882,813 checked warmup probes at an
eight-row stride, and three consecutive 30-second samples. Short final pages
at SSTable boundaries can remain cold after warmup; cache hit/miss counters
measure the actual behavior. Samples within one warmed process are descriptive
observations, not independent fixture replications or statistical proof.

Cache budgets use **decimal bytes**: 100 MB, 1 GB, 10 GB and 20 GB. Linux
filesystem caching stays enabled and is never reset. These measurements expose
engine cache misses, syscall and decompression costs with a warm filesystem;
they do not measure cold physical-device random I/O. WAL remains enabled with
WAL fsync disabled. Loading, compaction, verification, preparation, warmup and
close are outside the timed samples. No other benchmark runs alongside them.

The matrix hashes every SSTable before and after execution and rejects changes,
query failures, incomplete samples, sequence changes and cache-budget violations.
The baseline and final matrices must also have identical SSTable inventories.
The final LRU and CLOCK matrices use one binary, in that order, with all four
cache sizes run serially for each policy. The default remains LRU. The optimized
LRU refreshes recency on sampled hits; it is not the baseline's every-hit LRU.

## Results

Median checked SELECTs/s from three 30-second samples per configuration:

| Decoded cache | Original baseline | Optimized LRU | CLOCK | CLOCK vs LRU |
|---|---:|---:|---:|---:|
| 100 MB | 428,383 | 820,350 | 810,507 | -1.2% |
| 1 GB | 488,465 | 874,495 | 863,915 | -1.2% |
| 10 GB | 906,187 | 1,652,845 | 1,787,150 | +8.1% |
| 20 GB | 935,891 | 1,497,809 | 1,504,238 | +0.4% |

CLOCK improves the 10 GB case by 8.1% with effectively the same hit rate. Its
median p99 upper bound also falls by 27.5%. The smaller caches show roughly 1.2%
lower throughput with CLOCK; the fully fitting 20 GB cache shows similar
throughput and latency. This one ordered uniform-read comparison supports
keeping CLOCK selectable, with LRU still the default. It does not establish a
general winner for other key distributions, scans or mixed writes.

Median p99 upper bounds and whole-process peak RSS, including warmup:

| Cache | Hit rate, both policies | LRU p99 µs | CLOCK p99 µs | LRU RSS GB | CLOCK RSS GB |
|---|---:|---:|---:|---:|---:|
| 100 MB | 0.92% | 1,376.3 | 1,441.8 | 0.526 | 0.526 |
| 1 GB | 9.25% | 1,114.1 | 1,114.1 | 3.246 | 3.268 |
| 10 GB | 92.46% | 655.4 | 475.1 | 29.310 | 29.299 |
| 20 GB | 100.00% | 311.3 | 311.3 | 32.110 | 32.294 |

At 100 MB, optimized LRU uses 0.526 GB peak RSS versus 4.559 GB in the original
baseline, and allocates about 8,575 bytes/query versus 15,411. At 20 GB, the
decoded cache retains only 10.815 GB, while process RSS also includes cache
metadata, decoder state, active results and Go heap headroom. A cache budget is
not a process memory limit. The 20 GB runs have lower p99 but lower throughput
than the 10 GB runs; the data does not show monotonic throughput with cache size.

The source changes improve LRU throughput by 1.60–1.92× over the original
baseline. CLOCK reaches 1.97× baseline throughput at 10 GB. Complete per-sample
latencies, allocations, commands, fingerprints and inventory evidence are in the
[saved comparison](benchmarks/select/2026-10-15/summary.json).

## Performance findings and fixes

1. **Redundant projection maps and repeated metadata growth.** Plain column
   SELECTs copied a decoded row into an expression input map and then another
   projected output map. Column metadata grew through several allocations.
   The executor now sizes metadata slices up front and keeps the owned decoded
   map for small unaliased, unqualified physical-table projections, pruning
   unselected fields only after filtering, sorting and locking. Joins, aliases,
   expressions, grouping and windows retain their general paths. Reference
   validation, residual predicates and transaction overlays remain intact.

2. **Decoder state multiplied by files and CPU count.** Each SSTable retained
   `GOMAXPROCS` Zstd decoder states. The baseline 100 MB diagnostic profile
   attributed roughly 1.5 GiB of sampled in-use heap to decoder buffers and
   scratch, despite the much smaller page-cache budget. DecodeAll-only decoders
   are now borrowed from a shared `sync.Pool`, one per active decoding operation.
   They start no streaming goroutines or external resources; GC can discard idle
   states. Decoded results retain their own allocations and legacy frame/window
   formats remain readable.

3. **Transient encoded buffers on every cache miss.** A compressed read allocated
   both encoded input and decoded output. Small encoded inputs now use bounded
   8 KiB scratch buffers after a reader observes compressed blocks. Raw-only
   files retain their allocation path. A raw page in a mixed-codec file is
   detached before scratch is returned. Large pages retain their previous path;
   checksum and decoding errors still propagate.

4. **Exclusive cache lookup and LRU updates.** The baseline block profile
   attributed about 157 aggregate seconds of sampled waiting to cache lookup
   mutexes during a 15-second diagnostic. Immutable readers now use shared
   shard locks, atomic statistics and sampled LRU refreshes. A refresh rechecks
   membership under the writer lock, including concurrent eviction and close.
   The 16 shards and their exact byte budgets are unchanged.

5. **Allocation under the insertion lock.** Once reads became faster, insertion
   became the main sampled cache wait. Steady-state eviction now reuses one
   victim's cache record and list node; additional victims are removed when a
   larger page requires them. This removes record/node allocation and possible
   GC assistance from that part of the writer critical section. Returned
   immutable readers keep their original data even when their old cache record
   is recycled.

6. **LRU writers for a completely fitting working set.** A 20 GB intermediate
   run still refreshed LRU lists, queuing readers behind writer locks even
   though its entire 10.815 GB decoded working set fit. Shards now start
   maintaining sampled recency after their first eviction. In separate
   15-second diagnostics, aggregate sampled query blocking dropped from about
   475 seconds to 4.5 seconds, and the p99 upper bound fell from about 0.754 ms
   to 0.221 ms. This primarily fixes blocking and latency tails; it does not
   imply a matching throughput increase. Full-cache CPU profiles still show
   typed-key/block decoding, owned-result allocation and shared atomic reader
   bookkeeping. Required snapshot and lifecycle protection remains enabled.

7. **CLOCK comparison.** `Options.BlockCachePolicy` now accepts `lru` or `clock`.
   CLOCK retains the same 16 independently locked shards and exact byte budgets.
   It sets a reference bit on point hits after admission pressure starts, then
   clears bits while advancing a shard-local hand to find a second-chance
   victim. Hits require no list writes or writer-lock upgrade. Scan lookups do
   not refresh reference bits. Both policies reuse victim records and skip
   global byte-counter updates when replacement leaves occupancy unchanged.
   Changing policies on reopen preserves the SSTables and logical data.
   In the separate 15-second 10 GB diagnostics, sampled cache blocking totals
   627 aggregate seconds for LRU over 20.77 million queries versus 539 seconds
   for CLOCK over 24.52 million queries: approximately 30.2 versus 22.0 µs of
   sampled waiting per query. These sums include concurrently waiting workers,
   not elapsed wall time. CLOCK removes hit-update writer waits; admission
   still needs exclusive shard locks and remains a cost under cache pressure.
   See the retained [LRU](benchmarks/select/2026-10-15/profile-policy-lru-10000000000-block.txt)
   and [CLOCK](benchmarks/select/2026-10-15/profile-policy-clock-10000000000-block.txt)
   block profiles. Their profiled throughput is excluded from the results.

Profiles are diagnostic and add overhead; none of their throughput values are
used in the result table. Final policy profiles run serially after both matrices
and use separate output paths. Intermediate optimization matrices are retained
as diagnostic artifacts and excluded from the final comparison.

## Validation and reproduction

Permanent tests cover 100 concurrent prepared readers, BLOB/map/column metadata
ownership, residual predicates, hidden sorting fields, aliases, private writes
and rollback. Storage tests cover cache counters and budgets, sampled touches,
multi-victim eviction, immutable reader lifetime during record reuse, concurrent
close, mixed raw/compressed pages, malformed frames, checksums and reuse after
errors. Tests create database and file artifacts with `t.TempDir()`.
CLOCK tests additionally cover second chances, scan reads, variable-size
multi-victim eviction, concurrent byte accounting and close, invalid policy
rejection before database creation, and policy changes on reopen. The checked
benchmark harness's small seed/reuse test runs both policies.

The root module is also checked with Go 1.22.2 using `go build -o slopdb .`,
`go test ./... -count=1 -timeout 10m`,
`go test -race ./... -count=1 -timeout 10m`, `go vet ./...` and
`git diff --check`. Timings are not test thresholds.

See the [benchmark command documentation](../cmd/select-bench/README.md).
Raw reports, profiles, logs, resources and full inventories remain on `test`
under `/home/ubuntu/slopdb-bench/select-20261015/`; use `before/`, `policy-lru/`
and `policy-clock/`. The earlier `final/` is the completed LRU run before the
selectable-policy comparison.
The reusable database remains under the user's home directory.
