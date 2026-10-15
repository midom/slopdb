# Scaling small-row SELECTs beyond the initial cache fixes

This follows the [initial LRU/CLOCK comparison](small-row-select-performance.md).
The [next optimization report](small-row-select-final-optimizations.md) covers
subsequent point-index decoding, private buffers, cache admissions and catalog
gate changes; the measurements below retain their original source fingerprints.
The baseline here is that already optimized implementation, with 16 cache
shards. It is not the original `940d63d` baseline. Measurements use the same
39,062,500-row, 10,000,000,000-logical-byte seed on `test`, 100 independent
sessions and checked prepared full-row primary-key SELECTs. The seed remains
at `/home/ubuntu/slopdb-small-row-10gb`.

The host has 60 ARM Neoverse-N1 cores and approximately 58 GiB RAM. An independent
six-second allocation-free CPU control consumed 59.96 CPU cores. The inspected
cgroups had unlimited CPU quotas and no recorded throttling. Go uses all 60
execution slots; their being busy does not mean all time produces useful SQL
work. Per-sample user/system CPU seconds now distinguish useful throughput,
CPU consumption and wall-clock waiting.

## Results

Median checked SELECTs/s with 100 readers, default runtime settings, three
30-second samples per fresh process:

| Cache | Previous CLOCK | Current CLOCK | Gain | Current LRU |
|---|---:|---:|---:|---:|
| 100 MB | 810,507 | 1,290,306 | 59.2% | 1,283,087 |
| 1 GB | 863,915 | 1,494,712 | 73.0% | 1,392,311 |
| 10 GB | 1,787,150 | 2,527,890 | 41.4% | 2,532,439 |
| 20 GB | 1,504,238 | 2,262,230 | 50.4% | 2,244,249 |

CLOCK and LRU now have similar throughput in the 10 GB and fully fitting
20 GB cases. The code changes benefit both policies; the default remains LRU.

Per-sample CPU measurements and median CLOCK p99 upper bounds:

| Cache | Before CPU cores¹ | Current CPU cores | Current CPU µs/query | Before p99 µs | Current p99 µs |
|---|---:|---:|---:|---:|---:|
| 100 MB | 28.8 | 37.0 | 28.6 | 1,441.8 | 1,441.8 |
| 1 GB | 30.2 | 42.6 | 28.5 | 1,114.1 | 1,114.1 |
| 10 GB | 43.9 | 43.6 | 17.2 | 475.1 | 720.9 |
| 20 GB | 35.4 | 34.4 | 15.3 | 311.3 | 147.5 |

¹ Fresh exploratory controls: one 20-second sample at smaller caches, one
12-second sample at 20 GB. CPU measurements include validation and Go GC.
The larger cache gains throughput while using slightly fewer CPU seconds per
wall second; its CPU cost per query falls from about 22.8 to 15.2 µs. At 100 MB
and 1 GB, more independent work also raises CPU use. The 10 GB default-runtime
p99 gets worse despite higher throughput; this is a measured tradeoff, not an
improvement in every latency metric.

At 20 GB, allocations fall from about 3,640 to 2,608 bytes/query and p99 from
311 to 147 µs. More cache does not imply higher throughput: the fully fitting
case still allocates owned result maps, metadata and BLOBs and encounters
allocator/GC costs. Its lower p99 can be useful independently of throughput.

The separate fully fitting-cache scaling sweep gives:

| Readers | Before SELECTs/s | Current SELECTs/s | Before CPU cores | Current CPU cores |
|---|---:|---:|---:|---:|
| 1 | 76,378 | 103,767 | 1.3 | 1.1 |
| 8 | 544,257 | 755,058 | 8.7 | 8.6 |
| 16 | 1,043,336 | 1,480,202 | 17.0 | 17.1 |
| 32 | 1,553,043 | 2,150,348 | 28.6 | 27.6 |
| 60 | 1,590,151 | 2,256,217 | 35.1 | 33.8 |
| 100 | 1,550,046 | 2,299,330 | 35.4 | 34.3 |

At 100 readers this sweep is about 48% faster. Scaling from one to 100 readers
increases from about 20.3× to 22.2×. The curve still flattens above 32–60 readers;
it does not show linear scaling to all 60 cores. These shorter measurements
use different processes from the cache matrix.

With the same binary and explicit `GOGC=200 GOMEMLIMIT=46GiB`, three additional
30-second samples show the memory/runtime tradeoff:

| Cache | SELECTs/s | Gain over current default GC | CPU cores | Host CPU use | p99 µs | Peak RSS GB |
|---|---:|---:|---:|---:|---:|---:|
| 10 GB | 3,465,345 | 37.1% | 53.7 | 89.4% | 426.0 | 45.30 |
| 20 GB | 2,907,939 | 28.5% | 39.1 | 65.1% | 114.7 | 47.05 |

The default-runtime CLOCK peak RSS is about 30.04 GB at 10 GB cache and
31.61 GB at 20 GB cache. Extra GC headroom improves throughput and the 10 GB
latency tail, but requires considerably more resident memory. The 10 GB tuned
run reaches 89% of the available CPU capacity; the library itself does not
change application-global runtime settings.

Complete samples, commands, fingerprints and integrity evidence are in the
[saved comparison](benchmarks/select-scaling/2026-10-15/summary.json).

A subsequent [GOGC=200-only matrix](small-row-select-gogc-200.md) reruns all four cache sizes with `GOMEMLIMIT` unset and records the isolated GC-setting effect.

## Changes and why they help

1. **More independent cache admissions.** Increase the decoded-block cache from
   16 to 128 padded shards, keeping the total byte budget unchanged. Miss
   insertions and LRU refreshes no longer queue so many readers behind the same
   writer lock. CLOCK and LRU retain their existing eviction rules. Capacity
   remains partitioned: a decoded allocation larger than its shard budget
   bypasses admission, and skew can leave some shards underused. These runs use
   4 KiB physical pages, which fit comfortably in every tested shard budget.

2. **Distributed read-view references.** SQL point reads use their stable reader
   IDs to choose one of 64 registration locks and padded reference counters.
   This replaces shared `viewMu` reader-count updates and global view-reference
   updates on each point read. Publication excludes all registrations in fixed
   order before retiring a view. A retired view releases its file pins exactly
   once, after both legacy and distributed references reach zero. Disk reads
   still hold no publication lock, and returned bytes remain caller-owned.

3. **Remove an unnecessary snapshot gate.** Snapshot registration already holds
   the bucket lock used by the compaction cutoff walker. Read the published
   watermark and register under that lock, without also updating global
   `viewMu` reader state. The cutoff captures its watermark before walking
   buckets: a later registration either appears in the walk or reads a sequence
   at least as new as the cutoff. Legacy snapshot registration uses the same
   argument under `snapshotMu`. Version retention stays enabled.

4. **Distribute shutdown bookkeeping.** Stable session IDs select one of 128
   padded execution-reader counters. Database shutdown publishes writer intent
   and checks every counter before entering. Readers still register before
   rechecking writer intent, and cancellable slow waits retain writer preference.
   A lock waiter releases and reacquires its original counter. The legacy gate
   remains available for metadata/maintenance calls. The preceding diagnostic
   attributed about 7.5% of sampled CPU to the old global reader increments and
   decrements after the storage-view bottleneck had been removed.

5. **One cancellation context for the common case.** Calls without a configured
   query timeout share one cause-aware cancellation context for session shutdown
   and transaction aborts. This avoids a second nested cancel context, its child
   map and extra Done channel. Explicit deadlines keep their existing timeout
   path. Parent cancellation, abort causes, publication rollback and streaming
   Rows lifetimes retain their normal behavior.

6. **Compile simple point projections.** Eligible prepared physical-table reads
   reuse validated projection metadata rather than allocate a copied SELECT AST,
   expression projection and general key-batch scaffolding on every execution.
   Each result still receives its own column metadata, row map and BLOB data.
   Runtime bindings, residual predicates, LIMIT/OFFSET, transaction overlays and
   normal statement snapshots remain active. Qualified/aliased expressions,
   joins, ordering, grouping, locking reads and query instrumentation retain the
   general path. Catalog/statistics changes invalidate the compiled recipe.

7. **Reuse private point-lookup scratch.** Borrow index/data key buffers from a
   `sync.Pool`. These buffers never back returned values. Discard oversized
   scratch rather than retain arbitrary large user keys in the pool.

## Measurement and runtime headroom

Additional 20 GB, 100-reader controls kept the same binary and reduced only
`GOMAXPROCS`. With default GC, 16 execution slots reached 1.23M SELECTs/s and
32 reached 1.94M, below the 60-slot matrix's 2.26M. With extra GC headroom,
32 slots reached 2.34M versus 2.91M at 60. These exploratory 20-second controls
support retaining all 60 slots; restricting concurrency is not the scaling fix.

The default-runtime cache matrices retain three consecutive 30-second samples
per fresh process and the earlier stride-eight checked warmup. CLOCK runs first,
then LRU. A separate CLOCK scaling curve uses 1, 8, 16, 32, 60 and 100 readers,
with two 15-second samples per process at the fully fitting 20 GB cache. The
exploratory pre-change curve used one 12-second sample per reader count. These
are descriptive observations from one fixture and ordered runs, not independent
replications or a statistical significance claim.

The database/CPU settings, Go 1.26.0 compiler, 4 KiB pages, deterministic key
seeds and full-result validation stay fixed. Linux filesystem caching remains
enabled and is never reset. This is not a cold-device random-I/O test. Every
matrix hashes all SSTables before and after, rejects query failures, changing
sequence numbers or excess cache occupancy, and matches the original seed's
full inventory.

CPU profiles after the lock changes show allocation and runtime sweeping as a
remaining limit, even while Go reports no idle execution slots. The library does
not change process-global GC settings. Separate runs explicitly set
`GOGC=200 GOMEMLIMIT=46GiB` on this 58 GiB host to measure additional GC headroom.
These settings can increase resident memory substantially; their results are
reported separately from code-only/default-runtime gains. Peak RSS includes
warmup, decoded cache metadata, active results and Go heap headroom, not just the
configured decoded-page budget.

Separate 15-second diagnostics show about 4.77 aggregate seconds of sampled
query mutex waiting over 21.45 million fully cached baseline queries, versus
1.47 seconds over 31.71 million queries in the frozen final build. Shared atomic
cache-line traffic is visible in CPU profiles rather than these blocked-time
totals. At 10 GB, cache-admission locks still block readers: final diagnostics
attribute about 386 aggregate seconds over 36.37 million queries with default
GC, and 377 seconds over 47.52 million with extra headroom. Concurrent waiting
times are summed and must not be mistaken for elapsed wall time.

The retained [CPU](benchmarks/select-scaling/2026-10-15/profile-final-20gb-cpu.txt)
and [blocking](benchmarks/select-scaling/2026-10-15/profile-final-20gb-block.txt)
reports show the remaining costs. Diagnostic throughput is excluded from the
result tables. Heap allocation profiles are cumulative from process startup,
including warmup; per-query allocation comparisons use timed-sample counters.

## Correctness and reproduction

Permanent tests cover 256 sharded lifecycle readers overlapping exclusive
writers, writer preference and cancellation across colliding counters, shutdown,
lock waits and online-index publication. Storage tests retain old views across
flush/compaction, mix legacy and distributed references, check last-reference
file closure and exercise 100 concurrent readers with colliding IDs during
atomic two-key writes and compaction. Snapshot retention and owned values are
checked together.

Prepared projection tests exercise repeated and coercing bindings, NULL/missing
keys, residual predicates, pagination, BLOB/map/metadata ownership, invalid
aggregates on empty answers, private writes, rollback, ALTER and table-name reuse.
An internal regression confirms that the compiled projection is actually reused.
All database and file artifacts use `t.TempDir()`.

The required checks run locally with Go 1.22.2: root build, complete ordinary
and race suites, `go vet ./...` and `git diff --check`. An initial verification
attempt exhausted local disk space; old task-generated build-cache outputs were
removed and local report copies compressed before rerunning the checks. Raw
benchmark reports on `test` were retained.

See the [benchmark documentation](../cmd/select-bench/README.md) for both runners.
Raw reports, resource counters, profiles, commands and source fingerprints are
under `/home/ubuntu/slopdb-bench/select-scaling-20261015/` on `test`.
That directory also contains the human-readable `report.md` and a complete
`source-final.tar.gz` snapshot with permanent tests, plus SHA256 metadata.
