# Database page-size comparisons, 2026-10-12

SlopDB’s fully rewritten 64 KiB fixtures use **20.3% less TPC-C
storage** and **20.7% less TPC-H storage** than its fresh 4 KiB controls.
These are the page-matched-window results requested in the latest refresh.

Fresh SlopDB fixtures match the Zstd encoder window to each configured block
target: 4 KiB, 16 KiB, 64 KiB or 1 MiB. Final sizes are measured only after
VACUUM atomically merges and rewrites every SSTable, including existing L6 files,
into L6. Every reopened report proves all L0–L5 file/byte counts are zero and
records `full_bottom_level_rewrite`. L6 can contain several output files.
Closed pre-maintenance sizes remain diagnostic inventories.

The datasets retain the original source hashes: 10,518,120 TPC-C rows across
256 warehouses (a replicated committed snapshot), and 1,731,999 TPC-H SF=0.2
rows with eight tables, fourteen indexes, ANALYZE and the Q15 view. Loading uses
4,096-row BulkInsert transactions and up to 256 rows per prepared INSERT.
WAL remains enabled with WAL fsync disabled; SSTables and MANIFEST remain synced.

All eight fresh SlopDB cases measure loading/storage only. No TPC-H query pack or
TPC-C throughput benchmark runs in this refresh, including at 1 MiB. TPC-C checks
every row and column before/after maintenance; TPC-H checks persisted table
counts against hash-verified source inputs, without full-column verification.
Earlier timing results below used an 8 MiB window and are historical evidence.

## Current storage comparison

SlopDB rows below are fresh matched-window/full-rewrite results. SQLite/DuckDB
rows are the explicitly reused previous native measurements. SQLite and DuckDB
reject 1 MiB pages; those requests remain unsupported.

### TPC-C replicated snapshot

| Engine | Page/block target | Reported load s | Closed diagnostic MiB | Final maintained MiB | Closed allocated MiB | Final allocated MiB |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| slopdb | 4 KiB (default) | 182.9 | 191.79 | 182.74 | 192.11 | 183.05 |
| slopdb | 16 KiB | 185.0 | 158.26 | 149.03 | 158.72 | 149.47 |
| slopdb | 64 KiB | 179.2 | 154.80 | 145.66 | 155.32 | 146.13 |
| slopdb | 1 MiB | 170.7 | 159.44 | 150.14 | 159.82 | 150.54 |
| sqlite | 4 KiB (default) | 47.0 | 461.68 | 438.36 | 461.69 | 438.36 |
| sqlite | 16 KiB | 45.1 | 460.02 | 436.30 | 460.02 | 436.30 |
| sqlite | 64 KiB | 47.6 | 460.38 | 436.69 | 460.38 | 436.70 |
| sqlite | 1 MiB | Unsupported | — | — | — | — |
| duckdb | 16 KiB | 30.2 | 571.78 | 571.78 | 571.79 | 571.79 |
| duckdb | 64 KiB | 20.7 | 265.95 | 265.95 | 265.96 | 265.96 |
| duckdb | 256 KiB (default) | 22.0 | 272.26 | 272.26 | 272.27 | 272.27 |
| duckdb | 1 MiB | Unsupported | — | — | — | — |

### TPC-H SF=0.2

| Engine | Page/block target | Reported load s | Closed diagnostic MiB | Final maintained MiB | Closed allocated MiB | Final allocated MiB |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| slopdb | 4 KiB (default) | 100.6 | 269.67 | 214.54 | 269.92 | 214.80 |
| slopdb | 16 KiB | 96.8 | 238.47 | 181.96 | 238.75 | 182.23 |
| slopdb | 64 KiB | 94.0 | 226.83 | 170.14 | 227.07 | 170.38 |
| slopdb | 1 MiB | 91.8 | 220.70 | 163.77 | 220.94 | 164.01 |
| sqlite | 4 KiB (default) | 17.3 | 321.10 | 318.91 | 321.11 | 318.92 |
| sqlite | 16 KiB | 16.9 | 317.86 | 315.66 | 317.87 | 315.66 |
| sqlite | 64 KiB | 16.6 | 318.44 | 316.19 | 318.45 | 316.20 |
| sqlite | 1 MiB | Unsupported | — | — | — | — |
| duckdb | 16 KiB | 18.1 | 272.07 | 272.07 | 272.08 | 272.08 |
| duckdb | 64 KiB | 17.2 | 233.95 | 233.95 | 233.96 | 233.96 |
| duckdb | 256 KiB (default) | 16.8 | 203.76 | 203.76 | 203.77 | 203.77 |
| duckdb | 1 MiB | Unsupported | — | — | — | — |

TPC-C’s final 1 MiB footprint is **150.14 MiB**, versus
**145.66 MiB** at 64 KiB. Both now use the full bottom-level
rewrite, with all L0–L5 files absent. This difference therefore remains after
eliminating promotion-only maintenance; this run does not isolate the contribution
of block layout, prefix encoding or codec decisions. Larger targets do not
guarantee smaller files.

Reported TPC-C snapshot load time includes DDL/data/index loading and close/reopen
for SlopDB, and checkpoint/close for native engines; full-value verification is
separate. SlopDB TPC-H load time includes indexes and ANALYZE but excludes
close/reopen and count checks. Native loaders have their own timing boundaries.
These are recorded loading durations rather than identical bulk-throughput scores.
Each size is one fresh load; sequence bytes and output file boundaries can vary.

The current storage harness is `c6003992f379e81f4ef07d7fa4c459a09ac75573`; exact compiled
Go revisions, executable SHA256 hashes and `go version -m` output are retained in
the [fresh manifest](benchmarks/storage/pages-matched-window/storage-comparison.json).
SQLite/DuckDB storage rows are reused unchanged from the previous archive at
harness revision `9a6e9de0b8c4d84a7652b1700d59e7e2054238d4` (Python SQLite 3.45.1 and
DuckDB 1.5.6). They were loaded from identical source hashes. This refresh does
not rerun those native engines or the historical query/transaction benchmarks.
Their native VACUUM/CHECKPOINT operations differ from SlopDB’s full L6 rewrite.

[Fresh raw cells and hashes](benchmarks/storage/pages-matched-window/manifest.json)
preserve both diagnostic and final inventories. The previous
[8 MiB-window storage archive](benchmarks/storage/pages/storage-comparison.json)
remains intact; its SlopDB sizes are superseded and are excluded from current tables.

## Historical native footprint diagnostic

DuckDB’s 16 KiB TPC-C file is **571.78 MiB**, versus **265.95 MiB at 64 KiB**.
Its reported 36,593 blocks include 36,586 used blocks and only seven free
blocks: **0.109 MiB** of free capacity. At 64 KiB all 4,255 blocks are used;
at the 256 KiB control all 1,089 are used. VACUUM + CHECKPOINT left these
inventories unchanged. The small-block penalty is therefore occupied block
allocation, not a large freelist waiting to be reclaimed. Used blocks include
indexes, metadata and internal slack, and are not equivalent to raw row payload.
Per-column/index packing was not captured, so its precise cause remains open.

## Historical TPC-H execution with 8 MiB SlopDB windows

| Engine | Page/block target | Sum of 22 query medians, s | Relative to engine default |
| --- | --- | ---: | ---: |
| slopdb | 4 KiB (default) | 39.176 | 1.000× elapsed |
| slopdb | 16 KiB | 58.755 | 1.500× elapsed |
| slopdb | 64 KiB | 140.654 | 3.590× elapsed |
| slopdb | 1 MiB | Not run: load-only | — |
| sqlite | 4 KiB (default) | 7.005 | 1.000× elapsed |
| sqlite | 16 KiB | 7.643 | 1.091× elapsed |
| sqlite | 64 KiB | 11.455 | 1.635× elapsed |
| sqlite | 1 MiB | Unsupported | — |
| duckdb | 16 KiB | 1.057 | 1.480× elapsed |
| duckdb | 64 KiB | 0.771 | 1.080× elapsed |
| duckdb | 256 KiB (default) | 0.714 | 1.000× elapsed |
| duckdb | 1 MiB | Unsupported | — |

Relative elapsed time below 1× is faster than that engine’s default.
[Every query median and sample](benchmarks/storage/pages/query-medians.json) is retained.

### SlopDB lookup-heavy query regressions

| Query | 4 KiB seconds | 64 KiB seconds | 4 KiB allocated MiB | 64 KiB allocated MiB |
| --- | ---: | ---: | ---: | ---: |
| Q05 | 6.819 | 31.168 | 1368.4 | 9994.9 |
| Q06 | 2.057 | 16.420 | 291.7 | 5249.5 |
| Q07 | 3.189 | 17.344 | 646.4 | 5748.4 |
| Q12 | 2.426 | 17.104 | 388.9 | 5472.6 |

These queries use secondary indexes that do not contain all the requested
lineitem values: Q5/Q7 use supplier access, Q6 ship-date access, and Q12
receipt-date access. Their saved EXPLAIN plans retain indexed access across
page sizes for Q5/Q6/Q12; Q7’s outer EXPLAIN hides its derived join. The
measured samples show increased elapsed time and cumulative allocation. The
allocation deltas cover the whole Go process during execution, including
background work, and do not measure cache hits or physical device I/O.

The code provides a plausible mechanism. On a decoded-cache miss, the
[SSTable reader](../storage/sstable.go) allocates the stored block and decodes
the entire compressed frame with `DecodeAll`, including rows unrelated to a
point lookup. The fixed 128 MiB [block cache](../storage/block_cache.go) has
sixteen 8 MiB shards; larger decoded blocks leave room for fewer blocks in
each shard. The [access cost model](../sql/statistics.go) weights noncovering
lookups by row count with fixed factors of three through six; the
[join probe cost](../sql/join_plan.go) uses `4 + 3 * fanout`. Neither model
scales those costs with `PageSize`. It can therefore select similar lookup
paths while the bytes decoded per miss grow. These are code-based explanations;
the TPC-H reports contain no measured cache counters. Page-aware lookup costs,
covering projections and scan/probe choices are candidates for further work.

## Historical TPC-C execution with 8 MiB SlopDB windows

| Engine | Page/block target | Committed TPS | p95 latency, ms | p99 latency, ms | Failed transactions | Status | Retries | Shutdown cancellations |
| --- | --- | ---: | ---: | ---: | ---: | --- | ---: | ---: |
| slopdb | 4 KiB (default) | 2,114.4 | 15.285 | 24.341 | 0 | ok | 0 | 10 |
| slopdb | 16 KiB | 2,200.6 | 14.542 | 23.082 | 0 | ok | 0 | 10 |
| slopdb | 64 KiB | 2,238.9 | 14.072 | 22.761 | 0 | ok | 0 | 10 |
| slopdb | 1 MiB | Not run: load-only | — | — | — | skipped | — | — |
| sqlite | 4 KiB (default) | 2,579.6 | 1.193 | 128.831 | 0 | ok | 1142 | 10 |
| sqlite | 16 KiB | 1,575.1 | 1.375 | 179.604 | 48 | failed | 1365 | 9 |
| sqlite | 64 KiB | 1,039.3 | 1.863 | 252.096 | 110 | failed | 1463 | 9 |
| sqlite | 1 MiB | Unsupported | — | — | — | unsupported | — | — |
| duckdb | 16 KiB | 219.0 | 136.807 | 220.478 | 356 | failed | 9902 | 10 |
| duckdb | 64 KiB | 223.6 | 134.990 | 214.905 | 374 | failed | 10056 | 10 |
| duckdb | 256 KiB (default) | 224.1 | 133.656 | 209.788 | 347 | failed | 9732 | 9 |
| duckdb | 1 MiB | Unsupported | — | — | — | unsupported | — | — |

Committed TPS is retained for completed runs alongside their transaction failure
counts and status. SQLite contention can exhaust the common five-retry cap under
its 250 ms writer-lock budget: those cells remain failed rather than clean
throughput results, even when all thirty consistency assertions pass. Settings
and failure classification were kept unchanged; failed reports are retained.
DuckDB’s failed cells similarly exhausted the retry cap on optimistic write
conflicts. Mean/p95/p99 latency samples contain only successful logical
transactions, including retries before a successful commit. Failed, exhausted
and canceled transactions are excluded, so SQLite’s low successful p95 does
not describe the lock waits of its failed transactions.

## Superseded exploratory runs

The earlier page matrix at revision `24cd6a2f9fe7127bf3a3947b404f58f5482ffd6b`
was stopped before completion when the workload requirements changed. Its partial
results are superseded: they preceded the bulk-loading and large-window compression
fixes and are not used for the final tables. No interrupted sample is a successful
timing.

## Historical execution settings, validation and limits

The following settings describe the earlier 8 MiB-window execution/storage archive.
Those jobs ran serially on the same machine. Load, preparation, validation,
maintenance and close/reopen are outside transaction/query timing. OS caches
are not reset. TPC-H uses one measured sample per query after one warmup at benchmarked
sizes; its total sums twenty-two medians, each a single sample. SlopDB 1 MiB is
explicitly excluded from query timing and query-answer checks. The TPC-H runner’s
`-load-only` records the skip explicitly; its Q15 view is schema setup. TPC-C has one run per
cell, which is descriptive evidence rather than statistical proof. Each storage
size also comes from one fresh load. Equal SQL values do not imply identical
internal sequence bytes or compaction/file boundaries, so footprint deltas are
observed outcomes rather than a decomposition of codec efficiency alone.

WAL remains enabled and WAL fsync is disabled. SlopDB and DuckDB retain
checkpoint and data-file synchronization; DuckDB bypass uses a Linux preload
shim restricted to the exact benchmark WAL file. SQLite uses synchronous=OFF,
which also disables checkpoint synchronization, so durability costs differ.
SlopDB uses READ COMMITTED with row locks, SQLite serializes writers with
BEGIN IMMEDIATE, and DuckDB uses snapshot isolation with optimistic conflict
retries. These transaction and contention semantics also affect throughput.

SlopDB keeps its 4 MiB memtable, 128 MiB decoded-block cache and auto compression
fixed. SQLite uses a 128 MiB cache budget per connection and an approximately
64 MiB automatic-checkpoint threshold, expressed in pages for each size. Timed
TPC-C can open eleven SQLite connections; its aggregate cache allowance differs
from SlopDB’s shared 128 MiB cache. DuckDB uses one execution thread and a 1 GB
process memory limit. Budgets stay constant within each engine across page sizes;
total memory allowances are not equal across engines. SlopDB’s default 64 MiB
operator budget is a soft working-set estimate, not a process memory limit;
results and some intermediate shapes still materialize. Page sizes describe
different physical units: SlopDB's uncompressed variable-block target, SQLite's
fixed B-tree page and DuckDB's persistent block. SlopDB blocks are compressed
and are not padded to the requested target.

Sizes sum all regular database files after clean close, including indexes,
catalog metadata and locks. Allocated totals use filesystem block counts,
including directory blocks. Input data, reports,
profiles and query spill files are outside the database directories. Large task-created
databases were removed only after both validation phases and their per-file
inventories were saved; source inputs and earlier databases were preserved. MiB means
1,048,576 bytes. Maintenance is SlopDB VACUUM, SQLite VACUUM, or DuckDB VACUUM
followed by CHECKPOINT; these operations have different semantics. DuckDB
VACUUM itself does not reclaim file space.

Every TPC-C storage value is checked after reopening and after maintenance.
TPC-H checks all twenty-two complete answers against independently archived
DuckDB results at benchmarked sizes, then checks them again after maintenance.
The 1 MiB TPC-H load-only case verifies persisted row counts/schema and
hash-verified source inputs; it does not verify every stored column or execute
query-answer checks.
Timed TPC-C requires all fifteen consistency assertions immediately after execution and after
reopening. Expected rollbacks, shutdown cancellations and retries remain
separate from unexpected failures.

See [page-size support](page-sizes.md), [measurement protocol](page-size-protocol.md)
and [portable comparison runners](../cmd/tpcc/compare/README.md).

The storage harness used revision `9a6e9de0b8c4d84a7652b1700d59e7e2054238d4`; the timed TPC-C harness used
`9a6e9de0b8c4d84a7652b1700d59e7e2054238d4`. Compiled Go engines/runners used
`4e55bd60114e679dcb4b96adb12790e416d1cca6`; the later harness revision changed
Python null-report handling only. Python storage SQLite was 3.45.1;
DuckDB was 1.5.6. Native TPC-C used SQLite 3.51.3 and DuckDB 1.5.6, with
Go 1.24.0; SlopDB runners used Go 1.22.2. Build metadata and versions are in
the [raw throughput manifest](benchmarks/storage/pages/tpcc-comparison.json).
[Raw storage cells](benchmarks/storage/pages/storage-comparison.json),
[artifact hashes](benchmarks/storage/pages/manifest.json) and complete query reports
preserve exact configurations, input hashes, per-file inventories and validation.
