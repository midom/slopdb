# Storage comparisons above 100 MiB

The latest [canonical SQL comparison](canonical-storage-comparison.md) reloads
all three engines from exactly the same frozen SQL per dataset. Every supported
cell passes full typed value fingerprints after reopen and after maintenance.
Final SlopDB size requires matching compression windows and every SSTable in L6.
These tables supersede prior loaders and reused native measurements.

| Dataset | Rows | Logical payload MiB | Canonical SQL MiB | Verified per supported cell |
| --- | ---: | ---: | ---: | --- |
| TPC-C | 10,518,120 | 680.69 | 306.49 | Every column, twice |
| TPC-H | 1,731,999 | 225.00 | 235.64 | Every column, twice |

## Current canonical TPC-C final storage

| Engine | Page/block | Final files MiB | Final allocated MiB | Load s (diagnostic) |
| --- | --- | ---: | ---: | ---: |
| SlopDB | 4 KiB (default) | 182.50 | 182.84 | 244.8 |
| SlopDB | 16 KiB | 148.83 | 149.25 | 243.3 |
| SlopDB | 64 KiB | 145.44 | 145.91 | 240.9 |
| SlopDB | 1 MiB | 150.04 | 150.41 | 241.1 |
| SQLite | 4 KiB (default) | 449.99 | 450.00 | 155.0 |
| SQLite | 16 KiB | 447.91 | 447.92 | 150.3 |
| SQLite | 64 KiB | 448.50 | 448.52 | 149.3 |
| SQLite | 1 MiB | Unsupported | — | — |
| DuckDB | 16 KiB | 574.11 | 574.12 | 719.3 |
| DuckDB | 64 KiB | 268.01 | 268.02 | 782.2 |
| DuckDB | 256 KiB (default) | 277.76 | 277.77 | 772.2 |
| DuckDB | 1 MiB | Unsupported | — | — |

## Current canonical TPC-H final storage

| Engine | Page/block | Final files MiB | Final allocated MiB | Load s (diagnostic) |
| --- | --- | ---: | ---: | ---: |
| SlopDB | 4 KiB (default) | 214.51 | 214.78 | 107.4 |
| SlopDB | 16 KiB | 181.95 | 182.23 | 103.6 |
| SlopDB | 64 KiB | 170.12 | 170.39 | 102.5 |
| SlopDB | 1 MiB | 163.71 | 163.98 | 101.6 |
| SQLite | 4 KiB (default) | 324.05 | 324.05 | 81.4 |
| SQLite | 16 KiB | 320.84 | 320.86 | 80.6 |
| SQLite | 64 KiB | 321.75 | 321.76 | 79.9 |
| SQLite | 1 MiB | Unsupported | — | — |
| DuckDB | 16 KiB | 271.65 | 271.66 | 232.8 |
| DuckDB | 64 KiB | 232.89 | 232.89 | 262.5 |
| DuckDB | 256 KiB (default) | 200.26 | 200.27 | 235.8 |
| DuckDB | 1 MiB | Unsupported | — | — |

[Current methods and raw evidence](canonical-storage-comparison.md) describe the
identical schema/SQL, exact equality gates and each engine's maintenance/durability.
All following measurements are historical and do not establish the current
canonical SQL comparison.

## Historical matched-window storage measurements, 2026-10-12

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

SlopDB’s maintained 64 KiB footprints are 20.3% / 20.7% smaller than
its fresh TPC-C / TPC-H 4 KiB controls. Native engine rows below are unchanged
previous measurements, identified separately in the provenance paragraph.

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

The current storage harness is `c6003992f379e81f4ef07d7fa4c459a09ac75573`; exact compiled
Go revisions, executable SHA256 hashes and `go version -m` output are retained in
the [fresh manifest](benchmarks/storage/pages-matched-window/storage-comparison.json).
SQLite/DuckDB storage rows are reused unchanged from the previous archive at
harness revision `9a6e9de0b8c4d84a7652b1700d59e7e2054238d4` (Python SQLite 3.45.1 and
DuckDB 1.5.6). They were loaded from identical source hashes. This refresh does
not rerun those native engines or the historical query/transaction benchmarks.
Their native VACUUM/CHECKPOINT operations differ from SlopDB’s full L6 rewrite.

See the [complete page-size comparison](page-size-comparison.md) for loading
boundaries, historical performance figures, durability differences and the
[fresh report archive](benchmarks/storage/pages-matched-window/manifest.json).

## Historical default-size comparison, 2026-10-11

All three engines were measured on larger, identical datasets for both workloads.
Every database exceeds 100 MiB before and after maintenance. After maintenance,
SlopDB uses less space than SQLite and DuckDB for the replicated TPC-C snapshot.
For TPC-H SF=0.2, SlopDB is about 4.1% larger than DuckDB; SQLite is larger than
both. These larger runs extend the [small-dataset comparison](storage-comparison.md).

## Database-file sizes

MiB means 1,048,576 bytes. Sizes include all database files, indexes, metadata and
lock files, after clean close. Input data, reports, executables and profiles are
outside the database directories. No WAL or SHM files remain in these measurements.

| Dataset and state | SlopDB MiB | SQLite MiB | DuckDB MiB |
| --- | ---: | ---: | ---: |
| TPC-C snapshot, closed | 182.64 | 461.68 | 278.51 |
| TPC-C snapshot, after maintenance | 176.17 | 438.36 | 278.51 |
| TPC-H SF=0.2, closed | 214.19 | 321.10 | 203.51 |
| TPC-H SF=0.2, after maintenance | 211.76 | 318.91 | 203.51 |

Maintenance used SlopDB VACUUM, SQLite VACUUM and DuckDB VACUUM followed by
CHECKPOINT. The before and after states were recorded serially on the fresh
databases created for this comparison. Older benchmark databases were preserved.

For the TPC-C snapshot after maintenance, SlopDB is 59.8% smaller than SQLite
and 36.7% smaller than DuckDB. The modest SlopDB compaction reductions at this
scale contrast with the small tests: most data was already compressed through
normal background compaction. Default compression is auto: uncompressed L0,
zstd in later levels. DuckDB file sizes did not change after maintenance.

## Datasets and validation

TPC-H uses genuine DBGEN SF=0.2 inputs: **1,731,999 rows**, including 1,199,969
lineitems and 300,000 orders. All engines load identical hash-verified inputs,
the same eight tables, primary keys, fourteen secondary indexes, ANALYZE and
Q15 view. SQLite uses an on-disk database. SlopDB and the DuckDB Go driver each
completed all 22 queries without failures or timeouts. SQLite results matched
both engines, and all answers also matched native TPC-H qualification results.
All 22 queries were checked again for every engine after maintenance.

TPC-C uses **10,518,120 rows**, including 8,216,320 order lines, 859,520 orders
and 812,544 history rows. One committed benchmark snapshot was replicated 128
times into 256 disjoint warehouses. Warehouse references and history IDs were
shifted consistently; the 1,000-item catalog is shared. All three engines use
the same values, declared primary keys and customer/order index. Every value
was compared after reopening and again after maintenance. Repeated snapshot
values can affect compression; this is an equal-data storage comparison rather
than a new timed transaction throughput run.

The large SlopDB validation pass exposed a planning gap for simple SELECT
queries using BETWEEN on composite primary keys. Equivalent >= / <= predicates,
primary-key ordering and a limit of expected rows plus one enabled bounded
validation reads. Exact table counts and every expected value were still checked.
This adjustment applied to the validation helper during the storage measurements.
The 22 TPC-H query definitions are the existing repository query pack. No
production SQL or storage code was changed for those measurements.

The planning gap is now fixed: the shared tuple planner extracts inclusive
integer bounds from BETWEEN, including signed literals and prepared parameters,
for composite primary keys and secondary indexes. The original predicate remains
a residual check to preserve mixed-type and NULL behavior when only one bound
is usable. NOT BETWEEN and disjunctive predicates retain their existing paths.
On this database, selecting 20 order lines from warehouses 129–130 took
11,366.934 / 11,103.007 ms before the fix and 1.149 / 0.140 ms after it. Answers
were identical. These are first/repeated execution samples from separate CLI
processes; OS caches were not reset. See the [query measurement](benchmarks/storage/between-fix.json).

## Settings and evidence

SlopDB used `f45ea982792cd25c94478b76dd4bf20ac83bb8eb`, Go 1.22.2, a 4 MiB memtable, 4 KiB blocks and
default auto compression. DuckDB is 1.5.6; its TPC-H Go runner uses one execution
thread and a 1 GB memory limit. New SQLite files use Python SQLite 3.45.1.
WAL remains enabled and WAL fsync is disabled during new loads. DuckDB uses the
existing Linux preload shim restricted to each benchmark's exact WAL file;
other checkpoint/file syncing stays enabled. Files were synced after close
before recording final filesystem allocation.

Load, validation and maintenance jobs can overlap. Saved query timings are
diagnostic; this run establishes storage footprints and correctness rather
than a controlled throughput comparison. Database directories and input hashes
are recorded in the [raw size report](benchmarks/storage/large-comparison.json),
along with exact per-file apparent bytes, allocated bytes, row counts, SSTable
levels, page/block statistics and validation status. Allocated directory totals
were independently checked using `du -s -B1`.

Full query reports: [SlopDB](benchmarks/storage/large-tpch-slopdb.json) and
[DuckDB](benchmarks/storage/large-tpch-duckdb.json). See the
[TPC-H runner](../cmd/tpch/README.md) and
[TPC-C comparison runner](../cmd/tpcc/compare/README.md) for schemas and workload
definitions. Large inputs and databases remain outside the repository under
the paths recorded in the reports.
