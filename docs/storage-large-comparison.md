# Storage comparisons above 100 MiB

The [page-size comparison](page-size-comparison.md) extends these measurements
with fresh equal-data fixtures at multiple page sizes, physical allocation and
controlled TPC-C/TPC-H timings. The dated results below remain historical.

## Page-size storage measurements, 2026-10-12

These fresh fixtures preserve the same source hashes and row counts as the
historical large comparison. Every TPC-C value and, at benchmarked sizes, all
twenty-two TPC-H answers were checked after reopening and again after maintenance.
SlopDB 1 MiB is bulk-load/storage-only: TPC-H checks persisted row counts/schema
and hash-verified inputs, with no query-answer or full-column verification.
TPC-C storage still checks every value at 1 MiB. Page targets have
different meanings across engines; SQLite and DuckDB cannot use 1 MiB.

### TPC-C replicated snapshot

| Engine | Page/block target | Reported load s | Closed MiB | After maintenance MiB | Closed allocated MiB | After allocated MiB |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| slopdb | 4 KiB (default) | 180.0 | 185.27 | 176.19 | 185.61 | 176.50 |
| slopdb | 16 KiB | 182.8 | 156.89 | 147.66 | 157.38 | 148.11 |
| slopdb | 64 KiB | 177.9 | 154.74 | 145.60 | 155.20 | 146.06 |
| slopdb | 1 MiB | 170.3 | 159.44 | 150.14 | 159.82 | 150.53 |
| sqlite | 4 KiB (default) | 47.0 | 461.68 | 438.36 | 461.69 | 438.36 |
| sqlite | 16 KiB | 45.1 | 460.02 | 436.30 | 460.02 | 436.30 |
| sqlite | 64 KiB | 47.6 | 460.38 | 436.69 | 460.38 | 436.70 |
| sqlite | 1 MiB | Unsupported | — | — | — | — |
| duckdb | 16 KiB | 30.2 | 571.78 | 571.78 | 571.79 | 571.79 |
| duckdb | 64 KiB | 20.7 | 265.95 | 265.95 | 265.96 | 265.96 |
| duckdb | 256 KiB (default) | 22.0 | 272.26 | 272.26 | 272.27 | 272.27 |
| duckdb | 1 MiB | Unsupported | — | — | — | — |

### TPC-H SF=0.2

| Engine | Page/block target | Reported load s | Closed MiB | After maintenance MiB | Closed allocated MiB | After allocated MiB |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| slopdb | 4 KiB (default) | 100.4 | 267.31 | 211.80 | 267.57 | 212.03 |
| slopdb | 16 KiB | 107.3 | 237.93 | 181.34 | 238.18 | 181.61 |
| slopdb | 64 KiB | 94.9 | 226.83 | 170.15 | 227.09 | 170.42 |
| slopdb | 1 MiB | 91.4 | 220.70 | 163.77 | 220.94 | 163.99 |
| sqlite | 4 KiB (default) | 17.3 | 321.10 | 318.91 | 321.11 | 318.92 |
| sqlite | 16 KiB | 16.9 | 317.86 | 315.66 | 317.87 | 315.66 |
| sqlite | 64 KiB | 16.6 | 318.44 | 316.19 | 318.45 | 316.20 |
| sqlite | 1 MiB | Unsupported | — | — | — | — |
| duckdb | 16 KiB | 18.1 | 272.07 | 272.07 | 272.08 | 272.08 |
| duckdb | 64 KiB | 17.2 | 233.95 | 233.95 | 233.96 | 233.96 |
| duckdb | 256 KiB (default) | 16.8 | 203.76 | 203.76 | 203.77 | 203.77 |
| duckdb | 1 MiB | Unsupported | — | — | — | — |

See the [complete page-size comparison](page-size-comparison.md) for TPC-H and
TPC-C timings, exact revisions, engine versions, durability differences, input
hashes and raw inventories. SQLite storage used Python SQLite 3.45.1; throughput
used the Go driver’s SQLite 3.51.3. Native default controls use SQLite 4 KiB
and DuckDB 256 KiB, while SlopDB defaults to 4 KiB. SlopDB’s maintained 64 KiB
footprints are 17.4% / 19.7% smaller than its new TPC-C / TPC-H 4 KiB controls;
its 1 MiB TPC-C footprint is larger than 64 KiB. Larger targets do not guarantee
better packing or faster queries.

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
