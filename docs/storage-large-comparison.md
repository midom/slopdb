# Storage comparisons above 100 MiB, 2026-10-11

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

SlopDB uses the current checkout, Go 1.22.2, a 4 MiB memtable, 4 KiB blocks and
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
