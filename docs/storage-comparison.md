# Database storage comparisons

The [page-size comparison](page-size-comparison.md) adds fresh 16 KiB, 64 KiB
and 1 MiB SlopDB measurements, supported native sizes and default-size controls.
It includes large equal-data storage fixtures and separate TPC-C/TPC-H timings.
At 64 KiB, maintained SlopDB footprints are 145.60 MiB for the 10.5-million-row
TPC-C snapshot and 170.15 MiB for TPC-H SF=0.2. Larger pages reduced storage
but slowed this TPC-H query workload; the new report preserves both outcomes.

## Historical small-dataset comparison, 2026-10-11

The [larger comparison](storage-large-comparison.md) measures both workloads
above 100 MiB per engine, including TPC-H SF=0.2 and a 10.5-million-row TPC-C
snapshot.

SlopDB's footprint depends substantially on compaction. On an identical TPC-C
snapshot it occupies 8.07 MiB after loading and closing, and 1.39 MiB after
VACUUM. SQLite occupies 3.31 / 3.16 MiB and DuckDB 6.26 / 6.26 MiB. At TPC-H
SF=0.01, SlopDB is smaller than both engines even before forced compaction.

These are local measurements of the repository's reduced TPC-C workload and
fractional TPC-H data. MiB means 1,048,576 bytes. Sizes include all database
files, indexes, metadata and lock files; they exclude reports, input tables,
binaries and profiles. All measured databases are closed, with no WAL files
remaining. They describe persistent storage, rather than peak disk use while
loading or running transactions.

## Equal-data comparisons

| Dataset and storage state | SlopDB MiB | SQLite MiB | DuckDB MiB |
| --- | ---: | ---: | ---: |
| TPC-C identical snapshot, closed | 8.07 | 3.31 | 6.26 |
| TPC-C identical snapshot, after maintenance | 1.39 | 3.16 | 6.26 |
| TPC-H SF=0.001, closed | 3.16 | 1.63 | 11.01 |
| TPC-H SF=0.001, after maintenance | 1.03 | 1.62 | 11.01 |
| TPC-H SF=0.01, closed | 13.07 | 15.70 | 19.26 |
| TPC-H SF=0.01, after maintenance | 10.28 | 15.60 | 19.26 |

TPC-C uses the committed snapshot from the saved DuckDB comparison run:
83,165 rows across nine tables, including 6,715 orders, 64,190 order lines and
6,348 history rows. All engines were freshly rebuilt from exactly the same
values, with the same declared primary keys and customer/order index. Native
DuckDB BIGINT columns map to INTEGER in SlopDB and SQLite. Every value was
compared in primary-key order after reopening and again after maintenance.
SlopDB inserts used prepared statements and batches of 1,000 rows.

TPC-H uses the retained SlopDB and DuckDB benchmark databases and freshly loaded
on-disk SQLite databases. Earlier SQLite timings came from an in-memory
verifier, which has no database directory to measure. New SQLite loads use the
same hash-verified DBGEN inputs, schema, fourteen secondary indexes, ANALYZE and
Q15 view. SF=0.001 has 8,695 total rows, including 6,005 lineitems; SF=0.01 has
86,805 rows, including 60,175 lineitems. All 22 SQLite query results match both
archived engines at both scales. After maintenance, all 22 queries were checked
again for every engine and scale, for 132 additional result comparisons.

Maintenance ran on private copies, preserving the original benchmark databases:
SlopDB VACUUM flushes and compacts SSTable levels; SQLite VACUUM rewrites its
database; DuckDB executed VACUUM followed by CHECKPOINT. These are engine-specific
operations. DuckDB's file size did not change in these runs.

New SQLite files use Python SQLite 3.45.1; DuckDB uses 1.5.6. The original TPC-C
SQLite run used the Go driver's SQLite 3.51.3. SlopDB used `f45ea982792cd25c94478b76dd4bf20ac83bb8eb`
and its default storage settings: 4 MiB memtable, 4 KiB blocks, and auto
compression. WAL was retained with WAL fsync disabled for new SlopDB/SQLite
loads; sizes were measured after clean shutdown/checkpointing.

## Original one-minute TPC-C databases

| Engine | Committed transactions | Stored rows | Closed file bytes | MiB | Files |
| --- | ---: | ---: | ---: | ---: | ---: |
| SlopDB | 133,339 | 732,587 | 18,951,452 | 18.07 | 19 |
| SQLite | 159,838 | 872,168 | 37,978,112 | 36.22 | 1 |
| DuckDB | 14,225 | 83,165 | 6,565,888 | 6.26 | 1 |

These describe the retained [one-minute benchmark runs](tpcc-comparison.md).
Different throughput produced different amounts of data: SlopDB has 603,420
order lines, SQLite 719,100 and DuckDB 64,190. These total sizes should be read
alongside the row counts; use the identical snapshot above to compare storage
on equal data. Existing database sizes were verified unchanged after inspection;
SlopDB row counts were obtained from private copies.

## Files, allocation and compaction

| Closed dataset | SlopDB SSTables + metadata files | SQLite files | DuckDB files |
| --- | --- | --- | --- |
| TPC-C identical snapshot | 4 SSTables + MANIFEST + LOCK | 1 SQLite file | 1 DuckDB file |
| TPC-H SF=0.001 | 3 SSTables + MANIFEST + LOCK | 1 SQLite file | 1 DuckDB file |
| TPC-H SF=0.01 | 8 SSTables + MANIFEST + LOCK | 1 SQLite file | 1 DuckDB file |

SlopDB's auto policy leaves L0 uncompressed and uses zstd in later levels. The
fresh TPC-C snapshot has four L0 files totaling 8,460,818 bytes. Its metadata
and lock file total just 582 bytes. The small TPC-H database also has all data
in L0. Full compaction moves these files to compressed levels, explaining the
large reductions. At SF=0.01, 9,715,747 SSTable bytes were already in L1 and
3,990,196 bytes in L0 before maintenance.

DuckDB's measured allocation units are 256 KiB blocks. The closed TPC-H files
have five free blocks at SF=0.001 and six at SF=0.01, included in their sizes.
Reported DuckDB sizes immediately after loading were 8.01 and 16.51 MiB;
the retained files after Q15 setup, query execution and close are 11.01 and
19.26 MiB. Those different observation points should not be mixed in a table.

Filesystem allocation was measured independently using `st_blocks * 512`,
including directory blocks, and checked against `du -s -B1`. For the closed
equal-data cases:

| Dataset | SlopDB allocated MiB | SQLite allocated MiB | DuckDB allocated MiB |
| --- | ---: | ---: | ---: |
| TPC-C identical snapshot | 8.10 | 3.32 | 6.27 |
| TPC-H SF=0.001 | 3.18 | 1.64 | 11.02 |
| TPC-H SF=0.01 | 13.10 | 15.71 | 19.27 |

The [raw comparison](benchmarks/storage/comparison.json) records exact apparent
and allocated bytes for every file, database paths, table counts, input hashes,
SSTable level inventories, page/block statistics and maintenance results.
Normal close and background compaction do not guarantee minimum database size;
fresh-load layout and write history affect these measurements.

## Reproducing the measurements

Use the [TPC-C comparison runner](../cmd/tpcc/compare/README.md) and
[TPC-H runner](../cmd/tpch/README.md) for the schemas, indexes and input data.
For an equal-data TPC-C comparison, export a single committed database in
primary-key order and load its rows and indexes into fresh databases for each
engine. Check row counts and complete values after reopening. For TPC-H,
load identical DBGEN files into on-disk SQLite rather than the verifier's
in-memory database, then execute and compare all 22 queries.

Close databases before recording size. Sum regular files' `stat().st_size`
for apparent bytes; use `du -s -B1 <database-directory>` for filesystem allocation.
Exclude artifacts by storing them outside the database directory. Retain any
WAL or sidecar files that exist when measuring. To compare maintenance effects,
copy the closed database, run the operations listed above, close it again and
repeat size and result checks.
