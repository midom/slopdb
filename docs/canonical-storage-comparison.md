# Canonical SQL storage comparison, 2026-10-12

Every storage case now replays the **same frozen `.sql` file** for its dataset.
All SlopDB, SQLite and DuckDB results below are fresh loads; no previous native
measurements or equal-duration benchmark databases are reused. Each of the twenty
supported cases verifies every column's exact typed SHA256, row count and logical
payload size after clean close/reopen, and repeats the complete check after
maintenance and another close/reopen. All checks passed. Four native 1 MiB
requests remain unsupported.

## Equal logical data

| Dataset | Rows | Logical payload MiB | Canonical SQL MiB | Verified per supported cell |
| --- | ---: | ---: | ---: | --- |
| TPC-C | 10,518,120 | 680.69 | 306.49 | Every column, twice |
| TPC-H | 1,731,999 | 225.00 | 235.64 | Every column, twice |

The exact common logical payload totals are **713,759,360 bytes for TPC-C** and
**235,929,611 bytes for TPC-H**.

`logical_bytes` counts non-NULL row payload only: int64/float64 eight bytes, text
UTF-8 bytes, NULL zero. It excludes hash framing, SQL text, schema and indexes.
Those physical structures are included in database sizes. Each table also records
its exact framed byte count and SHA256. Integers remain exact int64, floating
values use exact finite IEEE-754 bytes with signed zero normalized, and text uses
exact UTF-8 bytes. The independent Python/Go implementations verify every table
against the frozen manifest. [The protocol](page-size-protocol.md) defines the
encoding and gates; [archived manifests](benchmarks/storage/pages-canonical/manifest.json)
retain complete input hashes and every table's fingerprints.

- TPC-C SQL SHA256: `8772c9bfcb2672d1b18bfb6aa98059489d26962083c94568f089be211eccd543`.
- TPC-H SQL SHA256: `921ac0b2c9114c3e309c1b14ba987e1657c5c8fa04b87aa84452056f629b1ed3`.

The TPC-C fixture freezes a replicated committed snapshot, with warehouse/history
keys shifted once during generation: nine tables, 256 warehouses, 128 replicas
except the shared 1,000-row item catalog. TPC-H freezes DBGEN SF=0.2: eight tables,
fourteen secondary indexes, ANALYZE and the Q15 view. The SQL declares portable
BIGINT/DOUBLE/TEXT types with explicit NOT NULL primary keys. This conversion
happens once before freezing the SQL, never during replay. It can change native
physical layouts relative to older engine-specific schemas, so old footprints
are historical rather than controlled baselines.

## Final storage after maintenance

SlopDB's Zstd window matches its configured block target: 4 KiB, 16 KiB, 64 KiB
or 1 MiB. VACUUM atomically merges and rewrites **all SSTables directly into L6**,
including preexisting L6 files. Every final SlopDB cell proves zero files/bytes in L0–L5,
`full_bottom_level_rewrite`, and an exact match between manifest L6 files and the
physical directory. L6 can contain multiple SSTables. No workload, TPC-H query
pack, transaction benchmark or query performance test runs at any size.

### TPC-C canonical SQL

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

### TPC-H canonical SQL

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

At the common 64 KiB target, SlopDB uses 145.44 MiB for TPC-C and 170.12 MiB
for TPC-H, versus DuckDB's 268.01/232.89 MiB and SQLite's 448.50/321.75 MiB.
Among tested SlopDB targets, 64 KiB gives the smallest TPC-C footprint and
1 MiB the smallest TPC-H footprint. Defaults give a different comparison:
SlopDB's 4 KiB TPC-H fixture is 214.51 MiB, larger than DuckDB's 200.26 MiB at
its 256 KiB default. These are storage outcomes, not a fastest-page verdict.
**SlopDB's default stays 4 KiB.** Current performance was not measured; older
8 MiB-window timings do not establish performance under this configuration.

Final file bytes sum every regular database file after close, including indexes,
catalog metadata and locks. Allocated bytes use filesystem block allocation,
including directory blocks. No WAL remains after close. Frozen SQL, logs,
reports, binaries and temporary reference staging are outside database size
accounting. Pre-maintenance file inventories are retained as diagnostics, not
used in these final comparison tables. MiB means 1,048,576 bytes.

Native maintenance is SQLite VACUUM or DuckDB VACUUM + CHECKPOINT. These differ
from SlopDB's physical rewrite; DuckDB VACUUM does not itself reclaim disk space.
Both native engines report their actual stored page/block size after reopening,
so unsupported 1 MiB requests cannot silently substitute another size.

## Loading, durability and reproducibility

Every supported TPC-C case executes 46,242 SQL statements; every supported TPC-H
case executes 7,650. Every engine uses the same original statements and transaction boundaries:
up to 256 rows per INSERT and 4,096 rows per transaction. Native Python streams
quote/comment-aware statements; there is no COPY shortcut, appender, independent
random data generation or per-engine type rewrite. SlopDB parses and executes
those statements through its normal API. SQL hashes are verified before replay
and again during/after execution. Source dumps and sidecar manifests remain under
`/tmp/slopdb-bench/canonical-sql-20261012/`; their large SQL files are intentionally
kept outside Git. The deterministic generator, pinned source hashes and generation
settings support reproduction.

WAL is enabled with WAL fsync disabled. SlopDB and DuckDB retain data/checkpoint
synchronization; DuckDB's Linux preload wrapper bypasses only its exact cell WAL
path and every cell proves skipped sync calls. SQLite synchronous=OFF also
disables data/checkpoint synchronization. SlopDB retains a 4 MiB memtable,
128 MiB block cache and auto compression; SQLite retains 128 MiB cache and an
approximately 64 MiB WAL checkpoint threshold; DuckDB uses one execution thread
and a 1 GB memory limit. These budgets stay fixed across each engine's page
variants, rather than equal across engines.

Load seconds are diagnostic durations of SQL replay plus clean close/checkpoint,
with the initial hash/open and full-column validation outside the timer. SlopDB
hashes replayed bytes while executing; native replay rechecks the file hash before
close. Those details and engine costs differ, so these are not certified bulk
throughput scores. Each cell is one fresh load, all cases ran serially, and OS
caches were not reset. Internal sequence bytes and file boundaries can vary even
with identical logical values; measured footprints do not isolate codec efficiency.

The profiled TPC-C DuckDB 16 KiB cell separately reports **719.268s loading**,
**53.267s initial verification**, **52.214s final verification**, and
**0.009s maintenance**. Those phases are distinct; total cell elapsed also
includes setup and file accounting.

A 10-second CPU profile overlaps the TPC-C DuckDB 16 KiB loading duration, so
that diagnostic duration also includes sampling overhead. Of 904 samples,
749 (82.85%) are attributed to DuckDB's native path including libc calls with
DuckDB ancestors, and 133 (14.71%) to Python including its libc calls. Native
stacks include parsing, expression binding/rewriting/execution and vector-cache
allocation. DuckDB documents [INSERT parsing/processing overhead](https://duckdb.org/docs/current/data/insert)
and recommends batching and transactions; these loads already use 256-row INSERTs
within 4,096-row transactions. Python is not the main bottleneck in this interval.
These are sampled
CPU shares during one portion of loading, not whole-load timing fractions or
engine throughput scores. [The attribution summary](benchmarks/storage/pages-canonical/duck-phase-cpu-samples.json)
retains the original DSO breakdown and hashes/paths for raw profiles preserved
outside Git.

A separate 10-second sample of the first reopened order_line fingerprint scan
collected 910 samples: 79.78% Python interpreter self time and 14.95% DuckDB.
Including Python-attributed libc and serialization/hash helpers puts that Python
validation path at 84.18%. Python dominates exact full-row verification
in this interval, while native DuckDB dominates the sampled SQL-loading interval.
That first verification duration also overlaps profiling.
[The verification summary](benchmarks/storage/pages-canonical/duck-phase-cpu-samples.json)
keeps its evidence separate. Total cell elapsed includes loading, two validations
and maintenance, and must not be interpreted as `load_seconds`. The splitter and
harness remained unchanged throughout all cells.

The compiled SlopDB source revision is `071907c90d551b7b1826002b1c3ddbf46a8d3782`.
Binary SHA256 and `go version -m` output, host/version metadata, all twenty
successful raw reports, unsupported requests, file inventories and artifact hashes
are retained in the [TPC-C](benchmarks/storage/pages-canonical/tpcc/comparison.json)
and [TPC-H](benchmarks/storage/pages-canonical/tpch/comparison.json) archives.
Task-created databases were discarded only after both verification phases and
saved inventories; frozen inputs and reports were preserved.

See [page-size support](page-sizes.md), [measurement protocol](page-size-protocol.md)
and [large storage history](storage-large-comparison.md). Earlier 8 MiB-window
query/transaction timings remain historical and were not rerun by this storage task.
