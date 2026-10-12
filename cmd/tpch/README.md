# TPC-H query benchmark

This runner executes all 22 TPC-H query shapes on genuine DBGEN data. It uses
fixed qualification parameters, one query stream, prepared statements, and
per-query deadlines. It records failures and timeouts and continues through the
query set. There are no refresh streams or certified QphH scores.

## Generate data and run SlopDB

The Go runner accepts all eight standard `*.tbl` files from DBGEN, with or
without the trailing `|`. The optional Python generator uses DuckDB's DBGEN
implementation and saves row counts, input SHA-256 hashes, native queries, and
native reference answers. Python DuckDB is a development dependency only.

```sh
python3 -m venv /tmp/tpch-env
/tmp/tpch-env/bin/pip install duckdb==1.5.6
/tmp/tpch-env/bin/python cmd/tpch/generate.py --sf 0.001 --out /tmp/tpch-sf001

go build -o /tmp/slopdb-tpch ./cmd/tpch
/tmp/slopdb-tpch -data /tmp/tpch-sf001 -timeout 5s -json /tmp/tpch-slopdb.json
```

The generator supports `--extension /path/to/tpch.duckdb_extension` for an
already downloaded extension. DuckDB and the extension must have matching
versions. Generation is deterministic. Existing output and database directories
are never removed or reused.

Default SlopDB flags:

| Flag | Default | Purpose |
| --- | --- | --- |
| `-data` | required | Directory containing eight DBGEN tables |
| `-dir` | fresh `/tmp` directory | SlopDB database directory |
| `-queries` | `all` | Query numbers, e.g. `1,6,13` |
| `-runs` | `1` | Measured executions per query |
| `-warmups` | `0` | Unmeasured executions before each query |
| `-timeout` | `10s` | Deadline per execution |
| `-prepared` | `true` | Parse each query once before measuring |
| `-indexes` | `true` | Create indexes from `indexes.sql` |
| `-wal-fsync` | `false` | Enable WAL fsync during loading |
| `-load-batch-rows` | `4096` | Maximum rows per atomic bulk-load transaction |
| `-json` | database's `benchmark.json` | Full results and timings |
| `-profile-dir` | disabled | Separate CPU profile for each query |

WAL stays enabled. SSTable and MANIFEST synchronization stay enabled. All
loading, index creation, ANALYZE, close/reopen, persisted row-count checks, view
setup, preparation, and EXPLAIN happen outside individual query timings. Queries
run on committed data after reopening. The JSON query-phase duration includes
preparation, EXPLAIN, result reporting, and profiler start/stop overhead; use the
individual samples to measure SQL execution.

SlopDB loading uses `Database.BulkInsert` with bounded transactions and prepared
multi-row INSERTs of up to 256 rows. `-load-batch-rows` bounds transaction rows;
reports record the bulk mode, configured bound and committed transaction count.
Constraints, indexes and WAL remain enabled. A malformed row or SQL error rolls
back its current chunk and fails the load; earlier committed chunks can remain
in the failed fresh directory. Such a directory is never reused as a successful
benchmark fixture. The replicated TPC-C storage loader accepts the same flag.

Samples include elapsed time, process CPU time, Go allocated bytes and allocation
counts, output row count, status, and error. Allocation counters include
background Go work during execution. Peak RSS is for the entire process,
including loading. A timeout measures work before cancellation; its elapsed
time is a lower bound on completion time. It is never included in a successful
median. Nonzero exit status reports failed, canceled, or timed out executions;
the runner still writes the report. Each query's final successful result is
saved for independent comparison.

## Compare DuckDB through its Go driver

`duckdb/` is a separate Go module using
[`github.com/duckdb/duckdb-go/v2`](https://github.com/duckdb/duckdb-go). Its
pinned driver is v2.10506.0 (DuckDB v1.5.6), requires CGO and Go 1.24 or newer,
and leaves SlopDB's module and runtime dependencies unchanged.

```sh
go -C cmd/tpch/duckdb build -o /tmp/tpch-duckdb .
/tmp/tpch-duckdb -query-root cmd/tpch -data /tmp/tpch-sf001 \
  -threads 1 -timeout 5s -json /tmp/tpch-duckdb.json

python3 cmd/tpch/verify.py -h
python3 cmd/tpch/verify.py --data /tmp/tpch-sf001 \
  --report /tmp/tpch-slopdb.json --duckdb-report /tmp/tpch-duckdb.json \
  --out /tmp/tpch-verification.json
```

Both Go runners use the exact same query files, index definitions, inputs, and
result materialization. DuckDB uses BIGINT/DOUBLE to match SlopDB's 64-bit
INTEGER/REAL rather than DuckDB's 32-bit INTEGER/REAL types. It loads through the
native appender, checkpoints a fresh disk database, then reopens and checks row
counts before querying. Loading is excluded from the query comparison. DuckDB
runs with one execution thread by default; `-threads 4` measures parallelism
separately. The memory limit defaults to 1GB. Its report includes the actual
engine and toolchain versions, process CPU time, result rows, and physical
EXPLAIN plans. Go allocation counters would omit its native allocations, so the
DuckDB runner does not present them as comparable memory usage.

The standard-library Python verifier loads the same files into SQLite, creates
the same schema/indexes, checks hashes and row counts, and executes all 22 query
files. It checks result multisets, numeric tolerance, and declared ORDER BY keys.
When native reference answers exist, it validates every dialect translation
against them. It can also validate multiple `--duckdb-report` files. SQLite's
reference database is in memory; its timings are a diagnostic reference rather
than a comparison of identical storage configurations. Timed-out SlopDB queries
remain incomplete and cause the verifier to exit unsuccessfully.

## SQL and dataset choices

The eight schemas use NOT NULL values, 64-bit integers/reals, and ISO dates
stored as TEXT. The declared primary keys are clustered in SlopDB. `partsupp`
uses a nonunique `(ps_partkey, ps_suppkey)` index and an implicit row ID because
DBGEN at very small fractional scales can produce repeated supplier keys for a
part. Keeping all those rows preserves the generator's native query answers;
deduplicating them would change the data. All other logical primary keys remain
PRIMARY KEY constraints.

Query changes are limited to SlopDB's dialect: ISO date constants, SUBSTR for
year and phone-prefix extraction, explicit equality JOIN ... ON instead of
comma joins, an explicit Q13 aggregate alias, and a persistent Q15 revenue view
instead of a CTE. Q15's view is created before measurement; evaluating it is
part of query execution. No query is replaced by a precomputed answer. Year
outputs are ISO year strings instead of integer EXTRACT results. Fixed numeric
qualification parameters are used at every scale, including Q11's 0.0001
threshold; this is not a generated random QGEN stream.

Fractional scales are for development: SF=0.001 has 6,005 line items. Below this
scale DuckDB's generator may produce minimum-size tables rather than linearly
scaled counts; always inspect the manifest. Small scales also produce empty
results for several qualification queries. The benchmark accepts normal larger
DBGEN datasets; the generator never runs during measurement.

SlopDB EXPLAIN describes eligible join order and available index access. Runtime
cardinality can change the choice between indexed probes and hash joins, and
fallback joins/subqueries are not fully described. Use CPU profiles and executor
source to establish actual work.

```sh
/tmp/slopdb-tpch -data /tmp/tpch-sf001 -queries 3,4,9,20 -timeout 5s \
  -profile-dir /tmp/tpch-profiles -json /tmp/tpch-profile.json
go tool pprof -top -cum /tmp/slopdb-tpch /tmp/tpch-profiles/q03.cpu.pprof
```

Queries follow the qualification definitions in the
[TPC-H specification](https://www.tpc.org/TPC_Documents_Current_Versions/pdf/TPC-H_v3.0.1.pdf)
and DuckDB's [TPC-H extension](https://github.com/duckdb/duckdb/tree/main/extension/tpch).
Local measurements and optimization priorities are in
[the performance report](../../docs/tpch-performance.md).

Measured execution improvements and remaining bottlenecks are documented in [TPC-H execution improvements](../../docs/tpch-optimization.md).

The latest indexed joins, selective execution, allocation measurements, and
TPC-C regression results are in [Join execution improvements](../../docs/tpch-join-plans.md).

Both Go runners accept `-page-size <bytes>` and record the actual configured
size in JSON. SlopDB targets uncompressed SSTable data blocks; DuckDB uses fixed
storage blocks. DuckDB rejects 1 MiB (its maximum is 256 KiB).

For equal-data storage comparisons, build `go build -o /tmp/slopdb-bench/storage-snapshot
./cmd/storage-snapshot`, the two TPC-H runners, and run
`scripts/storage_page_matrix.py --help`. The matrix keeps reports and data
outside the repository, preserves fresh directories, runs variants serially,
and records closed versus compacted/VACUUM/checkpoint file and allocated sizes.
The TPC-C storage fixture uses a portable JSON snapshot with warehouse/history
IDs shifted per replica; it is separate from the timed transaction workload.
TPC-H supports independent answer checking through `--reference-report`.
Native DuckDB requires its Python package and the existing Linux WAL-only
preload wrapper for loads with WAL fsync disabled. Unsupported sizes are
reported as unsupported rather than substituted with a different size.
