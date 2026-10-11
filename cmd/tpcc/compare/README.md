# Three-engine TPC-C-style comparison

This separate Go module adds SQLite and DuckDB drivers while keeping SlopDB's
CLI and library free of those native dependencies. All engines execute the
shared workload in `internal/tpcc`, including data generation, transactions,
prepared query templates, retries, timing, and validation. This is the existing
reduced TPC-C-inspired workload, not a certified TPC-C score.

Measured results and raw reports are in the [TPC-C-style comparison](../../../docs/tpcc-comparison.md).

Build from this directory:

```sh
mkdir -p /tmp/slopdb-bench
go build -o /tmp/slopdb-bench/tpcc-compare .
```

Run sequentially from the repository root:

```sh
/tmp/slopdb-bench/tpcc-compare -engine slopdb -workers 10 -duration 1m -json /tmp/slopdb-bench/tpcc-slopdb.json
/tmp/slopdb-bench/tpcc-compare -engine sqlite -workers 10 -duration 1m -json /tmp/slopdb-bench/tpcc-sqlite.json
cmd/tpcc/compare/run-duckdb.sh -workers 10 -duration 1m -json /tmp/slopdb-bench/tpcc-duckdb.json
```

Each run uses a fresh on-disk database. Defaults match the SlopDB runner: two
warehouses, ten districts each, 100 customers per district, 1,000 items, seed 42,
five retries, a 250 ms lock wait, and a five-second transaction deadline. The
mix is 45% new-order, 43% payment, and 4% each order-status/delivery/stock-level.
Retries reuse the same logical inputs. Preparation, loading, five transaction
preflights, consistency validation, and close/reopen are outside the timed
minute. All 15 consistency checks run after execution and again after reopening.
Failed logical transactions after retry exhaustion are reported separately;
the runner saves JSON and exits nonzero when failures or invalid data occur.

The native drivers reserve one connection per worker and prepare every template,
including all stock-level IN-list arities, before timing. The same comparison
binary can execute SlopDB so all engines use the same Go toolchain. The SlopDB
adapter preserves a fresh SlopDB transaction/session per logical attempt.
Native results are converted to the same int64-based result maps, so these are
Go API/workload measurements, including driver and result conversion overhead.

## Transactions and durability

- SlopDB retains READ COMMITTED, row locks, and `FOR UPDATE SKIP LOCKED`.
  WAL is enabled with WAL fsync disabled by default. SSTable/MANIFEST sync stays
  enabled.
- SQLite uses WAL and `synchronous=OFF`, verified on opening. Writers use
  `BEGIN IMMEDIATE`; read-only transactions use `BEGIN`. Its busy timeout is the
  same 250 ms default lock-wait budget. Busy/locked errors enter the shared retry
  policy. SQLite serializes writers; row-lock clauses are removed.
- DuckDB uses native snapshot transactions and retries optimistic write conflicts.
  Row-lock clauses are removed. The harness uses one execution thread per query,
  a 1GB memory limit, and BIGINT for the workload's INTEGER fields.

DuckDB 1.5.6 does not expose a normal SQL setting to disable WAL fsync. For the
requested benchmark mode, `run-duckdb.sh` builds a Linux-only LD_PRELOAD shim
that skips `fsync` and `fdatasync` **only for the exact benchmark WAL path**.
Other files, including checkpoints, pass through to the real functions. The
runner rejects missing/mismatched hooks and requires a nonzero skipped-sync
counter in the final report. This is a benchmark-only alteration, not DuckDB's
standard durability mode. SQLite's OFF setting also disables checkpoint sync;
these durability modes are not identical.

For normal durable native runs, invoke the binary with `-wal-fsync`; DuckDB
then requires no preload wrapper and SQLite uses `synchronous=FULL`. SlopDB's
same flag enables its WAL fsync. The wrapper owns the engine and fresh database
path; use the binary directly to choose a different path or durable mode.

Concurrent throughput is not a comparison of identical locking protocols.
In particular, DuckDB can exhaust five retries on hot warehouse/district rows,
and replacing SKIP LOCKED with optimistic retry changes delivery scheduling.
Read-only snapshots also differ from SlopDB's READ COMMITTED reads. Compare
committed throughput together with failure/retry counts and tail latency.

`-page-size <bytes>` sets the native storage size before creating the database
and checks its actual value. SQLite accepts powers of two from 512 through
65536 bytes; DuckDB accepts 16384 through 262144. Both reject 1 MiB explicitly.
SQLite uses a 128 MiB cache budget and a byte-equivalent 64 MiB WAL checkpoint
threshold across page sizes; DuckDB retains one execution thread and 1 GB memory.
