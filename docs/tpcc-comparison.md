# TPC-C-style comparison, 2026-10-11

SQLite committed 19.9% more transactions per second than SlopDB in this run.
SlopDB committed 9.37x as many as DuckDB and had a 4.29x lower p99 latency than
SQLite. This is the repository's reduced TPC-C-inspired workload, not a
compliant TPC-C benchmark or tpmC score.

| Engine | Commits | Committed tx/sec | Mean ms | P50 ms | P95 ms | P99 ms | Retries | Failed logical transactions |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| SlopDB | 133,339 | 2,222.31 | 4.477 | 2.910 | 14.834 | 24.452 | 0 | 0 |
| SQLite 3.51.3 | 159,838 | 2,663.96 | 3.724 | 0.368 | 1.199 | 104.821 | 1,071 | 0 |
| DuckDB 1.5.6 | 14,225 | 237.08 | 38.069 | 17.490 | 126.125 | 206.641 | 10,401 | 372 |

All three passed all 15 accounting and row-count checks after execution and
all 15 again after closing and reopening. Expected invalid-item rollbacks
were 582, 670 and 64 respectively; shutdown cancellations were 8, 10 and 10.
These are counted separately from unexpected failures. Latencies describe
successful logical transactions and include retries; they exclude failed
transactions, expected rollbacks and shutdown cancellations.

## Transaction breakdown

Transaction throughput reflects the shared workload mix; workers choose
45% new-order, 43% payment and 4% each of the other transactions.

| Transaction | SlopDB tx/sec | SQLite tx/sec | DuckDB tx/sec | SlopDB mean ms | SQLite mean ms | DuckDB mean ms |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| New-order | 999.03 | 1,192.47 | 101.92 | 4.581 | 4.218 | 68.850 |
| Payment | 954.55 | 1,149.17 | 105.80 | 3.791 | 3.643 | 8.247 |
| Order-status | 89.47 | 107.97 | 10.23 | 0.432 | 0.137 | 5.096 |
| Delivery | 89.77 | 108.02 | 9.07 | 16.891 | 5.453 | 109.352 |
| Stock-level | 89.50 | 106.35 | 10.07 | 2.238 | 0.939 | 9.184 |

SQLite's successful transactions were usually fast, but its 1,071 lock-timeout
retries and 1,226 ms maximum latency show a long tail. Writers use BEGIN
IMMEDIATE with a 250 ms busy timeout. SlopDB's row-lock mode had no retries in
this run and a maximum latency of 83.95 ms. These observations do not establish
the exact CPU or wait-time breakdown; no profiler ran during measurement.

DuckDB had 10,401 optimistic write-conflict retries. After the five-retry
budget, 319 new-orders, 49 deliveries and 4 payments failed. It therefore
exited with status 1 while saving its report; all committed-data checks still
passed. New-orders account for 7,570 retries and deliveries for 804. Retry
work contributes to their high latency. Even read-only order-status and
stock-level transactions were slower than the other engines here, so
contention alone does not explain the entire gap. DuckDB documents its
[optimistic concurrency and conflict behavior](https://duckdb.org/docs/lts/connect/concurrency).

## Measurement and engine settings

Runs were sequential, each with 10 concurrent workers for 60 seconds on the
same Linux ARM64 host with four Neoverse-N1 CPUs. All used Go 1.24.0, the shared
workload, prepared statements, seed 42, two warehouses, ten districts per
warehouse, 100 customers per district and 1,000 items. The retry budget was
five and the per-attempt transaction timeout was five seconds. Data loading,
statement preparation, transaction preflights, validation and reopen were
outside the timed window. Commits already started at shutdown finish; reported
drain times were 0.001, 0.043 and 0.002 seconds respectively. Throughput divides
commits by the measured run duration, which was approximately 60 seconds in
each case.

All databases were on disk, with WAL enabled and WAL fsync disabled:

- SlopDB used DisableWALSync, READ COMMITTED reads, row locking and SKIP LOCKED
  delivery selection. SSTable and MANIFEST sync remained enabled.
- SQLite used WAL and synchronous=OFF, checked on opening. The native driver
  was github.com/mattn/go-sqlite3 v1.14.43. It serializes writers; row-lock
  clauses were removed. OFF also disables checkpoint sync.
- DuckDB used github.com/duckdb/duckdb-go/v2 v2.10506.0, one execution thread
  per query and a 1GB memory limit. INTEGER columns become BIGINT. Snapshot
  transactions use optimistic conflict retries, with row-lock clauses removed.
  DuckDB 1.5.6 has no ordinary SQL setting to disable WAL fsync. The Linux
  benchmark wrapper bypassed fsync/fdatasync only for its exact WAL file and
  recorded **13,108 skipped calls**. Other file and checkpoint syncing remained
  enabled. This is a benchmark-only modification to DuckDB's durability mode.

The native adapters reserve one connection per worker and prepare all 327
templates before timing. Results are converted to the same SlopDB result maps,
including exact int64 values. This measures the Go API workload, including
native driver and conversion overhead. Removing SKIP LOCKED changes delivery
scheduling; snapshot reads and SQLite's writer serialization also differ from
SlopDB. Durability and concurrency modes therefore are not identical.

These are individual one-minute measurements without confidence intervals.
The hot two-warehouse workload favors transactional access patterns; the
[TPC-H results](tpch-optimization.md) have a different engine ranking.

## Reproduction and validation

See the [three-engine runner](../cmd/tpcc/compare/README.md) for build commands
and the DuckDB WAL wrapper. The root `go run ./cmd/tpcc` command still runs
SlopDB without adding native database dependencies to the root module.

Build, vet and package checks passed in both Go modules. The prepared API
review passed 2,071 checks under the race detector. Short native and SlopDB
runs passed all accounting checks and reopen checks; native driver settings,
retry classification, fresh-path protection and exact-WAL sync interception
were also checked. The full reports were audited for identical workload
configuration, per-kind count totals, throughput arithmetic and all 30
consistency checks per engine.

Raw reports: [SlopDB](benchmarks/tpcc/slopdb-comparison.json),
[SQLite](benchmarks/tpcc/sqlite-comparison.json),
[DuckDB](benchmarks/tpcc/duckdb-comparison.json).
The [machine-readable comparison](benchmarks/tpcc/comparison.json) includes
throughput and p99 ratios.
