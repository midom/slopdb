# SlopDB TPC-C-style load tester

Run from the repository root:

```sh
go run ./cmd/tpcc -workers 10 -duration 1m -json /tmp/slopdb-tpcc-results.json
```

The default reuses prepared statements and keeps WAL enabled with WAL fsync
disabled. Use `-prepared=false` to compare SQL text execution with the same
workload. Statement preparation occurs before measurement, including every
stock-level IN-list size. The API and 64-bit pagination behavior are documented
in [Prepared statements](../../docs/prepared-statements.md).

Use `-wal-fsync` to measure with WAL fsync enabled. SSTable and MANIFEST syncing still use storage defaults.
Each run creates a fresh database under `/tmp/slopdb-tpcc-*` and prints its path;
the database remains available for inspection. `-dir` accepts a path that does
not yet exist and never deletes an existing database.

The workload has nine tables: warehouse, district, customer, item, stock, orders,
new_order, order_line, and history. Defaults are 2 warehouses, 10 districts per
warehouse, 100 customers per district, 1,000 items, and 30 seed orders per
district. Composite primary keys and a customer/order index support lookups.
Amounts are integer cents.

Workers choose new-order (45%), payment (43%), order-status (4%), delivery (4%),
and stock-level (4%) transactions. New orders have 5–15 distinct items, occasional
remote stock, and a 1% intentional invalid-item rollback. Payments can select
remote customers. Delivery processes up to one queued order per district using
`FOR UPDATE SKIP LOCKED`. Stock-level checks recent orders. Each worker owns an independent session, with a fresh explicit transaction for
every attempt and deadlock detection enabled. Fixed workload statements reuse
worker-private prepared result storage; normal adapter calls return owned results.
Arbitrary adapter calls cause that worker session to be replaced before its next
transaction, preserving isolation of variables, temporary tables and other
connection state.
Contention failures roll back and retry the same inputs with bounded backoff.

Loading, SQL preflight, validation, and shutdown are excluded from the requested
measurement duration. At its end, workers stop starting transactions and cancel
unfinished SQL. Commits already started finish, and drain time is reported
separately. Latency covers a successful logical transaction including any
retries; expected rollbacks, unexpected failures, and shutdown cancellations
have separate counters. Throughput counts committed transactions only.

The report includes transaction counts, throughput, latency percentiles, retry
causes, failures, and 15 accounting/row-count consistency checks. Checks run
again after closing and reopening the database. Unexpected failures or failed
checks produce a nonzero exit status. `-json` saves the same metrics for analysis.

The latest one-minute regression measurement after analytical join changes is
in [Join execution improvements](../../docs/tpch-join-plans.md).

This is an inspired load test with reduced scale, uniform customer/item choices,
no terminal think time, and simplified tax, discount, and customer fields. It is
not a compliant TPC-C benchmark, and the reported new-orders/minute is not tpmC.
See `go run ./cmd/tpcc -help` for scale, seed, retry, and timeout settings.

## Execution improvements (2026-10-11)

The same default scale, seed, ten workers, and sixty-second measurement window
were used before and after the SQL integrity and composite-query changes. WAL
was enabled and WAL fsync disabled in both runs. Loading and validation are
outside the window.

| Metric | Before | After |
| --- | ---: | ---: |
| Committed transactions | 4,704 | 120,095 |
| Transactions/second | 78.40 | 2,001.58 |
| Failed transactions | 2 | 0 |
| Retries | 116 | 0 |
| Delivery mean latency | 2,514.96 ms | 16.74 ms |
| Order-status mean latency | 89.09 ms | 0.47 ms |
| Stock-level mean latency | 160.40 ms | 2.15 ms |
| Payment mean latency | 24.08 ms | 4.05 ms |
| New-order mean latency | 5.21 ms | 5.43 ms |

Throughput improved by 25.5x. The after run had 520 intentional invalid-item
rollbacks and 10 shutdown cancellations, counted separately from failures.
All 15 accounting and row-count checks passed after the run and after reopening.
A second optimized minute run measured 1,997.10 transactions/second, also without
failures or retries. These are local measurements, not universal performance
guarantees or a compliant TPC-C score.

Delivery SUM/UPDATE queries now scan only an order's lines through their clustered
primary-key prefix. Recent-order stock-level queries use a bounded twenty-order
range and batch primary-key IN reads. Order-status queries seek backward through
the customer/order index and fetch the current base row. Queue reads stop after
the requested unlocked row. Complete primary-key lookups take priority over
covering indexes, preventing delivery's order lookup from scanning every order
in a district. Private transaction writes retain the same storage range bounds.

On the original 23,660-line database, separate five-execution statement checks
measured delivery's SUM at approximately 0.11 ms (previously 68.8 ms), its
14-line UPDATE at 0.27 ms (52.3 ms), and the recent-order DISTINCT query at 0.94 ms
(72.8 ms). These standalone timings exclude concurrent contention.

The raw local reports are `/tmp/slopdb-tpcc-results.json` (before) and
`/tmp/slopdb-tpcc-final-results.json` (after). SQL behavior and compatibility
changes are described in [SQL semantics](../../docs/sql-semantics.md).

A final ten-second profile measured 1,981.43 transactions/second with no failures
or retries and all consistency checks passing. The earlier covering-index scan
no longer dominates CPU. The largest cumulative SQL waits were payment's hot
warehouse update and district locks; frequently executed order-line inserts and
stock updates accounted for much of the remaining work. The original diagnostic
run recorded a CPU profile and per-query latencies.

## Prepared statement comparison (2026-10-11)

A fresh pair of runs used ten workers, sixty seconds, the default reduced scale
and seed, and WAL enabled without WAL fsync. The only mode difference was
`-prepared=false` versus the default prepared execution. Both use the same
statement templates, inputs, and transaction order. Preparation is outside the
measurement window; preparing all 327 templates took approximately 22 ms.

| Metric | SQL text | Prepared |
| --- | ---: | ---: |
| Committed transactions | 120,007 | 132,242 |
| Transactions/second | 2,000.11 | 2,204.01 |
| Mean transaction latency (ms) | 4.97 | 4.52 |
| P95 transaction latency (ms) | 15.64 | 15.21 |
| P99 transaction latency (ms) | 24.08 | 25.21 |
| Unexpected failures | 0 | 0 |
| Retries | 0 | 0 |

Prepared execution improved throughput by 10.2%. Mean latency fell, while
p99 latency increased slightly. Both runs passed all 15 accounting checks after
execution and after reopening. The prepared run had 577 expected invalid-item
rollbacks and 10 shutdown cancellations, separately counted from failures.

Reports: `/tmp/slopdb-tpcc-text-results.json` and
`/tmp/slopdb-tpcc-prepared-results.json`. These are individual local runs, not
universal performance guarantees. Preparation removes SQL parsing; query
planning, statement snapshots, row/catalog synchronization, and row allocation
still take place on execution.

A separate ten-second CPU profile of prepared execution contained no SQL
parser or tokenizer frames. Binding accounted for approximately 1.1% of sampled
CPU time. The profiling run also had
zero failures or retries and passed all checks after execution and reopening.

SQLite and DuckDB comparisons use the [shared three-engine runner](compare/README.md).
The latest [one-minute results](../../docs/tpcc-comparison.md) include throughput,
tail latency, failures, retry counts and durability settings for all three engines.

`-page-size 16384`, `65536` or `1048576` selects SlopDB's uncompressed
SSTable data-block target. The requested and actual sizes appear in JSON.
Keep the byte-budgeted block cache and memtable constant when comparing sizes.

## Workload profiles

Use `-profile-dir /tmp/slopdb-bench/tpcc-profiles` for a diagnostic run. It saves
`cpu.pprof`, `mutex.pprof`, `block.pprof` and `allocs.pprof`; inspect them with
`go tool pprof`. CPU, mutex and block sampling cover the measured workload plus
drain, excluding loading, preparation, validation and close. The allocation
profile is cumulative from process start through drain, including setup.
Collect throughput separately without profiling, since sampling adds overhead.
See [catalog profiling and rename checks](../../docs/catalog-performance.md).

## Allocation reuse

The SlopDB adapter keeps one independent session per worker and private result
slots per prepared template. Slots are used only by the workload helpers, which
consume each result before executing that template again. Public transaction
execution still returns owned results. Completed transaction handles remain
closed when the same worker starts a later transaction; cancelled statements and
failed attempts retain normal rollback and retry behavior. New-order item buffers
and stock-level argument buffers also belong to one worker and survive retries.

Run `go test ./internal/tpcc` for all five transaction paths, expected invalid-item
rollbacks, cancellation, lock waits, retained results, and all 15 consistency
checks before and after reopening. `BenchmarkPreparedTransactions` reports
allocations for real prepared transactions at a small deterministic scale. Its
setup is excluded, and each iteration rolls back to preserve the same fixture;
it measures transaction execution and rollback rather than committed throughput.
Use `go test ./internal/tpcc -run '^$' -bench BenchmarkPreparedTransactions -benchmem`
for an allocation comparison, separately from other performance workloads.
