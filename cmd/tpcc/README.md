# SlopDB TPC-C-style load tester

Run from the repository root:

```sh
go run ./cmd/tpcc -workers 10 -duration 1m -json /tmp/slopdb-tpcc-results.json
```

The default keeps WAL enabled and disables WAL fsync. Use `-wal-fsync` to measure
with WAL fsync enabled. SSTable and MANIFEST syncing still use storage defaults.
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
`FOR UPDATE SKIP LOCKED`. Stock-level checks recent orders. Each transaction uses
an independent `Database.Begin()` session, with deadlock detection enabled.
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
stock updates accounted for much of the remaining work. The profile and query
latencies are `/tmp/godb/tpcc-final.cpu.pprof` and
`/tmp/godb/tpcc-final-query-timings.json`.
