# TPC-H execution improvements, 2026-10-11

This report records the first analytical improvements. The subsequent
[join execution report](tpch-join-plans.md) contains the current measurements,
indexed joins, join reordering, OR access paths, and streaming subquery changes.

All 22 query shapes now complete and match SQLite and DuckDB at both SF=0.001 and SF=0.01. At SF=0.001, their combined execution time is 0.471 seconds. The previous executor completed nine queries and timed out on thirteen after spending 69.49 seconds across those attempts. This is a completed workload after the changes; the older elapsed time is not a completed-suite score.

## Changes

- Equality joins build hash buckets and combine only matching rows. Composite equality keys preserve duplicates, NULL behavior, exact int64 comparisons, integral REAL equivalence, and LEFT JOIN unmatched rows. Other join predicates retain the nested loop fallback.
- Deterministic local predicates run before joins. Uncorrelated deterministic subqueries can move with those predicates; correlated work remains after selective joins. Locking queries retain physical row origins.
- Physical join inputs decode only referenced columns, including outer references inside nested subqueries. Unused TEXT/BLOB fields are skipped without copying. Whole-row scans no longer retain a second key-to-row map.
- Subquery results and IN membership sets are reused within one root SELECT, keyed by lexically resolved outer values. Views are included in dependency analysis. Volatile functions, locking reads, and uncertain scopes disable reuse. Caches clear when SELECT finishes, including on errors, and correlated result caches are capped at 4,096 entries per subquery.
- Exact typed outer comparisons become execution-local literals so clustered and secondary indexes can bound correlated reads. Indexed equality conjuncts are usable within AND predicates; the complete residual predicate still executes.
- Ordinary full-scan aggregation keeps one scratch row and per-group accumulators. It decodes only referenced columns and preserves DISTINCT, empty groups, NULLs, HAVING, integer SUM overflow, mixed integer/REAL SUM, and ordering. Selective index plans and locking queries retain their existing paths.

## SF=0.001 comparison

Both SlopDB runs use the same data, indexes, prepared queries, one measured execution, no explicit warmup, and a five-second deadline. Loading, EXPLAIN, preparation, and reopening are excluded. WAL is enabled with fsync disabled. Baseline timeout values are completion-time lower bounds.

| Query | Previous ms | Current ms | Current allocated MiB |
| --- | ---: | ---: | ---: |
| Q01 Pricing summary | 69.12 | 14.95 | 2.10 |
| Q02 Minimum cost supplier | 744.09 | 1.71 | 1.11 |
| Q03 Shipping priority | >5,000 (timeout) | 19.67 | 8.73 |
| Q04 Order priority | 1257.89 | 15.68 | 10.87 |
| Q05 Local supplier volume | >5,000 (timeout) | 18.42 | 10.80 |
| Q06 Forecasting revenue | 22.06 | 8.54 | 0.81 |
| Q07 Volume shipping | >5,000 (timeout) | 63.83 | 41.48 |
| Q08 National market share | >5,000 (timeout) | 15.19 | 8.43 |
| Q09 Product type profit | >5,000 (timeout) | 36.12 | 26.33 |
| Q10 Returned item reporting | >5,000 (timeout) | 21.48 | 10.87 |
| Q11 Important stock identification | 108.70 | 4.22 | 1.82 |
| Q12 Shipping modes | >5,000 (timeout) | 21.10 | 8.20 |
| Q13 Customer distribution | 1087.55 | 20.19 | 10.72 |
| Q14 Promotion effect | >5,000 (timeout) | 25.12 | 8.29 |
| Q15 Top supplier | 253.62 | 14.49 | 1.46 |
| Q16 Parts supplier relationship | 648.25 | 5.20 | 2.15 |
| Q17 Small quantity order revenue | >5,000 (timeout) | 20.95 | 7.85 |
| Q18 Large volume customer | >5,000 (timeout) | 22.68 | 10.39 |
| Q19 Discounted revenue | >5,000 (timeout) | 74.62 | 38.08 |
| Q20 Potential part promotion | >5,000 (timeout) | 7.61 | 5.74 |
| Q21 Suppliers who kept orders waiting | >5,000 (timeout) | 36.33 | 19.73 |
| Q22 Global sales opportunity | 86.02 | 2.65 | 1.54 |

The current 22 executions allocated 237.5 MiB in total, versus approximately 85.2 GiB across the previous timed and canceled attempts. These are cumulative allocations, not peak live memory. DuckDB's earlier one-thread run completed in 44.93 ms; SlopDB remains about 10.5 times slower at this small scale. SQLite's in-memory timings are saved with the correctness report.

## SF=0.01 scaling

This dataset contains 60,175 lineitems, 15,000 orders, 2,000 parts, and 1,500 customers. All 22 queries return nonempty output, and all 66 measured executions succeeded. Each query has one warmup and three measured runs; the table reports medians. DuckDB uses one native execution thread. SQLite is an in-memory correctness reference, so its storage setup differs.

| Query | SlopDB ms | DuckDB ms |
| --- | ---: | ---: |
| Q01 | 147.96 | 3.95 |
| Q02 | 284.98 | 3.03 |
| Q03 | 203.33 | 2.90 |
| Q04 | 155.93 | 3.03 |
| Q05 | 196.28 | 3.38 |
| Q06 | 89.77 | 1.15 |
| Q07 | 663.64 | 3.26 |
| Q08 | 155.62 | 3.43 |
| Q09 | 316.82 | 4.65 |
| Q10 | 213.82 | 4.78 |
| Q11 | 89.05 | 3.07 |
| Q12 | 216.97 | 2.54 |
| Q13 | 201.04 | 3.06 |
| Q14 | 250.65 | 1.58 |
| Q15 | 150.24 | 1.81 |
| Q16 | 48.36 | 2.12 |
| Q17 | 204.59 | 0.72 |
| Q18 | 232.78 | 4.82 |
| Q19 | 708.40 | 2.25 |
| Q20 | 72.12 | 3.26 |
| Q21 | 459.11 | 7.02 |
| Q22 | 28.93 | 1.57 |

The sum of query medians is 5.090 seconds for SlopDB and 67.40 ms for DuckDB: a remaining 75.5x gap. This is a sum of independently measured medians, not a timed suite execution or QphH score. Before these changes, only Q1 and Q6 were measured at this scale: their medians were 678.76 ms and 258.87 ms, respectively.

An intermediate run exposed repeated revenue-view scans in Q15: statement-local reuse reduced it from about 7.5 seconds to roughly 150 ms. A Q21 profile still showed substantial map copying and garbage collection; projecting its join inputs, including correctly handling SELECT * inside EXISTS, reduced its median from about 1.1 seconds to about 460 ms.

## Transactional regression check

Fresh sequential prepared TPC-C-style runs used ten workers, one minute, the same default scale and seed, WAL enabled, and WAL fsync disabled. The baseline binary was built before these analytical changes. Loading, preparation, and validation were outside the measurement window.

| Metric | Before | After |
| --- | ---: | ---: |
| Committed transactions | 132,148 | 131,186 |
| Transactions/sec | 2,202.46 | 2,186.43 |
| Mean latency ms | 4.518 | 4.551 |
| P95 latency ms | 15.088 | 15.147 |
| Unexpected failures | 0 | 0 |
| Retries | 0 | 0 |
| Consistency checks, after run and reopen | 15/15 each | 15/15 each |

Throughput differed by 0.73% in this single pair of runs; the analytical changes preserve approximately 2,200 transactions/sec on this workload. This pair is not enough to establish statistical equivalence. Raw reports: [before](benchmarks/tpcc/before-analytical-changes.json), [after](benchmarks/tpcc/after-analytical-changes.json).

## Validation and remaining work

The verifier checked complete outputs against SQLite and Go DuckDB, including duplicate rows, ordering with ties, exact integers, and floating-point tolerances. Query translations also matched native DuckDB TPC-H answers at both scales. The final large run covers nonempty results for all 22 queries.

Execution checks passed: 599 existing SQL differential cases; 98 new join/group/subquery cases against SQLite; 16 cache invalidation, transaction, volatility, SUM overflow, and exact numeric checks; 134 SQL semantics checks; 302 tuple-plan comparisons; 177 indexed read checks; 45 read/qualification/view checks; and 2,071 prepared API checks with the race detector. Primary-key locking and indexed-update checks passed. `go build -o slopdb .` and `go vet ./...` passed. The legacy four runners listed in AGENTS.md are absent from this workspace; the available review programs were used instead. New temporary programs follow the repository convention under `/tmp/godb`.

The remaining analytical gap is material. Equality joins still materialize Go row maps, join order is fixed, scans use per-row expression interpretation, and correlated execution has no general decorrelation planner. There is no columnar/vectorized execution, cost-based join reordering, spillable hash aggregation, or general semi-join rewrite. Non-equality joins retain quadratic fallback behavior. EXPLAIN does not yet describe join or subquery operators, so its baseline scan lines are insufficient to identify the complete executed plan.

## Reproduce and inspect

```sh
go build -o /tmp/godb/slopdb-tpch-optimized ./cmd/tpch
/tmp/godb/slopdb-tpch-optimized -data /tmp/godb/tpch-sf001 -timeout 5s -json /tmp/godb/tpch-performance-optimized-sf001.json
/tmp/godb/slopdb-tpch-optimized -data /tmp/godb/tpch-sf01 -timeout 30s -warmups 1 -runs 3 -json /tmp/godb/tpch-performance-optimized-sf01.json
python3 cmd/tpch/verify.py --data /tmp/godb/tpch-sf01 --report /tmp/godb/tpch-performance-optimized-sf01.json --duckdb-report /tmp/godb/tpch-duckdb-sf01.json
```

Raw reports: [small SlopDB](benchmarks/tpch/sf001-slopdb-optimized.json), [large SlopDB](benchmarks/tpch/sf01-slopdb-optimized.json), [small validation](benchmarks/tpch/sf001-optimized-reference.json), and [large validation](benchmarks/tpch/sf01-optimized-reference.json). Baseline engine comparisons and profile analysis remain in [TPC-H baseline performance](tpch-performance.md).
