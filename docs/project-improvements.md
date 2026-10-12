# Execution, APIs and SQL improvements

The parallel improvement branches are integrated into `main`. Each branch used
an isolated Git worktree, committed its implementation and permanent tests, and
was merged with its history intact. Additional integration tests cover prepared
CTE/window/UPSERT execution, CTE shadowing in optimized EXISTS, computed ordering
aliases in cursors, and high-fanout hash joins under small memory budgets.

| Area | Integrated behavior |
| --- | --- |
| Access paths | Shared bounds for scalar/composite keys, BETWEEN, constant expressions and finite IN/OR sets; bounded forward/reverse cursors and qualified Top-K predicates |
| Typed execution | Slot-bound, batched single-table aggregation for eligible deterministic expressions; ordinary execution remains available for other shapes |
| Actual profiling | EXPLAIN ANALYZE reports reads, probes, join candidates, lock waits, allocation deltas and spill work |
| Statistics | Persisted bounded samples, distinct estimates, NULL fractions, common values and histograms influence index/scan and join choices |
| Operator memory | Overflow-safe Top-K, external sorting, spill aggregation, hash buckets and deduplication with configurable working budgets |
| Prepared plans | Private reusable bindings and plan recipes, fresh parameter bounds and snapshot reads, catalog/statistics invalidation |
| Application APIs | Streaming rows, atomic bulk/CSV import, online committed-snapshot backup and integrity checking |
| Performance checks | Portable bounded-read invariants, microbenchmarks, repeatable workload matrices, resource/storage counters and CI artifacts |
| UPSERT | ON CONFLICT targets, DO NOTHING/DO UPDATE, excluded values, normal constraint/trigger/index/locking behavior |
| CTEs | Scoped/dependent WITH, UNION/UNION ALL recursion, DML and persisted views, configurable int64 recursion guardrails |
| Windows | Ranking, navigation and aggregate functions, named windows and ROWS/RANGE/GROUPS frames |

Feature details and restrictions are documented in
[typed execution](typed-execution.md), [profiling](query-profiling.md),
[statistics](query-statistics.md), [memory](query-memory.md),
[prepared statements](prepared-statements.md), [application operations](application-operations.md),
[performance checks](performance-regressions.md), [UPSERT](upsert.md),
[CTEs](common-table-expressions.md) and [windows](window-functions.md).

The default 64 MiB operator budget is a soft working-set estimate, not a process
memory cap. Exec output and some intermediate operators still materialize.
Windows reject budget exhaustion rather than spilling. Recursive queries and
UPSERT support the documented subset; unsupported shapes produce errors.
Statistics remain unchanged after DML until ANALYZE. Logical profiling bytes and
storage file counters do not measure physical device I/O.

[Paired benchmark results](benchmarks/improvements/README.md) preserve all twenty-two
TPC-H queries, TPC-C throughput, correctness evidence and measured revisions.
TPC-H total measured query time fell 34.8%; individual query regressions remain.
The single paired TPC-C runs measured 2,139.5 and 2,125.8 transactions/second.
