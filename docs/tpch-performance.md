# TPC-H execution performance, 2026-10-11

SlopDB's analytical executor before the execution improvements was much slower than either reference
engine, even at a small development scale. Nine of 22 queries completed;
thirteen exceeded a five-second deadline. All completed SlopDB results matched
SQLite, while DuckDB completed all 22 queries and matched SQLite and native
TPC-H answers. These measurements point to join and subquery execution, with
substantial row-map allocation overhead.

The subsequent changes and verified complete runs are documented in [TPC-H execution improvements](tpch-optimization.md).

## Workload and verification

The [`cmd/tpch` runner](../cmd/tpch/README.md) loads genuine DBGEN files, executes
all 22 query shapes, checks persisted row counts, exports complete result sets,
and captures per-query time, process CPU, allocations, and optional profiles.
The DuckDB Go baseline uses `github.com/duckdb/duckdb-go/v2 v2.10506.0`, with
DuckDB v1.5.6, in its own module.

Tests used a four-core ARM64 Neoverse-N1 host, Linux, SlopDB built with Go 1.22.2,
and the DuckDB runner built with Go 1.24.0. SQLite was v3.45.1. Engines were run
sequentially. Both Go runners used fresh on-disk databases, reopened after
loading and indexing, and prepared statements. SlopDB's WAL was enabled without
WAL fsync. The read-only measurement window excludes loading, preparing,
EXPLAIN, schema setup, and close/reopen; no workload writes require WAL fsync.
DuckDB had one execution thread and a 1GB memory limit. SQLite's independent
reference database was in memory, with the same indexes; those timings are
illustrative rather than a comparison of identical storage configurations.

SF=0.001 contained region=5, nation=25, supplier=10, customer=150, part=200,
partsupp=800, orders=1,500, and lineitem=6,005 rows. All eight row counts passed
after reopening both disk databases. Query definitions, input hashes, exact
settings, engine/toolchain versions, and output rows are saved with the results.

The main run measured one execution per query with no explicit warmups.
Cross-query cache effects and short-query measurement noise apply. Timeout
latencies are lower bounds on completion time. This is a TPC-H development
workload at fractional scale, with documented SQL adaptations and fixed
qualification parameters, not a certified QphH result. Several queries return
empty sets at this scale; validation also used SF=0.01, where every native query
has nonempty output.

The Python verifier checked all 22 translated query outputs against DuckDB's
native TPC-H qualification queries, on both datasets. It also checked Go
DuckDB's outputs against SQLite, including ORDER BY keys, numeric tolerances,
and preservation of duplicate rows. All nine completed SlopDB queries matched
at SF=0.001. A minimum-size DBGEN smoke dataset ran all 22 SlopDB queries twice
without errors and matched the references; because many outputs are empty,
this does not establish full-scale correctness for the timed-out queries.

## Per-query results at SF=0.001

All elapsed times are milliseconds. Allocation volume is cumulative Go bytes
allocated during the SlopDB execution, not peak live memory. Native DuckDB
allocations are not measured by Go's allocation counters and are not compared
with them.

| Query | SlopDB ms | DuckDB, 1 thread ms | SQLite ms | SlopDB allocated MiB |
| --- | ---: | ---: | ---: | ---: |
| Q01 Pricing summary | 69.12 | 4.50 | 7.25 | 63 |
| Q02 Minimum cost supplier | 744.09 | 4.81 | 0.22 | 1209 |
| Q03 Shipping priority | >5,000 (timeout) | 3.44 | 0.44 | 7340 |
| Q04 Order priority | 1257.89 | 1.62 | 0.16 | 1023 |
| Q05 Local supplier volume | >5,000 (timeout) | 1.57 | 0.48 | 7340 |
| Q06 Forecasting revenue | 22.06 | 0.38 | 0.40 | 21 |
| Q07 Volume shipping | >5,000 (timeout) | 2.19 | 2.09 | 7612 |
| Q08 National market share | >5,000 (timeout) | 3.63 | 0.26 | 5454 |
| Q09 Product type profit | >5,000 (timeout) | 3.17 | 1.45 | 5455 |
| Q10 Returned item reporting | >5,000 (timeout) | 2.58 | 0.56 | 7340 |
| Q11 Important stock identification | 108.70 | 1.00 | 0.28 | 152 |
| Q12 Shipping modes | >5,000 (timeout) | 2.31 | 0.52 | 5550 |
| Q13 Customer distribution | 1087.55 | 1.00 | 1.05 | 1425 |
| Q14 Promotion effect | >5,000 (timeout) | 0.69 | 0.14 | 5915 |
| Q15 Top supplier | 253.62 | 1.09 | 0.41 | 237 |
| Q16 Parts supplier relationship | 648.25 | 2.31 | 0.34 | 983 |
| Q17 Small quantity order revenue | >5,000 (timeout) | 0.64 | 0.08 | 5907 |
| Q18 Large volume customer | >5,000 (timeout) | 2.12 | 1.60 | 7194 |
| Q19 Discounted revenue | >5,000 (timeout) | 0.92 | 0.24 | 5933 |
| Q20 Potential part promotion | >5,000 (timeout) | 1.61 | 0.15 | 3449 |
| Q21 Suppliers who kept orders waiting | >5,000 (timeout) | 1.44 | 3.53 | 7588 |
| Q22 Global sales opportunity | 86.02 | 1.93 | 0.23 | 56 |

DuckDB's total execution time across all 22 queries was 44.93 ms. SlopDB spent
69.49 seconds before returning nine answers and canceling thirteen queries;
that is not a completed-suite runtime. There is consequently no valid complete
SlopDB suite score or direct total-runtime speedup to report. Its peak process
RSS was 171 MiB and its cumulative allocation volume was approximately 85 GiB
across the query executions: allocation churn, not an 85 GiB live dataset.
Its loaded database occupied 3.16 MiB; DuckDB's occupied 8.01 MiB.

A separate DuckDB four-thread run completed in 45.88 ms of total query execution,
versus 44.93 ms with one thread. This dataset is too small to demonstrate a
parallelism benefit. The profiles discussed below come from a separate SlopDB
run so profiler overhead does not affect the primary timings.

## Why the queries are slow

### Nested-loop joins copy wide maps before checking equality

[`executeJoins`](../sql/executor.go) scans and materializes every joined input,
loops over each left/right pair, calls `combineRowsWithAlias`, then evaluates
ON. It creates a new map and copies both rows for every candidate pair,
including the pairs immediately rejected by equality predicates. Joined-side
primary keys and secondary indexes are not probed by this path. WHERE filtering
occurs after the joins, so a selective customer segment, date, nation, or part
predicate often does not reduce intermediate work.

For Q3, customer-to-orders considers 150 × 1,500 pairs; the subsequent
orders-to-lineitem stage can consider 1,500 × 6,005 = 9,007,500 pairs before
applying the remaining predicates. Q3 allocated approximately 7.2 GiB during
five seconds without finishing. A separate CPU profile attributed 63.2% of
samples cumulatively to row combination and 31.7% to background GC workers.
Q9 showed 65.8% in row combination and 28.1% in background GC workers. Much of
the CPU is moving/hash-indexing column values and tracing objects that never
contribute to an answer.

DuckDB's saved Q3 physical plan uses two HASH_JOIN operators and pushes
segment/date predicates and column projections into its input scans. Q9 uses
five HASH_JOIN operators. These plans show concrete alternatives to SlopDB's
candidate-pair row copying; their displayed row counts are estimates, not
measured scan counters.

This explains the multi-table timeouts in Q3/Q5/Q7/Q8/Q9/Q10/Q12/Q14/Q17/Q18/
Q19/Q21. Source inspection establishes the candidate-pair algorithm; the
profiles measure the corresponding work. They do not count actual pairs.

### Correlated subqueries repeatedly materialize the inner relation

`evalSubquery` calls SELECT with an outer row, and current access planning
extracts constant literal predicates rather than parameterized join keys from
that row. EXISTS obtains a complete inner result and then tests its length;
it does not stop at the first qualifying row.

Q4 should test whether each qualifying order has any late-receipt line.
It instead repeatedly scans and decodes lineitem rows. It took 1,258 ms and
allocated approximately 1,023 MiB for a five-row answer. Its profile placed
75.3% of cumulative CPU in subquery evaluation, 63.3% in table scanning, and
41.0% in row decoding. These cumulative figures overlap and must not be added.

Q20 combines nested IN membership tests and a correlated quantity aggregate.
It timed out after allocating approximately 3.4 GiB in more than 30 million
objects. Its profile placed 79.3% of sampled CPU in subquery evaluation and
46.9% in table scanning. Re-evaluating uncorrelated subqueries for each outer
row also compounds the work. DuckDB's Q4/Q20 plans include SEMI joins and delimiter joins for their
subqueries, avoiding SlopDB's independent inner SELECT for every outer row.
Q15's revenue view is recomputed as part of its
join and maximum-revenue subquery.

### Scans and aggregates materialize rows and projected copies

Even without joins, the current scan decodes rows into maps and the SELECT
pipeline retains/materializes rows before aggregation or projection. Q1's
four-row aggregate answer allocated 62.6 MiB at 6,005 line items; Q6's
one-row answer allocated 21.1 MiB. Eliminating parsing alone cannot remove
these execution costs. Prepared statements were already enabled for all
measured SlopDB executions.

## Scan scaling at SF=0.01

A larger dataset contained 60,175 line items and 15,000 orders. SlopDB's
scan checks and the DuckDB comparison used one warmup followed by three
executions per query. The figures below are medians. SQLite remains a
single-run in-memory reference.

| Query | SlopDB ms | DuckDB, 1 thread ms | SQLite ms | SlopDB/DuckDB |
| --- | ---: | ---: | ---: | ---: |
| Q1 pricing summary | 678.76 | 3.95 | 94.08 | 172× |
| Q6 forecasting revenue | 258.87 | 1.15 | 5.87 | 225× |

SlopDB's Q1 allocated approximately 676 MiB per execution for four output rows;
Q6 allocated approximately 216 MiB for one output row. Relative to the smaller
single-execution run, time and allocation volume increase roughly with input
size. These two measurements use different warmup/repetition settings, so
precise scaling ratios should not be interpreted as a fitted model.

DuckDB completed and passed reference comparisons for all 22 larger queries,
with three executions of each and no failures or timeouts. The sum of its
per-query medians was 67.40 ms; that sum is not a separately measured suite
sample. The larger SlopDB run covered Q1/Q6 only; other larger SlopDB query
times are unmeasured.

## Optimization priorities

1. Add equality hash joins or index-driven nested-loop joins, choose join order,
   and push applicable filters into each input before joining. Avoid creating
   a combined map for rejected pairs. These changes target the dominant
   multi-table cost rather than adding more indexes unused by join execution.
2. Add statement-scoped caching of uncorrelated subqueries; parameterized index
   probes, semijoins/anti-joins, early-exit EXISTS, and grouping/decorrelation for
   correlated aggregates. Preserve NULL behavior, duplicate semantics, and the
   current statement snapshot.
3. Scan only needed columns and stream filters/aggregates through compact typed
   rows. Construct output maps after filtering/grouping. This targets scan
   allocation volume and garbage collection independently of join planning.
4. Extend EXPLAIN with actual joins, subquery operators, rows read/emitted, and
   timing/allocation counters so future regressions can be attributed directly.

The existing TPC-C optimizations accelerate primary-key lookups, bounded index
ranges, and point updates. TPC-H exposes general joins and large aggregates,
which use different executor paths. SQL parsing, WAL fsync, transaction row
contention, and adding workers are not the primary costs demonstrated here.

## Saved evidence and reproduction

- [SlopDB SF=0.001](benchmarks/tpch/sf001-slopdb.json),
  [DuckDB one thread](benchmarks/tpch/sf001-duckdb.json),
  [DuckDB four threads](benchmarks/tpch/sf001-duckdb-threads4.json), and
  [SQLite/native verification](benchmarks/tpch/sf001-reference.json).
- [SlopDB larger scans](benchmarks/tpch/sf01-slopdb-scans.json),
  [DuckDB SF=0.01](benchmarks/tpch/sf01-duckdb.json), and
  [larger reference verification](benchmarks/tpch/sf01-reference.json).
- Separate per-query CPU profiles are under `/tmp/godb/tpch-profiles/`; diagnostic
  settings and timings are in `/tmp/godb/tpch-profile-results.json`.

Run commands, schemas, indexes, generator requirements, and SQL translations are
in the [benchmark README](../cmd/tpch/README.md). Input hashes and counts in the
reports permit checking regenerated data before comparing results. Full reports
retain their original local data/database paths for provenance.
