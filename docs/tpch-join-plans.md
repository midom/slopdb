# Indexed joins, selective execution, and allocation improvements

The additional execution changes reduce the sum of 22 TPC-H query medians at SF=0.01 from 5.057 seconds to 0.892 seconds: **5.67x faster**. Allocated bytes for one execution of each query fall from 2,534.5 MiB to 235.8 MiB, a 90.7% reduction. All 22 answers match SQLite and DuckDB at SF=0.001 and SF=0.01. These results extend the [earlier execution improvements](tpch-optimization.md).

## Problems addressed

- Fixed join order and large unfiltered inputs: INNER/CROSS joins now choose a connected order using ANALYZE counts, index bounds, and small filtered input cardinalities. The planner considers every starting relation and refines estimates after inspecting selective inputs. LEFT joins preserve their original order and ON/WHERE stages.
- Scanning an entire relation for a few matches: join readers probe clustered primary keys, composite prefixes, and secondary indexes. Larger inputs can use hash joins when repeated probes would cost more. Safe numeric key equivalence, duplicate matches, and NULL behavior are preserved.
- OR predicates preventing index use: bounded AND/OR expansion produces multiple access paths, deduplicates overlapping row keys, and derives necessary local filters even across multi-table OR branches. Every access path retains full residual predicate evaluation.
- Correlated rescans: exact typed outer comparisons bind into execution-local predicates across joined subqueries. Indexed aggregation streams matching rows; EXISTS stops at its first qualifying row without evaluating unused projection expressions. Uncorrelated IN sets can drive indexed access instead of scanning every candidate row.
- Wide intermediate row maps and allocation pressure: joins share a scratch row, project before retaining results, and feed aggregate accumulators directly. Groups keep only required representative values. Scans decode referenced columns and batch secondary-index base-row reads.
- Repeated LIKE allocation: compiled patterns are reused within a statement, and wildcard matching avoids allocating a dynamic-programming matrix for every row. Unicode underscores, ASCII case folding, ESCAPE, and newline matching retain SQL behavior.

Statement-local point-read caches are bounded by 4,096 entries and 16 MiB of raw keys/values. Correlated-result and join-probe caches also cap retained rows at 8,192. Cache cleanup runs after statements, including errors; prepared queries, transaction writes, and rollback do not reuse stale results. Volatile expressions, uncertain scopes, derived join inputs, USING/NATURAL, and locking reads retain established fallback paths. EXPLAIN now reports eligible join order and available index access; runtime cardinality can change the final choice between probes and hashing.

## Controlled TPC-H comparison

Both SlopDB binaries use Go 1.22.2 on the same four-core Linux/ARM64 host, identical DBGEN inputs and indexes, prepared statements, fresh disk databases reopened after loading, one warmup and three measured executions per query. WAL is enabled and WAL fsync disabled. Loading, ANALYZE, preparation, EXPLAIN, and reopening are outside each query timing. All 66 measured executions succeed at each tested scale. SF=0.01 has 60,175 lineitems and 15,000 orders.

| Query | SlopDB before ms | SlopDB after ms | Speedup | DuckDB ms | SQLite ms |
| --- | ---: | ---: | ---: | ---: | ---: |
| Q01 | 149.13 | 154.25 | 0.97x | 3.95 | 92.08 |
| Q02 | 280.43 | 1.47 | 191.29x | 3.03 | 0.41 |
| Q03 | 202.79 | 80.57 | 2.52x | 2.90 | 7.29 |
| Q04 | 155.83 | 20.27 | 7.69x | 3.03 | 1.49 |
| Q05 | 194.80 | 64.69 | 3.01x | 3.38 | 3.55 |
| Q06 | 88.77 | 41.79 | 2.12x | 1.15 | 5.85 |
| Q07 | 658.25 | 38.05 | 17.30x | 3.26 | 30.33 |
| Q08 | 157.07 | 11.66 | 13.48x | 3.43 | 0.85 |
| Q09 | 318.35 | 103.57 | 3.07x | 4.65 | 12.65 |
| Q10 | 210.66 | 30.43 | 6.92x | 4.78 | 4.37 |
| Q11 | 87.34 | 4.90 | 17.82x | 3.07 | 1.10 |
| Q12 | 212.63 | 74.02 | 2.87x | 2.54 | 6.75 |
| Q13 | 196.92 | 77.13 | 2.55x | 3.06 | 11.35 |
| Q14 | 249.53 | 8.82 | 28.30x | 1.58 | 0.95 |
| Q15 | 149.74 | 16.07 | 9.32x | 1.81 | 4.29 |
| Q16 | 48.08 | 19.29 | 2.49x | 2.12 | 2.06 |
| Q17 | 209.23 | 2.22 | 94.41x | 0.72 | 0.23 |
| Q18 | 232.46 | 89.99 | 2.58x | 4.82 | 15.25 |
| Q19 | 700.40 | 3.88 | 180.34x | 2.25 | 0.82 |
| Q20 | 67.34 | 7.87 | 8.56x | 3.26 | 0.82 |
| Q21 | 457.99 | 31.32 | 14.62x | 7.02 | 1.48 |
| Q22 | 29.32 | 9.52 | 3.08x | 1.57 | 1.02 |

The DuckDB column uses the existing same-input Go-driver run with one native execution thread. SQLite is the new in-memory Python correctness-reference run, one execution per query. Its timing is useful for identifying execution gaps but differs in storage and timing methodology. DuckDB totals 67.40 ms; current SlopDB remains 13.2x slower by the sum of query medians. No new DuckDB or SQLite executor changes are included.

Q02 improves through selective part filtering and indexed supplier/partsupp probes. Q17 combines a selective outer join with indexed correlated AVG. Q19 distributes its OR branches into narrow inputs and probes matching lineitems. Q14 bounds the date range before joining parts. Q21 uses selective join order and indexed EXISTS/NOT EXISTS probes. Q07/Q09 avoid retaining wide maps from their derived queries. Q13 benefits from allocation-free LIKE matching and streamed grouping.

Q01, a broad scan and aggregation, is 3.4% slower in this pair of runs; these changes do not improve every query. Scan-heavy Q01/Q06/Q18, remaining map/expression work, and general correlated-query decorrelation still leave substantial headroom. This is a bounded heuristic planner, without histograms, exhaustive plan search, columnar/vectorized execution, or spillable aggregation. These sums are independently measured query medians, not suite wall time or certified QphH.

## TPC-C regression measurement

Fresh sequential runs use 10 concurrent workers for 60 seconds, the same default scale, seed 42, prepared statements, WAL enabled, and **WAL fsync disabled**. Loading and consistency validation are excluded.

| Metric | Before | After |
| --- | ---: | ---: |
| Committed transactions | 130,663 | 129,613 |
| Transactions/sec | 2,177.70 | 2,160.18 |
| Mean latency ms | 4.568 | 4.606 |
| P95 latency ms | 15.084 | 14.982 |
| P99 latency ms | 24.275 | 24.321 |
| Unexpected failures / retries | 0 / 0 | 0 / 0 |
| Checks after run / reopen | 15/15 / 15/15 | 15/15 / 15/15 |

Throughput differs by -0.80% in this single pair of runs. The before/after runs respectively record 567 / 566 intentional invalid-item rollbacks and 10 / 10 shutdown cancellations, separately from failures. This workload remains around 2,200 committed transactions/sec; the measurements do not establish statistical equivalence or a compliant TPC-C score.

## Correctness and reproduction

The regression suites now live in the repository and run with `go test ./...`
and `go test -race ./...`; see [Testing](../TESTING.md) for their locations,
reference fixtures and selective commands.

The new SQLite differential suite passes 3,594 checks across fresh and reopened databases: INNER/LEFT/CROSS joins, mixed outer joins, grouping, NULLs, overlapping OR paths, raw TEXT/BLOB index delimiters, composite keys, correlated aggregates, IN sets, EXISTS with 64-bit pagination, and randomized Unicode/ESCAPE LIKE patterns. Another 319 checks pass with the race detector for prepared joins, transaction/savepoint changes, cache invalidation, indexed DML, validation errors, concurrency, and cancellation.

Existing validation also passes: 599 SQL differential cases, 98 execution cases, 16 cache/numeric cases, 134 SQL semantics checks, 302 tuple-plan comparisons, 177 indexed-read cases, 45 read/view checks, primary-key locking checks, 2,071 prepared API checks with the race detector, and 4,000 concurrent indexed MVCC updates with the race detector. Build and vet pass.

Generate DBGEN inputs in `/tmp/slopdb-bench/tpch-sf01` as described in the
[benchmark README](../cmd/tpch/README.md), then run:

```sh
mkdir -p /tmp/slopdb-bench
go build -o slopdb .
go vet ./...
go build -o /tmp/slopdb-bench/slopdb-tpch-joins ./cmd/tpch
/tmp/slopdb-bench/slopdb-tpch-joins -data /tmp/slopdb-bench/tpch-sf01 -timeout 30s \
  -warmups 1 -runs 3 -json /tmp/slopdb-bench/tpch-after-joins-final.json
python3 cmd/tpch/verify.py --data /tmp/slopdb-bench/tpch-sf01 \
  --report /tmp/slopdb-bench/tpch-after-joins-final.json \
  --duckdb-report docs/benchmarks/tpch/sf01-duckdb.json

go run ./cmd/tpcc -workers 10 -duration 1m \
  -json /tmp/slopdb-bench/tpcc-after-joins-final.json
```

Raw reports: [TPC-H before](benchmarks/tpch/sf01-before-join-plans.json), [TPC-H after](benchmarks/tpch/sf01-join-plans.json), [SF=0.01 verification](benchmarks/tpch/sf01-join-plans-reference.json), [SF=0.001 execution](benchmarks/tpch/sf001-join-plans.json), [SF=0.001 verification](benchmarks/tpch/sf001-join-plans-reference.json), [calculated comparison](benchmarks/tpch/join-plan-comparison.json), [TPC-C before](benchmarks/tpcc/before-join-plans.json), and [TPC-C after](benchmarks/tpcc/after-join-plans.json).
