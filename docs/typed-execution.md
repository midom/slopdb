# Typed analytical execution

Single-table streaming aggregates now bind referenced columns to compact numeric
slots and compile supported expressions into a statement-local instruction
stream. Physical readers decode up to 128 rows into a reusable value batch;
filters, group keys and aggregate arguments use slots instead of string-keyed
row maps and recursive AST interpretation. This path runs automatically for
eligible queries, including the TPC-H Q1 and Q6 shapes.

The existing physical reader supplies full scans, bounded primary/secondary
access, point lookups, covering payloads and deduplicated OR branches. Both
execution modes share snapshot/transaction-overlay handling, index checks,
iterator closure, cancellation and aggregate state transitions. Representative
values are retained once per group; public result maps are built after
aggregation. Unused variable-width columns are skipped without constructing
strings or BLOBs. Batch values own decoded text/BLOB contents before reader
buffers can be reused.

Compiled expressions support literals, bound parameters, column references,
arithmetic, comparisons with ordinary affinity rules, AND/OR, BETWEEN/NOT
BETWEEN, IS/IS NOT, unary signs/NOT and concatenation. Boolean instructions jump
over short-circuited branches. SQL Values retain exact int64, REAL, NULL, TEXT
and BLOB representations; arithmetic and comparisons share the interpreted
executor's helpers. Aggregate DISTINCT, integer overflow, numeric errors and
empty-input results use the same aggregate states as before.

Unsupported expressions, including scalar functions, CASE, IN and subqueries,
retain the general executor. Joins, ordinary nonaggregate projections and final
group projection/HAVING/ORDER BY continue using their existing execution paths.
Eligible prepared statements reuse the compiled instruction structure; ordinary
statements compile during execution. Registers belong to each private execution
tree and are not shared concurrently. The typed kernels do not add SIMD or an
on-disk columnar format. Operator spilling is described separately in
[query memory](query-memory.md).

## Verification and kernel benchmarks

The permanent tests cover independently expected expression results, 2,200
interpreted-versus-compiled value combinations, skipped columns and buffer
ownership. End-to-end tests cover multiple batches and a partial tail, Q1/Q6
results from independent arithmetic, grouping, aliases, DISTINCT, BLOB extrema,
full/indexed/overlapping access, prepared bindings, transaction overlays,
rollback, cancellation, reopen, empty input and numeric errors. Existing SQL,
execution and join reference suites also exercise the enabled path.

```sh
go test ./sql -run '^TestTyped'
go test ./tests/regression -run '^TestTypedAggregateExecution$'
go test ./sql -run '^$' -bench 'BenchmarkTypedQ[16]Kernel' -benchmem -count=3
```

An ARM64 development run measured these medians per encoded input row:

| Kernel | Map/AST | Typed batches | Allocated bytes | Allocations |
| --- | ---: | ---: | ---: | ---: |
| Q6 predicate and SUM argument | 1,497 ns | 818 ns | 176 → 16 | 3 → 1 |
| Q1 predicate, group-key encoding and seven aggregates | 1,660 ns | 1,339 ns | 32 → 16 | 3 → 1 |

These are execution kernels over identical encoded inputs, not complete TPC-H
query scores. Setup/compilation, storage I/O and group-table lookups are outside
the timed kernels. Concurrent development jobs caused substantial timing
variance, especially in Q1 (1,061–2,431 ns for typed batches); allocation counts
were stable. Run controlled complete-query benchmarks before extrapolating
these timings to a dataset or throughput claim.
