# Tests

Run from the repository root:

```sh
go build -o slopdb .
go test ./...
go test -race ./...
go vet ./...
```

The Go tests include the regression and functionality checks previously kept as
standalone programs. They use isolated temporary directories, close databases on
failure, and work from a fresh checkout.
Concurrent invocations do not share database paths. Test names can be selected
with the normal `-run` flag:

```sh
go test ./tests/regression -run 'Test(Join|Prepared|SQL|Execution)' -v
go test -race ./tests/regression -run 'Test(Concurrent|IndexedMVCC|BatchTransactions|ForeignKey)' -v
go test ./sql -run TestMalformedParserInputs
go test ./sql -run '^$' -fuzz FuzzParse -fuzztime 30s
```

## Coverage

| Area | Repository tests | Original programs |
| --- | --- | --- |
| Prepared API, typed binding, cancellation, ownership, 64-bit pagination, concurrent reuse | `tests/regression/prepared_statements_test.go`, `db/result_test.go` | `prepared_statement_review.go`, `covering_ownership_review.go` |
| SQL constraints, NULLs, foreign keys, validation and numeric semantics | `tests/regression/sql_standard_test.go`, `legacy_primary_keys_test.go` | `sql_standard_review.go`, `legacy_primary_review.go` |
| SQL execution, grouping, joins, subqueries, OR paths, LIKE and index boundaries | `tests/regression/sql_differential_test.go`, `testdata/` | `sql_differential_review.go`, `performance_execution_review.go`, `join_optimization_review.go` |
| Statement caches, prepared joins, transactions, savepoints and changing index keys | `tests/regression/execution_state_test.go`, `join_state_test.go` | `performance_state_review.go`, `join_state_review.go` |
| Read plans, projection, views and indexed/scan equivalence | `tests/regression/read_executor_test.go`, `indexed_read_plans_test.go`, `tuple_read_plans_test.go` | `standard_read_executor_review.go`, `standard_read_plan_strict_review.go`, `tuple_plan_review.go` |
| Inline/composite indexes, uniqueness, ALTER, dump/restore and diagnostic repair | `tests/regression/inline_indexes_test.go` | `standard_inline_indexes_review.go` |
| Stable object IDs, catalog-only renames, rollback/name reuse and interrupted legacy migration | `db/object_ids_test.go`, `sql/object_ids_test.go` | Compares physical keys, values and sequence numbers; verifies reopen and migration phases |
| Concurrent renames, prepared-plan name reuse, pinned point readers, gate exclusion and narrow deadlock detection | `db/catalog_concurrency_test.go`, `db/execution_gate_test.go` | Exact update totals, explicit waiter barriers and unrelated-shard isolation |
| Extended ALTER, named constraints, type conversion, dependency cascade and crash atomicity | `tests/regression/alter_extended_test.go` | Column/index/view actions, failed multi-action rollback, SQL dump roundtrip and committed/uncommitted WAL recovery |
| SHOW/DESCRIBE, catalog metadata, table-status estimates and persistence | `tests/regression/show_and_describe_test.go`, `show_table_status_test.go`, `storage/range_estimate_test.go` | `show_test_runner.go`; bounded block sampling, compression, MVCC and concurrent estimates |
| API concurrency, context cancellation and read corruption | `tests/regression/database_api_test.go` | `api_cache_diagnostics_review.go` |
| Transactions, locking, snapshot isolation, atomic publication and foreign-key races | `tests/regression/batch_transactions_test.go`, `concurrent_execution_test.go`, `indexed_mvcc_test.go`, `primary_key_locking_test.go`, `foreign_concurrency_test.go` | `standard_batch_transactions_review.go`, `standard_concurrent_execution_review.go`, `standard_indexed_mvcc_review.go`, `performance_lock_review.go`, `foreign_concurrency_review.go` |
| Randomized storage model, snapshots, tombstones, compaction and reopen | `tests/regression/storage_model_test.go` | `standard_current_storage_review.go` |
| Bounded/reverse iterators, long keys, batch reads and snapshot lifetime | `tests/regression/bounded_iterator_test.go`, `reverse_and_multiget_test.go`, `point_readers_test.go`, `snapshot_readers_test.go`, `snapshot_release_test.go` | Corresponding `bounded_iterator`, `reverse_multi`, `point_reader`, `snapshot_reader`, `snapshot_release` review programs |
| Concurrent memtables, atomic bulk/group commits, flush and shutdown | `tests/regression/atomic_memtable_test.go`, `bulk_memtable_test.go`, `group_commit_test.go`, `flush_test.go`, `db/shutdown_concurrency_test.go` | Corresponding review programs |
| SSTable metadata, filters, checksums, compressed blocks, corruption and compression policy | `tests/regression/sstable_metadata_test.go`, `compression_codecs_test.go`, `level_compression_test.go`, `bloom_quality_test.go`, `storage/bloom_test.go` | `current_metadata_review.go`, `current_codecs_review.go`, `current_level_compression_review.go`, `bloom_quality_review.go`; conservative unknown-filter checks from `bloom_compatibility_review.go` |
| WAL/no-fsync/no-WAL modes, abrupt-exit recovery, rollback, damaged files, write/Close errors | `tests/regression/recovery_test.go`, `durability_test.go` | `recovery_review.go`, `no_wal_review.go`, `no_wal_fsync_review.go` |
| CLI formatting, schema planning, atomic CSV import, backup/integrity commands and page-size options | `internal/cli/*_test.go`, `tests/regression/cli_page_size_test.go` | CLI unit tests live with the implementation; process tests build the root entry point |
| CLI EOF/exit/bail/SIGTERM persistence, `.open` and flush errors | `tests/regression/cli_linux_test.go`, compression CLI tests | `no_wal_cli_review.go` and the current compression review programs |
| Malformed parser input and fuzz seeds | `sql/parser_test.go` | `parser_review.go`; also covers the ALTER-column semicolon regression found while migrating |
| Optional MySQL quoting, string escapes, input splitting, SQL exports and persistence | `sql/mysql_quoting_test.go`, `internal/cli/mysql_quoting_test.go`, `tests/regression/mysql_quoting_test.go` | Tokenizer/parser cases, API sessions/transactions/preparation/streaming, dump restore, CLI import/read/open and default-mode compatibility |

The SQL reference tests execute 599 semantics queries, 98 execution queries, and
1,797 join/index/LIKE queries both before and after reopening: **4,291 reference
checks**. The parser tests also cover 15,000 deterministically generated inputs.
Other suites include 2,071 prepared API checks, 4,000 indexed concurrent updates,
32-worker atomic transaction checks, and 320,000 concurrent memtable insertions.
These are functional assertions; no test requires achieving a machine-dependent
throughput or latency score.

Additional suites cover CTEs and recursive guardrails, UPSERT constraints and
concurrency, 51 independent window reference queries, prepared-plan invalidation,
distribution statistics, actual EXPLAIN ANALYZE counters, bounded access,
operator spilling and cleanup, streaming cursor ordering, bulk import, online
backup and integrity checks. Cross-feature tests exercise prepared CTE/window/
UPSERT execution and lexical CTE scopes in optimized subqueries. See
[integrated improvements](docs/project-improvements.md) and
[performance checks](docs/performance-regressions.md).

Crash recovery uses a separate instance of the Go test executable that deliberately
exits without closing the database. CLI tests build the current CLI into a temporary
directory automatically, including `-race` when the test suite uses the race
detector. Linux terminal/SIGTERM checks use a pseudo-terminal. Actual fsync syscall
verification runs when Linux `strace` is installed; otherwise that additional test
is skipped. WAL recording, replay, durability modes and error checks still run
without `strace`.

The command-line implementation and its unit tests live in `internal/cli/`.
The root `main.go` delegates to that package, so plain `go build` from the
repository root still produces the `slopdb` executable.

## Reference fixtures

The SQLite answers and their setup SQL are checked into
[`tests/regression/testdata`](tests/regression/testdata). Running Go tests requires
neither Python nor SQLite/DuckDB installations. To independently recompute answers,
use Python's standard-library SQLite driver:

```sh
python3 tests/regression/testdata/regenerate_references.py
go test ./tests/regression -run 'Test(SQLDifferential|ExecutionDifferential|JoinDifferential)$'
```

To also regenerate the deterministic join/LIKE query set and inputs:

```sh
python3 tests/regression/testdata/generate_join_cases.py
python3 tests/regression/testdata/regenerate_references.py
```

Numeric comparisons preserve exact int64 values; REAL answers use an absolute
tolerance of `1e-8`. Queries preserve their saved result order. Changes to fixtures
should be reviewed as SQL behavior changes, rather than replacing expected values
with SlopDB's own output.

The migration updates old `godb/...` imports and standalone `panic`/exit handling
for the current Go test runner. Diagnostic index repair explicitly uses
`VerifyIndexesOnOpen`; ordinary opens trust maintained indexes. Superseded duplicate
programs, old nullable-primary-key/loose-type expectations, removed index-maintenance
markers and deprecated compression options are not current regression requirements.

## Benchmarks and engine comparisons

Large DBGEN inputs, profiling scripts and benchmark result archives are separate
from the ordinary test suite. The existing [TPC-H runner](cmd/tpch/README.md),
[TPC-C load tester](cmd/tpcc/README.md) and [cross-engine TPC-C runner](cmd/tpcc/compare/README.md)
document their inputs and commands. The native comparison runners live in nested
Go modules and have their own Go/CGO requirements; `go test ./...` covers the root
module without installing those drivers.

Catalog performance benchmarks are in `db/catalog_benchmark_test.go`. See
[catalog measurements](docs/catalog-performance.md) for isolated point SELECTs,
profile findings, concurrency settings and saved source fingerprints.
