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
| Conditional index cleanup after COMMIT/RELEASE, rollback, cleanup-error retry and recovery | `db/index_cleanup_test.go`, `sql/index_cleanup_test.go` | Ordinary commits skip metadata scans and the catalog writer lock; committed drops reclaim entries and retain retry requests after errors |
| Extended ALTER, named constraints, type conversion, dependency cascade and crash atomicity | `tests/regression/alter_extended_test.go` | Column/index/view actions, failed multi-action rollback, SQL dump roundtrip and committed/uncommitted WAL recovery |
| SHOW/DESCRIBE, catalog metadata, table-status estimates and persistence | `tests/regression/show_and_describe_test.go`, `show_table_status_test.go`, `storage/range_estimate_test.go` | `show_test_runner.go`; bounded block sampling, compression, MVCC and concurrent estimates |
| API concurrency, context cancellation and read corruption | `tests/regression/database_api_test.go` | `api_cache_diagnostics_review.go` |
| Transactions, locking, snapshot isolation, atomic publication and foreign-key races | `tests/regression/batch_transactions_test.go`, `concurrent_execution_test.go`, `indexed_mvcc_test.go`, `primary_key_locking_test.go`, `foreign_concurrency_test.go` | `standard_batch_transactions_review.go`, `standard_concurrent_execution_review.go`, `standard_indexed_mvcc_review.go`, `performance_lock_review.go`, `foreign_concurrency_review.go` |
| Randomized storage model, snapshots, tombstones, compaction and reopen | `tests/regression/storage_model_test.go` | `standard_current_storage_review.go` |
| Bounded/reverse iterators, long keys, batch reads and snapshot lifetime | `tests/regression/bounded_iterator_test.go`, `reverse_and_multiget_test.go`, `point_readers_test.go`, `snapshot_readers_test.go`, `snapshot_release_test.go` | Corresponding `bounded_iterator`, `reverse_multi`, `point_reader`, `snapshot_reader`, `snapshot_release` review programs |
| Concurrent memtables, atomic bulk/group commits, flush and shutdown | `tests/regression/atomic_memtable_test.go`, `bulk_memtable_test.go`, `group_commit_test.go`, `flush_test.go`, `db/shutdown_concurrency_test.go` | Corresponding review programs |
| SSTable metadata, filters, checksums, compressed blocks, corruption and compression policy | `tests/regression/sstable_metadata_test.go`, `compression_codecs_test.go`, `level_compression_test.go`, `bloom_quality_test.go`, `storage/bloom_test.go` | `current_metadata_review.go`, `current_codecs_review.go`, `current_level_compression_review.go`, `bloom_quality_review.go`; conservative unknown-filter checks from `bloom_compatibility_review.go` |
| WAL/no-fsync/no-WAL modes, abrupt-exit recovery, rollback, damaged files, write/Close errors | `tests/regression/recovery_test.go`, `durability_test.go` | `recovery_review.go`, `no_wal_review.go`, `no_wal_fsync_review.go` |
| Background rotation fsync, durable barriers across generations, out-of-order completion, write/flush/shutdown errors and certified journal recovery | `storage/wal_sync*_test.go`, `replication/wal_rotation_test.go` | Gates real WAL syncs to verify foreground progress and barrier waits; syscall checks of directory-entry persistence, abrupt-exit recovery and full reconstruction from logs-only voters |
| CLI formatting, schema planning, atomic CSV import, backup/integrity commands and page-size options | `internal/cli/*_test.go`, `tests/regression/cli_page_size_test.go` | CLI unit tests live with the implementation; process tests build the root entry point |
| CLI EOF/exit/bail/SIGTERM persistence, `.open` and flush errors | `tests/regression/cli_linux_test.go`, compression CLI tests | `no_wal_cli_review.go` and the current compression review programs |
| MySQL wire protocol, TCP/Unix driver compatibility, password and TLS/client certificate authentication, transactions, prepared binding, framing, malformed input and CLI shutdown persistence | `mysqlserver/*_test.go`, `tests/regression/mysql_server_linux_test.go` | Standard Go MySQL driver, optional native client and raw protocol checks; isolated databases/sockets and generated temporary CAs/certificates |
| PostgreSQL emulation, lib/pq and pgx, SCRAM, TLS/mutual TLS, extended queries, portals, cancellation, batch rollback, malformed messages and CLI persistence | `postgresserver/*_test.go`, `tests/regression/postgres_server_linux_test.go` | Independent network drivers, raw protocol messages and temporary certificate fixtures |
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

Raft replication tests live in `replication/`: TCP/Unix seed snapshots, nonvoter
catch-up/promotion, leadership transfer/failover, minority rejection, durable
restart and duplicate replay, offline snapshot installation, concurrent writes,
corruption/truncation rejection and mutual TLS.
`replication/roles_test.go` covers permanent nonvoters, rejected promotion,
nonvoter reads and restart, two full copies plus a logs-only voter over TCP/Unix
in sync/async modes, failover, quorum writes, restart, role identity, election
and direct-transfer exclusion, metadata-only application and recovery from a
journal after full-node payload loss.
`replication/durability_test.go` covers async/durable commit, periodic flushing,
lag admission, certificate validation, recovering voters and all-cold recovery
after removing volatile log suffixes and application generations. Wire tests
cover prepared durable commits and unknown confirmation outcomes on both SQL
protocols. `tests/regression/replication_async_linux_test.go` uses SIGKILL and
the offline CLI recovery procedure to check stable-prefix restoration.
When `strace` is available, `replication/durability_syscall_linux_test.go` checks
that async commits skip WAL fsync and explicit durable barriers issue it.
`replication/read_index_test.go` covers reads without log appends and cancellation
while committed entries await native application. Replica-read tests cover
TCP/Unix and sync/async modes, prepared/streamed reads, catalog changes during
pinned streams, read-only transactions, rejected writes/locks, proof expiry,
application backlog/cancellation, restart discovery, leadership transitions and
lost quorum. The same read-index flow runs over mutual TLS, and wire tests
exercise MySQL/PostgreSQL prepared reads and stale-read failures. SQL tests check
conservative classification of nested queries, CTEs and windows.
Durable-read tests exercise PREVIOUS/NEXT over TCP/Unix, sync/async modes,
MySQL/PostgreSQL prepared execution and mutual TLS. A held remote prepare proves
that later rows/catalog changes are excluded from a captured fence, including
after VACUUM. Tests cover idle NEXT requests, streaming/materialized snapshots,
minimum-position conflicts, private-write rejection, missing-view errors,
bounded candidate retention, cancellation after Raft enqueue, replica re-cloning,
and returning to live prepared writes. Storage tests transfer a global snapshot
pin to a statement reader and verify survival through full compaction.
Position tests check master/session queries, commit responses, concurrent-commit
coverage, delayed native application, explicit/prepared waits, deadline/cancel
handling, minimum-position Exec/streaming reads, full-width indexes, tampered
and wrong-incarnation tokens, log compaction and stable-point recovery. Raw wire
tests decode MySQL plain/session-tracked OK metadata and PostgreSQL simple and
extended implicit-commit ParameterStatus; integration tests use both drivers.
The quorum-flush pipeline
test holds a remote prepare request and verifies later writes can commit without
being included in the earlier durable certificate or losing their lag timestamp.
Application batching tests cover ordered range deletions, configuration and
page-size boundaries, replay, reopen, oversized atomic commands and failed
suffixes. Log-codec tests cover mixed legacy/binary records, timestamps, malformed
framing, history hashes and caller ownership. Transaction-control tests verify
that data access still checks a quorum and commit cannot succeed without one.
`replication/command_json_test.go` checks byte-for-byte standard JSON compatibility,
legacy/malformed representations, ownership and checkpoint integer boundaries.
`FuzzCommandJSONCompatibility` compares the decoder with Go's standard library.
Proposal-gate tests cover independent enqueue progress, exclusive flush barriers
and the limit on unresolved large commands.
`BenchmarkRaftHotRowTransactions` reuses the transaction-throughput fixture with
every session updating row 1, measuring write contention through commit. It
checks the exact shared-row total, untouched rows, replica equality, integrity
and restart/stable recovery outside timing, as does `BenchmarkRaftTransactions`
for independent rows.
`BenchmarkRaftLogsOnlyTransactions` uses the same fixture and explicit prepared
transactions with one full node and two logs-only voters. It checks membership,
exact totals, and independently rebuilds each journal's snapshot plus WAL into
a full SQL database for integrity checks, then verifies cold restart/stable
recovery outside timing. Select matching `raft3`/`raft3-async` worker counts in
`BenchmarkRaftTransactions` for the three-full-node baseline. Put `TMPDIR` on a
disk filesystem for fsync measurements; `test` has a RAM-backed `/tmp`. Use a
short temporary path when also running Unix socket tests.
See the [disk-backed journal-voter measurements](docs/benchmarks/raft/2026-10-24/logs-only.md)
for a matched matrix with source fingerprints and per-run results.
For one-case diagnostics, `-raft-bench-latencies` writes per-transaction stage
timings, `-raft-bench-trace` records the timed execution window, and
`-raft-bench-contention` writes timed block/mutex profiles. Keep their paths
outside the repository and use one sample per invocation. See the
[tail-latency investigation](docs/benchmarks/raft/2026-10-24/latency-investigation.md)
for measured waits, syscall evidence and isolated diagnostic controls.
`tests/regression/replication_linux_test.go` exercises CLI MySQL/PostgreSQL
replication through SIGKILL/restart and SIGTERM, plus durability flag validation.
`internal/wire/listen_test.go` checks stale Unix socket recovery and preservation
of live sockets and regular files. All artifacts use `t.TempDir()`.

## MediaWiki and MySQL compatibility

`db/mysql_function_*_test.go` and `sql/mysql_function_*_test.go` cover MySQL
function values, exact/approximate coercion, seeded randomness, NULLs, binary and
Unicode inputs, prepared metadata, stored views, cancellation and expansion
budgets. `sql/testdata/mysql_format_locales.json` contains independent MySQL
answers for the documented FORMAT locale set. The optional temporal reference
check uses `SLOPDB_MYSQL_REFERENCE_SOCKET` to compare deterministic generated
calendar/week queries with an existing isolated MySQL server; ordinary tests
require no external reference server.
`mysqlserver/mysql_functions_reference_test.go` checks 360 independent Oracle
fixtures in text and prepared modes (720 protocol checks), including invalid
calls. `scripts/regenerate_mysql_function_references.py --socket ...` regenerates
the factual fixture using MySQL alone; never derive expected answers from SlopDB.
Regex tests cover Unicode offsets, occurrence counts, capture replacements,
empty-match progress, RE2 boundaries, cancellation and statement-cache cleanup.

`tests/regression/mediawiki_test.go` loads all 64 tables from the factual
MediaWiki 1.46.2 schema fixture, then checks binary predicates, GROUP_CONCAT,
fulltext joins, revision deletion/restoration, reopen and integrity. It needs no
PHP, MySQL installation or external scripts. Additional tests cover temporary
isolation, LIKE schema copies, joined mutations, Unicode charset changes and
ALTER index prefixes. `db/mysql_types_test.go` and `db/mysql_fulltext_test.go`
cover persistent types, assignment modes, prepared bindings and search budgets.
`mysqlserver/*test.go` exercises declared wire types, warnings, named locks,
account metadata and virtual schema isolation over client connections.

`db/temporary_lsm_test.go` exercises reserved-range temporary rows, WAL exclusion,
flush/reopen and crash cleanup. `db/prefix_indexes_test.go` covers physical keys,
prefix uniqueness and query residuals. `db/fulltext_inverted_test.go` checks
posting maintenance, online builds, reopen and selective document reads.
`db/isolation_test.go` covers retained snapshots, current locking/mutation reads,
private writes, prepared/streaming queries and snapshot release through compaction.
`db/namespaces_test.go` and the protocol namespace tests cover qualified objects,
search paths, prepared binding and transactionally switching default schemas.
Legacy child-database migration tests use real crash points and retain original
source directories.

Numeric and JSON tests cover full uint64, exact DECIMAL keys/arithmetic, binary
row/spill formats, JSON paths/functions and fresh/reopened data. MySQL prepared
metadata tests assert that preparation does not evaluate user expressions.
Multiple-result and compression tests exercise standard drivers and raw frames.
`storage/change_log_test.go` and `db/change_log_test.go` verify transactional CDC
before/after images, group commit boundaries, restart/crash recovery, deterministic
replica records, schema checkpoints and temporary/index exclusion.
`replication/change_log_snapshot_test.go` checks exact journal preservation
through the native snapshot decoder. `mysqlserver/binlog_test.go` checks event
CRCs, exact row families, GTID filtering, live waits, rotation, byte-offset resume,
reopen and unsupported transaction shapes. With Oracle MySQL installed, run the
independent replica/applier check using:

```sh
SLOPDB_TEST_MYSQL_REPLICA=1 go test ./mysqlserver -run '^TestBinlogIndependentMySQLReplicaApply$' -count=1 -timeout 2m
```

It initializes an isolated server under `t.TempDir()`, applies real events using
GTID auto-positioning and shuts down through SQL. It is optional; the ordinary
suite needs no reference database server.

```sh
go test ./db ./sql ./mysqlserver ./tests/regression -run 'Test(MySQL|MediaWiki|Temporary)' -count=1
```

For the separately installed application, `scripts/mediawiki_acceptance.py`
checks HTTP/API results and redacts credential fields. See
[MediaWiki compatibility](docs/mediawiki.md) for invocation and limits. Keep its
reports, application files and test databases outside the repository.

### PostgreSQL compatibility

`db/postgres_functions_test.go` uses checked-in independent PostgreSQL 16.15
answers in `db/testdata/postgres_functions.json` (254 cases checked through both
direct and prepared execution, 508 scalar checks); the adjacent regeneration
script requires a separate PostgreSQL installation and never derives answers
from SlopDB. Ordinary tests need no external server. `db/postgres_catalog_test.go`
checks virtual catalogs, index/constraint metadata, namespace resolution,
transactional DDL, rollback and reopen. `postgresserver/catalog_test.go` and
`functions_test.go` verify pgx Describe types and simple/extended execution.

`sql/postgres_types_test.go` and `postgresserver/postgres_types_test.go` cover
casts, declarations, independent PostgreSQL binary fixtures and client codecs.
`db/result_projection_test.go` checks independent duplicate output values across
prepared/materialized/streaming SELECT, RETURNING, UNION, joins, aggregates and
windows. PostgreSQL/MySQL protocol and CLI output tests preserve duplicate
labels while retaining each result slot.

`postgresserver/copy_test.go` exercises pgx.CopyFrom, lib/pq.CopyIn, text/CSV/binary
streams, typed values, protocol fragmentation, cancellation, failure recovery
and transaction atomicity. All databases and protocol test artifacts use
`t.TempDir()`.

```sh
go test ./sql ./db ./postgresserver -run 'TestPostgres|TestCopy' -count=1
go test -race ./db ./postgresserver -run 'TestPostgres|TestCopy' -count=1
```

LSM SQL inspection is covered by `storage/status_test.go`,
`db/lsm_status_test.go`, and `replication/sql_status_test.go`: both memtable
implementations, concurrent publication/snapshot pins, flush/full compaction,
reopen, passive counters, filters/prepared execution, and actual MySQL/PostgreSQL
inspection on a Raft leader and follower.

Shared frontend behavior is covered by `tests/regression/dual_protocol_test.go`,
`internal/cli/wire_server_test.go`, `db/isolation_postgres_test.go`, and
`postgresserver/isolation_test.go`: independent parser policies on simultaneous
listeners, cross-protocol visibility, close/reopen, stable snapshot mutation
conflicts and real SQLSTATE errors after a row-lock wait. Catalog projections
have owning-package tests for native/MySQL/PG, and typed user variables, stored
collations, column spelling/table-local indexes, temporary ON COMMIT behavior,
full-text configurations, shared bulk imports and client-local ingestion each
have permanent regression suites.
