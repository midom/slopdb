# SlopDB

SlopDB is an embedded SQL database written in Go, with an interactive command-line
interface and an LSM-tree storage engine. It combines a SQLite-like SQL dialect
with concurrent transactions, prepared statements, indexed query execution and
tools for inspecting database behavior.

Each database lives in a directory containing its write-ahead log, sorted table
files and manifest. The storage engine is implemented in Go; SQLite and DuckDB
are optional benchmark comparisons.

## Main features

- **SQL:** joins, subqueries, grouping, compound queries, UPSERT, common table
  expressions, recursive queries and window functions. Tables support composite
  primary keys, secondary indexes, constraints, views and triggers. Text
  collation supports canonical Unicode equivalence (NFC/NFD), compatibility
  equivalence (NFKC/NFKD), and optional Unicode case folding. SQL `BINARY`
  overrides the database-wide collation for expressions or columns.
- **Transactions:** independent sessions, READ COMMITTED statement snapshots,
  atomic commits, row and unique-key locks, savepoints, cancellation and optional
  deadlock detection.
- **Go APIs:** prepared statements with typed parameters, streaming query results,
  atomic bulk insertion, online backup and integrity checking.
- **Query execution:** bounded primary-key and index scans, covering reads,
  indexed and hash joins, batched typed aggregation, Top-K sorting and disk
  spilling. ANALYZE statistics guide access and join choices; EXPLAIN ANALYZE
  reports actual execution work. LIMIT and OFFSET use 64-bit fields.
- **Storage:** MVCC versions, checksummed WAL and SSTables, bloom filters,
  background compaction, configurable compression, bounded block caches and
  persistent object IDs for atomic catalog-only renames.

Supported SQL behavior and restrictions are described in
[SQL semantics](docs/sql-semantics.md). Feature-specific guides cover
[UPSERT](docs/upsert.md), [CTEs](docs/common-table-expressions.md) and
[window functions](docs/window-functions.md).
[ALTER operations](docs/alter-table.md) covers schema changes, constraints,
atomic renames and legacy storage upgrades.
[Text collation](docs/text-collation.md) explains Unicode compatibility,
`BINARY` overrides and index rebuilds.
[Catalog performance](docs/catalog-performance.md) records point SELECT and
transaction measurements, profiling and concurrent-rename consistency checks.

## Quick start

Build from the repository root with Go 1.22.2 or newer:

```sh
go build -o slopdb .
./slopdb ./example-db
```

In the SQL prompt:

```sql
CREATE TABLE accounts (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    balance INTEGER NOT NULL CHECK (balance >= 0)
);

INSERT INTO accounts VALUES (1, 'Ada', 100), (2, 'Lin', 250);
SELECT name, balance FROM accounts ORDER BY balance DESC;
```

Use `.help` to see CLI commands, `.tables` and `.schema` to inspect the catalog,
and `.quit` to close the database. SQL files can also be piped into the CLI:

```sh
./slopdb ./example-db < queries.sql
```

Redirected stdin (pipes and files) accepts SQL only by default. Dot commands,
including `.system`, `.shell`, `.read` and `.output`, are rejected with exit
status 1. To run a CLI script containing dot commands, explicitly enable them:

```sh
./slopdb --allow-dot-commands ./example-db < cli-script.sql
```

Interactive terminal sessions enable dot commands automatically. Text inside SQL
strings or comments is SQL content, so strings containing `.system` are unaffected.

WAL recording and fsync are enabled by default. For measurements that deliberately
omit WAL fsync, use `./slopdb --no-wal-fsync ./example-db`; recent writes may be
lost on power failure in that mode. Always check database close errors when using
the Go API.

## Using SlopDB from Go

The `db` package provides `Open`, `Exec`, `Query`, `Prepare`, independent
`Session` handles and `Tx` transactions. Context-aware methods support deadlines
and cancellation; `storage.Options` configures durability, caches, compression
and query working budgets.

See [prepared statements](docs/prepared-statements.md) for parameter binding and
transaction examples, and [application operations](docs/application-operations.md)
for row iteration, bulk loading, backup and integrity checks. Close streaming
results when finished so their snapshots and resources can be released.

## Design

The implementation has three main layers:

| Layer | Responsibility |
| --- | --- |
| `db/` | Application APIs, sessions, transaction lifecycle and lock coordination |
| `sql/` | Tokenization, parsing, validation, planning and execution |
| `storage/` | MVCC LSM storage, WAL recovery, SSTables, caches and compaction |

The CLI lives in `main.go` and `cli.go`. Rows use typed binary encoding; catalog
definitions persist separately. Query operators have a configurable soft memory
budget, with spilling for supported operators. This budget is not a process
memory limit, and some results and intermediate operations still materialize.
See [query memory](docs/query-memory.md), [statistics](docs/query-statistics.md)
and [profiling](docs/query-profiling.md) for details.

## Tests and benchmarks

```sh
go test ./... -count=1 -timeout 10m
go test -race ./... -count=1 -timeout 10m
go vet ./...
```

The repository includes SQL reference fixtures, concurrency and transaction
tests, storage models, corruption checks and crash-recovery tests. Ordinary Go
tests use isolated temporary databases and require no external database engine.
See [TESTING.md](TESTING.md) for selective commands and coverage.

- [TPC-C-inspired load tester](cmd/tpcc/README.md): concurrent transactional work,
  latency measurements and consistency checks.
- [TPC-H query runner](cmd/tpch/README.md): all 22 query shapes on DBGEN inputs,
  with complete-result verification.
- [Engine comparisons](cmd/tpcc/compare/README.md): SlopDB, SQLite and DuckDB;
  native comparison modules have separate Go/CGO requirements.
- [Performance checks](docs/performance-regressions.md): repeatable measurements,
  microbenchmarks and CI artifacts.

These are development benchmarks rather than certified TPC scores. Compare runs
with matching data, concurrency, execution mode and durability settings.
