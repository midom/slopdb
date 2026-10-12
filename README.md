# SlopDB

SlopDB is an embedded SQL database written in Go. It provides a SQLite-like SQL
dialect, concurrent transactions, a Go API and an interactive command-line
interface. Each database lives in a directory and runs without an external
database server.

## Features

### SQL, data and schema

- Joins, subqueries, grouping, compound queries, UPSERT, common table expressions,
  supported recursive queries and window functions.
- Tables with primary keys, secondary indexes, constraints, views and triggers.
- Text and binary data, including `VARCHAR` and `VARBINARY` with custom lengths.
- Unicode-aware text comparison with normalization, case-insensitive comparison
  and binary overrides.
- Schema changes and atomic renames, including online index creation and removal.
  Planning tools show expected work and locking before changes.

### Concurrency and transactions

- Concurrent SQL sessions and transactions with atomic commits.
- Savepoints, locking reads, configurable lock waits and optional deadlock
  detection.
- Context deadlines and cancellation, plus an optional index publication policy
  that rolls back blocking transactions after a configurable wait.

### Performance

- Indexed queries, optimized joins, aggregation and sorting.
- Statistics that guide query planning, with `EXPLAIN` and `EXPLAIN ANALYZE` for
  understanding plans and actual execution.
- Memory controls, compression and caching for query and storage performance.
- Prepared statements, streaming results and bulk loading.

### Durability and operations

- Durable writes and crash recovery enabled by default.
- Online backups, integrity checks and storage maintenance.
- Schema inspection, database status, live lock diagnostics and actionable
  syntax errors.

### Go API and CLI

- Go APIs for SQL execution, typed parameters, sessions and transactions.
- An interactive SQL prompt, SQL file execution and commands for inspecting
  databases and troubleshooting queries.

Supported behavior and restrictions are documented in
[SQL semantics](docs/sql-semantics.md). See the guides below for feature details.

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

Use `.help` to see CLI commands, `.tables` and `.schema` to inspect tables, and
`.quit` to close the database. Run a SQL file with:

```sh
./slopdb ./example-db < queries.sql
```

Pipes and redirected files accept SQL only by default. For scripts containing
CLI dot commands, enable them explicitly:

```sh
./slopdb --allow-dot-commands ./example-db < cli-script.sql
```

Interactive terminal sessions enable dot commands automatically. See
[application operations](docs/application-operations.md) for settings, shutdown,
backups and routine maintenance.

## Using SlopDB from Go

The `db` package provides `Open`, `Exec`, `Query`, `Prepare`, independent
`Session` handles and `Tx` transactions. Context-aware methods support deadlines
and cancellation. Options configure durability, compression, caches and query
budgets.

See [prepared statements](docs/prepared-statements.md) for parameter binding and
transaction examples, and [application operations](docs/application-operations.md)
for row iteration, bulk loading, backup and integrity checks. Close streaming
results when finished and check database close errors.

## Documentation

| Area | Guides |
| --- | --- |
| SQL and data | [Semantics](docs/sql-semantics.md), [Unicode text comparison](docs/text-collation.md), [Common SQL surprises](docs/sql-surprises.md), [UPSERT](docs/upsert.md), [CTEs](docs/common-table-expressions.md), [Window functions](docs/window-functions.md) |
| Schema changes | [ALTER TABLE](docs/alter-table.md), [Online indexes](docs/online-indexes.md), [Planning changes](docs/schema-change-planning.md) |
| Applications and operations | [Prepared statements](docs/prepared-statements.md), [Application operations](docs/application-operations.md), [Diagnostics](docs/diagnostics.md) |
| Query performance | [Statistics](docs/query-statistics.md), [Profiling](docs/query-profiling.md), [Query memory](docs/query-memory.md), [Catalog performance](docs/catalog-performance.md) |

## Tests and benchmarks

```sh
go test ./... -count=1 -timeout 10m
go test -race ./... -count=1 -timeout 10m
go vet ./...
```

The repository includes SQL reference fixtures, concurrency and transaction
tests, corruption checks and crash-recovery tests. Ordinary Go tests use isolated
temporary databases and require no external database engine. See
[TESTING.md](TESTING.md) for selective commands and coverage.

- [TPC-C-inspired load tester](cmd/tpcc/README.md): concurrent transactional work,
  latency measurements and consistency checks.
- [TPC-H query runner](cmd/tpch/README.md): all 22 query shapes on DBGEN inputs,
  with complete-result verification.
- [Engine comparisons](cmd/tpcc/compare/README.md): SlopDB, SQLite and DuckDB.
- [Performance checks](docs/performance-regressions.md): repeatable measurements,
  microbenchmarks and CI artifacts.

These are development benchmarks rather than certified TPC scores. Compare runs
with matching data, concurrency, execution mode and durability settings.
