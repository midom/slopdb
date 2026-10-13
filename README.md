# SlopDB

SlopDB is an embedded SQL database written in Go. It provides a SQLite-like SQL
dialect, concurrent transactions, a Go API and an interactive command-line
interface. Each database lives in a directory and runs without an external
database server.

## Features

### Queries and data

- SQL querying for application lookups, reporting and analysis.
- Data validation and relational integrity, with text and binary data support.
- Unicode-aware text comparison and optional case-insensitive matching.
- Schema evolution, atomic renames and online index creation and removal.

### Transactions and concurrency

- Independent concurrent sessions and atomic transactions.
- Savepoints, locking reads and optional deadlock detection.
- Configurable wait limits, deadlines and cancellation.
- Optional rollback of blocking transactions during online index publication.

### Performance

- Indexed query execution and automatic query planning.
- Reusable application queries, streaming results and bulk loading.
- Query planning and profiling tools for investigating slow queries.
- Configurable memory use, compression and caching.

### Durability and operations

- Durable writes and crash recovery enabled by default.
- Online backups, integrity checks and storage maintenance.
- Schema inspection, database status and live lock diagnostics.
- Actionable errors and tools for planning database changes.

### Integration and tools

- An embedded Go API with no external database server.
- An interactive command-line interface and script execution.
- Guides for application development, troubleshooting and database operations.

See the [documentation](#documentation) for feature details and restrictions.

## Quick start

Build from the repository root with Go 1.22.2 or newer:

```sh
go build -o slopdb .
./slopdb ./example-db
```

The [getting started guide](docs/getting-started.md) walks through creating a
table, querying data and running scripts.

## Using SlopDB from Go

The Go API supports querying, concurrent application sessions and transactions,
with deadlines and cancellation. Options let applications choose resource and
durability settings.

See [prepared statements](docs/prepared-statements.md) for parameter binding and
transaction examples, and [application operations](docs/application-operations.md)
for working with results, loading data, backups and integrity checks.

## Documentation

| Area | Guides |
| --- | --- |
| Getting started | [First database and scripts](docs/getting-started.md) |
| Working with data | [Supported behavior](docs/sql-semantics.md), [International text](docs/text-collation.md), [Unexpected query results](docs/sql-surprises.md) |
| Changing a database | [Schema changes](docs/alter-table.md), [Online changes](docs/online-indexes.md), [Planning changes](docs/schema-change-planning.md) |
| Applications and operations | [Application queries](docs/prepared-statements.md), [Operations](docs/application-operations.md), [Table status](docs/table-status.md), [Troubleshooting](docs/diagnostics.md) |
| Performance | [Profiling](docs/query-profiling.md), [Resource use](docs/query-memory.md), [Measurements](docs/catalog-performance.md) |

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
