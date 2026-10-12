# Getting started with SlopDB

Build from the repository root with Go 1.22.2 or newer, then open a database:

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
[application operations](application-operations.md) for settings, shutdown,
backups and routine maintenance.

## Using the Go API

The `db` package provides `Open`, `Exec`, `Query`, `Prepare`, independent
`Session` handles and `Tx` transactions. Context-aware methods support deadlines
and cancellation. Close streaming results when finished and check database
close errors.

See [prepared statements](prepared-statements.md) for parameter binding and
transaction examples, and [application operations](application-operations.md)
for row iteration, bulk loading, backup and integrity checks.
