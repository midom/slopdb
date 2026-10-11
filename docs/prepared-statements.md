# Prepared statements

`Database.Prepare` parses one SQL statement once. Use anonymous `?` placeholders
for values, including `LIMIT` and `OFFSET`. Executions bind typed values into
private reusable ASTs and skip SQL tokenization and parsing. Schema lookup and
query planning use the current catalog and statement snapshot, so creating,
dropping, or changing an index or table does not retain a stale execution plan.

```go
stmt, err := database.Prepare("SELECT balance FROM customer WHERE w_id=? AND d_id=? AND c_id=?")
if err != nil {
    return err
}
defer stmt.Close()

result, err := stmt.ExecContext(ctx, 1, 2, 3)
```

A statement prepared on a database can be shared concurrently across its
sessions and transactions. Execute it on a transaction to use that transaction's
locks and private writes:

```go
update, err := database.Prepare("UPDATE stock SET quantity=quantity-? WHERE w_id=? AND i_id=?")
if err != nil {
    return err
}
defer update.Close()

tx, err := database.BeginContext(ctx)
if err != nil {
    return err
}
defer tx.Rollback()
if _, err = tx.ExecPreparedContext(ctx, update, 5, 1, 42); err != nil {
    return err
}
return tx.Commit()
```

`Session.Prepare` and `Tx.Prepare` bind the statement to that session or
transaction; their statement's `Exec` methods execute there. A bound statement
cannot be moved to another session or used after the original context closes.
Database-prepared statements can be passed to `Session.ExecPrepared` or
`Tx.ExecPrepared`, but cannot be moved to another database. The context variants
are `PrepareContext`, `ExecContext`, and `ExecPreparedContext`. Per-call timeout
settings are available through `Stmt.ExecWithOptions`,
`Stmt.ExecContextWithOptions`, and session/transaction
`ExecPreparedContextWithOptions`.

`Stmt.NumInput` reports the required argument count. Bindings accept signed Go
integers, unsigned integers up to `MaxInt64`, finite floating-point values,
strings, booleans (SQL integer 0/1), `[]byte` blobs, `sql.Value`, and `nil` for
SQL NULL. Blob arguments are copied. A nil `[]byte` binds NULL; an empty non-nil
slice binds an empty blob. Wrong argument counts, unsupported values,
non-finite reals, and integers outside the signed 64-bit range fail before SQL
execution. SQL text inside a string argument remains data.

Parameters stand for expressions, not identifiers, aliases, SQL fragments, or
persistent schema definitions. Named and numbered parameters are not supported.
Multi-statement strings are rejected by Prepare. Parameterized schema
statements (including defaults, checks, views, and triggers) are rejected;
constant schema statements can still be prepared and reused. A bound integer
in `ORDER BY ?` or `GROUP BY ?` is a constant expression, not a column ordinal.

`Close` is idempotent and rejects new executions with `db.ErrStmtClosed`; an
already-started execution may finish. Closing a statement does not close its
session or roll back its transaction.

## 64-bit pagination

SELECT, compound SELECT, UPDATE, and DELETE store both `Limit` and `Offset` as
explicit `int64` fields. Text literals and prepared arguments support the full
signed 64-bit range; out-of-range literals and arguments are rejected rather
than narrowed to the platform's `int`. A bound LIMIT/OFFSET value must be an
integer, not NULL, a string, or a real. Negative LIMIT retains the existing
unlimited convention; negative OFFSET becomes zero.

Scan windows saturate `LIMIT + OFFSET` at `MaxInt64` to prevent overflow. Result
slicing converts an offset or limit to a native index only after comparing it
with the actual slice length. Large pagination values do not cause a
proportional preallocation. Explicit `int64` AST fields change source
compatibility for code constructing ASTs with native `int` variables; convert
those fields with `int64(value)`.

## TPC-C-inspired tester

The tester now defaults to prepared statements. Every workload statement and
stock-level IN-list arity is prepared before measurement; there is no fallback
to parsing SQL during the prepared workload. Preparation duration and execution
mode are recorded in JSON. Loading, preflight, accounting checks, and reopening
remain outside the benchmark window. The prepared path uses a prepared COMMIT;
the SQL text comparison uses the same workload templates, parameters,
transaction order, and scale.

```sh
go run ./cmd/tpcc -workers 10 -duration 1m -json /tmp/slopdb-tpcc-prepared-results.json
go run ./cmd/tpcc -workers 10 -duration 1m -prepared=false -json /tmp/slopdb-tpcc-text-results.json
```

WAL stays enabled and WAL fsync remains disabled by default. This is a reduced,
TPC-C-inspired workload, not a compliant TPC-C score.
