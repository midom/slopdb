# Query iteration, bulk loading, backup and integrity

`Database.Query`, `Session.Query`, `Tx.Query` and `Stmt.Query` return `*db.Rows`.
Each has a `QueryContext` variant. A database query owns an independent session;
a session or transaction query sees its own private writes and serializes other
calls on that session until the result closes.

```go
rows, err := database.QueryContext(ctx,
    "SELECT id, name FROM customers WHERE id BETWEEN 10 AND 20 ORDER BY id")
if err != nil { return err }
defer rows.Close()
for rows.Next() {
    var id int64
    var name string
    if err := rows.Scan(&id, &name); err != nil { return err }
    fmt.Println(id, name)
}
return rows.Err()
```

Simple single-table filtering/projection queries stream from a pinned statement
snapshot. Clustered primary-key order in either direction, LIMIT/OFFSET,
primary-key equality and a single composite key interval are supported without
materializing all matches. Cursor memory does not grow with the result count.
Joins, grouping, arbitrary sorting, DISTINCT, views, subqueries and other blocking
operators use the ordinary executor. `Rows.Materialized()` makes that fallback
visible to callers. The row order matches the query; SQL without ORDER BY does
not promise a particular order.

Always close results when stopping early. Exhaustion, context cancellation and
database/session shutdown automatically release the iterator, snapshot and
session gate, including when the consumer never calls Next again. Do not keep
an idle result open indefinitely: it pins old MVCC versions. Next/Scan are meant
for one consumer; Close may run concurrently. Scan errors leave the current row
available for another Scan.

`Rows.Columns()` returns copied column definitions with constraint SQL text;
internal executable DEFAULT/CHECK ASTs are omitted. `Rows.Values()` returns
caller-owned SQL values in projection order. Scan accepts `*sql.Value`, `*int64`,
`*int`, `*float64`, `*string`, `*[]byte`, `*bool`, `*any`, and `database/sql.Scanner`
implementations such as `database/sql.NullInt64`. NULL fails for non-nullable
scalar destinations. Typed scanning rejects incompatible types, overflowing
native integers and integer-to-float conversions outside the exact integer
range of float64. Scanned BLOB buffers remain valid after Next and Close; changing
them cannot mutate the database. Prepared query bindings remain private until
their result closes, so concurrent executions do not reuse a live cursor's AST.

## Atomic bulk insertion

`Database.BulkInsert(ctx, table, columns, source)` consumes a `db.BulkSource`
callback returning `[]any` bindings and `io.EOF`. It prepares INSERT shapes in
batches of 256 rows and commits the entire import in one transaction. Ordinary
SQL affinity, defaults, primary/unique keys, checks, foreign keys, triggers and
index maintenance apply. Source errors, malformed rows, constraint failures and
cancellation roll back all inserted rows and return a count of zero.

```go
next := func(ctx context.Context) ([]any, error) {
    record, err := csvReader.Read()
    if err != nil { return nil, err }
    return []any{record[0], record[1]}, nil
}
count, err := database.BulkInsert(ctx, "customers", []string{"id", "name"}, next)
```

Sources execute synchronously and should honor the supplied context when their
own I/O may block. Row buffers and BLOB bindings may be reused on the next source
call. The transaction retains pending writes and locks until commit; invoke the
API in separate chunks if bounded transaction memory matters more than all-or-
nothing import semantics. The CLI `.import FILE TABLE` now uses this API: invalid
CSV or SQL data leaves no partially inserted rows. Automatic table creation for
an absent table still precedes import, so a failed import may leave an empty table.

## Consistent online backup

`Database.BackupContext(ctx, destination)` (or `Backup`) copies committed raw
key/value state at one pinned MVCC snapshot, including catalog, rows and indexes.
Other sessions continue executing while it copies. Private transaction writes
are excluded. This creates a durable, reopenable database, even when the source
has WAL or WAL fsync disabled. It does not copy changing WAL files or require a
source checkpoint. The copy is written in bounded batches to a temporary sibling
directory, flushed and synced, then published with a directory rename and parent
sync. Close and cancellation clean up incomplete staged copies.

The destination must not exist and must lie outside the source database directory.
Its parent directory must exist. Existing databases are never overwritten. Restore
by calling `db.Open(destination, options)`; no import/replay procedure is required.
The backup preserves logical state, not SSTable layout, historical MVCC versions,
original sequence numbers or WAL history. Long backups retain old versions and
use additional disk space. An error syncing the parent after publication can
leave a complete backup at the destination; inspect that path before retrying.

CLI: `.backup DIRECTORY`.

## Read-only integrity checking

`Database.CheckIntegrityContext(ctx)` (or `CheckIntegrity`) reports checked rows,
secondary entries, catalog objects, SSTables and physical entries. It validates
one SQL snapshot for row encoding/types, primary keys, CHECK/NOT NULL/UNIQUE
constraints, foreign-key references, clustered-key agreement and missing, stale
or extra index entries. Persisted catalog objects must agree with the committed
catalog. It also opens current immutable SSTables with the block cache disabled
to verify metadata/index/data checksums, internal key ordering, sizes, manifest
agreement and bloom-filter false negatives, including obsolete row versions.

The checker never repairs or rewrites data. Diagnostics name the table, row,
index or SSTable concerned, and cancellation releases all readers. It may use
memory proportional to the number of distinct UNIQUE values, and is intended
for explicit operational checks. Unflushed data is checked logically; live WAL
history is not reread. A nullable column omitted from a legacy row after ALTER
ADD COLUMN is interpreted as NULL, matching normal execution.

CLI: `.integrity`, which prints `ok` and verification counts or an error.
`.bail on` exits with a failure status when either backup or integrity fails.
Dot commands are available in interactive terminal sessions. Scripts redirected
into the CLI must opt in with `--allow-dot-commands`; otherwise encountering a dot
command reports an error and exits with status 1. The setting applies to all dot
commands, including shell execution, file access and exit commands.
