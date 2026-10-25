# Embedded Go database/sql driver

Import `slopdb/driver` to register the `slopdb` driver. DSNs are plain database
directory paths. Connections to the same canonical path, including symlink
aliases, share one native engine within the process. Each pooled connection has
an independent SQL session. The last connection closes and flushes that engine.

```go
import (
    "database/sql"
    _ "slopdb/driver"
)

pool, err := sql.Open("slopdb", "/var/lib/myapp/database")
if err != nil { /* handle error */ }
defer pool.Close()
pool.SetMaxOpenConns(16)
err = pool.PingContext(ctx)
```

To share an already-open native database with an application or benchmark, use
`driver.NewConnector(database)` and `sql.OpenDB(connector)`, or the convenience
`driver.OpenDB(database)`. These pools never close the caller's engine. Close the
pool before closing the native `db.Database`.

```go
import (
    "database/sql"
    "slopdb/db"
    slopdriver "slopdb/driver"
)

native, err := db.Open(directory, options)
if err != nil { /* handle error */ }
connector, err := slopdriver.NewConnector(native)
if err != nil { /* handle error */ }
pool := sql.OpenDB(connector)
// Use pool, then check both shutdown results in this order:
err = pool.Close()
err = native.Close()
```

The driver uses native SQL syntax and positional `?` bindings. Prepared statements
retain native plans. Parameters accept normal driver values, `driver.Valuer`,
`slopdb/sql.Value`, and unsigned Go integers without narrowing. Named parameters
are rejected. Signed integers return `int64`; unsigned values return decimal
strings; DECIMAL and JSON return exact strings; BLOBs return byte slices; native
TEXT and temporal text return strings. Logical BOOLEAN values return `bool`.
Standard column metadata includes declaration names, bounded lengths,
nullability, decimal precision/scale, and suggested Go scan types.

SELECTs use native streaming rows when eligible, with the native materialized
fallback for complex queries. Queries such as `INSERT ... RETURNING` execute once
and expose their owned materialized results. Close rows promptly: a live result
owns the connection's session snapshot. A second operation on the same connection
returns an error while rows remain open. Queries on other connections continue.
Cancellation releases native streaming resources, and returned/scanned BLOBs do
not alias storage pages or later results.

Native streaming rows reuse their private row maps and BLOB decoding buffers
between calls. The driver inherits this reuse while preserving the ownership
and lifetime rules of values returned through `database/sql`. Scanning BLOBs
into `[]byte` or `any` returns independent bytes. `database/sql.RawBytes` follows
its standard borrowed lifetime, ending at the next `Next`, `Scan` or `Close`.

`BeginTx` supports default, READ COMMITTED, and native REPEATABLE READ isolation.
Read-only and other isolation options fail explicitly. Commit and rollback use
the same connection/session as their statements. Errors after execution are
never converted into `driver.ErrBadConn`, preventing automatic mutation retries.

Pool reuse clears session changes by rolling back and replacing dirty sessions.
This removes schema/search-path changes, temporary tables, variables, dialect and
transaction settings. Materialized SELECTs are conservatively dirty, including
stored-view/session-function execution. Safe physical streaming reads retain
clean sessions and prepared plans. A statement wrapper reparses lazily after a
session reset. Use `pool.Conn(ctx)` or a transaction when work requires persistent
session state between statements; ordinary pool operations may use any session.

The path registry synchronizes engine connection/close operations only. Queries
never acquire its global mutex. A path opened independently with `db.Open` is not
automatically registered: use `NewConnector` when sharing that existing handle.
The driver adds no network transport and no separate storage engine.
