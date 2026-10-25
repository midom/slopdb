# SQL and lock diagnostics

## SQL parse errors

`sql.Parse`, `sql.Prepare`, and database execution report a line and column for
syntax errors. Messages name the expected punctuation or keyword, for example:

```text
line 1, column 10: expected ")", got end of SQL
```

This is the result of parsing `SELECT (1`; add the missing closing parenthesis.
Callers can use `errors.As(err, &parseErr)` with `*sql.ParseError` to obtain its
one-based `Line` and `Column` and zero-based `ByteOffset`. The location points
at the current token, or at the end of the input when SQL ends early. Columns
count Unicode code points, while the byte offset can be used to slice the
original SQL string. Invalid numeric and BLOB literals point at the literal
that failed conversion. A database call may wrap this error with additional
context; `errors.As` still finds the location.

## Live lock waits

`Database.LockWaits()` returns a snapshot of sessions currently waiting for
catalog, row, or unique-value locks. Each entry has a database-local session
ID, IDs of blocking sessions, lock mode, resource kind, elapsed wait duration,
and the resource. Row resources are hex-encoded physical keys. Unique lock
resources contain framed table/column names and their predicate values appear
in `Values`.

`Database.SessionID()` identifies the default session;
`Session.SessionID()` and `Tx.SessionID()` identify independent handles. These
methods do not wait for the session's current SQL operation. Nil or closed
handles return zero. Results from `LockWaits()` own their slices and BLOB values,
so callers can retain or modify the snapshot without changing lock state.

```go
waits := database.LockWaits()
for _, wait := range waits {
    fmt.Printf("session %d waits %s for %s; blockers: %v\n",
        wait.SessionID, wait.WaitDuration, wait.ResourceKind,
        wait.BlockingSessionIDs)
}
```

In the CLI, `.locks` prints the same snapshot, including lock mode and unique
predicate values. It reports `No active lock waits.` when nothing is waiting,
and respects `.output` redirection. Dot commands require an interactive terminal
or the `--allow-dot-commands` flag when reading redirected input.

This method needs no SQL session, so it remains callable while another session
is blocked on a catalog change. The snapshot is diagnostic: a blocker may commit
as soon as the method returns. Session IDs are only meaningful within the
current `Database` instance and are not persisted across reopen. A wait that
times out or is cancelled disappears from subsequent snapshots.

## Lock failures

Lock timeouts and detected deadlocks report the waiting session ID and resource.
Use `errors.As(err, &lockErr)` with `*db.LockError` to inspect `SessionID`,
`ResourceKind`, and `Resource`. Its wrapped error preserves `errors.Is` checks
for `db.ErrLockTimeout`, `db.ErrDeadlock`, and the timeout's
`context.DeadlineExceeded`.

A lock timeout cancels the statement and keeps earlier transaction work and
locks. Inspect `LockWaits()` from another goroutine or handle while the wait is
active, then let the owner finish, roll back the waiting transaction, or choose
a longer timeout. The failed wait is removed before the error is returned.
A detected deadlock rolls back its victim; retry the entire transaction using
a new transaction handle. Taking shared resources in a consistent order helps
avoid repeating a deadlock.

Standalone index publication can opt into cancelling blocking transactions
with `KILL TRANSACTIONS AFTER '250ms'`, or through
`db.ExecOptions.IndexPublicationAbortAfter`. This cancels and rolls back the
whole blocking explicit transaction, including its earlier statements. Its
operations report `db.ErrTransactionAborted`; use `errors.Is` to identify it.
Retry that work as a new transaction. Sessions remain usable after the abort,
while dedicated Tx handles end. Autocommit statements and read-only
transactions without conflicting catalog locks are not targeted. The default
publication behavior continues waiting until
blockers finish or the statement's timeout/cancellation takes effect. Use
`EXPLAIN` or `.plan` to see an index change's cancellation policy before
executing it; see [online indexes](online-indexes.md) for details.

## Query plans

For slow SQL, `EXPLAIN ANALYZE` includes actual reads, plan work, timings,
allocation, spill metrics, and time spent waiting for locks as `lock_wait_ms`.
Use `EXPLAIN` to inspect a plan without running the statement.

## LSM storage status

`SHOW LSM STATUS` works through native SQL, MySQL and PostgreSQL, including
prepared statements. It returns sorted `Variable_name`, `Value` text columns;
`SHOW LSM STATUS LIKE 'level.%'` selects the seven levels and
`SHOW LSM STATUS LIKE 'file.%'` shows individual SSTables. `WHERE` filters are
rejected. Status remains available on Raft followers without acquiring a
quorum read fence, proposing entries or enabling change capture.

Metric groups cover mutable/immutable memtable bytes, internal entry counts,
range tombstones, skip-list height/hash buckets and sequence bounds; WAL mode,
file generations, records, payload bytes, syncs and errors; completed/pending
flushes; active/scheduled/completed compactions and input/output bytes; snapshot
pins and oldest retained sequence; bloom, reader-cache and decoded-block-cache
counters and capacity; mmap arena usage/failures; and data-block read attempts,
actual bytes returned by file reads, decoded bytes and read/checksum errors.
Per-level rows report file count, bytes, target bytes, nonoverlapping ordering
and the compression policy for future writers. File rows include level, bytes,
maximum sequence, range tombstones, resident index/filter bytes, remaining seek
budget and compaction membership. Key bounds are hex-encoded physical user keys.
L0 has a file-count trigger instead of a byte target (`target_bytes=0`).

Counters are cumulative for the current open engine instance and reset on
restart. File/layout gauges survive restart as persisted metadata. Memtable
bytes are approximate internal allocation accounting; entries count versions
and deletion markers, rather than SQL rows. Compaction/flush byte counters
measure installed SSTable file sizes, rather than device traffic. Data-block
reads measure OS file calls, which can hit the OS page cache; metadata/startup
reads are excluded. Compression is a writer policy, since existing blocks can
use different codecs. Concurrent counter samples can advance independently;
the file layout itself is captured under the engine metadata lock.

Inspection reads existing metadata and bounded registration/cache structures;
it neither scans data blocks nor opens SSTables. The detailed snapshot is
available to Go callers as `Database.LSMStatus(ctx)` or `LSMTree.Status()`;
`LSMTree.Stats()` retains the legacy file-layout API; `LSMTree.Summary()`
uses cached per-level totals and resident index bytes without walking or copying
file metadata. Ordinary server variables use that summary. Fixed LIKE prefixes
such as `cache.%`, `memtable.%` and `level.%` omit file metadata from detailed
inspection, and filtered-out key bounds are not hex-encoded.
