# Online index creation and removal

Standalone `CREATE INDEX`, `CREATE UNIQUE INDEX`, and single-action
`ALTER TABLE ... ADD INDEX/KEY` statements build indexes while independent
sessions continue reading and committing inserts, updates, and deletes.

```sql
CREATE INDEX customer_name ON customers(name);
CREATE UNIQUE INDEX customer_external ON customers(tenant, external_id);
ALTER TABLE customers ADD INDEX tenant_name(tenant, name);
ALTER TABLE customers ADD UNIQUE KEY external_key(tenant, external_id);

DROP INDEX customer_name;
ALTER TABLE customers DROP INDEX tenant_name;
```

Use `EXPLAIN CREATE INDEX ...`, `EXPLAIN DROP INDEX ...`, or `EXPLAIN ALTER
TABLE ...` to inspect the work and locking before executing the change.

## Creation and visibility

The build reserves a permanent storage ID and scans a pinned committed
snapshot. Its entries remain invisible to query planning and constraint
checks until publication. Committed row changes are recorded during the scan
and replayed against subsequent snapshots. Rolled-back changes are excluded.
After publication, ordinary writes maintain the new index atomically with rows.
Prepared statements refresh their bindings when the catalog changes.

The scan and catch-up use a shared catalog lock, which permits normal DML.
Other schema changes wait during this work. Publication releases that shared
lock before acquiring an exclusive catalog lock, allowing concurrent builders
to finish without an upgrade deadlock. The table definition and collation are
checked again after this transition. A conflicting schema change rejects the
build instead of publishing stale entries.

An exclusive publication lock waits for existing writer transactions to finish
and briefly prevents new writers from acquiring the catalog lock. Final
reconciliation is limited to 256 captured row mutations; a larger delta is
caught up under a shared lock before retrying publication. The main table scan
and bulk index installation happen before this exclusive phase. Long-running
writer transactions can delay publication. Query cancellation and configured
query/lock timeouts apply throughout execution. This describes the amount of
work under the lock; storage I/O and the size of individual values still affect
elapsed time.

UNIQUE builds validate the final committed contents, with normal NULL,
composite-key, Unicode collation, and column `BINARY` rules. A duplicate removed
while building can allow publication; a duplicate committed during the build
causes failure. A failed build never publishes a partially usable index.

## Publication with transaction cancellation

Index changes wait for blocking transactions by default. A standalone index
change can instead cancel those transactions once publication has waited for a
specified duration:

```sql
CREATE INDEX customer_name ON customers(name)
  KILL TRANSACTIONS AFTER '250ms';
DROP INDEX customer_name KILL TRANSACTIONS AFTER '2s';
ALTER TABLE customers ADD INDEX tenant_name(tenant, name)
  KILL TRANSACTIONS AFTER '250ms';
ALTER TABLE customers DROP INDEX tenant_name
  KILL TRANSACTIONS AFTER '0s';
```

The threshold starts when the statement waits for the exclusive publication
lock, after the index build and catch-up. For drops it starts when acquiring the
exclusive catalog lock. `0s` requests immediate cancellation when there are
blockers. Durations must be quoted, nonnegative Go durations such as `250ms`,
`2s`, or `1m30s`. Malformed durations, negative values, and values beyond the
duration range are rejected.

Only explicit transactions holding a conflicting catalog lock are targeted.
Autocommit statements finish normally; read-only transactions without catalog
locks and unrelated queued waiters are not targeted. Active statements are
cancelled cooperatively and uncommitted changes are rolled back before the
lock is granted. A commit that has already published may finish successfully.
Applications receive `db.ErrTransactionAborted` for an aborted transaction and
must retry the entire transaction, including earlier statements. A Session
remains usable afterward, while a dedicated Tx handle ends. Query cancellation
and configured query/lock timeouts still apply; the threshold does not guarantee
that publication will finish within that time.

Cancellation before the threshold leaves blockers alone. Once a rollback is
requested, it completes even if the publishing statement is subsequently
cancelled. After the threshold expires, the policy also applies to older queued
transactions that become blocking owners while publication is still waiting.

The Go API also accepts an optional threshold in `db.ExecOptions`:

```go
abortAfter := 250 * time.Millisecond
_, err := database.ExecWithOptions(
    "CREATE INDEX customer_name ON customers(name)",
    db.ExecOptions{IndexPublicationAbortAfter: &abortAfter})
```

`nil` preserves ordinary waiting, and a pointer to zero requests immediate
cancellation. A negative threshold is rejected. If the SQL statement includes
the modifier, its threshold takes precedence over the per-call option. The
policy applies only to standalone autocommit CREATE/DROP INDEX and single-action ALTER TABLE ADD/DROP
INDEX. Explicit transactions, savepoints, multi-action ALTER, constraint
additions, and ALTER INDEX REBUILD reject it. SQL `EXPLAIN` and CLI `.plan`
show the chosen threshold without cancelling transactions or changing indexes.

## Memory, disk, and recovery

Entry installation uses batches of at most 256 rows or approximately 1 MiB,
with a single large row allowed to exceed that byte target. Temporary row
mappings and UNIQUE counts use a private disk store governed by the query
memory budget. Committed change logs are on disk and rotate after catch-up
passes. Temporary stores use `QueryTempDir`, or the operating system temporary
directory, outside the database directory. They are removed on normal success,
failure, and cancellation.

Invisible entries are staged in the database under the reserved index ID.
Durable cleanup metadata is written before staging begins and removed in the
same atomic batch that publishes the catalog entry. An interrupted build is
reclaimed on reopen. Backup captures these markers with its snapshot, so
opening a backup taken during a build also discards unpublished entries.
Reserved IDs are never reused. A build needs disk space for its new entries,
temporary mappings/counts, and changes committed while it runs.

## Dropping an index

Dropping an index commits a catalog change and a durable cleanup marker without
scanning rows or index entries. It needs an exclusive catalog lock to synchronize
with active writers. Current query plans stop using the index after catalog
publication. Readers with an older statement snapshot can finish using the
old entries.

Cleanup hides the obsolete prefix with a range tombstone. Compaction eventually
reclaims the physical bytes; `VACUUM` can perform a full rewrite when space
reclamation is needed. Recreating the same index name allocates a different
storage ID. Constraint-backed indexes and indexes required as the only foreign
key candidate remain protected from independent removal.

Cleanup failures after catalog publication explicitly report that the schema
change committed and index cleanup is pending. Reopening retries durable
cleanup; later index cleanup operations can also reclaim pending prefixes.
The committed catalog is authoritative when checking whether an index exists.

Post-commit cleanup runs only for sessions that staged an index drop. Ordinary
DML `COMMIT` and outermost `RELEASE` skip both the cleanup metadata scan and its
exclusive catalog lock. Transactional drops are reclaimed after commit; a
cleanup failure retains the session's request so a subsequent commit can retry.
Rolling back a drop may cause one harmless empty cleanup on that session's next
commit. Startup and online-build cleanup continue checking persisted markers.

## Transactions and multi-action ALTER

Inside explicit transactions and savepoints, index creation retains the
ordinary atomic DDL path: it takes an exclusive catalog lock, scans and stages
entries in the transaction, and obeys commit/rollback. Multi-action ALTER
statements use that path to preserve all-or-nothing behavior. Their index
creation work can block writers for the duration of the transaction.

Index drops use catalog-only removal and deferred range cleanup in both paths.
Rollback removes the pending cleanup marker and preserves the index. Online
creation applies to standalone index additions; constraint additions and
`ALTER INDEX ... REBUILD` retain their existing validation and locking paths.
