# slopdb — TODO

## Query Operators (10/10 done)

- [x] BETWEEN / NOT BETWEEN
- [x] IN / NOT IN (static + subquery)
- [x] LIKE / NOT LIKE / GLOB / NOT GLOB / REGEXP
- [x] DISTINCT
- [x] CASE WHEN ... THEN ... ELSE ... END
- [x] CAST(expr AS type)
- [x] UNION / UNION ALL / INTERSECT / EXCEPT

## Aggregates & Grouping (8/8 done)
## Joins (7/7 done)
## Subqueries (5/5 done)
## Data Types & Expressions (19/19 done)

- [x] Expanded MySQL string, numeric, temporal, encoding, UUID/IP, digest/compression and connection functions with independent MySQL reference checks; see [function coverage](docs/mysql-functions.md)

- [x] Optional MySQL default quoting for CLI/API input, prepared and streaming queries, with canonical schema persistence and matching SQL exports; see [SQL quoting](docs/sql-quoting.md)
## DML Extensions (5/5 done)

- [x] Multi-row INSERT, INSERT...SELECT, REPLACE, RETURNING
- [x] UPSERT (ON CONFLICT and MySQL ON DUPLICATE KEY UPDATE)

## Views & Triggers (5/5 done)
## Query Plan & Introspection (5/5 done)

- [x] SQL syntax locations, live lock waits/session IDs, typed lock timeout/deadlock diagnostics
- [x] EXPLAIN schema-change work and locking; CLI `.plan`, `.explain`, `.locks` and `.status`
- [x] MySQL-style `SHOW TABLE STATUS` with bounded LSM range sizes and sampled visible row estimates; see [table status](docs/table-status.md)
## Constraints (8/8 done)

- [x] Composite/self foreign keys, ON DELETE/UPDATE CASCADE and SET NULL/DEFAULT, DEFERRABLE constraints and SET CONSTRAINTS with transaction/savepoint validation
## Schema Operations (5/5 done)

- [x] VARCHAR/VARBINARY limits and catalog-only length increases; `ALLOW EXISTING` shrinking
- [x] Online standalone index additions and catalog-only index drops, with atomic publication and recovery cleanup
- [x] Conditional post-commit index cleanup; ordinary DML commits skip cleanup scans and the catalog writer lock
- [x] Optional publication policy that rolls back blocking transactions after a configurable grace period

See [online indexes](docs/online-indexes.md) and
[schema-change planning](docs/schema-change-planning.md) for work and lock behavior.
## Transactions (9/9 done)

- [x] BEGIN/COMMIT/ROLLBACK and savepoints, with statement rollback on errors
- [x] Independent concurrent transactions through `Database.Begin()` and SQL sessions through `Database.NewSession()`
- [x] Pessimistic row and UNIQUE value locking, with catalog locks for transactional DDL
- [x] Query timeout overrides, lock-wait timeouts and optional deadlock detection
- [x] SELECT FOR UPDATE / FOR SHARE / LOCK IN SHARE MODE, NOWAIT and SKIP LOCKED

READ COMMITTED is the native API default. MySQL sessions default to REPEATABLE
READ, with a retained consistent-read snapshot for plain SELECT and current
statement views for locking reads and mutations. Own writes overlay both views.
READ COMMITTED statements see the latest committed data plus private writes;
nonrepeatable reads and phantoms are allowed at that level. Gap locks and
SERIALIZABLE remain unsupported.
Independent sessions execute statements concurrently using MVCC snapshots;
commits publish atomic storage batches. Writers hold row and UNIQUE value locks
until commit or rollback. Blocked statements release their shared execution
view while waiting, then re-evaluate current committed rows. Ordinary SELECTs
do not acquire row locks.
Different rows in the same table can be written independently. Most DDL takes an
exclusive catalog lock; writes take shared catalog locks. Standalone index
creation scans and catches up under a shared catalog lock, allowing writers to
continue, then briefly takes the exclusive lock for publication. Index creation
inside explicit transactions or multi-action ALTER keeps the transactional path.
Index drops update metadata without scanning entries and reclaim physical keys
with MVCC range tombstones. Failed statements
release their new locks, while rollback to a savepoint retains transaction locks.
Generated IDs are reserved across active transactions, so rollback may leave gaps.
`Database.Exec` retains a default session; use independent handles for separate
goroutines' transactions. Closing a session or database rolls back pending
transactions and wakes lock waiters.

Append `FOR UPDATE` to SELECT for exclusive row locks, or `FOR SHARE` / `LOCK IN
SHARE MODE` for shared row locks. Shared readers can coexist; updates, deletes
and exclusive reads wait for incompatible locks. Locks last until commit or
rollback in a transaction, or until statement completion in autocommit. A
transaction can upgrade its shared lock when no other reader holds it.

Append `NOWAIT` after the locking clause to return `db.ErrLockNotAvailable`
immediately on row contention. Append `SKIP LOCKED` instead to omit conflicting
rows without waiting. These modifiers apply only to row locks; catalog waits
still use the configured lock timeout. NOWAIT failure rolls back the statement's
new locks and upgrades, preserving earlier transaction work and locks.

For example: `BEGIN; SELECT id FROM jobs WHERE status = 'ready' ORDER BY id
LIMIT 1 FOR UPDATE SKIP LOCKED;` then update the selected job and commit. Simple
queries sort candidates, skip unavailable rows, and continue until LIMIT is
filled. OFFSET counts available rows, and scanned OFFSET rows remain locked;
LIMIT 0 acquires no row locks. Joins lock participating rows from all direct
tables atomically per result, so a skipped join result leaves no partial locks.
Views retain underlying row keys through projection, grouping and DISTINCT.
Aggregate, GROUP BY and DISTINCT locking reads protect all matching input rows
before collapsing them; SKIP LOCKED removes unavailable input rows first.
Locking clauses do not propagate into derived tables or expression subqueries:
add a clause inside each nested SELECT that needs locks. Locking clauses on
compound SELECTs (UNION / INTERSECT / EXCEPT) are rejected. Empty results take no
row or gap locks, consistent with READ COMMITTED allowing phantoms.

`storage.Options.QueryTimeout` sets the default SQL call timeout (default zero,
unlimited); `LockTimeout` sets the wait budget from the first blocked acquisition
in a SQL call (default 30 seconds with `DefaultOptions`). `ExecWithOptions` and
`ExecContextWithOptions` accept `db.ExecOptions.Timeout` and `LockTimeout`
duration pointers: nil inherits the default. A zero query timeout disables its
limit; a zero lock timeout fails immediately with `db.ErrLockTimeout` on
contention, while uncontended operations still succeed. Negative durations
are rejected. Caller context deadlines still apply and cannot be extended by an
override. Query timeouts include queuing behind the database or session mutex;
SQL execution and storage waits respond to cancellation cooperatively.

Set `storage.Options.DeadlockDetection` to detect wait-for cycles (disabled by
default). The requesting transaction becomes the victim, is rolled back, and
returns `db.ErrDeadlock`; retry the entire transaction. `db.ErrLockTimeout` also
matches `context.DeadlineExceeded`. Lock timeout or query cancellation rolls back
only the interrupted statement, preserving earlier transaction writes and locks;
call rollback to release them. CLI flags are `--query-timeout`, `--lock-timeout`
and `--deadlock-detection`. Optimistic version validation remains a final safeguard
and reports `db.ErrConflict` if external writes bypass the lock manager.

## Storage Engine (13/13 done)

Normal startup reads SSTable indexes, bloom filters and checksummed metadata,
then replays any outstanding WAL records. SSTables store their maximum sequence,
key bounds and range tombstones between the index and the footer. Data block
CRCs are checked on reads.

SQL indexes are maintained atomically with their rows. Set
`storage.Options.VerifyIndexesOnOpen = true` to scan rows, validate indexed types
and reconcile indexes on every open for diagnostics. Ordinary writes continue
to enforce strict indexed types. Row-ID counters initialize lazily with bounded
reverse seeks. `VACUUM` flushes writes and compacts SSTables.

## Client protocols

- [x] MySQL 4.1 protocol over TCP and Unix sockets, password authentication, independent sessions, text queries and binary prepared execution; see [MySQL server](docs/mysql-server.md) for compatibility limits
- [x] MySQL TLS 1.2/1.3 with required transport and CA-verified client certificate authentication on both TCP and Unix sockets
- [x] Persistent MySQL accounts/password hashes; permission enforcement disabled
- [x] Virtual MySQL databases / PostgreSQL schemas in one physical LSM, cross-schema SQL and per-session default resolution
- [x] MediaWiki core schema and workflows: MySQL types/coercions, session settings, named locks, fulltext searches, schema copies, temporary tables and joined mutations; see [MediaWiki compatibility](docs/mediawiki.md)
- [x] Prepared result metadata before execution without evaluating user expressions
- [x] MySQL multiple statements/results and zlib protocol compression
- [x] WAL-less temporary LSM tables, inverted fulltext postings and physical UNIQUE/nonunique prefix indexes
- [x] Exact signed/unsigned integers, DECIMAL and JSON types with JSON functions/accessors
- [x] Durable typed CDC and MySQL row-binlog replication source with GTID/file-position resume; see [CDC boundaries](docs/mysql-cdc.md)
- [ ] MySQL replication receiver, journal retention/purge and automatic initial snapshots
- [x] PostgreSQL 3.0 emulation over TCP/Unix sockets with SCRAM-SHA-256, TLS/client CA authentication, simple/extended queries, prepared metadata, typed bindings, portals, cancellation and transaction recovery; see [PostgreSQL emulation](docs/postgres-server.md)
- [ ] Broader PostgreSQL system catalogs, COPY, newer wire versions and additional PostgreSQL-specific SQL/types

## Miscellaneous

- [x] SQL comments (-- and /* */)
- [x] NULLS FIRST / LAST in ORDER BY
- [x] ESCAPE for LIKE
- [x] Database-wide Unicode normalization and case insensitive collations, with BINARY expression and column overrides
- [x] IF NOT EXISTS (CREATE TABLE/INDEX)
- [x] Common Table Expressions, including dependent scopes and wrapped DML
- [x] Window functions: ranking, navigation, aggregate windows and frames
- [x] Recursive CTEs for supported single-SELECT UNION / UNION ALL shapes
- [ ] General recursive query shapes beyond the supported single recursive SELECT
- [ ] Additional window functions and FILTER/EXCLUDE/DISTINCT window extensions
- [ ] Table-valued functions, including generate_series, and virtual tables

## Replication

- [x] Raft-shipped atomic WAL batches, durable replay checkpoints and linearizable leader SQL
- [x] TCP/Unix snapshot clone, asynchronous nonvoter catch-up and checked voting promotion
- [x] Persistent non-voting replicas and logs-only voters with leadership exclusion, metadata-only application and journal-backed recovery
- [x] Authenticated control/membership services, TLS and CA-verified client certificates
- [x] Optional asynchronous commit, quorum-certified stable prefixes, durable barriers and offline recovery into a new cluster incarnation
- [x] Opt-in bounded-staleness follower/learner reads with quorum-confirmed progress, read-only transactions and consistent catalog/data snapshots
- [x] Signed logical commit positions in native/MySQL/PostgreSQL commit responses, master/session position queries, explicit visibility waits and per-call minimum positions
- [x] Previous/next durable read barriers, exact historical catalog/data views, per-session and per-call policies, compaction-safe retention and cancellable certification waits
- [ ] Automatic SQL routing and application request deduplication
- [x] Background WAL rotation fsync outside storage locks, with durable barriers covering all earlier generations, error propagation and shutdown joins

See [Raft replication](docs/replication.md) for operational limits.
