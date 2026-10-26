# Read execution and allocation reuse

Simple scans, aggregates, prepared mutations and join predicates share the SQL
execution layer, so these improvements apply to embedded Go, database/sql,
MySQL and PostgreSQL callers. Protocol result ownership is unchanged.

## Scan traversal

Reusable LSM merge cursors cache each child's current key, skip heap updates for
single-source advancement, and avoid redundant validity/key/snapshot wrappers.
A checksummed SSTable maximum sequence can prove that its snapshot filter is
unnecessary. Other files still filter every visible version normally. Seek
bounds compare user keys, including embedded NULs and prefix-related keys.

`CountRangeAtContext` traverses exact visible entries inside storage in batches,
without copying values or invoking a SQL callback per row. It still merges
versions, point deletions and range tombstones, propagates read errors and checks
cancellation during skipped as well as visible entries. It never estimates a
count from physical blocks.

## Maintained exact counts

New permanent tables have 64 signed count shards and a version marker in the
same physical LSM. Keys use the table's stable ID under the reserved
`\x00__row_counts_v1__` metadata prefix. Shards contain canonical JSON integer
values; the marker is `[1,64]`. A writer updates its session's shard in the same
atomic WAL/replication batch as its rows. A shard can be negative when another
session inserted the deleted row; only the sum is the table count.

Writers serialize only a changed count shard through its publication. Updates
whose existing-row baseline is proven do not perform another row lookup, change
counts or take the shard mutex. Final private mutations determine cardinality;
repeated writes, failed statements and savepoint rewinds do not double count.
Private commit scratch is reused with bounded retention. Drops retire count
metadata atomically; renames retain the stable ID and counters.

Unfiltered simple `COUNT(*)` and non-NULL literal counts read the shards at the
statement snapshot and apply private transaction writes against that snapshot.
The same rules preserve REPEATABLE READ and old snapshots across compaction.
No stale ANALYZE statistic substitutes for an exact count. EXPLAIN ANALYZE keeps
its scan so the reported input visits and bytes remain meaningful.

Legacy tables without markers use exact visible-key scans. `ANALYZE` backfills
them online using a pinned snapshot and commit-delta capture ordered with the
online-index publication gate. Ordinary startup and SELECT never initialize
metadata or perform maintenance writes. Replica maintenance stays disabled.
Temporary tables continue using their dedicated WAL-less range and exact scan.
Integrity checking validates counter records and compares maintained totals
against the independently decoded visible rows.

## Typed predicates and aggregates

Typed aggregate programs bind column slots, types and dialect rules once.
MySQL uses the common typed path where comparisons, DECIMAL/unsigned arithmetic,
NULLs and short-circuit evaluation have been proven equivalent. Unsupported
expressions and collations keep the expression evaluator.

Binary TEXT comparisons, including common date predicates, can inspect encoded
bytes directly and produce private boolean slots. Every unproven predicate stays
in its original residual order. Rebound parameter types and unusual physical
values fall back safely. Strings that must be retained remain owned.

Ungrouped COUNT/SUM/AVG/TOTAL/MIN/MAX without DISTINCT use one accumulator rather
than hashing the empty group and recalculating group memory for every row.
Memory checks still run at batch boundaries. Grouping, DISTINCT and spilling
retain their existing accounting paths.

## Bounded parallel reads

Large exact key counts and eligible ungrouped typed COUNT/MIN/MAX scans can use
multiple workers. Planning reads only resident block indexes, selecting byte
quantiles across a whole ordered LSM level. It returns complete, disjoint user
key intervals; each worker reads all overlapping sources at the same pinned
snapshot. A physical-byte floor avoids tiny tasks.

Workers have separate programs, registers, decode buffers and accumulators.
Admission borrows a shared CPU budget without waiting or spinning, divides the
query memory budget, and releases unused reservations when fewer partitions
are available. Concurrent aggregate queries receive fewer workers; a saturated
budget uses serial execution. Cancellation waits for every worker to finish
before releasing the parent snapshot.

Small or memtable-only ranges, private write overlays, profiling, grouped or
DISTINCT aggregation and variable-width typed inputs retain serial plans.
SUM/AVG/TOTAL remain serial because regrouping can change overflow and rounding
behavior. Their scalar accumulator and decoding optimizations still apply.

## Mutations and joins

Eligible prepared single-row INSERTs cache literal-to-column bindings and
private row/key/encoding scratch. Prepared point UPDATEs reuse their runtime
plan and copy unchanged encoded fields; constraint, locking and index
maintenance paths stay common. Prepared locking point reads retain normal
current-read, conflict, NOWAIT/SKIP LOCKED and result ownership rules.

Bounded tuple SELECTs decode only fields needed by projection, ordering,
grouping and the actual residual predicate. Supported join-stage predicates use
compact physical-column slots rather than repeated name-map lookup. Unsupported
and correlated expressions keep the existing evaluator. Retained join outputs,
BLOB winners and result maps never alias worker scratch.
