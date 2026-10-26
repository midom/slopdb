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

## Exact counts through execution

Every COUNT reads the visible entries at its statement snapshot. Unfiltered
simple COUNT(*) and non-NULL literal counts use the storage-owned batched count
scan, without row decoding, group maps or a SQL callback for every key. Large
ranges can use the bounded parallel scheduler described below.

Private writes and temporary tables use a key-only cursor with the normal
transaction overlay, preserving savepoints, REPEATABLE READ and session-local
temporary visibility. Numeric literals retain their dialect-specific lazy
validation on nonempty input. Profiling keeps ordinary measured visits so
EXPLAIN ANALYZE records actual rows and payload bytes.

Counts are never persisted, cached or maintained by mutations or ANALYZE. No
cardinality metadata, count-specific commit locks or counter writes are added
to the data path. ANALYZE statistics remain optimizer estimates, never query
answers.

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

Large exact scan counts and eligible ungrouped typed COUNT/MIN/MAX scans can use
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
