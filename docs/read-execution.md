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
scan, without row decoding, group maps or a SQL callback for every key. Each
query runs synchronously on its caller's goroutine, including large scans.

Private writes and temporary tables use a key-only cursor with the normal
transaction overlay, preserving savepoints, REPEATABLE READ and session-local
temporary visibility. Numeric literals retain their dialect-specific lazy
validation on nonempty input. Profiling keeps ordinary measured visits so
EXPLAIN ANALYZE records actual rows and payload bytes.

COUNT does not consult persisted totals or caches across statements. Mutations
and ANALYZE do not maintain count answers, and the data path has no added
count-specific metadata, commit locks or counter writes. ANALYZE statistics
remain optimizer estimates, never query answers.

## Typed predicates and aggregates

Typed aggregate programs bind column slots, types and dialect rules once.
MySQL uses the common typed path where comparisons, DECIMAL/unsigned arithmetic,
NULLs and short-circuit evaluation have been proven equivalent. Unsupported
expressions and collations keep the expression evaluator. Signed integer
comparisons and remainder operations can read operand registers directly and
write scalar results without intermediate copies of the full SQL value.
Mixed numeric types, ENUM/hex values and other coercions retain the common path.

Binary TEXT comparisons, including common date predicates, can inspect encoded
bytes directly and produce private boolean slots. Every unproven predicate stays
in its original residual order. Rebound parameter types and unusual physical
values fall back safely. Strings that must be retained remain owned.

Ungrouped COUNT/SUM/AVG/TOTAL/MIN/MAX without DISTINCT use one accumulator rather
than hashing the empty group and recalculating group memory for every row.
A fixed-width filtered COUNT(*) can fuse decoding, predicate evaluation and
counting into one reusable row instead of staging 128 rows. Predicate programs
inspect their result register directly, avoiding a returned SQL-value copy.
Other aggregates keep their batch processing and memory checks; grouping,
DISTINCT and spilling retain their existing accounting paths.

## Single-threaded query execution

Scans and aggregate operators run on the calling query's goroutine. They do not
partition ranges, spawn query workers, merge worker accumulators or reserve
shared CPU slots. Independent sessions can execute concurrently.

The execution paths focus on cached iterator keys, direct visible-key counting,
partial decoding, private reusable buffers and scalar instruction/accumulator
work. All operators use the statement snapshot and normal transaction overlay;
there is no parallel reduction that changes arithmetic order.

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
BLOB winners and result maps never alias execution scratch.
