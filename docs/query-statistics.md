# Query distribution statistics

`ANALYZE` now records column distributions as well as exact table row counts.
Column statistics include an exact NULL fraction, an approximate distinct-value
count, up to 16 sampled most-common values, and up to 128 sampled histogram
quantiles. Samples retain the declared SQL type, including int64 and BLOB values.

Analysis reads the statement's MVCC snapshot and its transaction's private
overlay once. It keeps a reservoir of at most 2,048 values and a 256-entry KMV
distinct sketch per column, so memory does not grow with the number of rows.
Values larger than 512 bytes disable that column's frequency/histogram hints;
NULL fractions and distinct sketches remain available. Distinct counts and
frequency/range estimates are approximate, and samples can miss rare values.

The planner uses these distributions to estimate equality, IN, NULL and range
predicates, choose between indexes and a row scan, and distinguish useful
secondary indexes when several predicates are available. Non-covering secondary
reads include an estimated row-lookup cost. Ordered LIMIT paths retain their
early-stop advantage. Legacy scalar readers respect the same costed scan choice.

Join planning estimates filtered input cardinalities and equijoin fanout using
distinct counts, NULL fractions and overlapping common values. Execution compares
estimated indexed probe work with the cost of reading and hashing a relation.
Small/selective inputs can still be read once to refine those estimates with
actual cardinalities. `EXPLAIN` join descriptions include estimated input rows.

Estimates never change SQL comparison semantics, remove residual predicates, or
change LEFT JOIN preservation and NULL matching. Unsupported predicates and
databases without distribution statistics retain conservative planning defaults.
Statistics are historical hints after later DML; rerun `ANALYZE` after substantial
changes. Column distributions are invalidated by DDL rather than retaining stale
column bindings. Prepared executions consult the current committed/private hints.

Distributions use a separate, versioned catalog key. Existing table-count metadata
and `sqlite_stat1` remain compatible with databases created before this change.
Statistics publish atomically with transaction commits, survive reopen, and follow
statement rollback, savepoints and independent-session catalog refresh.
