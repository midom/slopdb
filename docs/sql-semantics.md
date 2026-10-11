# SQL integrity and execution behavior

SlopDB implements a subset of SQL with SQLite and MySQL extensions. These rules
make supported operations enforce their declared meaning; they do not claim full
ISO SQL conformance.

## Keys and types

Every component of a PRIMARY KEY is implicitly NOT NULL. This applies to VALUES,
INSERT ... SELECT, defaults, and UPDATE, including composite keys. Explicit NULL
is rejected even for INTEGER PRIMARY KEY AUTOINCREMENT. Omit an AUTOINCREMENT
column to generate an ID; omitting an ordinary primary-key column is an error
unless its DEFAULT supplies a non-NULL value. Duplicate primary-key declarations
and invalid AUTOINCREMENT declarations are rejected.

All columns enforce their declared INTEGER, REAL, TEXT, or BLOB type after the
existing affinity conversions. For example, numeric text can become INTEGER,
and an INTEGER can become REAL, but arbitrary text cannot be stored in an
INTEGER column. Point updates and general DML follow the same rules. UNIQUE
continues to permit multiple NULL values, and CHECK accepts UNKNOWN as well as
TRUE.

On opening an older database, valid primary-key metadata is upgraded to NOT
NULL. A bounded probe detects the legacy NULL-key storage band. Opening a table
with legacy NULL keys fails with a repair message rather than hiding rows or
silently assigning new identities. Repair such rows using the previous version
before upgrading. Stored DEFAULT and CHECK expressions must parse and validate;
loading no longer silently drops a malformed constraint.

## Foreign keys

Column and table foreign keys are enforced against a PRIMARY KEY or UNIQUE
candidate key with compatible column types. Composite and self references are
supported, including reordered candidate-key columns and an omitted referenced
column list that defaults to the parent's primary key.

Validation uses the final statement state, permitting a multi-row self reference
to a parent inserted later in the same statement. Any NULL component exempts a
reference (MATCH SIMPLE). Missing parents, parent deletes or key changes that
would orphan children, and dropping a referenced table or its only supporting
unique index are rejected. Referenced parent rows are locked until transaction
completion, including UNIQUE-key references, so concurrent child inserts and
parent changes cannot both commit an orphan. Failed statements roll back their
writes. Renames preserve references, and transaction rollback restores the
original constraint metadata.

ON DELETE / ON UPDATE NO ACTION and RESTRICT are accepted with immediate
statement validation. CASCADE, SET NULL, SET DEFAULT, deferred constraints, and
other unsupported constraint options are rejected explicitly. Existing foreign
key data is not scanned or repaired automatically on open; older versions did
not enforce those references. New writes and changes to referenced parents are
validated.

## Expressions and grouping

Scalar subqueries return NULL for zero rows, a value for one row and one column,
and an error for extra rows or columns. Division by zero, integer overflow,
non-numeric arithmetic, invalid numeric casts, and floating arithmetic overflow
produce errors. Integer modulo requires integer operands.

A grouped query must use grouped expressions, aggregate expressions, constants,
or a simple table's columns functionally dependent on its grouped primary key.
Arbitrary values from the first row of a group are no longer accepted. Aggregate
calls are rejected in WHERE, JOIN ON, and GROUP BY, including nested aggregates.
COUNT() and COUNT(DISTINCT *) are invalid. Aggregate DISTINCT, aggregates inside
CASE/IN/BETWEEN, and aggregates used only in ORDER BY are evaluated correctly.
GROUP BY accepts output positions; supported grouping aliases resolve to their
expressions. Function names and argument counts are validated even on empty
inputs. DEFAULT expressions cannot refer to columns or subqueries, and CHECK
cannot contain a subquery or refer to an unknown column.

TRUE and FALSE are recognized. SQL three-valued NULL logic remains intact.
Nonbinary collations and unsupported conflict policies produce an error rather
than being ignored. The existing SQLite-style truth values, affinity, scalar
MIN/MAX, AUTOINCREMENT, REPLACE, TOTAL, GROUP_CONCAT, date functions, LIMIT, and
MySQL-style introspection remain dialect extensions. CTEs, window functions,
recursive CTEs, and other unimplemented language features remain outside this
subset.

## Composite query execution

Composite primary keys and secondary indexes use equality prefixes, finite
literal IN lists, and integer ranges. Full primary-key equality uses a direct
row lookup before considering covering indexes. Composite IN point sets use a
batched read view; duplicates and NULLs preserve predicate semantics. Ordered
ranges scan forward or backward and stop after LIMIT/OFFSET when safe. A
noncovering secondary lookup fetches the current base row. Aggregation,
DISTINCT, and residual predicates preserve their ordinary evaluation rules.

UPDATE and DELETE use bounded composite primary-key ranges, collect rows, and
close their iterators before writing. Transaction overlays merge only private
keys within the requested range, while the storage read view excludes unrelated
SSTables. FOR UPDATE SKIP LOCKED continues scanning to fill the requested LIMIT.
EXPLAIN reports these ranges, direct lookups, batched reads, and reverse scans.

## Validation

The focused checks in `/tmp/godb/` follow the repository's temporary-test-file
convention:

- `sql_standard_review.go`: 134 checks for key rejection, atomic rollback,
  foreign keys, concurrent-safe parent changes, renames, casts, numeric errors,
  grouping, scalar subqueries, DDL validation, empty-input DML validation, and
  reopen behavior.
- `tuple_plan_review.go`: 302 optimized-query versus forced-scan comparisons,
  including signed bounds, IN duplicates/NULL/mixed types, residual filters,
  aggregate/DISTINCT handling, ordering, aliases, private writes, and reopen.
- `sql_differential_review.go`: 599 overlapping SQL cases compared with Python's
  SQLite reference for three-valued logic, aggregates, GROUP BY, joins, and
  scalar subqueries. Intentional stricter behavior, such as zero division and
  NULL primary keys, is checked separately.
- `legacy_primary_review.go`: valid old-catalog upgrade and rejection of legacy
  NULL primary-key rows.
- `foreign_concurrency_review.go`: 80 competing child-insert/parent-delete races,
  UNIQUE parent locking, and composite queue SKIP LOCKED, also run with `-race`.

Available existing review runners passed 45 general read checks, 177 ordered
read checks, 4,000 concurrent indexed updates and reserved IDs, and 32 workers
performing atomic 100-row transactions in each of no-fsync, no-WAL, and durable
modes. Temporary copies use the current `slopdb` import path; the transaction
runner's raw-key assertions were updated for the existing sortable key format
and the newly rejected omitted ordinary primary key. Historical tests that
intentionally inserted mixed types into INTEGER columns are superseded by
strict-type cases. The four old test runners and the extracted SQLite/MySQL
suite runners named in the supplied instructions were absent in this workspace.

`go build -o slopdb .`, `go vet ./...`, and `git diff --check` passed. The
TPC-C-style workload passed a ten-worker race run, then a sixty-second ordinary
run with WAL enabled and fsync disabled, including all fifteen consistency
checks before and after close/reopen. Results are documented with the load
[tester](../cmd/tpcc/README.md).
