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

Automatic IDs use persistent counters by default. An ID allocated by a committed
INSERT remains reserved after its row is deleted and the database is reopened.

Set `storage.Options.DisableAutoIncrementPersistence = true`, or start the CLI
with `--no-autoincrement-persistence`, to keep these reservations in memory.
In this mode, reopening the database starts allocation above the highest row ID
still present. Deleted high IDs can therefore be used again. IDs stay reserved
within one open instance. Row data still uses the selected WAL and fsync mode.
The option applies to all automatic-ID columns in that open instance. Previously
stored counters are ignored for allocation while this option is enabled.

All columns enforce their declared INTEGER, REAL, TEXT, or BLOB type after the
existing affinity conversions. `VARCHAR(n)` uses TEXT storage and collation,
but limits the stored value to `n` Unicode code points. `VARBINARY(n)` uses BLOB
storage and limits the value to `n` bytes. Both require a positive integer length,
reject overlength INSERT/UPDATE/ALTER results without truncation, and retain
their declarations in schema introspection. Length is checked on the stored
text before Unicode collation normalization; each invalid UTF-8 byte counts as
one decoded replacement rune for this limit. BLOB values remain unaffected by
text collation. `CAST(... AS VARCHAR(n))` and `CAST(... AS VARBINARY(n))` use the
same limits. For example, numeric text can become INTEGER,
and an INTEGER can become REAL, but arbitrary text cannot be stored in an
INTEGER column. Point updates and general DML follow the same rules. UNIQUE
continues to permit multiple NULL values, and CHECK accepts UNKNOWN as well as
TRUE.

Increasing a VARCHAR or VARBINARY length with `ALTER COLUMN ... TYPE` updates
catalog metadata without rewriting rows or indexes. A shorter length checks
existing rows by default. `ALLOW EXISTING` makes a shorter length apply to new
values while preserving older oversized values on unrelated updates; explicit
assignments must meet the new length. See [ALTER table](alter-table.md).

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
Unsupported conflict policies produce an error rather than being ignored. The
existing SQLite-style truth values, affinity, scalar
MIN/MAX, AUTOINCREMENT, REPLACE, TOTAL, GROUP_CONCAT, date functions, LIMIT, and
MySQL-style introspection remain dialect extensions. Supported CTEs, recursive
CTEs, window functions and UPSERT now have their own feature documentation. Other unimplemented language features remain outside this
subset.

## Text collation

The database-wide default is `BINARY`, which compares TEXT as bytes and keeps
existing databases compatible. `PRAGMA collation` reads the current mode;
`PRAGMA collation='NFKC_CI'` or `db.SetCollation("NFKC_CI")` changes it. Supported
Unicode modes are `NFC`, `NFD`, `NFKC`, and `NFKD`, each optionally suffixed
`_CI` for Unicode case folding. NFC/NFD treat canonically equivalent strings
as equal; NFKC/NFKD also equate compatibility forms. Invalid UTF-8 text remains
bytewise so normalization does not silently replace stored bytes.

The mode affects TEXT equality, ordering, grouping, `LIKE` matching and text
index keys. `BINARY expression` or `expression COLLATE BINARY` overrides it for
that expression. A `TEXT BINARY` or `TEXT COLLATE BINARY` column keeps bytewise
comparison and indexing across database mode changes. Other column collations
are rejected. Changing the mode atomically rebuilds clustered TEXT primary
keys and secondary indexes. If newly equivalent values would violate a PRIMARY
KEY or UNIQUE constraint, the change fails and the old mode and indexes remain.
The new mode persists across reopening and participates in transaction rollback.
See [Text collation](text-collation.md) for examples and operational details.

## Composite query execution

Composite primary keys and secondary indexes use equality prefixes, finite
literal IN lists, and integer ranges. Full primary-key equality uses a direct
row lookup before considering covering indexes. Composite IN point sets use a
batched read view; duplicates and NULLs preserve predicate semantics. Ordered
ranges scan forward or backward and stop after LIMIT/OFFSET when safe. A
noncovering secondary lookup fetches the current base row. Aggregation,
DISTINCT, and residual predicates preserve their ordinary evaluation rules.

BETWEEN supplies inclusive bounds to composite primary-key and secondary-index
reads, including signed numeric literals and bound parameters. The original
predicate remains a residual check when only one bound is usable or another
condition tightens the range, preserving mixed-type and NULL semantics.

UPDATE and DELETE use bounded composite primary-key ranges, collect rows, and
close their iterators before writing. Transaction overlays merge only private
keys within the requested range, while the storage read view excludes unrelated
SSTables. FOR UPDATE SKIP LOCKED continues scanning to fill the requested LIMIT.
EXPLAIN reports these ranges, direct lookups, batched reads, and reverse scans.

## Validation

The focused checks live in `tests/regression/` and run with `go test ./...`;
see [Testing](../TESTING.md):

- `sql_standard_test.go`: 134 checks for key rejection, atomic rollback,
  foreign keys, concurrent-safe parent changes, renames, casts, numeric errors,
  grouping, scalar subqueries, DDL validation, empty-input DML validation, and
  reopen behavior.
- `tuple_read_plans_test.go`: optimized-query versus forced-scan comparisons,
  including BETWEEN, signed bounds, IN duplicates/NULL/mixed types, residual filters,
  aggregate/DISTINCT handling, ordering, aliases, private writes, and reopen.
- `between_read_plans_test.go`: prepared BETWEEN reads across the int64 range,
  NULLs, real bounds, 64-bit pagination, unlimited payload sorting and reopen.
- `sql_differential_test.go`: 599 overlapping SQL cases compared with Python's
  SQLite reference for three-valued logic, aggregates, GROUP BY, joins, and
  scalar subqueries. Intentional stricter behavior, such as zero division and
  NULL primary keys, is checked separately.
- `legacy_primary_keys_test.go`: valid old-catalog upgrade and rejection of legacy
  NULL primary-key rows.
- `foreign_concurrency_test.go`: 80 competing child-insert/parent-delete races,
  UNIQUE parent locking, and composite queue SKIP LOCKED, also run with `-race`.

The regression suites passed 45 general read checks, 177 ordered
read checks, 4,000 concurrent indexed updates and reserved IDs, and 32 workers
performing atomic 100-row transactions in each of no-fsync, no-WAL, and durable
modes. Transaction raw-key assertions use the sortable key format and check
rejection of omitted ordinary primary keys. Historical mixed-type INTEGER
expectations are superseded by strict-type cases.

`go build -o slopdb .`, `go vet ./...`, and `git diff --check` passed. The
TPC-C-style workload passed a ten-worker race run, then a sixty-second ordinary
run with WAL enabled and fsync disabled, including all fifteen consistency
checks before and after close/reopen. Results are documented with the load
[tester](../cmd/tpcc/README.md).

`INSERT ... ON CONFLICT` supports explicit PRIMARY KEY/UNIQUE targets, DO NOTHING, and DO UPDATE with excluded values and an optional WHERE. See [UPSERT semantics](upsert.md) for constraints, triggers, concurrency, generated IDs, and supported target syntax.
