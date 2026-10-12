# ALTER and persistent object IDs

Tables and indexes have separate logical names and persistent 64-bit storage IDs.
They share a monotonic ID allocator. Committed IDs are not reused after dropping
an object, including after reopening the database.

Row prefixes are `0x01 | 0xff | uint64-big-endian table ID | ':'`; secondary
index prefixes are `0x02 | 0xff | uint64-big-endian index ID | ':'`. Primary-key
components, hidden row IDs, and secondary-key values retain their existing
encodings. Secondary entries refer to the complete ID-prefixed row key.

## Atomic catalog renames

```sql
ALTER TABLE accounts RENAME TO customers;
ALTER TABLE customers RENAME COLUMN label TO name;
ALTER INDEX account_lookup RENAME TO customer_lookup;
RENAME TABLE customers TO temporary, archived TO customers, temporary TO archived;
```

Renames preserve storage IDs and existing row/index keys, payloads, and sequence
numbers. They do not scan rows or rebuild indexes. Foreign-key metadata, views,
triggers, indexes and counters follow the renamed object. Explicit view output
aliases remain unchanged. CTE and derived-table output bindings are preserved
when their underlying columns are renamed. Constraint names can also be renamed:

```sql
ALTER TABLE customers RENAME CONSTRAINT old_unique TO new_unique;
```

The catalog and its dependent definitions are published in one atomic storage
batch. BEGIN/COMMIT/ROLLBACK and savepoints apply. Other sessions see committed
catalog versions; a failed multi-action statement restores its original state.
Renames can wait for the catalog lock held by active transactions. Their work
depends on catalog definitions rather than the number of stored rows.

## Column changes

```sql
ALTER TABLE customers ADD COLUMN score INTEGER NOT NULL DEFAULT 0;
ALTER TABLE customers ADD COLUMN IF NOT EXISTS note TEXT;
ALTER TABLE customers DROP COLUMN IF EXISTS note;
ALTER TABLE customers ALTER COLUMN score SET DEFAULT 10;
ALTER TABLE customers ALTER COLUMN score DROP DEFAULT;
ALTER TABLE customers ALTER COLUMN score SET NOT NULL;
ALTER TABLE customers ALTER COLUMN score DROP NOT NULL;
ALTER TABLE customers ALTER COLUMN score TYPE REAL USING CAST(score AS REAL);
ALTER TABLE customers ALTER COLUMN score SET DATA TYPE DOUBLE PRECISION;
ALTER TABLE customers ALTER COLUMN label TYPE VARCHAR(200);
ALTER TABLE customers ALTER COLUMN label TYPE VARCHAR(80) ALLOW EXISTING;
ALTER TABLE customers MODIFY COLUMN score REAL NOT NULL DEFAULT 0 FIRST;
ALTER TABLE customers CHANGE COLUMN score rating REAL AFTER name;
```

ADD's COLUMN keyword is optional. Supported type targets are INTEGER/INT/BIGINT,
REAL/FLOAT/DOUBLE/DOUBLE PRECISION, TEXT, VARCHAR(n), BLOB and VARBINARY(n).
USING expressions read the old
row. Invalid conversions, unknown expressions, overflow, duplicate keys and
constraint violations reject the statement. Defaults affect future inserts;
changing a default does not replace existing values. Adding a column evaluates
its default for existing rows. A populated table cannot receive a NOT NULL
column whose backfill is NULL. PRIMARY KEY components always remain NOT NULL.

MODIFY and CHANGE replace the column definition, including its default,
nullability, inline CHECK and REFERENCES clauses; existing key constraints remain.
FIRST and AFTER are supported for ADD, MODIFY and CHANGE. Existing row values
are preserved when columns move. DROP cannot remove the last column.
RESTRICT is the default when a column has dependent constraints, indexes, views,
triggers or foreign keys; CASCADE removes those dependencies explicitly.
Dependent view chains and their attached triggers are removed together.

Column alterations use the database-wide Unicode collation, with BINARY as a
column-level override. `ALTER COLUMN c COLLATE BINARY` and type declarations
with `COLLATE BINARY` are accepted; unsupported column collations fail.

`ALTER COLUMN ... TYPE VARCHAR(n)` or `VARBINARY(n)` updates only the catalog
when the type and collation stay the same and the bounded length increases.
Rows and indexes retain their physical encodings, so the change does not scan
or rewrite them. A shorter length normally validates and rewrites existing rows;
it fails if any value exceeds the new limit. Add `ALLOW EXISTING` to a shorter
length to update only the catalog. Existing oversized values remain readable and
indexed. Inserts and assignments to that column must meet the new limit, even
when an assignment repeats an old value. Updates to other columns preserve old
oversized values. This setting persists across reopen. Catalog locking still
waits for active transactions and briefly coordinates with other sessions.

## Constraints and indexes

```sql
ALTER TABLE customers ADD CONSTRAINT customer_pk PRIMARY KEY (id);
ALTER TABLE customers ADD CONSTRAINT customer_unique UNIQUE (tenant, name);
ALTER TABLE customers ADD CONSTRAINT positive CHECK (rating >= 0);
ALTER TABLE orders ADD CONSTRAINT customer_fk FOREIGN KEY (customer_id)
    REFERENCES customers(id);
ALTER TABLE customers DROP CONSTRAINT IF EXISTS positive;
ALTER TABLE customers DROP PRIMARY KEY;
ALTER TABLE customers ADD INDEX lookup (tenant, name);
ALTER TABLE customers ADD UNIQUE KEY external_unique (tenant, name);
ALTER TABLE customers RENAME INDEX lookup TO customer_lookup;
ALTER INDEX customer_lookup REBUILD;
ALTER TABLE customers DROP INDEX customer_lookup;
```

Constraint additions validate existing data. UNIQUE allows multiple tuples with
NULL components, while PRIMARY KEY requires every component to be non-NULL.
Constraint-backed indexes cannot be dropped independently; use DROP CONSTRAINT.
DROP CONSTRAINT and DROP COLUMN support CASCADE/RESTRICT. Foreign-key row actions
remain NO ACTION/RESTRICT; DDL CASCADE does not enable ON DELETE CASCADE.
Unnamed constraints receive generated names, visible in SHOW CREATE TABLE.
Named constraints survive reopening and SQL dump/reload. ADD UNIQUE INDEX/KEY
creates an independently droppable unique index.

Standalone CREATE INDEX and single-action ALTER ADD INDEX/KEY build online:
independent sessions can commit DML during the scan and catch-up, followed by
bounded final reconciliation and atomic publication under an exclusive catalog
lock. Index drops change the catalog without scanning entries and reclaim the
obsolete prefix with a range tombstone, preserving pinned readers. Explicit
transactions and multi-action ALTER retain the ordinary atomic creation path.
See [online indexes](online-indexes.md) for locking, memory, recovery and UNIQUE
validation details.

Multiple actions can be separated by commas and are atomic:

```sql
ALTER TABLE customers ADD COLUMN extra TEXT, ALTER COLUMN rating SET DEFAULT 1;
ALTER TABLE IF EXISTS old_table DROP COLUMN IF EXISTS obsolete;
```

Changes requiring data conversion, reordering, validation or key rebuilding can
scan the table and materialize rows. They are not online, constant-time data
migrations. Table/index/column renames, default changes and the length-only
ALTER forms above avoid that work. Standalone index additions use the online
build described above; publication still coordinates with active writers.

## Views

```sql
ALTER VIEW customer_names RENAME TO names;
ALTER VIEW names AS SELECT id, name FROM customers;
ALTER VIEW names (identifier, label) AS SELECT id, name FROM customers;
```

ALTER INDEX and ALTER VIEW also accept IF EXISTS. These operations use the normal
catalog transaction and invalidate prepared bindings as needed.

## Existing databases

Opening a legacy name-prefixed database performs a one-time upgrade. It copies
rows and rebuilds secondary entries in bounded batches, retaining the original
namespaced data until the copies are flushed. The new catalog is then published
atomically and flushed before legacy ranges are retired. A persisted migration
plan makes interrupted copies and interrupted cleanup resumable. This upgrade
can take time proportional to the data size and temporarily needs space for both
layouts; subsequent opens do not repeat it. The new layout is not readable by
older binaries that assume names in row keys.

Backup preserves IDs and the allocator. A SQL dump creates fresh local IDs while
preserving logical schema and data. Historical storage measurements record their
source revision and predate this key-layout change.
