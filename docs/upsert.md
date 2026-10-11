# INSERT ... ON CONFLICT

SlopDB supports a single `ON CONFLICT` clause on `INSERT ... VALUES`, `INSERT ... SELECT`, and `INSERT ... DEFAULT VALUES`:

```sql
CREATE TABLE inventory (
    warehouse INTEGER,
    item INTEGER,
    quantity INTEGER NOT NULL CHECK (quantity >= 0),
    PRIMARY KEY (warehouse, item)
);

INSERT INTO inventory VALUES (1, 10, 5)
ON CONFLICT (warehouse, item)
DO UPDATE SET quantity = inventory.quantity + excluded.quantity
RETURNING warehouse, item, quantity;

INSERT INTO inventory VALUES (1, 10, 2)
ON CONFLICT (warehouse, item) DO NOTHING;
```

A conflict target must contain exactly the columns of an existing primary key or UNIQUE constraint/index. Composite targets may list their columns in a different order. With no target, any unique constraint can trigger the action. NULL values in UNIQUE columns do not conflict. Partial indexes, expression targets, target predicates, multiple clauses, and `REPLACE ... ON CONFLICT` are rejected. UPSERT on a view is unsupported.

`excluded.column` refers to the attempted row after default evaluation and column affinity conversion. Unqualified columns and `table.column` in `DO UPDATE` refer to the existing conflicting row. All SET expressions observe that row before its assignments; `DO UPDATE ... WHERE` can skip the update when its condition is false or NULL. The excluded scope is also available to correlated subqueries in these expressions.

Only uniqueness conflicts invoke these actions. Invalid types, NULL primary keys, NOT NULL and CHECK failures still fail the statement. The update action applies ordinary UPDATE constraints, foreign-key enforcement, row locks, primary-key changes, secondary-index maintenance, and BEFORE/AFTER UPDATE triggers. Any failure in the update action aborts the whole INSERT, including earlier rows and trigger writes. Foreign keys are validated against the final statement state as with normal DML; a row skipped by DO NOTHING introduces no foreign-key reference.

BEFORE INSERT triggers run for each attempted row. AFTER INSERT triggers run only for actual insertions. A conflict update runs the UPDATE triggers rather than AFTER INSERT. Prepared statements support parameters in VALUES, SET, and WHERE, and can be shared across independent sessions with private bindings. Ordinary transaction, statement rollback, and savepoint behavior applies. Concurrent writers use the existing row and unique-value lock manager and statement retry mechanism.

`Changes` counts actual insertions and conflict updates, including updates assigning unchanged values. Skipped rows are omitted from RETURNING and contribute no change. RETURNING contains the final inserted/updated values. `LastID` reports the last actual insertion in this INSERT; conflict updates and skipped rows do not create a LastID. Successful conflict actions reserve attempted AUTOINCREMENT IDs, so gaps may persist across reopening. Explicit NULL in an AUTOINCREMENT primary key remains an error; omit the column to generate an ID.

The implementation and regression cases are in [`sql/upsert.go`](../sql/upsert.go) and [`tests/regression/upsert_test.go`](../tests/regression/upsert_test.go).
