# SQL behavior to check when results are unexpected

These examples use a fresh database. They are covered by
`TestDocumentedSQLSurprises` in `tests/regression/sql_surprises_test.go`.
See [SQL semantics](sql-semantics.md) for the full rules and
[query troubleshooting](application-operations.md#explaining-a-slow-query)
for inspecting access paths.

## NULL is an unknown value

`NULL = NULL` returns NULL. Test missing values with `IS NULL`.
WHERE includes only TRUE rows, so both FALSE and NULL are filtered out.

```sql
SELECT NULL = NULL AS equality, NULL IS NULL AS missing;
```

The result is `equality = NULL`, `missing = 1`. A NULL in a `NOT IN` list
also makes a nonmatching result unknown:

```sql
SELECT 2 NOT IN (1, NULL) AS excluded;
```

The result is NULL. When the list comes from a table, use a correlated
`NOT EXISTS` to test whether a matching row exists, or explicitly exclude NULLs
from the subquery when that matches the intended meaning.

## LEFT JOIN filters can remove unmatched rows

```sql
CREATE TABLE customers(id INTEGER PRIMARY KEY);
CREATE TABLE orders(id INTEGER PRIMARY KEY, customer_id INTEGER, paid INTEGER);
INSERT INTO customers VALUES(1),(2);
INSERT INTO orders VALUES(10,1,1);
SELECT c.id AS customer_id, o.id AS order_id FROM customers c
LEFT JOIN orders o ON o.customer_id=c.id AND o.paid=1 ORDER BY c.id;
```

This returns `(1, 10)` and `(2, NULL)`. Moving `o.paid=1` from ON into
WHERE removes customer 2 because its missing order has `paid = NULL`.
Keep the filter in ON when unmatched customers should remain.

## Integer division and numeric conversions are explicit

```sql
SELECT 5/2 AS integer_ratio, CAST(5 AS REAL)/2 AS real_ratio;
```

The results are `2` and `2.5`. Cast an operand to REAL when a fractional
result is intended. Division by zero, overflowing INTEGER arithmetic and
`CAST('oops' AS INTEGER)` return errors. Invalid expressions such as unknown
column names and unsupported functions are rejected even on empty inputs.

## UNIQUE and CHECK treat NULL differently from NOT NULL

```sql
CREATE TABLE readings(id INTEGER PRIMARY KEY, value INTEGER UNIQUE CHECK(value>0));
INSERT INTO readings VALUES(1,NULL),(2,NULL);
```

Both rows are accepted: UNIQUE permits multiple NULLs and CHECK accepts an
unknown result. Declare `value INTEGER NOT NULL UNIQUE CHECK(value>0)` when
missing values should be rejected. Every PRIMARY KEY component is already
NOT NULL; an explicit NULL never requests an AUTOINCREMENT value. Omit that
column to generate an ID.

## Text equality depends on the database collation

```sql
PRAGMA collation='NFKC_CI';
SELECT 'Ａ'='a' AS folded, BINARY 'Ａ'='a' AS bytewise;
```

The results are `1` and `0`. NFKC applies compatibility normalization and
`_CI` applies case folding. BINARY compares the original bytes. Changing the
database collation rebuilds indexes and can fail when formerly distinct
PRIMARY KEY or UNIQUE values become equal. Stored strings keep their original
bytes. See [text collation](text-collation.md) for canonical versus compatibility
equivalence and invalid UTF-8 behavior.

## VARCHAR limits count stored code points

```sql
CREATE TABLE names(id INTEGER PRIMARY KEY, name VARCHAR(1));
INSERT INTO names VALUES(1,'é');
```

The precomposed character is one code point and fits. An `e` followed by
a combining acute accent is two code points and fails this limit, even when
the active collation considers the strings equal. VARBINARY limits count bytes.
Limits reject oversized writes rather than truncating them. `ALLOW EXISTING`
on a length shrink preserves older values; new assignments must meet the
new limit. See [ALTER table](alter-table.md).

## Ordering and transaction visibility need explicit choices

Without ORDER BY, SQL does not promise row order. A LIMIT without ORDER BY
does not pick a stable page. Order by a unique key to break ties when paginating.

SlopDB uses READ COMMITTED: each statement gets a fresh snapshot plus the
transaction's own writes. Repeating a SELECT within one transaction can see
another session's newly committed rows. Use a locking read when concurrent
modification must wait, and close streaming results promptly because they pin
their statement snapshot. [Prepared statements](prepared-statements.md) and
[application operations](application-operations.md) cover those APIs.
