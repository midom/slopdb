# Text collation and Unicode equivalence

SlopDB stores the original bytes of every `TEXT` value. A collation controls how
those values compare and how their index keys are built; it does not rewrite the
text returned by a query. The collation is a database-wide setting, with explicit
SQL `BINARY` overrides for individual expressions and columns.

## Modes

`BINARY` is the default for new and existing databases. It compares `TEXT` byte
for byte. Four Unicode normalization forms are available, each with an optional
`_CI` suffix for case insensitive comparison:

| Mode | Equivalence | Example |
| --- | --- | --- |
| `BINARY` | Identical bytes | `é` differs from `e` followed by a combining acute accent. |
| `NFC`, `NFD` | Canonical Unicode equivalence | Those two spellings of `é` compare equal. |
| `NFKC`, `NFKD` | Canonical and compatibility equivalence | Fullwidth `Ａ` compares equal to `A`; ligature `ﬀ` compares equal to `ff`. |
| Any Unicode mode with `_CI` | Its normalization equivalence plus Unicode case folding | Under `NFKC_CI`, `Ａ` compares equal to `a`. |

NFC and NFKC use composed normalized keys; NFD and NFKD use decomposed keys.
Equal values compare equal in either form, but their ordering can differ because
the normalized keys differ. `_CI` uses Unicode case folding, rather than a
locale-specific alphabetic order. For example, `Straße` and `STRASSE` compare
equal under `NFC_CI`. These modes are not locale-aware language collations.

Invalid UTF-8 in a `TEXT` value is compared bytewise. SlopDB leaves its bytes
unchanged instead of replacing invalid sequences during normalization. `BLOB`
values are unaffected by text collation.

## Read and change the setting

```sql
PRAGMA collation;                 -- returns the persisted mode
PRAGMA collation = 'NFC_CI';      -- canonical equivalence plus case folding
PRAGMA collation(NFKC_CI);       -- compatibility equivalence plus case folding
```

The Go `*db.Database` API also provides `database.SetCollation("NFKC_CI")` and
`database.SetCollationContext(ctx, "NFKC_CI")`. Mode names are case insensitive on
input; `PRAGMA collation` reports their uppercase spelling. Unsupported mode
names fail. The selected mode survives closing and reopening the database.

The setting affects `TEXT` equality and ordering, including predicates,
`ORDER BY`, grouping, `DISTINCT`, `IN`, uniqueness checks, and index lookups.
`LIKE` normalizes its input and pattern under Unicode modes before applying its
existing wildcard rules. `GLOB` and `REGEXP` retain their existing matching
behavior and do not use the collation. No mode changes numeric or `BLOB`
comparisons.

## Override one expression or column

```sql
SELECT * FROM names WHERE BINARY name = 'é';
SELECT * FROM names WHERE name COLLATE BINARY = 'é';

CREATE TABLE names (
    id INTEGER PRIMARY KEY,
    display_name TEXT,
    exact_code TEXT COLLATE BINARY UNIQUE
);
```

`BINARY expression` and `expression COLLATE BINARY` compare the expression's
text without Unicode normalization or case folding. `TEXT BINARY` is an
equivalent column declaration to `TEXT COLLATE BINARY`; it persists and keeps
that column's comparisons and index keys bytewise when the database mode
changes. Other per-column or per-expression collation names are unsupported.

For `LIKE`, a `BINARY` override bypasses Unicode normalization and folding.
`LIKE` still retains its existing ASCII case behavior and `_` rune matching;
use `=` for exact bytewise equality. `GLOB` and `REGEXP` follow their existing
rules regardless of a `BINARY` override.

## Changing a mode with existing data

A mode change rebuilds all secondary indexes and clustered `TEXT` primary keys
in one statement transaction. The original row text remains intact. A primary
key or `UNIQUE` index can acquire a new collision when previously distinct text
becomes equivalent. SlopDB rejects that change and leaves the old setting and
keys in place. Resolve the duplicate values and retry. The change can be part
of an explicit transaction; rollback restores the previous mode and keys.

For example, changing to `NFC` fails if a `UNIQUE` column contains both `é`
and `e` followed by a combining acute accent. A `TEXT COLLATE BINARY` column
continues to distinguish them, including in its indexes.

The rebuild reads and stages all affected rows and index mutations in memory.
Plan for temporary memory use and a period in which catalog changes and writers
wait on a large database. Unicode range and join plans may use a conservative
scan path where a bytewise index bound cannot preserve collation semantics.

Collation keys depend on the Unicode data supplied by the pinned
`golang.org/x/text` dependency. If that dependency's Unicode tables are updated,
rebuild the keys by changing to `BINARY` and back to the desired Unicode mode
before relying on indexed comparisons under the new tables.
