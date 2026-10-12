# Planning schema changes

Use SQL `EXPLAIN` or the CLI `.plan` command before changing a schema:

```text
EXPLAIN ALTER TABLE items ALTER COLUMN label TYPE VARCHAR(200);
EXPLAIN CREATE INDEX items_label ON items(label);
.plan DROP INDEX items_label
```

The command inspects the current catalog without executing the change or
scanning stored rows. Each action has an output row describing its expected
work, row scans, row rewrites, index rebuilding, and locking. It accepts
ALTER TABLE/INDEX/VIEW and CREATE/DROP INDEX. `EXPLAIN ANALYZE` accepts queries
only and does not execute schema changes.

| Work | Meaning |
| --- | --- |
| `catalog-only` | Changes metadata without scanning or rewriting rows. This includes renames, defaults, and a VARCHAR/VARBINARY length increase with unchanged storage encoding. `ALLOW EXISTING` makes a shorter limit catalog-only too. |
| `online index build` | Builds an index from a snapshot while writers continue, then reconciles committed changes before publication. |
| `index build` | Builds an index inside an explicit transaction or atomic multi-action ALTER, holding the exclusive catalog lock. |
| `index rebuild` | Scans table rows and regenerates existing index entries. |
| `table rewrite` | Scans or rewrites rows and maintained indexes. Its cost grows with table size. |
| `database rewrite` | A collation change regenerates clustered and secondary keys across the database. |
| `no-op` | An IF EXISTS/IF NOT EXISTS condition makes this action unnecessary. |

Online index creation uses a shared catalog lock while scanning and catching
up, then an exclusive catalog lock for publication. Other ALTER actions take
the exclusive catalog lock. Both lock modes can wait for conflicting active
transactions; the plan describes this explicitly. Standalone index additions
are online, while explicit transactions and multi-action ALTER retain their
atomic transactional behavior.

For standalone index changes, `KILL TRANSACTIONS AFTER '250ms'` requests
cancellation and rollback of transactions blocking publication after the
specified wait. `EXPLAIN` and `.plan` include that threshold in the locking and
detail fields. The explanation only describes the policy; it does not cancel
any transactions. The modifier is rejected for explicit transactions,
multi-action ALTER, and schema changes other than index additions and drops.
See [online indexes](online-indexes.md) for SQL examples and the Go API option.

A plan describes work from the current definition, including the column
changes of earlier actions in the same ALTER. It does not reserve a lock,
promise a duration, or validate every stored value and dependency. The actual
statement still checks its constraints. Run the change separately when ready.

For query work, use `EXPLAIN` and `EXPLAIN ANALYZE`. [Query profiling](query-profiling.md)
explains the estimates, actual reads, and timing fields.
