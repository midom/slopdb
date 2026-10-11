# Ordered GROUP BY and TPC-H Q18

Q18's inner query groups lineitems by `l_orderkey`. The table is already
clustered by `(l_orderkey,l_linenumber)`, so each order's rows are contiguous.
Retaining every group in a hash map added unnecessary work: at the default
64 MiB operator budget, the map crossed its limit and restarted the scan into
external sorting.

Aggregation now recognizes a GROUP BY consisting of a leading clustered
primary-key prefix over one ordered scan interval. Both typed and interpreted
execution retain one reusable accumulator, finalize its HAVING/projection when
the key changes, and continue to the next group. Rejected WHERE rows never
create or split groups. Aliases and ordinal GROUP BY expressions are resolved
before checking eligibility. Secondary index scans, point sets, multiple
intervals, joins and non-prefix expressions retain their existing grouping
paths because their visitation order does not prove this property.

A focused diagnostic on the existing SF=0.2 development database executed:

```sql
SELECT l_orderkey
FROM lineitem
GROUP BY l_orderkey
HAVING SUM(l_quantity) > 300;
```

| Inner-query evidence | Group map with spill | Ordered aggregation |
|---|---:|---:|
| EXPLAIN ANALYZE execution | 10.495 s | 3.078 s |
| Process allocation delta | 3.9406 GB | 0.5588 GB |
| Process allocations | 32.17 million | 3.91 million |
| Temporary spill output | 68.50 MB | 0 |
| Scan passes | Partial scan, then full scan | One full scan |
| Returned groups | 9 | 9 |

The earlier map scan read 645,504 rows before restarting across all 1,199,969
lineitems. The ordered operator consumes that input once. These are focused
CPU-profiled diagnostic executions, not repeated full Q18/TPC-H comparisons.
Full TPC-H comparisons verify complete query answers independently.

Regression tests cover typed/interpreted paths, HAVING, aliases and ordinals,
NULL aggregate arguments, DISTINCT and GROUP_CONCAT, BLOB/REAL clustered keys,
range filtering, pagination, prepared rebinding, rollback/reopen, cancellation
and expression errors. A typed execution invariant asserts that 100 groups
reuse the same accumulator pointer and retain at most 512 estimated accumulator
bytes rather than accumulating one object per group. EXPLAIN ANALYZE identifies
the path as `OrderedAggregate`.
