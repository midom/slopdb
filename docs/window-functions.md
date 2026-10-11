# Window functions

SlopDB supports `OVER (...)` and named windows declared with `WINDOW name AS (...)`.
Window expressions run after WHERE, GROUP BY and HAVING, then feed SELECT
projection, DISTINCT, ORDER BY, LIMIT and OFFSET. They preserve source rows and
can appear in SELECT expressions and ORDER BY. Use a derived table or CTE to
filter on a window result.

```sql
SELECT customer_id, amount,
       ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY created_at, id) AS n,
       SUM(amount) OVER (
           PARTITION BY customer_id ORDER BY created_at
           ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
       ) AS recent_total
FROM payments
ORDER BY customer_id, n;
```

Available functions are `ROW_NUMBER`, `RANK`, `DENSE_RANK`, `LAG`, `LEAD`,
`FIRST_VALUE`, `LAST_VALUE`, `NTH_VALUE`, and aggregate windows `COUNT`, `SUM`,
`AVG`, `MIN`, `MAX`. COUNT accepts `*`; the other aggregate windows take one
argument. LAG/LEAD take a value, optional nonnegative int64 offset (default 1),
and optional default for a missing row. NTH_VALUE takes a positive int64 position.
Offset functions preserve NULL input values and ignore the frame. Ranking
functions also ignore the frame; ties share RANK/DENSE_RANK and ROW_NUMBER counts
individual rows. Specify a unique ordering to make ROW_NUMBER deterministic
among peers.

Frames support `ROWS`, `RANGE`, and `GROUPS`, with `UNBOUNDED PRECEDING`,
`UNBOUNDED FOLLOWING`, `CURRENT ROW`, or constant offsets followed by `PRECEDING`
or `FOLLOWING`. A single bound implies an end of CURRENT ROW. ROWS/GROUPS offsets
must be nonnegative int64 values. RANGE offsets accept nonnegative numeric
values and require exactly one ORDER BY expression. Integer RANGE comparisons
remain exact beyond 2^53; integer offsets are clamped before addition/subtraction
to avoid overflow. Prepared parameters are supported in arguments and frame
bounds and belong to each execution's private AST.

With ORDER BY, the default frame is RANGE BETWEEN UNBOUNDED PRECEDING AND
CURRENT ROW, including every peer of the current row. Without ORDER BY, the
default frame covers the entire partition. FIRST_VALUE, LAST_VALUE and NTH_VALUE
respect these frames; in particular LAST_VALUE does not imply the last row of
the entire partition. Empty frames return COUNT=0 and NULL for the other
aggregates and value functions. NULLs are peers and respect NULLS FIRST/LAST.

Named windows may inherit partitioning and add ordering or a frame. Overriding
an existing PARTITION BY or ORDER BY is rejected. A framed named window can be
used directly as OVER name, but cannot be copied with OVER (name), matching the
SQL window inheritance restrictions.

Windows are rejected in WHERE, JOIN conditions, GROUP BY, HAVING and inside
another window or aggregate argument. Ordinary grouped aggregates may feed a
window: `SUM(SUM(amount)) OVER (ORDER BY customer_id)` is supported. DISTINCT
window aggregates, EXCLUDE clauses, FILTER clauses, PERCENT_RANK, CUME_DIST and
NTILE are unsupported.

Execution materializes and sorts partitions under the query memory budget.
Budget exhaustion returns an error; this implementation does not spill windows
to disk. Cancellation is checked while collecting, sorting and evaluating rows.
Overlapping aggregate frames advance both edges once per input row; MIN/MAX use
monotone queues, integer SUM stays exact, and REAL aggregation uses compensated
addition/removal. EXPLAIN ANALYZE includes a Window operator. Streaming API
queries use the materialized fallback for windows.

`tests/regression/window_functions_test.go` covers semantics, persistent views,
int64 boundaries, budgets, cursor fallback and concurrent prepared execution.
The checked-in fixture `tests/regression/testdata/window_reference.json` contains
51 independently generated SQLite query answers; regenerate it with:

```sh
python3 tests/regression/testdata/generate_window_references.py
```

Ordinary tests do not require SQLite or Python installations.
