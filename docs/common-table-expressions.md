# Common table expressions

`WITH` defines named query results scoped to one SELECT, INSERT, UPDATE or DELETE. Definitions can refer to earlier definitions. An inner WITH can shadow an outer name, and a CTE name shadows a physical table in query table references. DML target tables and stored view definitions continue to resolve against the catalog.

```sql
WITH recent(id, amount) AS (
    SELECT id, amount FROM payments WHERE amount > 100
), totals(amount) AS (
    SELECT SUM(amount) FROM recent
)
SELECT amount FROM totals;

WITH additions(id) AS (SELECT 10 UNION ALL SELECT 11)
INSERT INTO accounts(id) SELECT id FROM additions;
```

All CTEs are materialized once per statement, including unused definitions. Repeated references reuse those rows, preserving volatile expression values. `AS MATERIALIZED` is accepted; `AS NOT MATERIALIZED` is explicitly unsupported. Forward references and mutually recursive definitions are unsupported. An explicit column list must have the query's column count and unique names. Outputs without a column list must have unambiguous names; use explicit aliases or a CTE column list for duplicate names.

WITH queries work in scalar, EXISTS, IN and derived-table subqueries, compound SELECTs, prepared statements, stored views and the Query cursor API. Cursors use materialized fallback for WITH statements. Definitions are reevaluated with current bindings and snapshots each execution, rather than retained between prepared calls.

## Recursive queries

```sql
WITH RECURSIVE numbers(n) AS (
    SELECT 1
    UNION ALL
    SELECT n + 1 FROM numbers WHERE n < 10
)
SELECT n FROM numbers ORDER BY n;
```

A recursive definition requires one nonrecursive anchor followed by `UNION` or `UNION ALL` and one recursive SELECT. That SELECT must reference the recursive table exactly once directly in FROM or an INNER/CROSS join. Each iteration reads the preceding iteration's frontier. UNION removes duplicates against every previously emitted row, including duplicate NULLs; UNION ALL preserves duplicates. Recursion stops when the frontier becomes empty.

Aggregates, GROUP BY, HAVING, DISTINCT, outer joins, locking clauses, ORDER BY and LIMIT/OFFSET inside the recursive compound are rejected, as are nested or multiple recursive references and multiple recursive branches. Put final ordering and pagination in the outer query. These restrictions are reported as errors rather than evaluated with different semantics.

The executor checks caller cancellation during every iteration and emitted-row deduplication. The `storage.Options.MaxCTEIterations` and `MaxCTERows` fields are explicit int64 values. DefaultOptions sets guardrails of 100,000 iterations and 1,000,000 emitted rows; exceeding either returns an error. Callers may select larger 64-bit limits, or explicitly set zero to disable a guardrail. Negative limits are rejected. An outer LIMIT does not short-circuit eager recursive materialization. Keep a terminating predicate or supply a query context deadline.

EXPLAIN resolves output metadata and describes the materialized SELECT consumption path without evaluating CTE expressions or running recursive iteration. EXPLAIN ANALYZE executes it; the existing runtime operator profiles remain available. CTE definitions are materialized before that consumption profile and should not be interpreted as independently costed recursive operators.
