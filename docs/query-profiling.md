# Actual query execution metrics

`EXPLAIN ANALYZE SELECT ...` executes the SELECT and returns the existing plan
rows followed by actual SELECT, physical scan/lookup, join and sort metrics.
Plain EXPLAIN continues to describe the plan without executing it. Execution
errors and cancellation propagate to the caller; ANALYZE does not turn them
into a successful result row. Non-SELECT ANALYZE statements are rejected.

```sql
EXPLAIN ANALYZE
SELECT * FROM order_line
WHERE w_id BETWEEN 129 AND 130
ORDER BY w_id, d_id, o_id, number
LIMIT 20;
```

Additional numeric result columns report operator and parent IDs, rows examined,
index entries examined, rows returned, point lookups, encoded bytes read,
elapsed milliseconds, lock wait milliseconds and process heap allocations.
Estimated plan rows use NULL for actual metrics. `Actual...` rows record executed
operators and the `QueryProfile` row contains statement totals. Operators record
the actual hash/probe/nested-loop join choice, including cached-probe candidates.
Logical-stage input counts and physical read counts are distinct: statement
rows-examined totals count base-row reads, rather than summing every join stage.

Go callers also receive a structured `ExecResult.Profile` with the same
execution's operators and totals. Prepared EXPLAIN ANALYZE statements create
fresh profiles on each execution. Sessions retain private profile state, and
ordinary queries have a nil profile. Read counters include work from statement
retries; failed attempts remain visible as SELECT operators with an error.
Lock-wait measurements include waits between attempts.

Operator elapsed times include their children and must not be added together.
Byte counts are encoded key/value bytes inspected by logical storage readers,
including cached values and private transaction overlays; they are not device
I/O counters. Iterator positions count once despite multiple Key/Value calls.

`process_allocated_bytes` and `process_allocations` are Go runtime heap-counter
deltas across profiling. They include profiler overhead and any concurrent Go
work, so isolate benchmark runs when comparing allocation samples. They do not
claim per-operator allocations or peak memory. Profiling itself adds overhead,
including runtime heap snapshots; use normal executions for throughput timing.
