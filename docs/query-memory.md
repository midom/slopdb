# Query operator memory and spill files

SQL operators use a 64 MiB working-set budget by default. Set
`storage.Options.QueryMemoryBytes` to a positive byte count to change it for a
database, or pass `db.ExecOptions{MemoryBytes: &bytes}` for one execution.
Zero selects the default; negative values are rejected. Prepared executions,
independent sessions and transactions inherit these settings. The budget uses
conservative estimates of retained Go values rather than process RSS or total
allocations. Concurrent operators and sessions have separate budgets.

`storage.Options.QueryTempDir` selects the parent directory for temporary spill
files. An empty value uses the operating system's temporary directory. A named
parent directory must already exist and be writable; a parent inside the database
directory is rejected. Each operator creates a
private directory with restricted permissions outside the database directory;
normal completion, execution errors and cancellation close its files and remove
its directory. A process crash can leave temporary directories behind. These
files are disposable intermediate values, do not use the database's storage
format, and never change its WAL/durability setting.

For deterministic single-table `ORDER BY ... LIMIT ... OFFSET ...`, a maximum
heap retains the best `LIMIT + OFFSET` rows. The window remains an overflow-safe
int64. Existing ordered index scans keep their early stop. A window that grows
past the budget switches to external sorting; unrestricted sorts also spill
sorted runs. Runs are merged in generations with at most eight readers open.
NULL ordering, exact mixed integer/real comparisons, BLOBs, direction and stable
ties use the same SQL comparison rules. Sorting errors are propagated before
comparison. Simple projections are evaluated once per input row.

GROUP BY on a leading clustered primary-key prefix consumes one ordered physical
interval with one typed/ordinary accumulator and finalizes each group as its key
changes. This avoids retaining a group map or spilling/restarting an already
ordered input. Each finalized group closes and unregisters only its own DISTINCT
spill stores, and releases their engine/cache pointers before reusing the
accumulator. Statement cleanup detaches the remaining ownership list before
closing it, preserving cleanup after errors and cancellation.
GROUP BY aliases and ordinal references are resolved before
checking the prefix; secondary indexes, point sets, joins and multiple intervals
keep their existing safe paths.

Other streaming aggregation uses its typed/ordinary accumulators while their estimated
working set fits. If group cardinality or variable-width state exceeds the
budget, it restarts the input using the **same statement snapshot and private
transaction writes**, externally sorts by group identity, and accumulates one
group at a time. Aggregate DISTINCT sets spill to disk independently. Duplicate
arguments, NULLs, original group input order, HAVING and aggregate ORDER BY are
preserved. Aggregate input expressions with volatility stay on the existing
fallback path: oversized fallback aggregation reports an explicit memory-budget
error rather than changing how often volatile expressions execute.

Equality hash joins incrementally build projected input buckets. Oversized
buckets move to a private WAL-free SlopDB storage index keyed by escaped bucket
identity and a 64-bit insertion ordinal. Exact bounded seeks decode only the
matching bucket, one record at a time even for a highly duplicated key. A small
adjacent-bucket cache avoids repeated decoding of neighboring repeated probes.
The temporary index caps memtables, decoded block caches and open readers; very
tiny query budgets retain a minimum 64 KiB memtable/cache allowance as fixed
engine overhead. The indexed-probe cache also respects the budget. Legacy
joins can spill their bucket indexes while retaining their existing input-row
representation. DISTINCT result-key sets and overlapping access-path row-key
sets use disk-backed strategies when their keys exceed the budget.

This is an **operator working-set budget**, not a hard limit on total query
memory. `Exec` still materializes its output; one returned value or row can exceed
the budget, and `GROUP_CONCAT` must eventually materialize its result string.
Merge readers, codecs and I/O buffers add fixed overhead. Indexed-probe candidate
slices, legacy join inputs, derived-table/compound results and locking row-key
lists can still be materialized. Unsupported aggregation shapes have the
explicit limit above. Streaming result APIs and more general streaming input
operators can address those remaining materializations separately.

`EXPLAIN ANALYZE` reports `memory_limit_bytes`, `spill_bytes`, `spill_runs` and `spill_rows_read` on
its `QueryProfile` summary and API profile. Spill bytes count temporary sorted-run file
writes (including merge rewrites) plus installed temporary-index SSTable flush
and compaction output. They do not count logical inserts still in a temporary
memtable or manifest bytes. Spill runs count generated sorted runs plus completed
temporary-index flushes/compactions. `spill_rows_read` counts indexed spill
records actually decoded, excluding cache hits; missing probes decode zero
records. Metrics include the final temporary-index flush during cleanup. `TopK`, `ExternalSort`, `SpillAggregate` and `SpillHashJoin`
identify the relevant execution stages. These counters report actual work, not
only whether the query was eligible to spill.

The regression tests force 256/512-byte budgets and independently assert sorted
rows, aggregate formulas, duplicate join multiplicities, DISTINCT, int64 window
overflow, prepared execution, rollback/reopen, failure propagation and cleanup.
`BenchmarkTopKRetention` consumes the same 10,000 input rows for both cases:
Top-K10 retains copies only for heap candidates; full sorting retains all rows.
On the development ARM64 host, a short diagnostic run measured 68,304 bytes and
280 allocations for Top-K10 versus approximately 8.73 MB and 20,004 allocations
for full sort. These are operator microbenchmarks, not TPC throughput scores.
