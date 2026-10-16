# Memtable options

SlopDB uses a sorted skip list by default. Commit groups install rows serially.
Large sorted batches reuse predecessors. Scans seek and read entries without
copying and sorting the whole memtable.

## Parallel installation

To let callers in a commit group install their rows in parallel:

```go
opt := storage.DefaultOptions()
opt.ParallelMemTableWrites = true
```

The CLI and write benchmark accept `--parallel-memtable-writes`. This option
is off by default. It applies to the sorted memtable and is ignored when
`HashMemTable` is enabled. The skip list uses atomic links in both modes.

With parallel installation, the leader first orders WAL records and assigns
each writer a sequence span. It waits for all rows before it publishes the
group. Readers retain the old committed snapshot until publication. Rotation
and close also wait for installation to finish. WAL and publication remain
serial. One transaction still installs its own rows serially.

Parallel installation improved the one-row random INSERT and UPDATE tests,
but reduced median throughput by 8–21% in the 100-row INSERT tests. See
[the 100-row results](benchmarks/write/2026-10-16/batch100-inserts.md). Select
the option for workloads that benefit from it.

## Hash memtables

To select a hash memtable for an open database:

```go
opt := storage.DefaultOptions()
opt.HashMemTable = true
```

The CLI and write benchmark accept `--hash-memtable`. The option is off by
default. Hash memtables reduce the cost of point reads and writes. A scan or
range estimate must first sort a copy of the entries. A frozen memtable can
reuse that sorted copy; a write to a mutable memtable discards the cached copy.

The selected mode applies to initial memtables, WAL recovery, and all new
memtables after a flush. It does not change the WAL or SSTable format. Each
open can select either mode for the same database files.

## WAL disabled

Set `opt.DisableWAL = true`, or pass `--no-wal` to the CLI or write benchmark,
to disable WAL writes entirely. Completed flushes and a clean close save data
to SSTables. A crash can lose unflushed changes. This differs from
`DisableWALSync`, which still writes WAL records but skips WAL fsync.

See [the concurrent random UPDATE tests](benchmarks/write/2026-10-16/optional-memtable-no-wal.md)
for both memtable modes with WAL disabled.
