# Memtable options

SlopDB uses a concurrent sorted skip list by default. Point inserts update
links with atomic compare-and-swap operations. A scan can seek and read its
entries without copying and sorting the whole memtable.

Writers in a commit group install their own rows in parallel. The leader first
orders the WAL records and assigns each writer a sequence span. It waits for
all rows to be installed before it publishes the group. Readers see the old
committed snapshot until that publication. Rotation and close also wait for
the installation to finish. The WAL and publication order remain serial.

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
