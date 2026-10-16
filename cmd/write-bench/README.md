# Concurrent write benchmark

This tool runs one committed row change per transaction. Each row has an 8-byte
integer ID, an 8-byte integer value, and a 64-byte BLOB: 80 logical bytes.
Each writer owns one session and reuses a prepared statement.

Build and run it from the repository root:

```sh
GOGC=200 go build -o /tmp/slopdb-write-bench ./cmd/write-bench
GOGC=200 /tmp/slopdb-write-bench --db /tmp/fresh-insert-db \
  --workload insert-random --writers 200 --warmup 10 --seconds 30 --samples 3
```

Use a fresh database directory for each insert run. Available workloads are
`insert-random`, `insert-auto`, `update-random`, and `update-sequential`.
`insert-auto` omits the ID column and uses SQL `AUTOINCREMENT` allocation.
Add `--volatile-ids` to `insert-auto` to keep ID counters in memory. Persistent
counters are the default. WAL is enabled. Add `--durable` to enable WAL fsync;
the default keeps WAL writes but skips WAL fsync. Add `--no-wal` to disable
WAL entirely. This mode loses unflushed writes on a crash. The close/reopen
check tests a clean close. It does not test crash persistence.

The default memtable is a concurrent sorted skip list. Add `--hash-memtable` to use the
hash memtable for point operations. Scans and range estimates must sort its
entries. This option does not change the database file format.

The settings for this machine are 96 Go processors, a 20 GiB mmap block cache,
an 8 MiB write buffer, and eight background compaction jobs. `--mem-mib` changes
the write buffer size. `--procs` changes the Go processor count; its default is
96. Use `GOGC=200` for these benchmark results.

Updates need a copy of the saved small-row SELECT fixture. Its `bench` table
has columns `id`, `v`, and `payload`, with `v=id*17` before the run. The payload
contains the fixture's 64-byte deterministic BLOB. The default update set is
IDs 1 through 1,000,000; `--hot` changes that range. The tool fills that cache
before timing. Sequential updates visit existing IDs in insertion order.
Updates change `v` to `v+1` and preserve the entire payload.

The tool writes JSON lines with committed transactions/s, CPU use, sampled
latency, writer counts, WAL groups, flushes, compactions, and Go allocation/GC
counts. `latency_samples` records the number of measured latencies. Mean and
p99 latency fields are null when an interval has no latency samples. Time
includes completion of transactions that are in progress at the stop signal.
Warm-up uses phase `-1`. After timing, the tool closes and reopens
the database. It checks every inserted row and payload, or all updated rows and
the total value increase. These checks include warm-up transactions.

Add `--profile path` for a separate profile run. This collects CPU, mutex, blocking, and allocation profiles. Exclude profile runs from the primary result table.

## Random UPDATE at one million transactions/s

Use a fresh copy of the saved fixture for each run:

```sh
cp --reflink=always -a /tmp/slopdb-select-bench-20261014/database-small \
  /tmp/slopdb-update-million
GOGC=200 /tmp/slopdb-write-bench --db /tmp/slopdb-update-million \
  --workload update-random --writers 400 --procs 96 --mem-mib 8 --hash-memtable \
  --hot 1000000 --warmup 10 --seconds 30 --samples 3
```

The update set is one million existing rows in the saved 10 GB database. Each
transaction increments one value and preserves all 64 payload bytes. The default
WAL mode skips fsync. See the saved report under
`docs/benchmarks/write/2026-10-16/` for measured rates and the fsync control.
