# Persistent SSTable page-size targets

SlopDB's page size is the target size in bytes of an **uncompressed SSTable data
block**. It is not a fixed-size disk page, a file-allocation unit or the WAL block
size. Compression can make a block much smaller on disk. Whole records are never
split: a large row can exceed the target, and restart/checksum trailers add a
small amount of overhead. Indexes, bloom filters, metadata and the file footer
have their own sizes. The WAL retains its 32 KiB framing regardless of page size.

Select 16 KiB, 64 KiB or 1 MiB at database creation:

```go
options := storage.DefaultOptions()
options.PageSize = 1 << 20 // int64, bytes
options.DisableWALSync = true
openDB, err := db.Open(directory, options)
```

```sh
./slopdb --page-size=16384 database
./slopdb --page-size=65536 database
./slopdb --page-size=1048576 database
```

Explicit `PageSize`, CLI and SQL assignments accept powers of two from 512 bytes
to 64 MiB. Input validation uses int64 before conversion to allocation sizes;
negative, zero, nonintegral, non-power-of-two and oversized assignments fail.
`Options.PageSize=0` and `--page-size=0` mean **reuse the stored setting**, defaulting
to 4 KiB for a database without persisted configuration. The deprecated native
`Options.BlockSize` remains available for existing code, including tiny/non-power-
of-two test targets. A nondefault positive `BlockSize` overrides the stored target
when `PageSize=0`; default 4096 defers to persisted configuration. Explicit
`PageSize` always takes precedence. Legacy targets must be between 1 byte and
64 MiB and are persisted, even when outside the explicit PageSize range.

Inspect or change the target on an open database:

```sql
PRAGMA page_size;
PRAGMA page_size = 65536;
PRAGMA page_size(1048576);
VACUUM;
```

`Database.PageSize()` and `LSMTree.PageSize()` return int64 effective sizes.
`Database.SetPageSize[Context](size)` and `LSMTree.SetPageSize(size)` set the target;
`Database.Stats().PageSize` reports it too. All SQL sessions read the current
storage setting, rather than a cached cosmetic value. Assignments return no rows.
Changing page size is physical configuration, so SQL assignments reject an
explicit transaction or savepoint. The Database setter uses the default session
and follows that rule. An independent session can change the configuration while
other sessions use transactions.

Settings persist in a 24-byte `OPTIONS` file: eight-byte `SLOPPAGE` magic, uint32
version 1, uint64 size and CRC32, all integer fields little-endian. It is written
to a temporary sibling, synced, renamed and followed by directory sync. Opens
validate and load it **before WAL replay/checkpoint writers**; malformed
configuration produces an error and preserves recovery evidence. A legacy
database without OPTIONS opens normally and gains persisted configuration.
SSTable encoding and MANIFEST formats do not change, so existing files remain
readable. Online backup carries the current setting into its fresh destination;
integrity checking validates the configuration file too.

A change affects writers started after the setter completes. Existing files,
readers, snapshots and already-started writers retain their layout. New flushes
and compactions can coexist with old blocks safely because readers use encoded
block handles, not the configured target. `VACUUM` flushes committed data and
rewrites it through full compaction, including bottom-level files which would
otherwise remain untouched; pinned MVCC snapshots retain their required older
versions. Reopening with an explicit option/CLI flag changes and persists the
future target without an automatic rewrite. CLI `.open` carries an explicit CLI
selection to the next database; absent that selection, it honors the destination's
stored size. Avoid concurrent size changes when requesting a uniform VACUUM
layout: each new writer reads the current target when it starts.

Larger blocks reduce block/index overhead and can improve compression and
sequential reads, while point reads may decompress more unrelated rows. Writer
buffers and cached decoded blocks also become larger. Compare identical data,
compression, cache budgets and durability, and measure both point and scan work;
a 1 MiB target does not imply a 1 MiB minimum allocation for every stored row.

Zstd writers explicitly use an **8 MiB maximum match window**, normal-memory
match tables and one encoder worker per SSTable writer. This applies to explicit
`zstd` compression and to the default `auto` policy's compressed L1–L6 files;
default L0 files remain uncompressed. `storage.SSTableZstdWindowSize` reports the
configured maximum. Each SSTable data block is an independent zstd frame, so
history spans zstd's internal 128 KiB chunks within that block, but never spans
separate SSTable blocks. For a 1 MiB block the whole input is available as history.
Small frames advertise a correspondingly smaller effective window; a
single-segment frame records this through its content size.

The pinned codec already selected 8 MiB for `SpeedDefault`; making it explicit
prevents a later compression-level/default change from silently reducing the
window. We retain its existing search tables rather than doubling history to
16 MiB unnecessarily. With the pinned implementation, inputs larger than its
128 KiB internal chunk can allocate about 16 MiB of encoder history per active
writer, plus search tables and working buffers. This is independent of the
decoded-block cache budget. Gzip's format has a 32 KiB history limit, so increasing
the SSTable target cannot give gzip the same distant-match behavior.

The permanent compression-window test uses a 1 MiB value consisting of four
identical random 256 KiB quarters: nearby matches cannot explain compression.
It checks distant-match savings against a 64 KiB-window encoder, actual frame
headers, successful readback, CRC failure on corruption and readability of old
small-window frames. These are correctness checks, not throughput benchmarks.
See the pinned codec's [window and memory options](https://github.com/klauspost/compress/blob/v1.18.0/zstd/encoder_options.go)
and [independent-frame encoding](https://github.com/klauspost/compress/blob/v1.18.0/zstd/encoder.go).
