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
stored size. Full compaction performs one merge of all SSTables directly into L6, including
existing L6 files. Writes, flushes and page-size changes wait until that rewrite
finishes; existing readers keep their pinned snapshots.

Larger blocks reduce block/index overhead and can improve compression and
sequential reads, while point reads may decompress more unrelated rows. Writer
buffers and cached decoded blocks also become larger. Compare identical data,
compression, cache budgets and durability, and measure both point and scan work;
a 1 MiB target does not imply a 1 MiB minimum allocation for every stored row.

Zstd writers explicitly match their maximum back-reference window to the
configured uncompressed block target, using normal-memory matching, the default
compression level and one encoder worker. A 16 KiB, 64 KiB or 1 MiB target uses
that same Zstd window. The codec minimum is 1 KiB, so a 512-byte target uses a
1 KiB window. Legacy non-power-of-two `BlockSize` values round up to the next
power of two. `storage.SSTableZstdWindowSizeForBlock` reports this setting.

Each SSTable block is an independent frame: history can span Zstd's internal
128 KiB codec blocks within a page but resets between SSTable pages. Frames
smaller than the configured target may advertise a smaller effective window;
rows larger than the target remain readable but cannot extend compression
history beyond the configured window. Existing files using the previous 8 MiB
setting remain readable. Auto compression leaves L0 files uncompressed and uses
Zstd in L1-L6. Gzip and LZ4 keep their format-specific shorter history limits.

Permanent tests check the actual frame headers for 4 KiB, 16 KiB, 64 KiB, 1 MiB
and 2 MiB targets, including oversized rows, the codec minimum and legacy odd
targets. A 1 MiB distant-repeat fixture checks back-references across internal
codec blocks against a 64 KiB encoder, round-trip decoding and CRC failures.
Legacy 64 KiB and 8 MiB-window frames remain readable. These are correctness
checks, not throughput benchmarks. See the pinned codec's
[window options](https://github.com/klauspost/compress/blob/v1.18.0/zstd/encoder_options.go)
and [independent-frame encoding](https://github.com/klauspost/compress/blob/v1.18.0/zstd/encoder.go).
