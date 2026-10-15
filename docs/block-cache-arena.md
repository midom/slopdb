# Block cache memory

`Options.BlockCacheMemory` selects where cached decoded pages and cache records
are allocated. It applies when the database is opened and is not persisted.
The same database can be reopened with either setting and either eviction policy.

| Setting | Allocation |
| --- | --- |
| Empty or `BlockCacheMmap` (`"mmap"`, default) | Anonymous mmap arenas outside the Go heap |
| `BlockCacheHeap` (`"heap"`) | Go-owned page slices, immutable readers, and intrusive cache records |

```go
opt := storage.DefaultOptions()
opt.BlockCacheMemory = storage.BlockCacheHeap
database, err := db.Open(path, opt)
```

Both modes use 128 shards, the configured decoded-byte budget, and the chosen
LRU or CLOCK policy. `LSMStats.BlockCacheMemory` reports the effective setting.
Invalid mode values are rejected before the database directory is created.

## mmap allocation

Decoded cached SSTable pages use anonymous private memory mappings on Unix.
The page data is outside the Go heap. Go does not allocate, scan, or collect
these bytes. This does not require CGO or a change to the SSTable format.

The cache has 128 shards. Each shard has a separate arena allocator and lock.
The allocator uses 64-byte alignment, size bins, split free regions, and
coalescing. Allocation headers, boundary footers, and integer free-list links
are stored in the mapping. Resident page records, LRU/CLOCK links and reference
counts also live in the mapping. The mapping contains no Go pointers. The Go
lookup map stores integer offsets rather than pointers to page records.
Arena control records and short-lived pooled reader views remain on the Go
heap. Small compressed input and private decode buffers use separate bounded
Go pools; cache admission copies validated output into the arena and returns
decode scratch. Larger temporary decoding output can use owned Go buffers.

## Byte budget and memory accounting

`Options.BlockCacheSize` limits decoded page bytes in the cache. Pages are
charged by their exact decoded length, rather than the spare capacity of a Go
decompression buffer. The heap backend also charges exact decoded lengths.
LRU and CLOCK policies retain their existing behavior.

Mappings are created on first admission. Each populated shard reserves up to
twice its decoded-byte budget, rounded to an operating system page. This extra
virtual address space permits allocator headers, alignment, fragmentation,
and outstanding borrowed pages. Physical pages are populated on demand. The
allocator does not create more mappings when this reserve is exhausted.
Admission is skipped until a region becomes available. OS allocation failure
also skips admission; cached page data never falls back to the Go heap.

`LSMStats` reports:

| Field | Meaning |
| --- | --- |
| `BlockCacheBytes` | Decoded bytes in current cache entries |
| `BlockCacheArenaBytes` | Reserved mapping bytes, including allocator reserve |
| `BlockCacheArenaInUseBytes` | Live allocation spans, including alignment, headers, and evicted pages that remain pinned |
| `BlockCacheArenaFailures` | Allocation, mapping, or unmapping failures |

Mapped bytes are virtual address space, and do not directly measure RSS.
Cache records and other engine memory add to process RSS. Go's heap size,
`GOGC`, and `GOMEMLIMIT` exclude the arena payload. Use process RSS and arena
counters with the Go heap counters to measure total application memory.
The library does not change application GC settings.

In heap mode, cache data and records are managed by Go. `GOGC` and `GOMEMLIMIT`
include these allocations. Arena byte and failure counters remain zero. Heap
admission copies private decode scratch into an owned slice before storing it.
Eviction can reuse a cache record, but never changes a borrowed reader or page.
Close drops cache ownership; Go collects pages when no reader or returned value
retains them. No anonymous mapping is created in heap mode.

## Reader lifetime in mmap mode

A cache entry owns one page reference. A lookup pins the page under the cache
shard lock with an atomic increment in the page header. Normal releases use
an atomic decrement. These operations do not take the allocator mutex. Only
the release that reaches zero takes that mutex to free and coalesce the region.
The live allocation count keeps the mapping available while this final release
waits for the mutex. A new pin requires cache membership or an existing pin;
a reference count of zero cannot be revived. CLOCK reference bits also use
atomic stores so concurrent hits do not share an arena mutex.

Point reads keep the winning page pinned until the result is
copied. Iterators pin their current page and release it on movement or close.
Public point results and exposed iterator values retain independent storage.
Snapshot reads, multi-key reads, and table size estimates use the same pins.

Eviction releases cache ownership. A borrowed page is freed only when its last
reference is released. Cache close prevents further admission and releases
all cache ownership. A shard mapping is unmapped after its last outstanding
page reference is released. Close does not invalidate an active iterator's
current page. Applications must still close iterators and the database.

On a platform without Unix anonymous mapping support, decoded cache admission
in mmap mode is bypassed. Reads continue with temporary owned decoding buffers.
The failure counter records the unavailable arena allocation.
Select heap mode to retain decoded pages on these platforms.
