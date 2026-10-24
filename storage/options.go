package storage

import (
	"fmt"
	"math"
	"os"
	"time"
)

// CompressionType selects compression for newly written SSTable data blocks.
// Readers detect each block's codec independently of this setting.
type CompressionType string

// BlockCachePolicy selects eviction for decoded SSTable pages. It does not
// change persisted data or the separate SSTable-reader cache.
type BlockCachePolicy string

// BlockCacheMemory selects allocation for cached decoded pages and records.
// It is an in-memory setting and does not change the SSTable format.
type BlockCacheMemory string

const (
	BlockCacheHeap BlockCacheMemory = "heap"
	BlockCacheMmap BlockCacheMemory = "mmap"
)

const (
	BlockCacheLRU   BlockCachePolicy = "lru"
	BlockCacheCLOCK BlockCachePolicy = "clock"
)

const (
	AutoCompression   CompressionType = "auto" // no compression in L0, zstd in L1 and higher
	NoCompression     CompressionType = "none"
	GzipCompression   CompressionType = "gzip"
	ZstdCompression   CompressionType = "zstd" // independent frames with a match window sized to the SSTable block target
	SnappyCompression CompressionType = "snappy"
	LZ4Compression    CompressionType = "lz4"
)

type Options struct {
	// Replica skips catalog maintenance writes; replicated catalogs must already use object IDs.
	Replica              bool
	MySQLQuoting         bool   // input SQL: double-quoted strings and MySQL backslash escapes; default false
	QueryMemoryBytes     int64  // per-operator working budget; zero selects 64 MiB, negative is invalid
	QueryTempDir         string // temporary operator spills; empty uses the OS temp directory
	MaxCTEIterations     int64  // recursive CTE iterations; default 100,000, zero unlimited
	MaxCTERows           int64  // recursive CTE emitted rows; default 1,000,000, zero unlimited
	Dir                  string
	HashMemTable         bool             // opt in to hash memtables; default skip list supports scans without sorting
	WriteBufferSize      int              // memtable size in bytes before flush (default 4MB)
	BlockSize            int              // legacy block target; PageSize/stored target take precedence over default 4096
	PageSize             int64            // uncompressed SSTable data-block target; 0 reuses stored size/default 4096
	MaxLevels            int              // number of levels (default 7)
	BloomBitsKey         int              // bits per key for bloom filter (default 10)
	BlockCacheSize       int              // decoded cached page bytes; default 128 MiB (metadata/reserve extra)
	BlockCachePolicy     BlockCachePolicy // empty/lru (default) or clock
	BlockCacheMemory     BlockCacheMemory // empty/mmap (default) or heap
	VerifySSTableChanges bool             // diagnostic: stat files before cache hits to detect external modification
	VerifyIndexesOnOpen  bool             // diagnostic: scan rows, validate indexed types and reconcile indexes on every open
	MaxOpenFiles         int              // SSTable LRU capacity (default 500); active read views/iterators pin additional readers
	CompactionL0Trigger  int              // number of L0 files to trigger compaction (default 4)
	BaseLevelSize        int64            // base level size in MB for L1 (default 10)
	MaxSeekCompaction    int              // seek compaction allowed_seeks threshold (default 100)
	MaxBackgroundJobs    int              // max disjoint background compactions (default 2); one immutable memtable flush runs separately
	CompressionType      CompressionType  // auto (default), none, gzip, zstd, snappy, or lz4
	L0CompressionType    CompressionType  // override L0 only; unset/auto inherits CompressionType's L0 policy
	DisableWAL           bool             // skip new WAL writes; unflushed data is lost on crash, Close flushes to SSTables
	DisableWALSync       bool             // skip WAL fsync, still flush records to OS; SSTables/MANIFEST stay synced; ignored if DisableWAL
	SyncOnClose          bool             // fsync MANIFEST and files on close
	CreateIfNotExist     bool             // create database if not exists
	ErrorIfExists        bool             // return error if database exists
	QueryTimeout         time.Duration    // default SQL call deadline; zero disables it
	LockTimeout          time.Duration    // maximum transaction lock wait per SQL call; zero fails immediately on contention
	DeadlockDetection    bool             // detect wait-for cycles and roll back the requesting transaction

	// Opt in to parallel sorted memtable installation. Off by default; ignored
	// with HashMemTable. Each transaction still installs its own rows serially.
	ParallelMemTableWrites bool

	// Keep automatic-ID reservations in memory. After reopen, allocation starts
	// above the highest surviving row ID, so deleted high IDs can be reused.
	DisableAutoIncrementPersistence bool
}

func (o *Options) compressionType(level int) (CompressionType, error) {
	if err := validateCompressionType(o.CompressionType); err != nil {
		return "", err
	}
	if err := validateCompressionType(o.L0CompressionType); err != nil {
		return "", fmt.Errorf("L0 compression: %w", err)
	}
	if level == 0 && o.L0CompressionType != "" && o.L0CompressionType != AutoCompression {
		return o.L0CompressionType, nil
	}
	switch o.CompressionType {
	case "", AutoCompression:
		if level == 0 {
			return NoCompression, nil
		}
		return ZstdCompression, nil
	default:
		return o.CompressionType, nil
	}
}

func validateCompressionType(codec CompressionType) error {
	switch codec {
	case "", AutoCompression, NoCompression, GzipCompression, ZstdCompression, SnappyCompression, LZ4Compression:
		return nil
	default:
		return fmt.Errorf("invalid compression type %q: want auto, none, gzip, zstd, snappy, or lz4", codec)
	}
}

func DefaultOptions() *Options {
	return &Options{
		MaxCTEIterations:    100000,
		MaxCTERows:          1000000,
		WriteBufferSize:     4 * 1024 * 1024,
		BlockSize:           4096,
		MaxLevels:           7,
		BloomBitsKey:        10,
		MaxOpenFiles:        500,
		BlockCacheSize:      defaultBlockCacheBytes,
		BlockCachePolicy:    BlockCacheLRU,
		BlockCacheMemory:    BlockCacheMmap,
		CompactionL0Trigger: 4,
		BaseLevelSize:       10,
		MaxSeekCompaction:   100,
		MaxBackgroundJobs:   2,
		SyncOnClose:         true,
		CreateIfNotExist:    true,
		LockTimeout:         30 * time.Second,
	}
}

func (o *Options) levelSizeMultiplier(level int) int64 {
	return o.BaseLevelSize * int64(math.Pow(10, float64(level-1)))
}

var _ = os.O_CREATE
