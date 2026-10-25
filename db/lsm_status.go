package db

import (
	"context"
	"encoding/hex"
	"fmt"
	"strconv"

	"slopdb/storage"
)

// LSMStatus observes local storage metadata without SQL admission, Raft fences,
// journal activation or data-block reads. Counters belong to this open engine
// instance; metadata gauges describe the current view.
func (d *Database) LSMStatus(ctx context.Context) (storage.LSMStats, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if e := ctx.Err(); e != nil {
		return storage.LSMStats{}, e
	}
	if d.closed.Load() {
		return storage.LSMStats{}, ErrSessionClosed
	}
	s := d.currentStorage.Load().Status()
	if e := ctx.Err(); e != nil {
		return storage.LSMStats{}, e
	}
	return s, nil
}

func lsmVariables(s storage.LSMStats) map[string]string {
	v := map[string]string{"engine": "LSM", "counter_scope": "since_open", "memtable.size_accounting": "approximate_internal_bytes", "file.key_encoding": "hex", "directory": s.Dir, "background.error": s.BackgroundError, "cache.policy": string(s.BlockCachePolicy), "cache.memory": string(s.BlockCacheMemory)}
	integer := func(k string, n int64) { v[k] = strconv.FormatInt(n, 10) }
	unsigned := func(k string, n uint64) { v[k] = strconv.FormatUint(n, 10) }
	boolean := func(k string, b bool) { v[k] = boolState(b) }
	for _, m := range []struct {
		name  string
		state storage.MemTableStats
	}{{"memtable", s.Memtable}, {"immutable_memtable", s.ImmutableMemtable}} {
		v[m.name+".kind"] = m.state.Kind
		integer(m.name+".bytes", m.state.SizeBytes)
		integer(m.name+".entries", m.state.Entries)
		integer(m.name+".range_tombstones", int64(m.state.RangeTombstones))
		unsigned(m.name+".max_sequence", m.state.MaxSequence)
		integer(m.name+".skiplist_height", int64(m.state.SkipListHeight))
		integer(m.name+".hash_buckets", int64(m.state.HashBuckets))
	}
	for k, n := range map[string]int64{
		"memtable.capacity_bytes": s.WriteBufferBytes, "page_size_bytes": s.PageSize,
		"write.groups": s.WriteGroups, "write.batches": s.WriteBatches,
		"wal.records": s.WALRecords, "wal.payload_bytes": s.WALPayloadBytes, "wal.syncs": s.WALSyncs, "wal.write_errors": s.WALWriteErrors, "wal.sync_errors": s.WALSyncErrors,
		"flush.completed": s.CompletedFlushes, "flush.output_bytes": s.FlushOutputBytes,
		"compaction.completed": s.CompletedCompactions, "compaction.input_bytes": s.CompactionInputBytes, "compaction.output_bytes": s.CompactionOutputBytes,
		"compaction.active": int64(s.ActiveCompactions), "compaction.files": int64(s.CompactingFiles), "compaction.max_background_jobs": int64(s.MaxBackgroundJobs), "compaction.l0_trigger_files": int64(s.CompactionL0Trigger), "compaction.base_level_bytes": s.BaseLevelBytes,
		"bloom.bits_per_key": int64(s.BloomBitsPerKey), "bloom.hits": s.BloomHits, "bloom.misses": s.BloomMisses,
		"cache.capacity_bytes": s.BlockCacheCapacityBytes, "cache.bytes": s.BlockCacheBytes, "cache.hits": s.BlockCacheHits, "cache.misses": s.BlockCacheMisses, "cache.admission_skips": s.BlockCacheAdmissionSkips,
		"cache.arena_bytes": s.BlockCacheArenaBytes, "cache.arena_in_use_bytes": s.BlockCacheArenaInUseBytes, "cache.arena_failures": s.BlockCacheArenaFailures,
		"table_cache.hits": s.CacheHits, "table_cache.misses": s.CacheMisses, "table_cache.readers": int64(s.CachedTableReaders), "table_cache.loading": int64(s.LoadingTableReaders), "table_cache.capacity": int64(s.MaxOpenFiles), "table_cache.point_index_bytes": s.PointIndexBytes,
		"read.data_block_attempts": s.DataBlockReadAttempts, "read.data_block_bytes": s.DataBlockReadBytes, "read.data_block_errors": s.DataBlockReadErrors, "read.decoded_block_bytes": s.DecodedBlockBytes,
		"snapshots.transaction_pins": int64(s.SnapshotPins), "snapshots.reader_pins": int64(s.ReaderSnapshotPins),
	} {
		integer(k, n)
	}
	for k, n := range map[string]uint64{"sequence.published": s.PublishedSequence, "sequence.assigned": s.SeqNum, "sequence.sstables": s.TableSequence, "snapshots.oldest_sequence": s.OldestSnapshot, "file.next_number": s.FileNum, "wal.file_number": s.WALFileNumber, "wal.flushing_file_number": s.FlushWALFileNumber} {
		unsigned(k, n)
	}
	for k, b := range map[string]bool{"closed": s.Closed, "wal.enabled": s.WALEnabled, "wal.active_writer": s.WALActive, "wal.fsync": s.WALEnabled && !s.WALSyncDisabled, "flush.pending": s.FlushPending, "compaction.scheduled": s.CompactionScheduled, "write.parallel_memtable": s.ParallelMemtableWrites} {
		boolean(k, b)
	}
	var totalFiles, totalBytes int64
	for level, l := range s.Levels {
		prefix := fmt.Sprintf("level.%d.", level)
		integer(prefix+"files", int64(l.FileCount))
		integer(prefix+"bytes", l.TotalSize)
		integer(prefix+"target_bytes", l.TargetBytes)
		v[prefix+"write_compression"] = string(l.WriteCompression)
		boolean(prefix+"ordered_nonoverlapping", l.Ordered)
		totalFiles += int64(l.FileCount)
		totalBytes += l.TotalSize
		for _, f := range l.Files {
			key := fmt.Sprintf("file.%020d.", f.Number)
			integer(key+"level", int64(level))
			unsigned(key+"bytes", f.Size)
			unsigned(key+"max_sequence", f.MaxSequence)
			v[key+"smallest_key_hex"], v[key+"largest_key_hex"] = hex.EncodeToString([]byte(f.Smallest)), hex.EncodeToString([]byte(f.Largest))
			integer(key+"remaining_seeks", int64(f.Seeks))
			integer(key+"range_tombstones", int64(f.RangeTombstones))
			integer(key+"index_bytes", int64(f.IndexBytes))
			integer(key+"filter_bytes", int64(f.FilterBytes))
			boolean(key+"compacting", f.Compacting)
		}
	}
	integer("sstables.files", totalFiles)
	integer("sstables.bytes", totalBytes)
	return v
}
