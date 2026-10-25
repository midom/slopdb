package storage

// MemTableStats describes internal versions, including deletion markers, rather
// than SQL rows. SizeBytes is the same approximate allocation accounting used
// by the flush threshold, not a measurement of total Go heap residency.
type MemTableStats struct {
	Kind                        string
	SizeBytes, Entries          int64
	RangeTombstones             int
	MaxSequence                 uint64
	SkipListHeight, HashBuckets int
}

func (m *MemTable) statistics() MemTableStats {
	if m == nil {
		return MemTableStats{}
	}
	s := MemTableStats{Kind: "skiplist", SizeBytes: m.approxSz.Load(), Entries: m.entries.Load(), MaxSequence: m.maxSeq.Load(), SkipListHeight: int(m.height.Load()), HashBuckets: len(m.buckets)}
	if m.head == nil {
		s.Kind = "hash"
	}
	m.mu.RLock()
	s.RangeTombstones = len(m.ranges)
	m.mu.RUnlock()
	return s
}

// populateStatusLocked reads existing metadata only. Caller holds l.mu, which
// pins the current memtables and file view. No disk reads or cache admissions.
func (l *LSMTree) populateStatusLocked(s *LSMStats) {
	for i := range l.tableCache.blocks.shards {
		shard := &l.tableCache.blocks.shards[i]
		s.DataBlockReadAttempts += shard.readAttempts.Load()
		s.DataBlockReadBytes += shard.readBytes.Load()
		s.DataBlockReadErrors += shard.readErrors.Load()
		s.DecodedBlockBytes += shard.decodedBytes.Load()
	}
	s.WALRecords, s.WALPayloadBytes, s.WALSyncs = l.walStats.records.Load(), l.walStats.payloadBytes.Load(), l.walStats.syncs.Load()
	s.WALWriteErrors, s.WALSyncErrors = l.walStats.writeErrors.Load(), l.walStats.syncErrors.Load()
	s.Memtable, s.ImmutableMemtable = l.mem.statistics(), l.imm.statistics()
	s.WriteBufferBytes, s.BlockCacheCapacityBytes = int64(l.opt.WriteBufferSize), int64(l.opt.BlockCacheSize)
	s.MaxOpenFiles, s.MaxBackgroundJobs = l.opt.MaxOpenFiles, l.opt.MaxBackgroundJobs
	s.CompactionL0Trigger, s.BaseLevelBytes, s.BloomBitsPerKey = l.opt.CompactionL0Trigger, l.opt.BaseLevelSize*1024*1024, l.opt.BloomBitsKey
	s.ParallelMemtableWrites = l.opt.ParallelMemTableWrites && !l.opt.HashMemTable
	s.FlushPending, s.CompactionScheduled, s.Closed = l.imm != nil, l.compactionScheduled, l.closed
	s.ActiveCompactions, s.CompactingFiles = l.compactions, len(l.compacting)
	if l.bgErr != nil {
		s.BackgroundError = l.bgErr.Error()
	}
	s.WALFileNumber, s.FlushWALFileNumber = l.walFileNum, l.flushWALNum
	s.PublishedSequence, s.TableSequence = l.published.Load(), l.tableSeq
	s.OldestSnapshot = s.PublishedSequence
	l.snapshotMu.Lock()
	for seq, count := range l.snapshots {
		s.SnapshotPins += count
		if seq < s.OldestSnapshot {
			s.OldestSnapshot = seq
		}
	}
	for i := range l.readerSnapshots {
		shard := &l.readerSnapshots[i]
		shard.mu.Lock()
		for seq, count := range shard.pins {
			s.ReaderSnapshotPins += count
			if seq < s.OldestSnapshot {
				s.OldestSnapshot = seq
			}
		}
		shard.mu.Unlock()
	}
	l.snapshotMu.Unlock()
	for i := range l.tableCache.shards {
		shard := &l.tableCache.shards[i]
		shard.mu.Lock()
		s.CachedTableReaders += len(shard.entries)
		s.LoadingTableReaders += len(shard.loading)
		shard.mu.Unlock()
	}
	for level := range s.Levels {
		if len(s.Levels[level].Files) == 0 {
			s.Levels[level].WriteCompression, _ = l.opt.compressionType(level)
			if level > 0 {
				s.Levels[level].TargetBytes = l.opt.levelSizeMultiplier(level) * 1024 * 1024
			}
			if l.view != nil {
				s.Levels[level].Ordered = l.view.ordered[level]
			}
			continue
		}
		readers := make(map[uint64]*SSTableReader, len(s.Levels[level].Files))
		if l.view != nil {
			for _, slot := range l.view.levels[level] {
				readers[slot.meta.Number] = slot.entry.reader
			}
		}
		s.Levels[level].WriteCompression, _ = l.opt.compressionType(level)
		if level > 0 {
			s.Levels[level].TargetBytes = l.opt.levelSizeMultiplier(level) * 1024 * 1024
		}
		if l.view != nil {
			s.Levels[level].Ordered = l.view.ordered[level]
		}
		for i := range s.Levels[level].Files {
			file := &s.Levels[level].Files[i]
			file.Compacting = l.compacting[file.Number]
			if r := readers[file.Number]; r != nil {
				file.MaxSequence, file.RangeTombstones = r.maxSeq, len(r.rangeDeletes)
				file.IndexBytes, file.FilterBytes = len(r.indexData), len(r.filterData)
			}
		}
	}
}
