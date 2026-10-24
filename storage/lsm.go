package storage

import (
	"bytes"
	"container/heap"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
)

const maxLevels = 7

type LSMTree struct {
	replication           atomic.Pointer[ReplicationHooks]
	mu                    sync.RWMutex
	writerMu              sync.Mutex
	viewMu                sync.RWMutex
	view                  *readView
	published             atomic.Uint64
	pageSize              atomic.Int64
	writeQueue            batchQueue
	walWriteScratch       []byte // writerMu protects encoding scratch
	writeGroups           atomic.Int64
	writeBatches          atomic.Int64
	completedCompactions  atomic.Int64
	compactionInputBytes  atomic.Int64
	compactionOutputBytes atomic.Int64
	completedFlushes      atomic.Int64
	flushOutputBytes      atomic.Int64
	opt                   *Options
	dir                   string
	lockFile              *os.File
	mem                   *MemTable
	imm                   *MemTable
	wal                   *WalWriter
	walFileNum            uint64
	flushWALNum           uint64
	seqNum                uint64
	tableSeq              uint64 // Upper bound on sequences in SSTables, including retired tables.
	levels                [maxLevels][]*FileMetaData
	fileNum               uint64
	closed                bool
	closeDone             chan struct{}
	closeErr              error
	cond                  *sync.Cond
	bgErr                 error
	tableCache            *tableCache
	flushWg               sync.WaitGroup
	compactionWg          sync.WaitGroup
	compactionScheduled   bool
	compactions           int
	compacting            map[uint64]bool
	snapshotMu            sync.Mutex
	snapshots             map[uint64]int
	readerSnapshots       [snapshotReaderShards]snapshotReaderShard

	bloomHits   atomic.Int64
	bloomMisses atomic.Int64
	cacheHits   atomic.Int64
	cacheMisses atomic.Int64
}

func NewLSMTree(dir string, opt *Options) (*LSMTree, error) {
	if opt == nil {
		opt = DefaultOptions()
	} else {
		copied := *opt
		opt = &copied
		if opt.BlockSize == 0 {
			opt.BlockSize = 4096
		}
		if opt.BloomBitsKey == 0 {
			opt.BloomBitsKey = 10
		}
		if opt.WriteBufferSize == 0 {
			opt.WriteBufferSize = 4 * 1024 * 1024
		}
		if opt.BlockCacheSize == 0 {
			opt.BlockCacheSize = defaultBlockCacheBytes
		}
		if opt.MaxOpenFiles == 0 {
			opt.MaxOpenFiles = 500
		}
		if opt.CompactionL0Trigger == 0 {
			opt.CompactionL0Trigger = 4
		}
		if opt.BaseLevelSize == 0 {
			opt.BaseLevelSize = 10
		}
		if opt.MaxSeekCompaction == 0 {
			opt.MaxSeekCompaction = 100
		}
		if opt.MaxBackgroundJobs == 0 {
			opt.MaxBackgroundJobs = 2
		}
	}
	if opt.BlockCacheSize < 0 || opt.WriteBufferSize < 0 || opt.BlockSize < 0 || opt.BloomBitsKey < 0 || opt.MaxOpenFiles < 0 || opt.CompactionL0Trigger < 0 || opt.BaseLevelSize < 0 || opt.MaxBackgroundJobs < 0 {
		return nil, fmt.Errorf("invalid negative storage option")
	}
	if opt.PageSize != 0 {
		if err := ValidatePageSize(opt.PageSize); err != nil {
			return nil, err
		}
	}
	if err := validateBlockTarget(int64(opt.BlockSize)); err != nil {
		return nil, err
	}
	opt.Dir = dir
	if opt.BlockCachePolicy == "" {
		opt.BlockCachePolicy = BlockCacheLRU
	}
	if opt.BlockCachePolicy != BlockCacheLRU && opt.BlockCachePolicy != BlockCacheCLOCK {
		return nil, fmt.Errorf("invalid block cache policy %q: want lru or clock", opt.BlockCachePolicy)
	}
	if opt.BlockCacheMemory == "" {
		opt.BlockCacheMemory = BlockCacheMmap
	}
	if opt.BlockCacheMemory != BlockCacheHeap && opt.BlockCacheMemory != BlockCacheMmap {
		return nil, fmt.Errorf("invalid block cache memory %q: want heap or mmap", opt.BlockCacheMemory)
	}
	if _, err := opt.compressionType(0); err != nil {
		return nil, err
	}

	entriesBefore, dirErr := os.ReadDir(dir)
	if dirErr != nil && !os.IsNotExist(dirErr) {
		return nil, dirErr
	}
	exists := false
	for _, entry := range entriesBefore {
		if entry.Name() == "MANIFEST" || entry.Name() == pageOptionsName || strings.HasSuffix(entry.Name(), ".sst") || strings.HasPrefix(entry.Name(), "WAL-") {
			exists = true
			break
		}
	}
	if !exists && !opt.CreateIfNotExist {
		return nil, fmt.Errorf("database does not exist")
	}
	if exists && opt.ErrorIfExists {
		return nil, fmt.Errorf("database already exists")
	}

	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}

	lockPath := filepath.Join(dir, "LOCK")
	lockFile, err := acquireLock(lockPath)
	if err != nil {
		return nil, err
	}

	lsm := &LSMTree{
		opt:        opt,
		dir:        dir,
		lockFile:   lockFile,
		mem:        newMemTableSized(opt.WriteBufferSize, opt.HashMemTable),
		seqNum:     0,
		fileNum:    0,
		tableCache: newTableCache(dir, opt.MaxOpenFiles, opt.BlockCacheSize, opt.BlockCachePolicy, opt.BlockCacheMemory),
		snapshots:  make(map[uint64]int),
		closeDone:  make(chan struct{}),
	}

	lsm.tableCache.verifyChanges = opt.VerifySSTableChanges
	lsm.cond = sync.NewCond(&lsm.mu)
	fail := func(err error) (*LSMTree, error) {
		if lsm.view != nil {
			lsm.view.release()
			lsm.view = nil
		}
		lsm.tableCache.close()
		lockFile.Close()
		// Keep the inode stable for concurrent open attempts.
		return nil, err
	}
	if err := lsm.initPageSize(); err != nil {
		return fail(err)
	}
	levels, maxFileNum, err := readManifestFile(dir)
	if err != nil {
		return fail(err)
	}
	if _, err := os.Stat(filepath.Join(dir, "MANIFEST")); os.IsNotExist(err) {
		levels, maxFileNum, err = lsm.recoverFromDirectory(dir)
		if err != nil {
			return fail(err)
		}
	}
	lsm.levels = levels
	lsm.fileNum = maxFileNum
	if lsm.fileNum == 0 {
		lsm.fileNum = 1
	}
	// File numbers and sequences must remain monotonic across reopen, including
	// orphaned files and WALs from interrupted flushes.
	entries, err := os.ReadDir(dir)
	if err != nil {
		return fail(err)
	}
	for _, entry := range entries {
		var num uint64
		if strings.HasSuffix(entry.Name(), ".sst") {
			num, _ = strconv.ParseUint(strings.TrimSuffix(entry.Name(), ".sst"), 10, 64)
		}
		if strings.HasPrefix(entry.Name(), "WAL-") {
			num, _ = strconv.ParseUint(strings.TrimSuffix(strings.TrimPrefix(entry.Name(), "WAL-"), ".log"), 10, 64)
		}
		if num >= lsm.fileNum {
			lsm.fileNum = num + 1
		}
	}
	for _, files := range lsm.levels {
		for _, f := range files {
			reader, err := lsm.tableCache.get(f.Number)
			if err != nil {
				return fail(err)
			}
			if reader.maxSeq > lsm.seqNum {
				lsm.seqNum = reader.maxSeq
			}
			f.hasRangeDeletes = len(reader.rangeDeletes) > 0
		}
	}
	lsm.tableSeq = lsm.seqNum
	if err := lsm.replayWALFiles(dir); err != nil {
		return fail(fmt.Errorf("failed to replay WAL: %w", err))
	}
	// Make recovered records durable before retiring their original WALs. Without
	// WAL fsync, checkpoint to SSTables rather than replace them with an unsynced WAL.
	if lsm.mem.ApproximateSize() > 0 && (opt.DisableWAL || opt.DisableWALSync) {
		lsm.imm = lsm.mem
		lsm.mem = newMemTableSized(opt.WriteBufferSize, opt.HashMemTable)
		lsm.mu.Lock()
		err := lsm.flushMemTableSync()
		lsm.mu.Unlock()
		if err != nil {
			return fail(fmt.Errorf("failed to flush recovered WAL: %w", err))
		}
	} else if lsm.mem.ApproximateSize() > 0 {
		if err := lsm.ensureWAL(); err != nil {
			return fail(err)
		}
		iter := lsm.mem.NewIterator()
		for iter.Valid() {
			k := iter.Key()
			if err := lsm.wal.AddRecord(EncodeWALRecord(k.OpType, k.UserKey, iter.Value(), k.SeqNum)); err != nil {
				iter.Close()
				lsm.wal.Close()
				return fail(err)
			}
			iter.Next()
		}
		iter.Close()
		if err := lsm.syncWAL(); err != nil {
			lsm.wal.Close()
			return fail(err)
		}
	}
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), "WAL-") && strings.HasSuffix(entry.Name(), ".log") {
			os.Remove(filepath.Join(dir, entry.Name()))
		}
	}

	lsm.published.Store(lsm.seqNum)
	lsm.mu.Lock()
	err = lsm.publishViewLocked()
	lsm.mu.Unlock()
	if err != nil {
		return fail(err)
	}
	return lsm, nil
}

func acquireLock(lockPath string) (*os.File, error) {
	f, err := os.OpenFile(lockPath, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		f.Close()
		return nil, fmt.Errorf("database is locked: %w", err)
	}
	if err := f.Truncate(0); err != nil {
		f.Close()
		return nil, err
	}
	if _, err := fmt.Fprintf(f, "%d\n", os.Getpid()); err != nil {
		f.Close()
		return nil, err
	}
	return f, nil
}

func (l *LSMTree) recoverFromDirectory(dir string) ([maxLevels][]*FileMetaData, uint64, error) {
	var levels [maxLevels][]*FileMetaData
	var maxNum uint64
	entries, err := os.ReadDir(dir)
	if err != nil {
		return levels, maxNum, err
	}
	for _, e := range entries {
		if !e.IsDir() && filepath.Ext(e.Name()) == ".sst" {
			num, err := strconv.ParseUint(strings.TrimSuffix(e.Name(), ".sst"), 10, 64)
			if err != nil {
				return levels, maxNum, fmt.Errorf("invalid SSTable name %s: %w", e.Name(), err)
			}
			if num >= maxNum {
				maxNum = num + 1
			}
			reader, err := l.tableCache.get(num)
			if err != nil {
				return levels, maxNum, fmt.Errorf("recover SSTable %s: %w", e.Name(), err)
			}
			meta := &FileMetaData{Number: num, FileSize: reader.fileSize, Smallest: reader.meta.Smallest,
				Largest: reader.meta.Largest, hasRangeDeletes: len(reader.rangeDeletes) > 0, AllowedSeeks: int32(l.opt.MaxSeekCompaction)}
			levels[0] = append(levels[0], meta)
		}
	}
	return levels, maxNum, nil
}

func (l *LSMTree) Put(key, value []byte) error {
	return l.writeMutations([]Mutation{{Key: key, Value: value}}, false, false)
}
func (l *LSMTree) Delete(key []byte) error {
	return l.writeMutations([]Mutation{{Key: key, Delete: true}}, false, false)
}

// Mutation is one entry in an atomic write batch.
type Mutation struct {
	Key, Value  []byte
	Delete      bool
	RangeDelete bool // Value is the exclusive end key
}

func (l *LSMTree) writeBatchDirect(batch []Mutation) error {
	return l.writeMutations(batch, true, false)
}
func (l *LSMTree) DeleteRange(start, end []byte) error {
	return l.writeMutations([]Mutation{{Key: start, Value: end}}, false, true)
}

// The writer orders WAL records and rotations. Readers use the committed
// watermark while installation is in progress; publication exposes a whole batch.
func (l *LSMTree) writeMutations(batch []Mutation, syncWAL, rangeDelete bool) error {
	return l.writeMutationsWithInstaller(batch, syncWAL, rangeDelete, nil)
}

// The optional installer lets queued callers populate the same skip list in
// parallel. writerMu still orders WAL, rotation, and whole-group publication.
func (l *LSMTree) writeMutationsWithInstaller(batch []Mutation, syncWAL, rangeDelete bool, install func(*MemTable, uint64)) error {
	if h := l.replication.Load(); h != nil {
		if rangeDelete {
			batch = append([]Mutation(nil), batch...)
			for i := range batch {
				batch[i].RangeDelete = true
			}
		}
		return h.Write(batch, nil)
	}
	return l.writeMutationsLocal(batch, syncWAL, rangeDelete, install)
}
func (l *LSMTree) writeMutationsLocal(batch []Mutation, syncWAL, rangeDelete bool, install func(*MemTable, uint64)) error {
	hasRanges := rangeDelete
	for _, w := range batch {
		hasRanges = hasRanges || w.RangeDelete
	}

	l.writerMu.Lock()
	defer l.writerMu.Unlock()
	l.mu.Lock()
	if l.closed {
		l.mu.Unlock()
		return fmt.Errorf("database closed")
	}
	if len(batch) == 0 {
		l.mu.Unlock()
		return nil
	}
	if err := l.makeRoomForWrite(); err != nil {
		l.mu.Unlock()
		return err
	}
	if !l.opt.DisableWAL {
		if err := l.ensureWAL(); err != nil {
			l.mu.Unlock()
			return err
		}
	}
	base, mem, wal := l.seqNum, l.mem, l.wal
	l.mu.Unlock()
	typ := func(w Mutation) uint8 {
		if rangeDelete || w.RangeDelete {
			return kTypeRangeDelete
		}
		if w.Delete {
			return kTypeDelete
		}
		return kTypeValue
	}
	if !l.opt.DisableWAL {
		capacity := 1
		for _, w := range batch {
			capacity += 9 + 3*binary.MaxVarintLen64 + len(w.Key) + len(w.Value)
		}
		data := l.walWriteScratch[:0]
		if cap(data) < capacity {
			data = make([]byte, 0, capacity)
		}
		data = data[:1]
		data[0] = 3
		for i, w := range batch {
			size := 9 + uvarintSize(uint64(len(w.Key))) + len(w.Key) + uvarintSize(uint64(len(w.Value))) + len(w.Value)
			data = binary.AppendUvarint(data, uint64(size))
			data = appendWALRecord(data, typ(w), w.Key, w.Value, base+uint64(i)+1)
		}
		if cap(data) <= 4<<20 {
			l.walWriteScratch = data
		} else {
			l.walWriteScratch = nil
		}
		err := wal.AddRecord(data)
		if err == nil && syncWAL && !l.opt.DisableWALSync {
			err = wal.Sync()
		}
		if err != nil {
			l.mu.Lock()
			l.bgErr = err
			l.cond.Broadcast()
			l.mu.Unlock()
			return err
		}
	}
	// Keep all installed nodes behind the old committed watermark. The installer
	// joins every participant before the group becomes visible or can rotate.
	if mem.head != nil && install != nil && !hasRanges {
		install(mem, base)
		var size int64
		for _, w := range batch {
			size += int64(len(w.Key) + len(w.Value) + 32)
		}
		mem.accountSorted(size, int64(len(batch)))
	} else {
		if mem.head == nil || hasRanges {
			mem.mu.Lock()
		}
		var sortedBatch []KeyValue
		if mem.head != nil && len(batch) >= 32 {
			sortedBatch = make([]KeyValue, 0, len(batch))
		}
		for i, w := range batch {
			ik := InternalKey{UserKey: w.Key, SeqNum: base + uint64(i) + 1, OpType: typ(w)}
			if sortedBatch != nil {
				sortedBatch = append(sortedBatch, KeyValue{Key: ik, Value: w.Value})
			} else {
				mem.insert(ik, w.Value)
			}
			if rangeDelete || w.RangeDelete {
				mem.ranges = append(mem.ranges, KeyValue{Key: InternalKey{UserKey: bytes.Clone(w.Key), SeqNum: ik.SeqNum, OpType: ik.OpType}, Value: bytes.Clone(w.Value)})
				mem.hasRanges.Store(true)
			}
		}
		if sortedBatch != nil {
			mem.insertSortedBatch(sortedBatch)
		}
		if mem.head == nil || hasRanges {
			mem.mu.Unlock()
		}
	}
	mem.maxSeq.Store(base + uint64(len(batch)))
	l.mu.Lock()
	l.seqNum = base + uint64(len(batch))
	l.published.Store(l.seqNum)
	l.mu.Unlock()
	return nil
}

func (l *LSMTree) appendWALRecord(opType uint8, key, value []byte, seq uint64) error {
	if l.opt.DisableWAL {
		return nil
	}
	if err := l.ensureWAL(); err != nil {
		return err
	}
	return l.wal.AddRecord(EncodeWALRecord(opType, key, value, seq))
}

func (l *LSMTree) ensureWAL() error {
	if l.wal != nil {
		return nil
	}
	l.walFileNum = l.fileNum
	l.fileNum++
	walPath := filepath.Join(l.dir, fmt.Sprintf("WAL-%d.log", l.walFileNum))
	wal, err := NewWalWriter(walPath)
	if err != nil {
		return err
	}
	l.wal = wal
	return nil
}

// AddRecord flushes records to the OS in every WAL mode. This controls only
// the additional durability barrier, including on rotation and shutdown.
func (l *LSMTree) syncWAL() error {
	if l.wal == nil || l.opt.DisableWALSync {
		return nil
	}
	return l.wal.Sync()
}

func (l *LSMTree) Get(key []byte) ([]byte, bool, error) {
	value, found, _, err := l.readPoint(key, 0, true)
	return value, found, err
}

// Snapshot pins the versions visible at the returned sequence. Call
// ReleaseSnapshot when it is no longer needed so compaction can reclaim them.
func (l *LSMTree) Snapshot() uint64 {
	// Registration and the cutoff walker share snapshotMu. Loading the
	// watermark under that lock is sufficient; no global view reader is needed.
	l.snapshotMu.Lock()
	snapshot := l.published.Load()
	l.snapshots[snapshot]++
	l.snapshotMu.Unlock()
	return snapshot
}
func (l *LSMTree) ReleaseSnapshot(snapshot uint64) {
	l.snapshotMu.Lock()
	defer l.snapshotMu.Unlock()
	if n := l.snapshots[snapshot]; n > 1 {
		l.snapshots[snapshot] = n - 1
	} else {
		delete(l.snapshots, snapshot)
	}
}

func (l *LSMTree) oldestSnapshotLocked() uint64 {
	l.snapshotMu.Lock()
	defer l.snapshotMu.Unlock()
	// Capture the watermark BEFORE walking buckets. A pin created in a bucket
	// already visited cannot be older than this cutoff; pins that captured an
	// earlier sequence hold that bucket lock until their registration finishes.
	oldest := l.published.Load()
	for snapshot := range l.snapshots {
		if snapshot < oldest {
			oldest = snapshot
		}
	}
	for i := range l.readerSnapshots {
		shard := &l.readerSnapshots[i]
		shard.mu.Lock()
		for seq := range shard.pins {
			if seq < oldest {
				oldest = seq
			}
		}
		shard.mu.Unlock()
	}
	return oldest
}

func retainVersions(kvs []KeyValue, oldest uint64) []KeyValue {
	out := kvs[:0]
	var key []byte
	below := false
	for _, kv := range kvs {
		if !bytes.Equal(key, kv.Key.UserKey) {
			key = kv.Key.UserKey
			below = false
		}
		if kv.Key.OpType == kTypeRangeDelete {
			out = append(out, kv)
			continue
		}
		if kv.Key.SeqNum > oldest {
			out = append(out, kv)
		} else if !below {
			out = append(out, kv)
			below = true
		}
	}
	return out
}

func (l *LSMTree) GetAt(key []byte, snapshot uint64) ([]byte, bool, error) {
	value, found, _, err := l.readPoint(key, snapshot, false)
	return value, found, err
}

// GetVersion returns the latest sequence affecting a key, including tombstones.
func (l *LSMTree) GetVersion(key []byte) ([]byte, bool, uint64, error) {
	return l.readPoint(key, 0, true)
}

// GetVersionAt includes point and range tombstones in snapshot validation.
func (l *LSMTree) GetVersionAt(key []byte, snapshot uint64) ([]byte, bool, uint64, error) {
	return l.readPoint(key, snapshot, false)
}

// ChangedSince checks only versions newer than a pinned statement snapshot.
// Old SSTables cannot invalidate the statement, so no block reads are needed.
func (l *LSMTree) ChangedSince(key []byte, snapshot uint64) (bool, error) {
	_, _, version, err := l.readPointSince(key, 0, true, snapshot, true)
	return version > snapshot, err
}

// Capture a safely pinned immutable memtable/file view and committed watermark.
// Compaction can retire files concurrently; the pins keep their descriptors and
// decoded blocks alive until this point read finishes. Disk I/O is never under mu.
func (l *LSMTree) readPoint(key []byte, snapshot uint64, latest bool) ([]byte, bool, uint64, error) {
	return l.readPointSince(key, snapshot, latest, 0, false)
}
func (l *LSMTree) readPointSince(key []byte, snapshot uint64, latest bool, since uint64, versionOnly bool) ([]byte, bool, uint64, error) {
	view, committed := l.acquireView()
	if view == nil {
		return nil, false, 0, fmt.Errorf("database closed")
	}
	if latest || snapshot > committed {
		snapshot = committed
	}
	defer view.release()
	return readPointFromView(view, key, snapshot, since, versionOnly)
}

// GetForReaderInto reads the latest committed value with sharded view pins.
// It captures the watermark together with the view; no caller snapshot pin is
// needed. The returned value uses caller-owned scratch, as GetAtForReaderInto.
func (l *LSMTree) GetForReaderInto(key []byte, reader uint64, dst []byte) ([]byte, bool, error) {
	return l.GetAtForReaderInto(key, ^uint64(0), reader, dst)
}

// GetAtForReader uses a stable reader ID to distribute read-view references.
// As with GetAt, the caller must keep the snapshot pinned for the operation.
func (l *LSMTree) GetAtForReader(key []byte, snapshot, reader uint64) ([]byte, bool, error) {
	return l.GetAtForReaderInto(key, snapshot, reader, nil)
}

// GetAtForReaderInto copies the value into dst[:0], growing it if necessary.
// The returned slice belongs to the caller and never aliases storage buffers.
// A stable reader and a pinned snapshot have the same contract as GetAtForReader.
func (l *LSMTree) GetAtForReaderInto(key []byte, snapshot, reader uint64, dst []byte) ([]byte, bool, error) {
	view, committed, shard := l.acquireViewForReader(reader)
	if view == nil {
		return nil, false, fmt.Errorf("database closed")
	}
	defer view.releaseReader(shard)
	if snapshot > committed {
		snapshot = committed
	}
	workspace := pointWorkspaces.Get().(*pointWorkspace)
	defer releasePointWorkspace(workspace)
	value, found, _, err := readPointWithWorkspaceMode(view, key, snapshot, 0, false, workspace, false)
	if err != nil || !found {
		return dst[:0], false, err
	}
	return append(dst[:0], value...), true, nil
}
func readPointFromView(view *readView, key []byte, snapshot, since uint64, versionOnly bool) ([]byte, bool, uint64, error) {
	workspace := pointWorkspaces.Get().(*pointWorkspace)
	defer releasePointWorkspace(workspace)
	return readPointWithWorkspace(view, key, snapshot, since, versionOnly, workspace)
}
func readPointWithWorkspace(view *readView, key []byte, snapshot, since uint64, versionOnly bool, workspace *pointWorkspace) ([]byte, bool, uint64, error) {
	return readPointWithWorkspaceMode(view, key, snapshot, since, versionOnly, workspace, true)
}

// Internal callers borrow values while the workspace pins the winning block.
// Public point reads copy values before releasing that workspace.
func readPointWithWorkspaceMode(view *readView, key []byte, snapshot, since uint64, versionOnly bool, workspace *pointWorkspace, copyValue bool) ([]byte, bool, uint64, error) {
	workspace.releaseBlocks()
	var best KeyValue
	found := false
	var rangeSeq uint64
	consider := func(kv KeyValue, ok bool, rd uint64) {
		if ok && (!found || kv.Key.SeqNum > best.Key.SeqNum) {
			workspace.bestBlock.release()
			workspace.bestBlock, workspace.block = workspace.block, nil
			best = kv
			found = true
		}
		if rd > rangeSeq {
			rangeSeq = rd
		}
		workspace.block.release()
		workspace.block = nil
	}
	kv, ok, rd := view.mem.lookupAt(key, snapshot)
	consider(kv, ok, rd)
	if view.imm != nil {
		kv, ok, rd := view.imm.lookupAt(key, snapshot)
		consider(kv, ok, rd)
	}
	if versionOnly && (best.Key.SeqNum > since || rangeSeq > since) {
		version := best.Key.SeqNum
		if rangeSeq > version {
			version = rangeSeq
		}
		return nil, false, version, nil
	}
	if (!found || best.Key.SeqNum <= view.tableSeq) && rangeSeq <= view.tableSeq {
		visit := func(slot viewFile) error {
			f, entry := slot.meta, slot.entry
			// A table cannot replace this point version or delete it with a range
			// tombstone when all of its records are at or below the winning sequence.
			if found && entry.reader.maxSeq <= best.Key.SeqNum {
				return nil
			}
			if len(entry.reader.rangeDeletes) == 0 && (bytes.Compare(key, f.Smallest.UserKey) < 0 || bytes.Compare(key, f.Largest.UserKey) > 0) {
				return nil
			}
			if versionOnly && entry.reader.maxSeq <= since {
				return nil
			}
			kv, ok, rd, err := entry.reader.pointLookup(key, snapshot, workspace)
			if err != nil {
				return err
			}
			consider(kv, ok, rd)
			return nil
		}
		for level, files := range view.levels {
			if len(files) == 0 {
				continue
			}
			if view.ordered[level] && !view.hasRanges[level] {
				index := sort.Search(len(files), func(i int) bool { return bytes.Compare(files[i].meta.Largest.UserKey, key) >= 0 })
				if index < len(files) {
					if err := visit(files[index]); err != nil {
						return nil, false, 0, err
					}
				}
			} else {
				for _, slot := range files {
					if err := visit(slot); err != nil {
						return nil, false, 0, err
					}
				}
			}
		}
	}
	if !found || best.Key.OpType == kTypeDelete || rangeSeq > best.Key.SeqNum {
		version := best.Key.SeqNum
		if rangeSeq > version {
			version = rangeSeq
		}
		return nil, false, version, nil
	}
	if versionOnly {
		return nil, true, best.Key.SeqNum, nil
	}
	if copyValue {
		return append([]byte(nil), best.Value...), true, best.Key.SeqNum, nil
	}
	return best.Value, true, best.Key.SeqNum, nil
}

func (l *LSMTree) NewIteratorAt(snapshot uint64) Iterator { return l.iteratorView(snapshot, false) }
func (l *LSMTree) openFileIteratorAt(f *FileMetaData, snapshot uint64) Iterator {
	return newMaxSeqIter(l.openFileIterator(f), snapshot)
}

func newMaxSeqIter(inner Iterator, maxSeq uint64) *maxSeqIter {
	m := &maxSeqIter{inner: inner, maxSeq: maxSeq}
	m.skipForward()
	return m
}

type maxSeqIter struct {
	inner  Iterator
	maxSeq uint64
}

func (m *maxSeqIter) skipForward() {
	for m.inner.Valid() && m.inner.Key().SeqNum > m.maxSeq {
		m.inner.Next()
	}
}
func (m *maxSeqIter) Valid() bool      { return m.inner.Valid() }
func (m *maxSeqIter) Next()            { m.inner.Next(); m.skipForward() }
func (m *maxSeqIter) Seek(key []byte)  { m.inner.Seek(key); m.skipForward() }
func (m *maxSeqIter) Key() InternalKey { return m.inner.Key() }
func (m *maxSeqIter) Value() []byte    { return m.inner.Value() }
func (m *maxSeqIter) Close()           { m.inner.Close() }
func (m *maxSeqIter) RangeTombstones() []KeyValue {
	var ranges []KeyValue
	for _, rd := range iteratorRanges(m.inner) {
		if rd.Key.SeqNum <= m.maxSeq {
			ranges = append(ranges, rd)
		}
	}
	return ranges
}
func (m *maxSeqIter) SkipUserKey(key []byte) { skipIteratorKey(m.inner, key); m.skipForward() }
func (m *maxSeqIter) Prev() {
	m.inner.Prev()
	for m.inner.Valid() && m.inner.Key().SeqNum > m.maxSeq {
		m.inner.Prev()
	}
}

func (l *LSMTree) NewIterator() Iterator { return l.iteratorView(0, true) }
func (l *LSMTree) openFileIterator(f *FileMetaData) Iterator {
	entry, err := l.tableCache.acquire(f.Number)
	if err != nil {
		return &emptyIterator{err: err}
	}
	return &fileIterator{reader: entry.reader, iter: entry.reader.newIterator(false, true, false), cache: l.tableCache, entry: entry}
}

func (l *LSMTree) makeRoomForWrite() error {
	for l.mem.ApproximateSize() >= l.opt.WriteBufferSize && l.imm != nil && l.bgErr == nil && !l.closed {
		l.cond.Wait()
	}
	if l.closed {
		return fmt.Errorf("database closed")
	}
	if l.bgErr != nil {
		return l.bgErr
	}
	if l.mem.ApproximateSize() < l.opt.WriteBufferSize {
		return nil
	}
	if l.wal != nil {
		if err := l.syncWAL(); err != nil {
			return err
		}
		if err := l.wal.Close(); err != nil {
			return err
		}
		l.wal = nil
	}
	l.imm = l.mem
	l.flushWALNum = l.walFileNum
	l.mem = newMemTableSized(l.opt.WriteBufferSize, l.opt.HashMemTable)
	if err := l.publishViewLocked(); err != nil {
		// Rotation already changed the writer's memtable. Stop writes rather
		// than accepting commits that the previous reader view cannot see.
		l.bgErr = err
		l.cond.Broadcast()
		return err
	}
	l.flushWg.Add(1)
	go func() {
		defer l.flushWg.Done()
		l.mu.Lock()
		defer l.mu.Unlock()
		defer l.cond.Broadcast()
		if err := l.flushMemTableSync(); err != nil {
			l.bgErr = err
			return
		}
		if !l.closed && len(l.levels[0]) >= l.opt.CompactionL0Trigger {
			l.scheduleCompaction(0)
		}
	}()
	return nil
}

func (l *LSMTree) saveManifest() error { return writeManifestFile(l.dir, l.levels, l.fileNum) }

// Scheduling and installation are serialized under mu; newly flushed files
// cannot be discarded by a compaction that captured an earlier level version.
// Inputs are reserved before a job starts; disjoint jobs can build concurrently.
// Installation/manifest changes remain serialized and preserve concurrent flushes.
func (l *LSMTree) scheduleCompaction(_ int) {
	if l.closed || l.bgErr != nil || l.compactionScheduled {
		return
	}
	if l.compacting == nil {
		l.compacting = make(map[uint64]bool)
	}
	limit := l.opt.MaxBackgroundJobs
	if limit < 1 {
		limit = 1
	}
	for l.compactions < limit {
		level, inputs := l.pickCompactionLocked()
		if level < 0 {
			return
		}
		for _, f := range inputs {
			l.compacting[f.Number] = true
		}
		oldest := l.oldestSnapshotLocked()
		l.compactions++
		l.compactionWg.Add(1)
		go func(level int, inputs []*FileMetaData, oldest uint64) {
			defer l.compactionWg.Done()
			output, err := l.buildCompaction(inputs, oldest, level+1)
			l.mu.Lock()
			defer l.mu.Unlock()
			if err == nil {
				err = l.installCompactionLocked(level, inputs, output)
			}
			if err != nil {
				l.bgErr = err
			}
			for _, f := range inputs {
				delete(l.compacting, f.Number)
			}
			l.compactions--
			l.cond.Broadcast()
			l.scheduleCompaction(0)
		}(level, inputs, oldest)
	}
}
func (l *LSMTree) pickCompactionLocked() (int, []*FileMetaData) {
	for level := 0; level < maxLevels-1; level++ {
		if level == 0 {
			if len(l.levels[0]) < l.opt.CompactionL0Trigger {
				continue
			}
		} else {
			var size int64
			for _, f := range l.levels[level] {
				size += int64(f.FileSize)
			}
			if size <= l.opt.levelSizeMultiplier(level)*1024*1024 {
				continue
			}
		}
		var best []*FileMetaData
		var bestCost uint64 = ^uint64(0)
		for i := range l.levels[level] {
			inputs := l.compactionInputsForLocked(level, i)
			busy := false
			var cost uint64
			for _, f := range inputs {
				if l.compacting[f.Number] {
					busy = true
					break
				}
				cost += f.FileSize
			}
			if !busy && cost < bestCost {
				best, bestCost = inputs, cost
			}
			if level == 0 {
				break
			}
		}
		if best != nil {
			return level, best
		}
	}
	return -1, nil
}

func (l *LSMTree) compactLevelLocked(level int) error {
	if level >= maxLevels-1 || len(l.levels[level]) == 0 {
		return nil
	}
	inputs := l.compactionInputsLocked(level)
	oldest := l.oldestSnapshotLocked()
	// Input files stay live until installation. New snapshots have sequences
	// at least as new as this cutoff; concurrent flushes only append files.
	l.mu.Unlock()
	output, err := l.buildCompaction(inputs, oldest, level+1)
	l.mu.Lock()
	if err != nil {
		return err
	}
	if err := l.installCompactionLocked(level, inputs, output); err != nil {
		return err
	}
	if level+1 < maxLevels-1 {
		var size int64
		for _, f := range l.levels[level+1] {
			size += int64(f.FileSize)
		}
		for size > l.opt.levelSizeMultiplier(level+1)*1024*1024 {
			if err := l.compactLevelLocked(level + 1); err != nil {
				return err
			}
			size = 0
			for _, f := range l.levels[level+1] {
				size += int64(f.FileSize)
			}
		}
	}
	return nil
}

func (l *LSMTree) installCompactionLocked(level int, inputs, output []*FileMetaData) error {
	return l.installCompactionToLevelLocked(inputs, output, level+1)
}

func (l *LSMTree) installCompactionToLevelLocked(inputs, output []*FileMetaData, destination int) error {
	cleanup := func() {
		for _, f := range output {
			l.tableCache.evict(f.Number)
			os.Remove(filepath.Join(l.dir, fmt.Sprintf("%d.sst", f.Number)))
		}
	}
	// Preserve files installed by a flush while the inputs were processed.
	old := l.levels
	removed := make(map[uint64]bool, len(inputs))
	for _, f := range inputs {
		removed[f.Number] = true
	}
	for lv := range l.levels {
		var kept []*FileMetaData
		for _, f := range l.levels[lv] {
			if !removed[f.Number] {
				kept = append(kept, f)
			}
		}
		l.levels[lv] = kept
	}
	l.levels[destination] = append(l.levels[destination], output...)
	if err := l.saveManifest(); err != nil {
		l.levels = old
		cleanup()
		return err
	}
	if err := l.publishViewLocked(); err != nil {
		return err
	}
	for _, f := range inputs {
		l.tableCache.evict(f.Number)
		os.Remove(filepath.Join(l.dir, fmt.Sprintf("%d.sst", f.Number)))
	}
	l.completedCompactions.Add(1)
	for _, f := range inputs {
		l.compactionInputBytes.Add(int64(f.FileSize))
	}
	for _, f := range output {
		l.compactionOutputBytes.Add(int64(f.FileSize))
	}
	return nil
}

func (l *LSMTree) writeTableLocked(kvs []KeyValue, level int) (*FileMetaData, error) {
	options := *l.opt
	options.PageSize = 0
	options.BlockSize = int(l.PageSize())
	compression, err := options.compressionType(level)
	if err != nil {
		return nil, err
	}
	options.CompressionType = compression
	// The destination's codec is resolved; a standalone writer treats its output
	// as L0, so remove the L0 override before constructing a compaction writer.
	options.L0CompressionType = ""
	num := l.fileNum
	l.fileNum++
	l.mu.Unlock()
	meta, err := l.writeTable(kvs, &options, num)
	l.mu.Lock()
	if err != nil {
		return nil, err
	}
	for _, kv := range kvs {
		if kv.Key.SeqNum > l.tableSeq {
			l.tableSeq = kv.Key.SeqNum
		}
	}
	return meta, nil
}

func (l *LSMTree) writeTable(kvs []KeyValue, options *Options, num uint64) (*FileMetaData, error) {
	path := filepath.Join(l.dir, fmt.Sprintf("%d.sst", num))
	writer, err := NewSSTableWriter(path, options)
	if err != nil {
		return nil, err
	}
	for _, kv := range kvs {
		if err = writer.Add(kv.Key, kv.Value); err != nil {
			writer.Close()
			os.Remove(path)
			return nil, err
		}
	}
	meta, err := writer.Finish()
	closeErr := writer.Close()
	if err == nil {
		err = closeErr
	}
	if err != nil {
		os.Remove(path)
		return nil, err
	}
	// The writer's metadata avoids data scans on both initial open and reopen.
	// Every fetched data block still checks its CRC.
	reader, err := openSSTable(path, l.tableCache.blocks, num)
	if err != nil {
		os.Remove(path)
		return nil, err
	}
	l.tableCache.install(num, reader)
	meta.Number = num
	meta.AllowedSeeks = int32(l.opt.MaxSeekCompaction)
	return &meta, nil
}

func (l *LSMTree) levelTotalSize(level int) int64 {
	var total int64
	l.mu.Lock()
	for _, f := range l.levels[level] {
		total += int64(f.FileSize)
	}
	l.mu.Unlock()
	return total
}

// CompactRange flushes pending writes and merges all current SSTables into the
// bottom level using the current page size and compression configuration. Writes
// and page-size changes wait; reads retain their snapshots throughout the rewrite.
func (l *LSMTree) CompactRange() error {
	l.writerMu.Lock()
	defer l.writerMu.Unlock()
	if err := l.flushWithWriterLocked(); err != nil {
		return err
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return fmt.Errorf("database closed")
	}
	if l.bgErr != nil {
		return l.bgErr
	}
	for l.compactionScheduled && !l.closed {
		l.cond.Wait()
	}
	if l.closed {
		return fmt.Errorf("database closed")
	}
	l.compactionScheduled = true
	defer func() { l.compactionScheduled = false; l.cond.Broadcast(); l.scheduleCompaction(0) }()
	for l.compactions > 0 && !l.closed {
		l.cond.Wait()
	}
	if l.closed {
		return fmt.Errorf("database closed")
	}
	if l.bgErr != nil {
		return l.bgErr
	}
	l.compactionWg.Add(1)
	defer l.compactionWg.Done()
	return l.compactAllFilesToBottomLocked()
}

func (l *LSMTree) replayWALFiles(dir string) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}

	var walFiles []string
	for _, e := range entries {
		if !e.IsDir() && strings.HasPrefix(e.Name(), "WAL-") && strings.HasSuffix(e.Name(), ".log") {
			walFiles = append(walFiles, e.Name())
		}
	}
	sort.Slice(walFiles, func(i, j int) bool {
		number := func(s string) uint64 {
			n, _ := strconv.ParseUint(strings.TrimSuffix(strings.TrimPrefix(s, "WAL-"), ".log"), 10, 64)
			return n
		}
		return number(walFiles[i]) < number(walFiles[j])
	})
	for _, wf := range walFiles {
		seq, err := l.replayOneWAL(dir, wf)
		if seq > l.seqNum {
			l.seqNum = seq
		}
		// A crash can leave only the final record incomplete. CRC errors are fatal.
		if err != nil && !errors.Is(err, io.ErrUnexpectedEOF) {
			return err
		}
	}

	return nil
}

func (l *LSMTree) replayOneWAL(dir, filename string) (uint64, error) {
	path := filepath.Join(dir, filename)
	reader, err := NewWalReader(path)
	if err != nil {
		return 0, err
	}
	defer reader.Close()

	maxSeq := uint64(0)
	for {
		record, err := reader.ReadRecord()
		if err == io.EOF {
			break
		}
		if err != nil {
			return maxSeq, err
		}

		records := [][]byte{record}
		if len(record) > 0 && record[0] == 3 {
			records = nil
			data := record[1:]
			for len(data) > 0 {
				entry, n := decodeBytes(data)
				if n <= 0 {
					return maxSeq, fmt.Errorf("invalid WAL batch")
				}
				records = append(records, entry)
				data = data[n:]
			}
		}
		// Validate the whole batch before applying any of it.
		for _, entry := range records {
			typ, key, _, seq := DecodeWALRecord(entry)
			if key == nil || seq == 0 || typ > kTypeRangeDelete {
				return maxSeq, fmt.Errorf("invalid WAL record")
			}
		}
		for _, entry := range records {
			typ, key, value, seq := DecodeWALRecord(entry)
			l.mem.Put(seq, typ, key, value)
			if seq > maxSeq {
				maxSeq = seq
			}
		}

	}

	return maxSeq, nil
}

// Caller holds mu. Failed flushes leave their WAL and immutable table intact.
func (l *LSMTree) flushMemTableSync() error {
	if l.imm == nil {
		return nil
	}
	imm := l.imm
	oldest := l.oldestSnapshotLocked()
	l.mu.Unlock()
	iter := imm.NewIterator()
	var kvs []KeyValue
	for iter.Valid() {
		kvs = append(kvs, KeyValue{Key: iter.Key(), Value: iter.Value()})
		iter.Next()
	}
	iter.Close()
	kvs = retainVersions(kvs, oldest)
	l.mu.Lock()

	if len(kvs) == 0 {
		l.imm = nil
		return nil
	}
	meta, err := l.writeTableLocked(kvs, 0)
	if err != nil {
		return err
	}
	previous := l.levels[0]
	l.levels[0] = append(append([]*FileMetaData(nil), previous...), meta)
	if err := l.saveManifest(); err != nil {
		l.levels[0] = previous
		l.tableCache.evict(meta.Number)
		os.Remove(filepath.Join(l.dir, fmt.Sprintf("%d.sst", meta.Number)))
		return err
	}
	l.imm = nil
	if err := l.publishViewLocked(); err != nil {
		return err
	}
	l.completedFlushes.Add(1)
	l.flushOutputBytes.Add(int64(meta.FileSize))
	if l.flushWALNum != 0 {
		return os.Remove(filepath.Join(l.dir, fmt.Sprintf("WAL-%d.log", l.flushWALNum)))
	}
	return nil
}

func (l *LSMTree) Close() error {
	l.writerMu.Lock()
	defer l.writerMu.Unlock()
	l.mu.Lock()
	if l.closed {
		done := l.closeDone
		l.mu.Unlock()
		<-done
		l.mu.Lock()
		defer l.mu.Unlock()
		return l.closeErr
	}
	l.closed = true
	oldView := l.replaceView(nil)
	if oldView != nil {
		oldView.release()
	}
	l.cond.Broadcast()
	l.mu.Unlock()
	l.flushWg.Wait()
	l.compactionWg.Wait()
	l.mu.Lock()
	defer l.mu.Unlock()
	defer close(l.closeDone)
	err := l.bgErr
	if l.wal != nil {
		if e := l.syncWAL(); err == nil {
			err = e
		}
		if e := l.wal.Close(); err == nil {
			err = e
		}
		l.wal = nil
	}
	if err == nil && l.imm == nil && l.mem.ApproximateSize() > 0 {
		l.imm = l.mem
		l.flushWALNum = l.walFileNum
		l.mem = newMemTableSized(l.opt.WriteBufferSize, l.opt.HashMemTable)
		err = l.flushMemTableSync()
	}
	if err == nil {
		err = l.saveManifest()
	}
	l.tableCache.close()
	if l.lockFile != nil {
		l.lockFile.Close()
		// Closing the descriptor releases the OS lock; never unlink its inode.
	}
	l.closeErr = err
	return err
}

func (l *LSMTree) ApproximateSizes() map[int]int64 {
	sizes := make(map[int]int64)
	for level := 0; level < maxLevels; level++ {
		sizes[level] = l.levelTotalSize(level)
	}
	return sizes
}

var _ = strings.HasPrefix

type memRefKeyCloser struct {
	mt *MemTable
}

func (m *memRefKeyCloser) Close() { m.mt.Unref() }

func wrapMemTableIter(it *MemTableIterator, mt *MemTable) Iterator {
	return &mtIterWrap{MemTableIterator: it, mt: mt}
}

type mtIterWrap struct {
	*MemTableIterator
	mt *MemTable
}

func (w *mtIterWrap) Close() {
	w.MemTableIterator.Close()
	w.mt.Unref()
}

type fileIterator struct {
	reader *SSTableReader
	iter   *SSTableIterator
	cache  *tableCache
	entry  *cacheEntry
	closed bool
}

func (f *fileIterator) Valid() bool { return f.iter.Valid() }
func (f *fileIterator) Next()       { f.iter.Next() }
func (f *fileIterator) Key() InternalKey {
	if f.iter.Valid() {
		return f.iter.Key()
	}
	return InternalKey{}
}
func (f *fileIterator) Value() []byte {
	if f.iter.Valid() {
		return f.iter.Value()
	}
	return nil
}
func (f *fileIterator) Seek(key []byte) { f.iter.Seek(key) }
func (f *fileIterator) Prev()           { f.iter.Prev() }
func (f *fileIterator) Close() {
	if !f.closed {
		f.closed = true
		f.iter.Close()
		f.cache.release(f.entry)
	}
}
func (f *fileIterator) RangeTombstones() []KeyValue { return f.reader.rangeDeletes }
func (f *fileIterator) SkipUserKey(key []byte) {
	for f.Valid() && bytes.Equal(f.Key().UserKey, key) {
		f.Next()
	}
}

func EncodeWALRecord(opType uint8, key, value []byte, seqNum uint64) []byte {
	buf := make([]byte, 0, 9+2*binary.MaxVarintLen64+len(key)+len(value))
	return appendWALRecord(buf, opType, key, value, seqNum)
}

func uvarintSize(value uint64) int {
	n := 1
	for value >= 128 {
		value >>= 7
		n++
	}
	return n
}

func appendWALRecord(buf []byte, opType uint8, key, value []byte, seqNum uint64) []byte {
	buf = append(buf, opType)
	buf = binary.AppendUvarint(buf, uint64(len(key)))
	buf = append(buf, key...)
	buf = binary.AppendUvarint(buf, uint64(len(value)))
	buf = append(buf, value...)
	buf = binary.LittleEndian.AppendUint64(buf, seqNum)
	return buf
}

func DecodeWALRecord(data []byte) (opType uint8, key, value []byte, seqNum uint64) {
	if len(data) < 11 {
		return
	}
	opType = data[0]
	key, n := decodeBytes(data[1:])
	if n <= 0 {
		return opType, nil, nil, 0
	}
	value, m := decodeBytes(data[1+n:])
	if m <= 0 || 1+n+m+8 != len(data) {
		return opType, nil, nil, 0
	}
	seqNum = binary.LittleEndian.Uint64(data[1+n+m:])
	return
}

func decodeBytes(data []byte) ([]byte, int) {
	length, n := binary.Uvarint(data)
	if n <= 0 || length > uint64(len(data)-n) {
		return nil, 0
	}
	return data[n : n+int(length)], n + int(length)
}

func IteratorError(it Iterator) error {
	if v, ok := it.(interface{ Err() error }); ok {
		return v.Err()
	}
	return nil
}
func (m *maxSeqIter) Err() error   { return IteratorError(m.inner) }
func (f *fileIterator) Err() error { return f.iter.Err() }

type Iterator interface {
	Valid() bool
	Next()
	Prev()
	Seek(key []byte)
	Key() InternalKey
	Value() []byte
	Close()
}

type emptyIterator struct{ err error }

func (e *emptyIterator) Err() error { return e.err }

func (e *emptyIterator) Valid() bool      { return false }
func (e *emptyIterator) Next()            {}
func (e *emptyIterator) Prev()            {}
func (e *emptyIterator) Seek(key []byte)  {}
func (e *emptyIterator) Key() InternalKey { return InternalKey{} }
func (e *emptyIterator) Value() []byte    { return nil }
func (e *emptyIterator) Close()           {}

type mergeIterator struct {
	err      error
	iters    []Iterator
	pq       mergeHeap
	lastKey  InternalKey
	lastVal  []byte
	valid    bool
	afterEnd bool
	reverse  bool
	closers  []ioCloser
	ranges   []KeyValue
}
type ioCloser interface{ Close() }

func iteratorRanges(it Iterator) []KeyValue {
	if provider, ok := it.(interface{ RangeTombstones() []KeyValue }); ok {
		return provider.RangeTombstones()
	}
	var ranges []KeyValue
	for it.Valid() {
		if it.Key().OpType == kTypeRangeDelete {
			ranges = append(ranges, KeyValue{Key: it.Key(), Value: it.Value()})
		}
		it.Next()
	}
	return ranges
}

func skipIteratorKey(it Iterator, key []byte) {
	if skipper, ok := it.(interface{ SkipUserKey([]byte) }); ok {
		skipper.SkipUserKey(key)
		return
	}
	for it.Valid() && bytes.Equal(it.Key().UserKey, key) {
		it.Next()
	}
}

func newMergeIterator(iters []Iterator, closers ...ioCloser) *mergeIterator {
	return newMergeIteratorAt(iters, nil, closers...)
}
func newMergeIteratorAt(iters []Iterator, key []byte, closers ...ioCloser) *mergeIterator {
	return newMergeIteratorDirectional(iters, key, false, closers...)
}
func newMergeIteratorDirectional(iters []Iterator, key []byte, reverse bool, closers ...ioCloser) *mergeIterator {
	m := &mergeIterator{iters: iters, closers: closers}
	// Range tombstones may start before Seek's target, so remember them all.
	for _, it := range iters {
		m.ranges = append(m.ranges, iteratorRanges(it)...)
	}
	for _, it := range iters {
		if err := IteratorError(it); err != nil {
			m.err = err
		}
	}

	if reverse {
		m.SeekBefore(key)
	} else {
		m.Seek(key)
	}
	return m
}
func (m *mergeIterator) Valid() bool { return m.valid }
func (m *mergeIterator) Err() error  { return m.err }
func (m *mergeIterator) Next() {
	if m.reverse {
		if !m.valid {
			return
		}
		key := m.lastKey.UserKey
		m.Seek(key)
		if m.err != nil {
			return
		}
	}

	if m.err != nil {
		m.valid = false
		return
	}
	for m.pq.Len() > 0 {
		key := m.pq[0].iter.Key().UserKey
		var best KeyValue
		found := false
		for m.pq.Len() > 0 && bytes.Equal(m.pq[0].iter.Key().UserKey, key) {
			top := m.pq[0]
			for top.iter.Valid() && bytes.Equal(top.iter.Key().UserKey, key) && top.iter.Key().OpType == kTypeRangeDelete {
				top.iter.Next()
			}
			k := top.iter.Key()
			if top.iter.Valid() && bytes.Equal(k.UserKey, key) && k.OpType != kTypeRangeDelete && (!found || k.SeqNum > best.Key.SeqNum) {
				best = KeyValue{Key: k, Value: top.iter.Value()}
				found = true
			}
			skipIteratorKey(top.iter, key)
			if err := IteratorError(top.iter); err != nil {
				m.err = err
				m.valid = false
				return
			}
			if top.iter.Valid() {
				heap.Fix(&m.pq, 0)
			} else {
				heap.Pop(&m.pq)
			}
		}
		if !found || best.Key.OpType == kTypeDelete {
			continue
		}
		deleted := false
		for _, rd := range m.ranges {
			if rd.Key.SeqNum > best.Key.SeqNum && bytes.Compare(key, rd.Key.UserKey) >= 0 && bytes.Compare(key, rd.Value) < 0 {
				deleted = true
				break
			}
		}
		if deleted {
			continue
		}
		m.lastKey = best.Key
		m.lastVal = best.Value
		m.valid = true
		m.afterEnd = false
		return
	}
	m.valid = false
	m.afterEnd = true
}
func (m *mergeIterator) Seek(key []byte) {
	m.reverse = false

	if m.err != nil {
		m.valid = false
		return
	}
	m.pq = nil
	for _, it := range m.iters {
		it.Seek(key)
		if err := IteratorError(it); err != nil {
			m.err = err
			m.valid = false
			m.pq = nil
			return
		}
		if it.Valid() {
			heap.Push(&m.pq, &mergeHeapItem{iter: it})
		}
	}
	m.valid = false
	m.Next()
}
func (m *mergeIterator) Key() InternalKey {
	if m.valid {
		return m.lastKey
	}
	return InternalKey{}
}
func (m *mergeIterator) Value() []byte {
	if m.valid {
		return m.lastVal
	}
	return nil
}
func (m *mergeIterator) Prev() {
	if m.err != nil {
		m.valid = false
		return
	}
	if !m.valid && !m.afterEnd {
		return
	}
	if !m.reverse {
		var target []byte
		if m.valid {
			target = m.lastKey.UserKey
		}
		m.pq = nil
		for _, it := range m.iters {
			if target == nil {
				seekIteratorLast(it)
			} else {
				it.Seek(target)
				it.Prev()
				for it.Valid() && bytes.Compare(it.Key().UserKey, target) >= 0 {
					it.Prev()
				}
			}
			if err := IteratorError(it); err != nil {
				m.err = err
				m.valid = false
				return
			}
			if it.Valid() {
				heap.Push(&m.pq, &mergeHeapItem{iter: it, reverse: true})
			}
		}
		m.reverse = true
	}
	m.prevVisible()
}

// Start directly before an exclusive upper bound, without first merging a
// forward result and re-seeking every child to change direction.
func (m *mergeIterator) SeekBefore(key []byte) {
	m.reverse = true
	m.valid = false
	m.afterEnd = false
	m.pq = nil
	if m.err != nil {
		return
	}
	for _, it := range m.iters {
		if key == nil {
			seekIteratorLast(it)
		} else {
			it.Seek(key)
			it.Prev()
		}
		if err := IteratorError(it); err != nil {
			m.err = err
			return
		}
		if it.Valid() {
			m.pq = append(m.pq, &mergeHeapItem{iter: it, reverse: true})
		}
	}
	heap.Init(&m.pq)
	m.prevVisible()
}
func (m *mergeIterator) prevVisible() {
	for m.pq.Len() > 0 {
		key := m.pq[0].iter.Key().UserKey
		var best KeyValue
		found := false
		for m.pq.Len() > 0 && bytes.Equal(m.pq[0].iter.Key().UserKey, key) {
			top := m.pq[0]
			for top.iter.Valid() && bytes.Equal(top.iter.Key().UserKey, key) {
				k := top.iter.Key()
				if k.OpType != kTypeRangeDelete && (!found || k.SeqNum > best.Key.SeqNum) {
					best = KeyValue{Key: k, Value: top.iter.Value()}
					found = true
				}
				top.iter.Prev()
			}
			if err := IteratorError(top.iter); err != nil {
				m.err = err
				m.valid = false
				return
			}
			if top.iter.Valid() {
				heap.Fix(&m.pq, 0)
			} else {
				heap.Pop(&m.pq)
			}
		}
		if !found || best.Key.OpType == kTypeDelete {
			continue
		}
		deleted := false
		for _, rd := range m.ranges {
			if rd.Key.SeqNum > best.Key.SeqNum && bytes.Compare(key, rd.Key.UserKey) >= 0 && bytes.Compare(key, rd.Value) < 0 {
				deleted = true
				break
			}
		}
		if deleted {
			continue
		}
		m.lastKey = best.Key
		m.lastVal = best.Value
		m.valid = true
		m.afterEnd = false
		return
	}
	m.valid = false
	m.afterEnd = false
}
func (m *mergeIterator) SeekToLast() {
	m.reverse = false
	m.valid = false
	m.afterEnd = true
	m.Prev()
}
func seekIteratorLast(it Iterator) {
	if last, ok := it.(interface{ SeekToLast() }); ok {
		last.SeekToLast()
		return
	}
	it.Seek(nil)
	for it.Valid() {
		it.Next()
	}
	it.Prev()
}
func (m *maxSeqIter) SeekToLast() {
	seekIteratorLast(m.inner)
	for m.inner.Valid() && m.inner.Key().SeqNum > m.maxSeq {
		m.inner.Prev()
	}
}
func (f *fileIterator) SeekToLast() { f.iter.SeekToLast() }

func (m *mergeIterator) Close() {
	for _, it := range m.iters {
		it.Close()
	}
	for _, c := range m.closers {
		c.Close()
	}
}

type mergeHeapItem struct {
	iter    Iterator
	reverse bool
}

type mergeHeap []*mergeHeapItem

func (h mergeHeap) Len() int { return len(h) }
func (h mergeHeap) Less(i, j int) bool {
	if h[i].reverse {
		return bytes.Compare(h[i].iter.Key().UserKey, h[j].iter.Key().UserKey) > 0
	}
	return h[i].iter.Key().Compare(h[j].iter.Key()) < 0
}
func (h mergeHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *mergeHeap) Push(x interface{}) {
	*h = append(*h, x.(*mergeHeapItem))
}
func (h *mergeHeap) Pop() interface{} {
	old := *h
	n := len(old)
	item := old[n-1]
	old[n-1] = nil
	*h = old[:n-1]
	return item
}

type LSMStats struct {
	BlockCachePolicy      BlockCachePolicy
	BlockCacheMemory      BlockCacheMemory
	PageSize              int64 // effective uncompressed SSTable data-block target
	CompletedCompactions  int64 // successfully installed compactions in this open instance
	CompactionInputBytes  int64 // total input SSTable file sizes, not device I/O
	CompactionOutputBytes int64 // installed output SSTable file sizes
	CompletedFlushes      int64 // successfully installed nonempty memtable flushes
	FlushOutputBytes      int64 // installed flush SSTable file sizes
	WriteGroups           int64 // successful grouped batch publications
	WriteBatches          int64 // successful caller batches in those groups
	Dir                   string
	MemSize               int
	ImmSize               int
	WALActive             bool
	WALSyncDisabled       bool
	SeqNum                uint64
	FileNum               uint64
	Levels                [maxLevels]LevelStats

	BloomHits                 int64
	BloomMisses               int64
	CacheHits                 int64
	CacheMisses               int64
	BlockCacheBytes           int64
	BlockCacheArenaBytes      int64 // reserved anonymous mapping bytes, including allocator reserve
	BlockCacheArenaInUseBytes int64 // live spans, including pinned evictions, headers and alignment
	BlockCacheArenaFailures   int64 // failed mapping/admission/unmapping operations
	BlockCacheHits            int64
	BlockCacheAdmissionSkips  int64
	BlockCacheMisses          int64
	PointIndexBytes           int64 // additional decoded index arrays in the current file view; outside the page budget
}

type LevelStats struct {
	FileCount int
	TotalSize int64
	Files     []FileStats
}

type FileStats struct {
	Number   uint64
	Size     uint64
	Smallest string
	Largest  string
	Seeks    int32
}

func (l *LSMTree) Stats() LSMStats {
	l.mu.RLock()
	defer l.mu.RUnlock()
	blockHits, blockMisses := l.tableCache.blocks.statistics()
	arenaMapped, arenaInUse, arenaFailures := l.tableCache.blocks.arenaStatistics()

	s := LSMStats{
		BlockCachePolicy:      l.opt.BlockCachePolicy,
		BlockCacheMemory:      l.opt.BlockCacheMemory,
		PageSize:              l.PageSize(),
		CompletedCompactions:  l.completedCompactions.Load(),
		CompactionInputBytes:  l.compactionInputBytes.Load(),
		CompactionOutputBytes: l.compactionOutputBytes.Load(),
		CompletedFlushes:      l.completedFlushes.Load(),
		FlushOutputBytes:      l.flushOutputBytes.Load(),
		WriteGroups:           l.writeGroups.Load(), WriteBatches: l.writeBatches.Load(),
		Dir:                       l.dir,
		MemSize:                   l.mem.ApproximateSize(),
		SeqNum:                    l.seqNum,
		FileNum:                   l.fileNum,
		WALActive:                 l.wal != nil,
		WALSyncDisabled:           l.opt.DisableWALSync && !l.opt.DisableWAL,
		BloomHits:                 l.bloomHits.Load(),
		BloomMisses:               l.bloomMisses.Load(),
		CacheHits:                 l.tableCache.Hits(),
		CacheMisses:               l.tableCache.Misses(),
		BlockCacheBytes:           l.tableCache.blocks.used.Load(),
		BlockCacheArenaBytes:      arenaMapped,
		BlockCacheArenaInUseBytes: arenaInUse,
		BlockCacheArenaFailures:   arenaFailures,
		BlockCacheHits:            blockHits,
		BlockCacheAdmissionSkips:  l.tableCache.blocks.skippedAdmissions(),
		BlockCacheMisses:          blockMisses,
	}
	if l.imm != nil {
		s.ImmSize = l.imm.ApproximateSize()
	}
	if l.view != nil {
		for _, slot := range l.view.files {
			s.PointIndexBytes += slot.entry.reader.pointIndexBytes()
		}
	}
	for lv := 0; lv < maxLevels; lv++ {
		var total int64
		for _, f := range l.levels[lv] {
			total += int64(f.FileSize)
			s.Levels[lv].Files = append(s.Levels[lv].Files, FileStats{
				Number:   f.Number,
				Size:     f.FileSize,
				Smallest: string(f.Smallest.UserKey),
				Largest:  string(f.Largest.UserKey),
				Seeks:    atomic.LoadInt32(&f.AllowedSeeks),
			})
		}
		s.Levels[lv].FileCount = len(l.levels[lv])
		s.Levels[lv].TotalSize = total
	}
	return s
}
