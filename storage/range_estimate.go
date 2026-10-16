package storage

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"math/rand"
	"slices"
	"sort"
)

const rangeBlockSamples = 8
const rangeVisibilitySamples = 8
const rangeMemExactEntries = 256
const rangeMemSampleEntries = 1024

// RangeEstimate describes [lower, upper) in a pinned committed read view.
// Nil bounds are unbounded. DiskBytes attributes compressed data blocks to
// this range, excluding shared filters, indexes, metadata, footer and WAL.
// MemoryBytes estimates encoded, unflushed records, excluding allocator overhead.
// Both include retained versions and tombstones. Rows and ValueBytes estimate
// visible values at snapshot, using bounded random samples and MVCC point probes.
// SampledBlocks counts direct block samples, excluding visibility point reads.
type RangeEstimate struct {
	DiskBytes, MemoryBytes int64
	Rows, ValueBytes       int64
	SampledBlocks          int64
}

// EstimateRange uses index seeks and byte offsets, reading at most two boundary
// blocks and eight randomly selected interior blocks per overlapping SSTable.
// It never constructs a disk range iterator. Memtables use an exact short prefix
// followed by weighted sampling on a skip-list level. Optional hash memtables
// first sort their append log, then sample the sorted index. Pin an older snapshot for
// the call's lifetime, as with GetAt; MaxUint64 selects the current watermark.
func (l *LSMTree) EstimateRange(ctx context.Context, lower, upper []byte, snapshot uint64) (RangeEstimate, error) {
	return l.estimateRange(ctx, lower, upper, snapshot, true)
}

// EstimateRangeBytes estimates physical sizes without MVCC visibility probes.
// Rows and ValueBytes are unset. Physical sizes describe the current read view.
func (l *LSMTree) EstimateRangeBytes(ctx context.Context, lower, upper []byte) (RangeEstimate, error) {
	return l.estimateRange(ctx, lower, upper, math.MaxUint64, false)
}

func (l *LSMTree) estimateRange(ctx context.Context, lower, upper []byte, snapshot uint64, countRows bool) (RangeEstimate, error) {
	var result RangeEstimate
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return result, err
	}
	if lower != nil && upper != nil && bytes.Compare(lower, upper) >= 0 {
		return result, fmt.Errorf("invalid estimate range")
	}
	view, committed := l.acquireView()
	if view == nil {
		return result, fmt.Errorf("database closed")
	}
	defer view.release()
	if snapshot > committed {
		snapshot = committed
	}
	e := rangeEstimator{ctx: ctx, view: view, snapshot: snapshot, countRows: countRows, rng: rand.New(rand.NewSource(rand.Int63()))}
	for _, mem := range []*MemTable{view.mem, view.imm} {
		if mem == nil {
			continue
		}
		size, rows, values, err := e.memtable(mem, lower, upper)
		if err != nil {
			return RangeEstimate{}, err
		}
		result.MemoryBytes = estimateAdd(result.MemoryBytes, estimateInt(size))
		result.Rows = estimateAdd(result.Rows, estimateInt(rows))
		result.ValueBytes = estimateAdd(result.ValueBytes, estimateInt(values))
	}
	for _, slot := range view.files {
		if err := ctx.Err(); err != nil {
			return RangeEstimate{}, err
		}
		if lower != nil && bytes.Compare(slot.meta.Largest.UserKey, lower) < 0 || upper != nil && bytes.Compare(slot.meta.Smallest.UserKey, upper) >= 0 {
			continue
		}
		size, rows, values, blocks, err := e.table(slot.entry.reader, lower, upper)
		if err != nil {
			return RangeEstimate{}, fmt.Errorf("estimate SSTable %d: %w", slot.meta.Number, err)
		}
		result.DiskBytes = estimateAdd(result.DiskBytes, estimateInt(size))
		result.Rows = estimateAdd(result.Rows, estimateInt(rows))
		result.ValueBytes = estimateAdd(result.ValueBytes, estimateInt(values))
		result.SampledBlocks += blocks
	}
	return result, nil
}

func estimateInt(v float64) int64 {
	if v >= math.MaxInt64 {
		return math.MaxInt64
	}
	if v <= 0 {
		return 0
	}
	return int64(math.Round(v))
}
func estimateAdd(a, b int64) int64 {
	if b > math.MaxInt64-a {
		return math.MaxInt64
	}
	return a + b
}

type rangeEstimator struct {
	ctx       context.Context
	view      *readView
	snapshot  uint64
	countRows bool
	rng       *rand.Rand
	workspace pointWorkspace
}

type estimateRecord struct {
	key        InternalKey
	valueBytes int
}
type estimateSample struct {
	count   int64
	records []estimateRecord
}

func (e *rangeEstimator) add(s *estimateSample, key InternalKey, size int) {
	if !e.countRows || key.OpType != kTypeValue || key.SeqNum > e.snapshot {
		return
	}
	s.count++
	slot := int64(len(s.records))
	if slot >= rangeVisibilitySamples {
		slot = e.rng.Int63n(s.count)
		if slot >= rangeVisibilitySamples {
			return
		}
	}
	key.UserKey = bytes.Clone(key.UserKey)
	record := estimateRecord{key, size}
	if int(slot) == len(s.records) {
		s.records = append(s.records, record)
	} else {
		s.records[slot] = record
	}
}

func (e *rangeEstimator) visible(s *estimateSample) (rows, values float64, err error) {
	if len(s.records) == 0 {
		return 0, 0, nil
	}
	for _, record := range s.records {
		if err = e.ctx.Err(); err != nil {
			return
		}
		// Check the winning version, including point and range tombstones,
		// rather than treating retained physical versions as additional rows.
		_, found, seq, readErr := readPointWithWorkspaceMode(e.view, record.key.UserKey, e.snapshot, 0, false, &e.workspace, false)
		if readErr != nil {
			return 0, 0, readErr
		}
		if found && seq == record.key.SeqNum {
			rows++
			values += float64(record.valueBytes)
		}
	}
	weight := float64(s.count) / float64(len(s.records))
	return rows * weight, values * weight, nil
}

func inEstimateRange(key, lower, upper []byte) bool {
	return (lower == nil || bytes.Compare(key, lower) >= 0) && (upper == nil || bytes.Compare(key, upper) < 0)
}

func (e *rangeEstimator) memtable(m *MemTable, lower, upper []byte) (size, rows, values float64, err error) {
	if m.head != nil {
		return e.sortedMemtable(m, lower, upper)
	}
	it := m.NewIteratorAt(e.snapshot)
	defer it.Close()
	it.Seek(lower)
	var exact estimateSample
	n := 0
	for ; it.Valid() && (upper == nil || bytes.Compare(it.Key().UserKey, upper) < 0) && n < rangeMemExactEntries; it.Next() {
		if err = e.ctx.Err(); err != nil {
			return
		}
		n++
		key, value := it.Key(), it.Value()
		size += float64(len(key.UserKey) + 8 + len(value))
		e.add(&exact, key, len(value))
	}
	rows, values, err = e.visible(&exact)
	if err != nil || !it.Valid() || upper != nil && bytes.Compare(it.Key().UserKey, upper) >= 0 {
		return
	}
	// The immutable sorted copy permits uniform sampling of the remaining
	// records without walking every entry. Account for snapshot visibility.
	start := it.pos
	end := len(it.nodes)
	if upper != nil {
		end, _ = slices.BinarySearchFunc(it.nodes, upper, func(n *memNode, k []byte) int { return bytes.Compare(n.key.UserKey, k) })
	}
	stride := (end - start + rangeMemSampleEntries - 1) / rangeMemSampleEntries
	if stride < 1 {
		stride = 1
	}
	var sampled estimateSample
	for pos := start; pos < end; pos += stride {
		if err = e.ctx.Err(); err != nil {
			return
		}
		node := it.nodes[pos]
		if node.key.SeqNum > e.snapshot {
			continue
		}
		size += float64(stride) * float64(len(node.key.UserKey)+8+len(node.value))
		e.add(&sampled, node.key, len(node.value))
	}
	r, v, err := e.visible(&sampled)
	return size, rows + r*float64(stride), values + v*float64(stride), err
}

func estimateHandle(r *SSTableReader, it *BlockIterator) (BlockHandle, error) {
	if err := it.Err(); err != nil {
		return BlockHandle{}, err
	}
	if !it.Valid() {
		return BlockHandle{}, fmt.Errorf("missing block handle")
	}
	data := it.Value()
	off, n := binary.Uvarint(data)
	if n <= 0 {
		return BlockHandle{}, fmt.Errorf("invalid block offset")
	}
	size, n := binary.Uvarint(data[n:])
	if n <= 0 || size < 5 || off > r.fileSize || size > r.fileSize-off {
		return BlockHandle{}, fmt.Errorf("invalid block size")
	}
	return BlockHandle{off, size}, nil
}

// blockAtOffset binary-searches index restart handles, then decodes at most one
// restart interval. Data blocks are contiguous and handles ordered by offset.
func blockAtOffset(r *SSTableReader, offset uint64) (BlockHandle, error) {
	var searchErr error
	i := sort.Search(r.indexBlock.numRestarts, func(i int) bool {
		it := BlockIterator{block: r.indexBlock, offset: r.indexBlock.restart(i)}
		it.Next()
		h, err := estimateHandle(r, &it)
		if err != nil {
			searchErr = err
			return true
		}
		return h.Offset > offset
	})
	if searchErr != nil {
		return BlockHandle{}, searchErr
	}
	if i == 0 {
		return BlockHandle{}, fmt.Errorf("offset before first data block")
	}
	it := BlockIterator{block: r.indexBlock, offset: r.indexBlock.restart(i - 1)}
	it.Next()
	for it.Valid() {
		h, err := estimateHandle(r, &it)
		if err != nil {
			return BlockHandle{}, err
		}
		if h.Offset <= offset && offset-h.Offset < h.Size {
			return h, nil
		}
		if h.Offset > offset {
			break
		}
		it.Next()
	}
	return BlockHandle{}, fmt.Errorf("offset outside data blocks")
}

func (e *rangeEstimator) block(r *SSTableReader, h BlockHandle, lower, upper []byte) (size, rows, values float64, err error) {
	if err = e.ctx.Err(); err != nil {
		return
	}
	b, err := r.loadBlockReaderMode(h.Offset, h.Size, false)
	if err != nil {
		return 0, 0, 0, err
	}
	defer b.release()
	it := b.NewIterator()
	it.reuseKey = true
	var sample estimateSample
	var total, selected int
	for it.Valid() {
		if err = e.ctx.Err(); err != nil {
			return
		}
		if len(it.Key()) < 8 {
			return 0, 0, 0, fmt.Errorf("invalid internal key")
		}
		key := ParseInternalKey(it.Key())
		span := it.offset - it.prevOffset
		total += span
		if inEstimateRange(key.UserKey, lower, upper) {
			selected += span
			e.add(&sample, key, len(it.Value()))
		}
		it.Next()
	}
	if err = it.Err(); err != nil {
		return
	}
	if total == 0 {
		return 0, 0, 0, fmt.Errorf("empty data block")
	}
	size = float64(h.Size) * float64(selected) / float64(total)
	rows, values, err = e.visible(&sample)
	return
}

func (e *rangeEstimator) table(r *SSTableReader, lower, upper []byte) (size, rows, values float64, blocks int64, err error) {
	it := r.indexBlock.NewIterator()
	if lower != nil {
		it.Seek(lower)
	}
	if err = it.Err(); err != nil || !it.Valid() {
		return
	}
	first, err := estimateHandle(r, it)
	if err != nil {
		return 0, 0, 0, 0, err
	}
	if upper != nil {
		it.Seek(upper)
	} else {
		it.SeekToLast()
	}
	if !it.Valid() && it.Err() == nil {
		it.SeekToLast()
	}
	last, err := estimateHandle(r, it)
	if err != nil {
		return 0, 0, 0, 0, err
	}
	if last.Offset < first.Offset {
		return 0, 0, 0, 0, fmt.Errorf("unordered data block handles")
	}
	size, rows, values, err = e.block(r, first, lower, upper)
	blocks++
	if err != nil || first.Offset == last.Offset {
		return
	}
	s, rr, vv, err := e.block(r, last, lower, upper)
	blocks++
	if err != nil {
		return 0, 0, 0, blocks, err
	}
	size += s
	rows += rr
	values += vv
	start, end := first.Offset+first.Size, last.Offset
	if start > end {
		return 0, 0, 0, blocks, fmt.Errorf("overlapping data block handles")
	}
	if start == end {
		return
	}
	size += float64(end - start)
	if !e.countRows {
		return
	}
	// Stratified random offsets weight blocks by their compressed size and
	// spread probes across the range instead of clustering in one region.
	// Average record density, rather than rows/block, handles mixed page sizes
	// and compression ratios without assuming a fixed block target.
	for i := 0; i < rangeBlockSamples; i++ {
		if err = e.ctx.Err(); err != nil {
			return
		}
		// Divide before multiplying so large ranges cannot overflow.
		span := end - start
		lo := span/rangeBlockSamples*uint64(i) + span%rangeBlockSamples*uint64(i)/rangeBlockSamples
		hi := span/rangeBlockSamples*uint64(i+1) + span%rangeBlockSamples*uint64(i+1)/rangeBlockSamples
		if lo == hi {
			continue
		}
		h, readErr := blockAtOffset(r, start+lo+uint64(e.rng.Int63n(int64(hi-lo))))
		if readErr != nil {
			return 0, 0, 0, blocks, readErr
		}
		_, rr, vv, readErr = e.block(r, h, lower, upper)
		blocks++
		if readErr != nil {
			return 0, 0, 0, blocks, readErr
		}
		weight := float64(hi-lo) / float64(h.Size)
		rows += rr * weight
		values += vv * weight
	}
	return
}

func (e *rangeEstimator) sortedMemtable(m *MemTable, lower, upper []byte) (size, rows, values float64, err error) {
	node := m.findGreaterOrEqual(InternalKey{UserKey: lower, SeqNum: math.MaxUint64})
	var exact estimateSample
	n := 0
	for ; node != nil && (upper == nil || bytes.Compare(node.key.UserKey, upper) < 0) && n < rangeMemExactEntries; node = node.next[0].Load() {
		if err = e.ctx.Err(); err != nil {
			return
		}
		n++
		if node.key.SeqNum > e.snapshot {
			continue
		}
		size += float64(len(node.key.UserKey) + 8 + len(node.value))
		e.add(&exact, node.key, len(node.value))
	}
	rows, values, err = e.visible(&exact)
	if err != nil || node == nil || upper != nil && bytes.Compare(node.key.UserKey, upper) >= 0 {
		return
	}
	// Promotion is independent of record contents, with probability 4^-level.
	// Choose a level expected to retain 256-1024 nodes over the whole buffer.
	// Use record count rather than bytes so large values do not undersample.
	m.mu.RLock()
	entries := m.entries
	m.mu.RUnlock()
	level, weight := 0, int64(1)
	for level < maxHeight-1 && entries/weight > rangeMemSampleEntries {
		level++
		weight *= branching
	}
	target := node.key
	current := m.head
	for h := int(m.height.Load()) - 1; h >= level; h-- {
		for next := current.next[h].Load(); next != nil && next.key.Compare(target) < 0; next = current.next[h].Load() {
			current = next
		}
	}
	var sampled estimateSample
	for node = current.next[level].Load(); node != nil && (upper == nil || bytes.Compare(node.key.UserKey, upper) < 0); node = node.next[level].Load() {
		if err = e.ctx.Err(); err != nil {
			return
		}
		if node.key.SeqNum > e.snapshot {
			continue
		}
		size += float64(weight) * float64(len(node.key.UserKey)+8+len(node.value))
		e.add(&sampled, node.key, len(node.value))
	}
	r, v, err := e.visible(&sampled)
	return size, rows + r*float64(weight), values + v*float64(weight), err
}
