package storage

import (
	"bytes"
	"context"
	"fmt"
	"sort"
)

// KeyRange is an owned [Lower, Upper) interval. Nil bounds are unbounded.
type KeyRange struct{ Lower, Upper []byte }

// PartitionRangeAtContext returns up to parts disjoint owned intervals covering
// [lower, upper). It consults only already-resident SSTable block indexes in one
// reader-sharded view; no data block is read and no row count is estimated.
// The largest overlapping table supplies physical-byte quantile boundaries.
// This is a scheduling heuristic, not a promise of equal visible row counts.
// Each planned part requires at least 256 KiB of physical bytes in that table;
// memtable-only or small ranges may return one interval. The caller keeps
// snapshot pinned while using the intervals with CountRangeAtContext; old and
// new keys/tombstones remain covered even when the view changes between calls.
// parts must be between 1 and 1024. Equal bounds produce one empty interval.
func (l *LSMTree) PartitionRangeAtContext(ctx context.Context, snapshot uint64, lower, upper []byte, reader uint64, parts int) ([]KeyRange, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if parts < 1 || parts > 1024 {
		return nil, fmt.Errorf("range partitions must be between 1 and 1024")
	}
	if lower != nil && upper != nil && bytes.Compare(lower, upper) > 0 {
		return nil, fmt.Errorf("invalid partition range")
	}
	view, _, shard := l.acquireViewForReader(reader)
	if view == nil {
		return nil, fmt.Errorf("database closed")
	}
	defer view.releaseReader(shard)
	// snapshot does not determine scheduling boundaries. Every returned range
	// is complete for every pinned snapshot, including keys only in memtables.
	var best *SSTableReader
	var first, last int
	var spanStart, spanBytes uint64
	if parts > 1 && !(lower != nil && upper != nil && bytes.Equal(lower, upper)) {
		for _, slot := range view.files {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			r := slot.entry.reader
			if len(r.pointIndex) == 0 || lower != nil && bytes.Compare(slot.meta.Largest.UserKey, lower) < 0 || upper != nil && bytes.Compare(slot.meta.Smallest.UserKey, upper) >= 0 {
				continue
			}
			lo, hi := 0, len(r.pointIndex)
			if lower != nil {
				lo = r.pointIndexSeek(lower)
			}
			if upper != nil {
				hi = r.pointIndexSeek(upper) + 1
				if hi > len(r.pointIndex) {
					hi = len(r.pointIndex)
				}
			}
			if lo >= hi {
				continue
			}
			start := r.pointIndex[lo].handle.Offset
			end := r.pointIndex[hi-1].handle.Offset + r.pointIndex[hi-1].handle.Size
			if end > start && end-start > spanBytes {
				best, first, last, spanStart, spanBytes = r, lo, hi, start, end-start
			}
		}
	}
	if best == nil {
		return []KeyRange{{bytes.Clone(lower), bytes.Clone(upper)}}, nil
	}
	// A fixed physical-byte floor avoids scheduling tiny tasks. Using only
	// the dominant table is conservative when other sources overlap it.
	byBytes := spanBytes / (256 << 10)
	if byBytes < 1 {
		byBytes = 1
	}
	if uint64(parts) > byBytes {
		parts = int(byBytes)
	}
	if parts > last-first+1 {
		parts = last - first + 1
	}
	ranges := make([]KeyRange, 0, parts)
	current := bytes.Clone(lower)
	for i := 1; i < parts; i++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		// Divide first to keep quantile arithmetic overflow-safe.
		target := spanStart + (spanBytes/uint64(parts))*uint64(i) + (spanBytes%uint64(parts))*uint64(i)/uint64(parts)
		position := first + sort.Search(last-first, func(j int) bool {
			handle := best.pointIndex[first+j].handle
			return handle.Offset+handle.Size >= target
		})
		if position >= last {
			continue
		}
		key := best.pointIndexKey(position)
		// Do not turn scheduling a very large user key into large retained
		// scratch. Skipping a candidate only reduces parallelism.
		if len(key) >= iteratorScratchLimit {
			continue
		}
		// Index keys are the last user key of a block. Appending NUL is the
		// immediate bytewise successor boundary, so all versions of that key
		// stay together without guessing typed/composite-key encodings.
		boundary := append(bytes.Clone(key), 0)
		if current != nil && bytes.Compare(boundary, current) <= 0 || upper != nil && bytes.Compare(boundary, upper) >= 0 {
			continue
		}
		ranges = append(ranges, KeyRange{Lower: current, Upper: boundary})
		// Adjacent output intervals own independent slices.
		current = bytes.Clone(boundary)
	}
	ranges = append(ranges, KeyRange{Lower: current, Upper: bytes.Clone(upper)})
	return ranges, nil
}
