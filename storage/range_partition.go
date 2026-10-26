package storage

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"sort"
)

// KeyRange is an owned [Lower, Upper) interval. Nil bounds are unbounded.
type KeyRange struct{ Lower, Upper []byte }

// PartitionRangeAtContext returns up to parts disjoint owned intervals covering
// [lower, upper). It consults only already-resident SSTable block indexes in one
// reader-sharded view; no data block is read and no row count is estimated.
// The largest overlapping nonoverlapping level supplies physical-byte quantile
// boundaries across all its selected tables. An unordered/overlapping level
// contributes only its largest table. This is a scheduling heuristic, not a
// promise of equal visible row counts. Each part requires at least 256 KiB of
// physical bytes across the selected source;
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
	var best []partitionSpan
	var spanBytes uint64
	var blocks int
	if parts > 1 && !(lower != nil && upper != nil && bytes.Equal(lower, upper)) {
		for level, files := range view.levels {
			var spans []partitionSpan
			var total uint64
			var totalBlocks int
			if view.ordered[level] {
				spans = make([]partitionSpan, 0, len(files))
			}
			for _, slot := range files {
				if err := ctx.Err(); err != nil {
					return nil, err
				}
				span, ok := partitionTableSpan(slot, lower, upper)
				if !ok {
					continue
				}
				if view.ordered[level] {
					if span.bytes > math.MaxUint64-total {
						return nil, fmt.Errorf("partition byte total overflows uint64")
					}
					total += span.bytes
					span.cumulative = total
					spans = append(spans, span)
					if totalBlocks < 1024 {
						totalBlocks += span.last - span.first
						if totalBlocks > 1024 {
							totalBlocks = 1024
						}
					}
				} else if span.bytes > spanBytes {
					span.cumulative = span.bytes
					best = []partitionSpan{span}
					spanBytes = span.bytes
					blocks = span.last - span.first
				}
			}
			if view.ordered[level] && total > spanBytes {
				best, spanBytes, blocks = spans, total, totalBlocks
			}
		}
	}
	if len(best) == 0 {
		return []KeyRange{{bytes.Clone(lower), bytes.Clone(upper)}}, nil
	}
	// Use the entire chosen nonoverlapping source for the floor, not a single
	// table: otherwise a large many-file L6 would collapse onto one worker.
	byBytes := spanBytes / (256 << 10)
	if byBytes < 1 {
		byBytes = 1
	}
	if uint64(parts) > byBytes {
		parts = int(byBytes)
	}
	if parts > blocks {
		parts = blocks
	}
	ranges := make([]KeyRange, 0, parts)
	current := bytes.Clone(lower)
	for i := 1; i < parts; i++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		// Divide first to keep quantile arithmetic overflow-safe.
		target := (spanBytes/uint64(parts))*uint64(i) + (spanBytes%uint64(parts))*uint64(i)/uint64(parts)
		source := sort.Search(len(best), func(j int) bool { return best[j].cumulative >= target })
		if source == len(best) {
			continue
		}
		span := best[source]
		before := span.cumulative - span.bytes
		physical := span.start + target - before
		position := span.first + sort.Search(span.last-span.first, func(j int) bool {
			handle := span.reader.pointIndex[span.first+j].handle
			return handle.Offset+handle.Size >= physical
		})
		if position >= span.last {
			continue
		}
		key := span.reader.pointIndexKey(position)
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

type partitionSpan struct {
	reader                   *SSTableReader
	first, last              int
	start, bytes, cumulative uint64
}

func partitionTableSpan(slot viewFile, lower, upper []byte) (partitionSpan, bool) {
	r := slot.entry.reader
	if len(r.pointIndex) == 0 || lower != nil && bytes.Compare(slot.meta.Largest.UserKey, lower) < 0 || upper != nil && bytes.Compare(slot.meta.Smallest.UserKey, upper) >= 0 {
		return partitionSpan{}, false
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
		return partitionSpan{}, false
	}
	start := r.pointIndex[lo].handle.Offset
	end := r.pointIndex[hi-1].handle.Offset + r.pointIndex[hi-1].handle.Size
	if end <= start {
		return partitionSpan{}, false
	}
	return partitionSpan{reader: r, first: lo, last: hi, start: start, bytes: end - start}, true
}
