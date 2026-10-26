package storage

import (
	"bytes"
	"context"
	"fmt"
	"math"
)

// CountRangeAtContext counts exact visible keys in [lower, upper) at snapshot.
// Nil bounds are unbounded; equal bounds are empty and reversed bounds fail.
// Snapshot zero is honored; snapshots above the committed sequence are clipped
// (MaxUint64 requests the latest committed view). The caller must keep snapshot
// pinned for the operation, as with GetAtForReader. reader selects a sharded
// view registration; one view is held until completion or cancellation.
//
// Versions, point deletes and range tombstones are merged normally. Values are
// never copied or SQL-decoded, but compressed row blocks still need decoding.
// Context checks run in bounded groups of visible and skipped entries. Any
// cancellation/read error returns zero and an error, never a partial count.
func (l *LSMTree) CountRangeAtContext(ctx context.Context, snapshot uint64, lower, upper []byte, reader uint64) (int64, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if lower != nil && upper != nil && bytes.Compare(lower, upper) > 0 {
		return 0, fmt.Errorf("invalid count range")
	}
	view, committed, shard := l.acquireViewForReader(reader)
	if view == nil {
		return 0, fmt.Errorf("database closed")
	}
	if lower != nil && upper != nil && bytes.Equal(lower, upper) {
		view.releaseReader(shard)
		return 0, nil
	}
	if snapshot > committed {
		snapshot = committed
	}
	var scanContext context.Context
	if ctx.Done() != nil {
		scanContext = ctx
	}
	it := l.iteratorRangeFromView(view, snapshot, lower, lower, upper, false, true, true, scanContext, &readerViewCloser{view: view, shard: shard})
	defer it.Close()
	var count int64
	for it.valid {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		// Traverse inside storage, bypassing per-row Iterator/SQL callbacks and
		// repeated bounds/validity wrappers. Boundaries are checked once per key.
		for batch := 0; batch < 256 && it.valid; batch++ {
			if upper != nil && bytes.Compare(it.lastKey.UserKey, upper) >= 0 {
				if err := ctx.Err(); err != nil {
					return 0, err
				}
				return count, nil
			}
			if count == math.MaxInt64 {
				return 0, fmt.Errorf("range count overflows int64")
			}
			count++
			it.visibleReusable(false)
		}
	}
	if err := it.err; err != nil {
		return 0, err
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	return count, nil
}

type readerViewCloser struct {
	view  *readView
	shard uint64
}

func (c *readerViewCloser) Close() {
	if c.view != nil {
		c.view.releaseReader(c.shard)
		c.view = nil
	}
}
