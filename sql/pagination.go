package sql

import "math"

// limitWindow saturates the number of rows needed before OFFSET is applied.
// Overflow must never wrap a large window into zero or a negative scan bound.
func limitWindow(limit, offset int64) int64 {
	if limit < 0 {
		return -1
	}
	if offset > math.MaxInt64-limit {
		return math.MaxInt64
	}
	return limit + offset
}
