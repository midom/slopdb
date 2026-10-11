package storage

import "testing"

// The conservative unknown-filter check came from bloom_compatibility_review.go.
// Historical on-disk formats that the current engine does not support are not
// fixtures for the current format.
func TestBloomUnknownStrategy(t *testing.T) {
	filter := NewBloomFilter(10)
	for _, data := range [][]byte{nil, {}, {0}, {0, 31}, {0, 31 | bloomRotatedDelta}} {
		if !filter.MayContain(data, []byte("absent")) {
			t.Fatalf("unknown or empty filter caused a false negative: %x", data)
		}
	}
}
