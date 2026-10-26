package storage

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"sort"
	"testing"
)

func TestSSTableSeekUsesUserBoundsWithNulExtensions(t *testing.T) {
	l := reusableTestTree(t, BlockCacheMmap)
	keys := [][]byte{[]byte{}, []byte("a"), []byte("a\x01"), []byte("a\x01\x00"), []byte("a\x01\x00x"), []byte("b")}
	for _, key := range keys {
		if err := l.Put(key, bytes.Repeat([]byte{'x'}, 180)); err != nil {
			t.Fatal(err)
		}
	}
	if err := l.CompactRange(); err != nil {
		t.Fatal(err)
	}
	for _, target := range [][]byte{[]byte("\x00"), []byte("a\x00"), []byte("a\x01\x00\x00"), []byte("a\x01\x00x\x00")} {
		index := sort.Search(len(keys), func(i int) bool { return bytes.Compare(keys[i], target) >= 0 })
		it := l.NewIteratorRange(target, nil, false)
		if !it.Valid() || !bytes.Equal(it.Key().UserKey, keys[index]) {
			t.Fatalf("Seek %q returned %q, want %q", target, it.Key().UserKey, keys[index])
		}
		it.Close()
		count, err := l.CountRangeAtContext(nil, math.MaxUint64, target, nil, 2)
		if err != nil || count != int64(len(keys)-index) {
			t.Fatal("raw user bound count changed", target, count, err)
		}
	}
}

// Compare complete exact counts against the ordinary key-iterator API. Loading,
// compaction and cache warmup are excluded; every timed result is checked.
func BenchmarkExactRangeCount(b *testing.B) {
	opts := pageTestOptions()
	opts.PageSize = 4096
	opts.BlockCacheSize = 32 << 20
	l, err := NewLSMTree(b.TempDir(), opts)
	if err != nil {
		b.Fatal(err)
	}
	defer l.Close()
	const rows = 32768
	entries := make([]Mutation, rows)
	for i := range entries {
		entries[i] = Mutation{Key: []byte(fmt.Sprintf("key%08d", i)), Value: bytes.Repeat([]byte{'v'}, 240)}
	}
	if err := l.WriteBatch(entries); err != nil {
		b.Fatal(err)
	}
	if err := l.CompactRange(); err != nil {
		b.Fatal(err)
	}
	seq := l.SnapshotForReader(3)
	defer l.ReleaseSnapshotForReader(3, seq)
	for _, mode := range []string{"iterator", "storage-count", "storage-cancellable-count"} {
		b.Run(mode, func(b *testing.B) {
			ctx := context.Background()
			if mode == "storage-cancellable-count" {
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				defer cancel()
			}
			run := func() int64 {
				if mode != "iterator" {
					n, err := l.CountRangeAtContext(ctx, seq, []byte("key"), []byte("kez"), 3)
					if err != nil {
						b.Fatal(err)
					}
					return n
				}
				it := l.NewIteratorAtRangeKeys(seq, []byte("key"), []byte("kez"), false)
				defer it.Close()
				var count int64
				for it.Valid() {
					count++
					it.Next()
				}
				if err := IteratorError(it); err != nil {
					b.Fatal(err)
				}
				return count
			}
			if run() != rows {
				b.Fatal("invalid fixture count")
			}
			b.ReportAllocs()
			b.SetBytes(rows * 240)
			b.ReportMetric(rows, "keys/op")
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if n := run(); n != rows {
					b.Fatal("count changed", n)
				}
			}
		})
	}
}
