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

func TestMetadataPartitionFloorAndOwnedBoundaries(t *testing.T) {
	opts := pageTestOptions()
	opts.PageSize = 4096
	l, err := NewLSMTree(t.TempDir(), opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Close() })
	const rows = 4096
	entries := make([]Mutation, rows)
	for i := range entries {
		entries[i] = Mutation{Key: []byte(fmt.Sprintf("key%08d", i)), Value: bytes.Repeat([]byte{'x'}, 240)}
	}
	if err := l.WriteBatch(entries); err != nil {
		t.Fatal(err)
	}
	if err := l.CompactRange(); err != nil {
		t.Fatal(err)
	}
	seq := l.SnapshotForReader(6)
	defer l.ReleaseSnapshotForReader(6, seq)
	before := l.Status().DataBlockReadAttempts
	parts, err := l.PartitionRangeAtContext(nil, seq, []byte("key"), []byte("kez"), 6, 60)
	if err != nil || len(parts) < 2 || len(parts) > 5 || l.Status().DataBlockReadAttempts != before {
		t.Fatal("metadata partition floor/read behavior", len(parts), err)
	}
	var total int64
	for i, part := range parts {
		if i > 0 && !bytes.Equal(parts[i-1].Upper, part.Lower) {
			t.Fatal("partition coverage gap")
		}
		got, err := l.CountRangeAtContext(nil, seq, part.Lower, part.Upper, uint64(10+i))
		if err != nil {
			t.Fatal(err)
		}
		var want int64
		for _, entry := range entries {
			if bytes.Compare(entry.Key, part.Lower) >= 0 && bytes.Compare(entry.Key, part.Upper) < 0 {
				want++
			}
		}
		if got != want {
			t.Fatal("partition violated exact user bounds", i, got, want)
		}
		total += got
	}
	if total != rows {
		t.Fatal("partitions changed exact count", total)
	}
	saved := bytes.Clone(parts[1].Lower)
	parts[0].Upper[0] ^= 0xff
	if !bytes.Equal(parts[1].Lower, saved) {
		t.Fatal("partition bounds alias")
	}
	narrow, err := l.PartitionRangeAtContext(nil, seq, entries[0].Key, entries[10].Key, 6, 60)
	if err != nil || len(narrow) != 1 {
		t.Fatal("small range spawned partitions", len(narrow), err)
	}
}

func TestMetadataPartitionsBalanceWholeLevelWithOverlaySources(t *testing.T) {
	opts := pageTestOptions()
	opts.PageSize = 4096
	opts.BaseLevelSize = 1
	l, err := NewLSMTree(t.TempDir(), opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Close() })
	const rows = 16384
	entries := make([]Mutation, rows)
	for i := range entries {
		entries[i] = Mutation{Key: []byte(fmt.Sprintf("key%08d", i)), Value: bytes.Repeat([]byte{'x'}, 240)}
	}
	if err := l.WriteBatch(entries); err != nil {
		t.Fatal(err)
	}
	if err := l.CompactRange(); err != nil {
		t.Fatal(err)
	}
	view, _ := l.acquireView()
	files := len(view.levels[maxLevels-1])
	view.release()
	if files < 4 {
		t.Fatal("fixture did not make multiple bottom files", files)
	}
	seq := l.SnapshotForReader(4)
	defer l.ReleaseSnapshotForReader(4, seq)
	// An overlapping L0 and private-to-this-view mutable memtable must not
	// prevent the chosen nonoverlapping level from supplying all split points.
	for i := 0; i < rows; i += 9 {
		if err := l.Put(entries[i].Key, bytes.Repeat([]byte{'n'}, 240)); err != nil {
			t.Fatal(err)
		}
	}
	if err := l.Flush(); err != nil {
		t.Fatal(err)
	}
	for i := rows; i < rows+256; i++ {
		if err := l.Put([]byte(fmt.Sprintf("key%08d", i)), []byte("new")); err != nil {
			t.Fatal(err)
		}
	}
	before := l.Status().DataBlockReadAttempts
	parts, err := l.PartitionRangeAtContext(nil, seq, []byte("key"), []byte("kez"), 4, 8)
	if err != nil || len(parts) != 8 || l.Status().DataBlockReadAttempts != before {
		t.Fatal("whole-level metadata plan", len(parts), err)
	}
	var oldTotal, newTotal int64
	for i, part := range parts {
		old, err := l.CountRangeAtContext(nil, seq, part.Lower, part.Upper, uint64(10+i))
		if err != nil {
			t.Fatal(err)
		}
		if old < rows/16 || old > rows/4 {
			t.Fatalf("partition%d owns%d/%d rows: splits collapsed onto one file", i, old, rows)
		}
		latest, err := l.CountRangeAtContext(nil, math.MaxUint64, part.Lower, part.Upper, uint64(20+i))
		if err != nil {
			t.Fatal(err)
		}
		oldTotal += old
		newTotal += latest
	}
	if oldTotal != rows || newTotal != rows+256 {
		t.Fatal("mixed-source partition coverage changed", oldTotal, newTotal)
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
