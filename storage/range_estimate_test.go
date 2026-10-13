package storage

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func openEstimateTree(t *testing.T, dir string, opt *Options) *LSMTree {
	t.Helper()
	l, err := NewLSMTree(dir, opt)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Close() })
	return l
}

func TestRangeEstimateSmallMVCC(t *testing.T) {
	l := openEstimateTree(t, t.TempDir(), pageTestOptions())
	ctx := context.Background()
	for _, kv := range []Mutation{{Key: []byte("a"), Value: []byte("old")}, {Key: []byte("b"), Value: []byte("second")}, {Key: []byte("z"), Value: bytes.Repeat([]byte("z"), 1000)}} {
		if err := l.Put(kv.Key, kv.Value); err != nil {
			t.Fatal(err)
		}
	}
	snapshot := l.Snapshot()
	defer l.ReleaseSnapshot(snapshot)
	if err := l.Put([]byte("a"), []byte("newer")); err != nil {
		t.Fatal(err)
	}
	if err := l.Delete([]byte("b")); err != nil {
		t.Fatal(err)
	}
	if err := l.Put([]byte("c"), []byte("third")); err != nil {
		t.Fatal(err)
	}
	check := func() {
		t.Helper()
		got, err := l.EstimateRange(ctx, []byte("a"), []byte("d"), math.MaxUint64)
		if err != nil || got.Rows != 2 || got.ValueBytes != 10 || got.DiskBytes+got.MemoryBytes <= 0 {
			t.Fatalf("current: %+v %v", got, err)
		}
		got, err = l.EstimateRange(ctx, []byte("a"), []byte("d"), snapshot)
		if err != nil || got.Rows != 2 || got.ValueBytes != 9 {
			t.Fatalf("snapshot: %+v %v", got, err)
		}
		absent, err := l.EstimateRange(ctx, []byte("e"), []byte("f"), math.MaxUint64)
		if err != nil || absent.Rows != 0 || absent.DiskBytes+absent.MemoryBytes != 0 {
			t.Fatalf("absent: %+v %v", absent, err)
		}
	}
	check()
	if err := l.CompactRange(); err != nil {
		t.Fatal(err)
	}
	check()
	if err := l.DeleteRange([]byte("a"), []byte("d")); err != nil {
		t.Fatal(err)
	}
	got, err := l.EstimateRange(ctx, []byte("a"), []byte("d"), math.MaxUint64)
	if err != nil || got.Rows != 0 || got.ValueBytes != 0 || got.DiskBytes+got.MemoryBytes <= 0 {
		t.Fatalf("range deletion: %+v %v", got, err)
	}
	if err := l.CompactRange(); err != nil {
		t.Fatal(err)
	}
	got, err = l.EstimateRange(ctx, []byte("a"), []byte("d"), snapshot)
	if err != nil || got.Rows != 2 {
		t.Fatalf("pinned deletion snapshot: %+v %v", got, err)
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err = l.EstimateRange(ctx, nil, nil, math.MaxUint64); err == nil {
		t.Fatal("estimated closed tree")
	}
}

func TestRangeEstimateBlocksAndCompression(t *testing.T) {
	for _, codec := range []CompressionType{NoCompression, GzipCompression, ZstdCompression, SnappyCompression, LZ4Compression} {
		t.Run(string(codec), func(t *testing.T) {
			dir := t.TempDir()
			opt := pageTestOptions()
			opt.CompressionType = codec
			opt.PageSize = 1024
			l := openEstimateTree(t, dir, opt)
			rng := rand.New(rand.NewSource(123))
			var batch []Mutation
			for i := 0; i < 5000; i++ {
				value := make([]byte, 40+i%160)
				// Alternate compressible and incompressible records to vary block sizes.
				if i%2 == 0 {
					rng.Read(value)
				} else {
					copy(value, bytes.Repeat([]byte("value"), len(value)/5+1))
				}
				batch = append(batch, Mutation{Key: []byte(fmt.Sprintf("row%08d", i)), Value: value})
			}
			if err := l.WriteBatch(batch); err != nil {
				t.Fatal(err)
			}
			if err := l.CompactRange(); err != nil {
				t.Fatal(err)
			}
			check := func() {
				t.Helper()
				view, _ := l.acquireView()
				defer view.release()
				var expected uint64
				var files int64
				for _, slot := range view.files {
					files++
					it := slot.entry.reader.indexBlock.NewIterator()
					for it.Valid() {
						h, err := estimateHandle(slot.entry.reader, it)
						if err != nil {
							t.Fatal(err)
						}
						expected += h.Size
						// Test random offset routing at both edges of every block, including
						// indexes spanning many restart intervals.
						for _, offset := range []uint64{h.Offset, h.Offset + h.Size - 1} {
							routed, err := blockAtOffset(slot.entry.reader, offset)
							if err != nil || routed != h {
								t.Fatalf("offset %d: %+v != %+v (%v)", offset, routed, h, err)
							}
						}
						it.Next()
					}
					if err := it.Err(); err != nil {
						t.Fatal(err)
					}
				}
				got, err := l.EstimateRange(context.Background(), nil, nil, math.MaxUint64)
				if err != nil || got.DiskBytes != int64(expected) || got.MemoryBytes != 0 || got.SampledBlocks > 10*files || got.Rows <= 0 {
					t.Fatalf("whole range: %+v expected bytes=%d (%v)", got, expected, err)
				}
				sizes, err := l.EstimateRangeBytes(context.Background(), nil, nil)
				if err != nil || sizes.DiskBytes != int64(expected) || sizes.SampledBlocks > 2*files || sizes.Rows != 0 {
					t.Fatalf("sizes only: %+v (%v)", sizes, err)
				}
				part, err := l.EstimateRange(context.Background(), []byte("row00001000"), []byte("row00002000"), math.MaxUint64)
				if err != nil || part.Rows <= 0 || part.DiskBytes <= 0 || part.DiskBytes >= got.DiskBytes {
					t.Fatalf("subrange: %+v (%v)", part, err)
				}
				// Boundary attribution is additive even when two ranges share a block.
				left, err := l.EstimateRangeBytes(context.Background(), nil, []byte("row00001234"))
				if err != nil {
					t.Fatal(err)
				}
				right, err := l.EstimateRangeBytes(context.Background(), []byte("row00001234"), nil)
				if err != nil || math.Abs(float64(left.DiskBytes+right.DiskBytes-int64(expected))) > 1 {
					t.Fatalf("partition bytes: %+v %+v expected=%d (%v)", left, right, expected, err)
				}
				// Check distribution accuracy over fixed seeds rather than imposing
				// a precision guarantee on a single random estimate.
				for _, bounds := range []struct {
					lower, upper []byte
					rows         float64
				}{
					{nil, nil, 5000},
					{[]byte("row00001000"), []byte("row00002000"), 1000},
				} {
					var sum float64
					for seed := int64(1); seed <= 32; seed++ {
						e := rangeEstimator{ctx: context.Background(), view: view, snapshot: math.MaxUint64, countRows: true, rng: rand.New(rand.NewSource(seed))}
						for _, slot := range view.files {
							_, rows, _, blocks, err := e.table(slot.entry.reader, bounds.lower, bounds.upper)
							if err != nil || blocks > 10 {
								t.Fatalf("sample: blocks=%d, %v", blocks, err)
							}
							sum += rows
						}
					}
					if math.Abs(sum/32-bounds.rows) > bounds.rows*0.1 {
						t.Fatalf("sample mean %.0f, expected %.0f", sum/32, bounds.rows)
					}
				}
			}
			check()
			if err := l.Close(); err != nil {
				t.Fatal(err)
			}
			l = openEstimateTree(t, dir, opt)
			check()
			if err := l.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestRangeEstimateMemtableSampling(t *testing.T) {
	opt := pageTestOptions()
	opt.WriteBufferSize = 32 << 20
	l := openEstimateTree(t, t.TempDir(), opt)
	emptySnapshot := l.Snapshot()
	defer l.ReleaseSnapshot(emptySnapshot)
	batch := make([]Mutation, 50000)
	var expected int64
	for i := range batch {
		batch[i] = Mutation{Key: []byte(fmt.Sprintf("row%08d", i)), Value: bytes.Repeat([]byte("v"), 100)}
		expected += int64(len(batch[i].Key) + 8 + len(batch[i].Value))
	}
	if err := l.WriteBatch(batch); err != nil {
		t.Fatal(err)
	}
	got, err := l.EstimateRange(context.Background(), []byte("row"), []byte("rox"), math.MaxUint64)
	if err != nil || got.DiskBytes != 0 || got.Rows < 35000 || got.Rows > 65000 || math.Abs(float64(got.MemoryBytes-expected)) > float64(expected)*0.3 {
		t.Fatalf("memtable estimate: %+v expected=%d (%v)", got, expected, err)
	}
	// Sparse/narrow ranges are exact even if the surrounding memtable is large.
	got, err = l.EstimateRange(context.Background(), []byte("row00001000"), []byte("row00001002"), math.MaxUint64)
	if err != nil || got.Rows != 2 || got.ValueBytes != 200 || got.MemoryBytes != 238 {
		t.Fatalf("short range: %+v (%v)", got, err)
	}
	old, err := l.EstimateRange(nil, nil, nil, emptySnapshot)
	if err != nil || old.Rows != 0 || old.ValueBytes != 0 || old.MemoryBytes != 0 {
		t.Fatalf("snapshot before large memtable writes: %+v (%v)", old, err)
	}
}

func TestRangeEstimateErrorsAndConcurrentViews(t *testing.T) {
	opt := pageTestOptions()
	l := openEstimateTree(t, t.TempDir(), opt)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := l.EstimateRange(ctx, nil, nil, math.MaxUint64); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancel: %v", err)
	}
	if _, err := l.EstimateRange(nil, []byte("z"), []byte("a"), math.MaxUint64); err == nil {
		t.Fatal("accepted backwards range")
	}
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			if err := l.Put([]byte(fmt.Sprintf("a%05d", i)), []byte("value")); err != nil {
				t.Error(err)
				return
			}
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 20; i++ {
			if _, err := l.EstimateRange(nil, []byte("a"), []byte("b"), math.MaxUint64); err != nil {
				t.Error(err)
				return
			}
			if i%5 == 0 {
				if err := l.CompactRange(); err != nil {
					t.Error(err)
					return
				}
			}
		}
	}()
	wg.Wait()
	if err := l.CompactRange(); err != nil {
		t.Fatal(err)
	}
	// Corrupt a guaranteed boundary sample in a sizes-only call, bypassing
	// visibility reads/cache. Errors must not turn into misleading zero sizes.
	view, _ := l.acquireView()
	slot := view.files[0]
	handle, err := estimateHandle(slot.entry.reader, slot.entry.reader.indexBlock.NewIterator())
	if err != nil {
		t.Fatal(err)
	}
	view.release()
	path := filepath.Join(l.dir, fmt.Sprintf("%d.sst", slot.meta.Number))
	file, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.WriteAt([]byte{0xff}, int64(handle.Offset)); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	// A previous read can cache this block; diagnostics must invalidate it.
	slot.entry.reader.verifyChanges = true
	if _, err := l.EstimateRangeBytes(context.Background(), nil, nil); err == nil {
		t.Fatal("corrupt sample accepted")
	}
}

func TestRangeEstimateOverlappingFilesAndPageSizes(t *testing.T) {
	dir := t.TempDir()
	opt := pageTestOptions()
	opt.CompressionType = ZstdCompression
	opt.PageSize = 512
	l := openEstimateTree(t, dir, opt)
	for _, key := range []string{"a", "b", "c"} {
		if err := l.Put([]byte(key), bytes.Repeat([]byte(key), 1000)); err != nil {
			t.Fatal(err)
		}
	}
	snapshot := l.Snapshot()
	defer l.ReleaseSnapshot(snapshot)
	if err := l.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := l.SetPageSize(8192); err != nil {
		t.Fatal(err)
	}
	if err := l.Put([]byte("b"), []byte("new")); err != nil {
		t.Fatal(err)
	}
	if err := l.Delete([]byte("a")); err != nil {
		t.Fatal(err)
	}
	if err := l.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := l.SetPageSize(1024); err != nil {
		t.Fatal(err)
	}
	if err := l.Put([]byte("c"), []byte("current")); err != nil {
		t.Fatal(err)
	}
	if err := l.Flush(); err != nil {
		t.Fatal(err)
	}
	if len(l.Stats().Levels[0].Files) != 3 {
		t.Fatal("fixture should have three overlapping L0 files")
	}
	current, err := l.EstimateRange(nil, nil, nil, math.MaxUint64)
	if err != nil || current.Rows != 2 || current.ValueBytes != 10 || current.MemoryBytes != 0 || current.SampledBlocks > 30 {
		t.Fatalf("current mixed files: %+v %v", current, err)
	}
	old, err := l.EstimateRange(nil, nil, nil, snapshot)
	if err != nil || old.Rows != 3 || old.ValueBytes != 3000 {
		t.Fatalf("mixed files snapshot: %+v %v", old, err)
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	l = openEstimateTree(t, dir, opt)
	current, err = l.EstimateRange(nil, nil, nil, math.MaxUint64)
	if err != nil || current.Rows != 2 || current.ValueBytes != 10 {
		t.Fatalf("reopened mixed files: %+v %v", current, err)
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
}
