package storage

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

type countRangeBounds struct{ lower, upper []byte }

func modelRangeCount(rows map[string][]byte, lower, upper []byte) int64 {
	var n int64
	for key := range rows {
		if (lower == nil || bytes.Compare([]byte(key), lower) >= 0) && (upper == nil || bytes.Compare([]byte(key), upper) < 0) {
			n++
		}
	}
	return n
}

func TestCountRangeExactSnapshotsVersionsTombstones(t *testing.T) {
	for _, memory := range []BlockCacheMemory{BlockCacheHeap, BlockCacheMmap} {
		t.Run(string(memory), func(t *testing.T) {
			l := reusableTestTree(t, memory)
			old, latest := make(map[string][]byte), make(map[string][]byte)
			for i := 0; i < 350; i++ {
				key := fmt.Sprintf("r%04d", i)
				value := bytes.Repeat([]byte{byte(i)}, 230)
				old[key], latest[key] = value, value
				if err := l.Put([]byte(key), value); err != nil {
					t.Fatal(err)
				}
				if i%70 == 69 {
					if err := l.Flush(); err != nil {
						t.Fatal(err)
					}
				}
			}
			for _, key := range []string{"", "\x00", "a\x00", "a\x00\xff", "z\xff"} {
				if err := l.Put([]byte(key), []byte{}); err != nil {
					t.Fatal(err)
				}
				old[key], latest[key] = []byte{}, []byte{}
			}
			seq := l.SnapshotForReader(12)
			defer l.ReleaseSnapshotForReader(12, seq)
			for i := 0; i < 350; i++ {
				key := fmt.Sprintf("r%04d", i)
				if i%3 == 0 {
					if err := l.Put([]byte(key), bytes.Repeat([]byte{'n'}, 240)); err != nil {
						t.Fatal(err)
					}
				}
				if i%7 == 0 {
					if err := l.Delete([]byte(key)); err != nil {
						t.Fatal(err)
					}
					delete(latest, key)
				}
			}
			if err := l.Flush(); err != nil {
				t.Fatal(err)
			}
			if err := l.DeleteRange([]byte("r0090"), []byte("r0130")); err != nil {
				t.Fatal(err)
			}
			for i := 90; i < 130; i++ {
				delete(latest, fmt.Sprintf("r%04d", i))
			}
			if err := l.Put([]byte("r0100"), []byte("resurrected")); err != nil {
				t.Fatal(err)
			}
			latest["r0100"] = []byte("resurrected")
			for _, compact := range []bool{false, true} {
				if compact {
					if err := l.CompactRange(); err != nil {
						t.Fatal(err)
					}
				}
				for _, version := range []struct {
					seq  uint64
					rows map[string][]byte
				}{{seq, old}, {math.MaxUint64, latest}, {0, map[string][]byte{}}} {
					for _, bounds := range []countRangeBounds{{nil, nil}, {[]byte("r0000"), []byte("r0350")}, {[]byte("r0050"), []byte("r0250")}, {[]byte("r0100"), []byte("r0100")}, {[]byte("missing"), []byte("n")}, {nil, []byte{}}, {[]byte("a\x00"), []byte("b")}} {
						want := modelRangeCount(version.rows, bounds.lower, bounds.upper)
						got, err := l.CountRangeAtContext(context.Background(), version.seq, bounds.lower, bounds.upper, 12)
						if err != nil || got != want {
							t.Fatalf("compact=%v snapshot=%d [%q,%q): count=%d want=%d error=%v", compact, version.seq, bounds.lower, bounds.upper, got, want, err)
						}

					}
				}
			}
		})
	}
}

// Cancel only when the scan cooperatively polls, avoiding timing/sleep-based
// tests while exercising cancellation inside skipped/deleted source groups.
type cancelOnCountPoll struct {
	context.Context
	cancel      context.CancelFunc
	polls, left int
}

func (c *cancelOnCountPoll) Err() error {
	c.polls++
	c.left--
	if c.left == 0 {
		c.cancel()
	}
	return c.Context.Err()
}

func TestCountRangeCancellationErrorsAndViewRelease(t *testing.T) {
	l := reusableTestTree(t, BlockCacheMmap)
	for i := 0; i < 1200; i++ {
		if err := l.Put([]byte(fmt.Sprintf("k%04d", i)), bytes.Repeat([]byte{'x'}, 180)); err != nil {
			t.Fatal(err)
		}
	}
	if err := l.CompactRange(); err != nil {
		t.Fatal(err)
	}
	for _, deleted := range []bool{false, true} {
		if deleted {
			if err := l.DeleteRange([]byte("k"), []byte("l")); err != nil {
				t.Fatal(err)
			}
		}
		base, cancel := context.WithCancel(context.Background())
		ctx := &cancelOnCountPoll{Context: base, cancel: cancel, left: 7}
		n, err := l.CountRangeAtContext(ctx, math.MaxUint64, []byte("k"), []byte("l"), 31)
		cancel()
		if n != 0 || !errors.Is(err, context.Canceled) || ctx.polls < 7 {
			t.Fatal("count returned partial/ignored cancellation", deleted, n, err, ctx.polls)
		}
		view, _ := l.acquireView()
		if view.readerRefs[31].count.Load() != 0 {
			t.Fatal("cancellation leaked sharded view")
		}
		view.release()
	}
	if _, err := l.CountRangeAtContext(nil, math.MaxUint64, []byte("z"), []byte("a"), 0); err == nil {
		t.Fatal("backwards count accepted")
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := l.CountRangeAtContext(nil, math.MaxUint64, nil, nil, 0); err == nil {
		t.Fatal("closed count succeeded")
	}
	if _, err := l.CountRangeAtContext(nil, math.MaxUint64, []byte("a"), []byte("a"), 0); err == nil {
		t.Fatal("closed empty count succeeded")
	}
}

func TestCountRangeCorruptionNeverReturnsPartialCount(t *testing.T) {
	l := reusableTestTree(t, BlockCacheMmap)
	for i := 0; i < 400; i++ {
		if err := l.Put([]byte(fmt.Sprintf("r%04d", i)), bytes.Repeat([]byte{byte(i)}, 190)); err != nil {
			t.Fatal(err)
		}
	}
	if err := l.CompactRange(); err != nil {
		t.Fatal(err)
	}
	if n, err := l.CountRangeAtContext(nil, math.MaxUint64, nil, nil, 17); err != nil || n != 400 {
		t.Fatal("fixture count", n, err)
	}
	view, _ := l.acquireView()
	slot := view.files[0]
	r := slot.entry.reader
	handle := r.pointIndex[len(r.pointIndex)/2].handle
	view.release()
	path := filepath.Join(l.dir, fmt.Sprintf("%d.sst", slot.meta.Number))
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteAt([]byte{0xff}, int64(handle.Offset)); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	r.verifyChanges = true
	modified := time.Unix(0, r.modified).Add(time.Second)
	if err := os.Chtimes(path, modified, modified); err != nil {
		t.Fatal(err)
	}
	if n, err := l.CountRangeAtContext(nil, math.MaxUint64, nil, nil, 17); err == nil || n != 0 {
		t.Fatal("corrupt count returned partial/success", n, err)
	}
}

func TestCountRangeConcurrentPublicationAndPinnedSnapshot(t *testing.T) {
	l := reusableTestTree(t, BlockCacheHeap)
	for i := 0; i < 150; i++ {
		if err := l.Put([]byte(fmt.Sprintf("r%04d", i)), []byte("old")); err != nil {
			t.Fatal(err)
		}
	}
	if err := l.Flush(); err != nil {
		t.Fatal(err)
	}
	seq := l.SnapshotForReader(9)
	defer l.ReleaseSnapshotForReader(9, seq)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 150; i++ {
			if err := l.Delete([]byte(fmt.Sprintf("r%04d", i))); err != nil {
				t.Error(err)
				return
			}
			if i%30 == 29 {
				if err := l.Flush(); err != nil {
					t.Error(err)
					return
				}
			}
		}
		if err := l.CompactRange(); err != nil {
			t.Error(err)
		}
	}()
	for worker := 0; worker < 4; worker++ {
		wg.Add(1)
		go func(reader uint64) {
			defer wg.Done()
			for pass := 0; pass < 12; pass++ {
				if n, err := l.CountRangeAtContext(nil, seq, []byte("r"), []byte("s"), reader); err != nil || n != 150 {
					t.Errorf("snapshot count=%d error=%v", n, err)
					return
				}
			}
		}(uint64(worker + 32))
	}
	wg.Wait()
	if n, err := l.CountRangeAtContext(nil, math.MaxUint64, nil, nil, 9); err != nil || n != 0 {
		t.Fatal("deleted latest keys remain", n, err)
	}
}
