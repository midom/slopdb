package storage

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math/rand"
	"path/filepath"
	"sort"
	"testing"
)

func TestPointIndexLeadingBytesPreserveCompleteKeyOrder(t *testing.T) {
	rng := rand.New(rand.NewSource(275))
	for _, prefix := range [][]byte{nil, bytes.Repeat([]byte{0}, 12), []byte("common:prefix:")} {
		keys := [][]byte{append([]byte(nil), prefix...)}
		for n := 0; n < 500; n++ {
			suffix := make([]byte, rng.Intn(24))
			for j := range suffix {
				// Deliberate NUL padding collisions and duplicate separators.
				suffix[j] = byte(rng.Intn(4))
			}
			keys = append(keys, append(append([]byte(nil), prefix...), suffix...))
		}
		sort.Slice(keys, func(i, j int) bool { return bytes.Compare(keys[i], keys[j]) < 0 })
		builder := NewBlockBuilder(4096)
		for _, key := range keys {
			builder.Add(key, EncodeBlockHandle(BlockHandle{Offset: 0, Size: 5}))
		}
		data := builder.Finish()
		r := &SSTableReader{indexData: data[:len(data)-4], fileSize: 5}
		r.indexBlock = NewBlockReader(r.indexData)
		if err := r.buildPointIndex(); err != nil {
			t.Fatal(err)
		}
		for _, key := range keys {
			for end := 0; end <= len(key); end++ {
				for _, target := range [][]byte{key[:end], append(append([]byte(nil), key[:end]...), 0)} {
					want := sort.Search(len(keys), func(i int) bool { return bytes.Compare(keys[i], target) >= 0 })
					if got := r.pointIndexSeek(target); got != want {
						t.Fatalf("prefix=%x target=%x: %d, want %d", prefix, target, got, want)
					}
				}
			}
		}
	}
}

func TestPointIndexRejectsMalformedHandlesAndOrder(t *testing.T) {
	for _, value := range [][]byte{nil, {128}, binary.AppendUvarint(nil, 100), EncodeBlockHandle(BlockHandle{Offset: 99, Size: 2})} {
		builder := NewBlockBuilder(512)
		builder.Add([]byte("key"), value)
		data := builder.Finish()
		r := &SSTableReader{indexData: data[:len(data)-4], fileSize: 100}
		r.indexBlock = NewBlockReader(r.indexData)
		if err := r.buildPointIndex(); err == nil {
			t.Fatalf("invalid block handle accepted: %x", value)
		}
	}
	builder := NewBlockBuilder(512)
	for _, key := range []string{"z", "a"} {
		builder.Add([]byte(key), EncodeBlockHandle(BlockHandle{Offset: 0, Size: 5}))
	}
	data := builder.Finish()
	r := &SSTableReader{indexData: data[:len(data)-4], fileSize: 5}
	r.indexBlock = NewBlockReader(r.indexData)
	if err := r.buildPointIndex(); err == nil {
		t.Fatal("unordered index accepted")
	}
}

func TestDecodedPointIndexVersionsAcrossPagesAndReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "versions.sst")
	opt := DefaultOptions()
	opt.PageSize = 512
	opt.CompressionType = ZstdCompression
	w, err := NewSSTableWriter(path, opt)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = w.Close() })
	for _, key := range []string{"a", "b", "c"} {
		for seq := uint64(80); seq > 0; seq-- {
			value := bytes.Repeat([]byte(fmt.Sprintf("%s:%02d;", key, seq)), 100)
			if err := w.Add(InternalKey{UserKey: []byte(key), SeqNum: seq, OpType: kTypeValue}, value); err != nil {
				t.Fatal(err)
			}
		}
	}
	if _, err := w.Finish(); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	for reopen := 0; reopen < 2; reopen++ {
		r, err := OpenSSTable(path)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = r.Close() })
		if len(r.pointIndex) < 100 {
			t.Fatal("fixture did not span duplicate index separators")
		}
		for _, key := range []string{"a", "b", "c"} {
			for _, snapshot := range []uint64{100, 80, 79, 40, 17, 1, 0} {
				value, found, err := r.GetAt([]byte(key), snapshot)
				if err != nil || found != (snapshot > 0) {
					t.Fatalf("%s/%d: found=%v err=%v", key, snapshot, found, err)
				}
				if found {
					seq := snapshot
					if seq > 80 {
						seq = 80
					}
					want := bytes.Repeat([]byte(fmt.Sprintf("%s:%02d;", key, seq)), 100)
					if !bytes.Equal(value, want) {
						t.Fatalf("wrong version for %s/%d", key, snapshot)
					}
					value[0] = 'x'
				}
			}
		}
		for _, key := range []string{"", "aa", "bb", "d"} {
			if _, found, err := r.Get([]byte(key)); found || err != nil {
				t.Fatalf("missing key %q: %v %v", key, found, err)
			}
		}
		if err := r.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestReaderIntoBufferOwnershipSnapshotsAndMissing(t *testing.T) {
	l := openReaderViewTest(t)
	key := []byte("key")
	if err := l.Put(key, []byte("old value")); err != nil {
		t.Fatal(err)
	}
	if err := l.Flush(); err != nil {
		t.Fatal(err)
	}
	seq := l.SnapshotForReader(7)
	defer l.ReleaseSnapshotForReader(7, seq)
	if err := l.Put(key, []byte("new value")); err != nil {
		t.Fatal(err)
	}
	dst := make([]byte, 0, 64)
	value, found, err := l.GetAtForReaderInto(key, seq, 7, dst)
	if err != nil || !found || string(value) != "old value" || &value[0] != &dst[:cap(dst)][0] {
		t.Fatalf("buffer/snapshot: %q %v %v", value, found, err)
	}
	value[0] = 'x'
	value, found, err = l.GetAtForReaderInto(key, seq, 7, value)
	if err != nil || !found || string(value) != "old value" {
		t.Fatalf("caller mutated stored value: %q %v %v", value, found, err)
	}
	value, found, err = l.GetAtForReaderInto([]byte("missing"), seq, 7, value)
	if err != nil || found || len(value) != 0 || cap(value) != cap(dst) {
		t.Fatalf("missing lost reusable buffer: %q %v %v", value, found, err)
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	if _, _, err := l.GetAtForReaderInto(key, seq, 7, dst); err == nil {
		t.Fatal("closed tree accepted buffered read")
	}
}
