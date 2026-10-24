package storage

import (
	"bytes"
	"fmt"
	"math/rand"
	"path/filepath"
	"testing"
)

func TestSSTableWriterReusesBuffersWithoutAliasing(t *testing.T) {
	for _, codec := range []CompressionType{NoCompression, GzipCompression, ZstdCompression, SnappyCompression, LZ4Compression} {
		for _, page := range []int{512, 4096} {
			t.Run(fmt.Sprintf("%s/%d", codec, page), func(t *testing.T) {
				opt := DefaultOptions()
				opt.CompressionType, opt.BlockSize = codec, page
				path := filepath.Join(t.TempDir(), "pages.sst")
				writer, err := NewSSTableWriter(path, opt)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = writer.Close() })
				const count = 256
				keys, values := make([][]byte, count), make([][]byte, count)
				rng := rand.New(rand.NewSource(741))
				var key, value []byte
				for i := 0; i < count; i++ {
					key = append(key[:0], fmt.Sprintf("key:%08d", i)...)
					size := []int{17, 513, 8192, 2048}[i%4]
					if cap(value) < size {
						value = make([]byte, size)
					} else {
						value = value[:size]
					}
					if i%3 == 0 {
						_, _ = rng.Read(value)
					} else {
						for j := range value {
							value[j] = byte(i)
						}
					}
					keys[i], values[i] = bytes.Clone(key), bytes.Clone(value)
					if err = writer.Add(InternalKey{UserKey: key, SeqNum: uint64(i + 1), OpType: kTypeValue}, value); err != nil {
						t.Fatal(err)
					}
					// Callers own these buffers and may reuse them immediately after Add.
					for j := range key {
						key[j] = 0xff
					}
					for j := range value {
						value[j] = 0xff
					}
				}
				meta, err := writer.Finish()
				if err != nil {
					t.Fatal(err)
				}
				if !bytes.Equal(meta.Smallest.UserKey, keys[0]) || !bytes.Equal(meta.Largest.UserKey, keys[count-1]) {
					t.Fatalf("aliased key bounds: %+v", meta)
				}
				if err = writer.Close(); err != nil {
					t.Fatal(err)
				}
				reader, err := OpenSSTable(path)
				if err != nil {
					t.Fatal(err)
				}
				defer reader.Close()
				it := reader.NewIterator()
				defer it.Close()
				for i := 0; i < count; i++ {
					if !it.Valid() || !bytes.Equal(it.Key().UserKey, keys[i]) || !bytes.Equal(it.Value(), values[i]) {
						t.Fatalf("row %d differs: %v", i, it.Err())
					}
					it.Next()
					got, found, err := reader.Get(keys[i])
					if err != nil || !found || !bytes.Equal(got, values[i]) {
						t.Fatalf("point read %d: found=%v err=%v", i, found, err)
					}
				}
				if it.Valid() || it.Err() != nil {
					t.Fatalf("iterator tail: %v", it.Err())
				}
			})
		}
	}
}

func TestBloomPrehashedEncodingCompatibility(t *testing.T) {
	keys := [][]byte{nil, {}, []byte("alpha"), []byte("beta"), []byte("alpha"), {0, 255, 1}}
	hashes := make([]uint32, len(keys))
	for i, key := range keys {
		hashes[i] = bloomKeyHash(key)
	}
	// Existing rotated-delta FNV-1a format; duplicate and empty keys count toward
	// the bit-array length. Changing representation must preserve these bytes.
	expected := []byte{0xb2, 0x89, 0x48, 0x00, 0x02, 0x88, 0x29, 0xa4, 0x86}
	filter := NewBloomFilter(10)
	direct := filter.Generate(keys)
	if !bytes.Equal(direct, expected) {
		t.Fatalf("filter encoding changed: %x", direct)
	}
	if got := filter.generateHashes(hashes); !bytes.Equal(got, expected) {
		t.Fatalf("prehashed encoding changed: %x", got)
	}
	for _, key := range keys {
		if !filter.MayContain(expected, key) {
			t.Fatalf("false negative for %x", key)
		}
	}
}
