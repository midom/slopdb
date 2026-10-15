package storage

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/klauspost/compress/zstd"
)

func TestCompressedReadBuffersMixedBlocksOwnershipAndErrors(t *testing.T) {
	encoder, err := zstd.NewWriter(nil, zstd.WithEncoderConcurrency(1))
	if err != nil {
		t.Fatal(err)
	}
	defer encoder.Close()
	values := [][]byte{bytes.Repeat([]byte("compressed"), 100), bytes.Repeat([]byte("raw page"), 100)}
	var blocks [][]byte
	for i, value := range values {
		builder := NewBlockBuilder(512)
		builder.Add(EncodeInternalKey(InternalKey{UserKey: []byte{byte('a' + i)}, SeqNum: 1, OpType: kTypeValue}), value)
		raw := builder.Finish()
		raw = raw[:len(raw)-4]
		codec := byte(kNoCompression)
		if i == 0 {
			raw = encoder.EncodeAll(raw, nil)
			codec = kZstdCompression
		}
		block := append(append([]byte(nil), raw...), make([]byte, 5)...)
		binary.LittleEndian.PutUint32(block[len(raw):], crc32.ChecksumIEEE(raw))
		block[len(block)-1] = codec
		blocks = append(blocks, block)
	}
	path := filepath.Join(t.TempDir(), "mixed-blocks")
	if err = os.WriteFile(path, append(append([]byte(nil), blocks[0]...), blocks[1]...), 0644); err != nil {
		t.Fatal(err)
	}
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	r := &SSTableReader{file: f, fileSize: uint64(len(blocks[0]) + len(blocks[1]))}
	if _, err = r.loadBlockReaderMode(0, uint64(len(blocks[0])), false); err != nil {
		t.Fatal(err)
	}
	if !r.hasCompressedBlocks.Load() {
		t.Fatal("compressed codec hint missing")
	}
	raw, err := r.loadBlockReaderMode(uint64(len(blocks[0])), uint64(len(blocks[1])), false)
	if err != nil {
		t.Fatal(err)
	}
	retained := append([]byte(nil), raw.data...)
	var wg sync.WaitGroup
	for worker := 0; worker < 100; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for n := 0; n < 20; n++ {
				i := (worker + n) % 2
				offset := uint64(0)
				if i == 1 {
					offset = uint64(len(blocks[0]))
				}
				block, e := r.loadBlockReaderMode(offset, uint64(len(blocks[i])), false)
				if e != nil {
					t.Error(e)
					return
				}
				it := block.NewIterator()
				if !it.Valid() || !bytes.Equal(it.Value(), values[i]) {
					t.Error("pooled scratch changed decoded contents")
					return
				}
			}
		}(worker)
	}
	wg.Wait()
	if !bytes.Equal(raw.data, retained) {
		t.Fatal("raw page retained pooled scratch")
	}
	if _, err = r.loadBlockReaderMode(r.fileSize+1, 10, false); err == nil {
		t.Fatal("short read accepted")
	}
	corrupt := append([]byte(nil), blocks[0]...)
	corrupt[0] ^= 1
	if err = os.WriteFile(path, append(corrupt, blocks[1]...), 0644); err != nil {
		t.Fatal(err)
	}
	if _, err = r.loadBlockReaderMode(0, uint64(len(blocks[0])), false); err == nil {
		t.Fatal("checksum corruption accepted")
	}
	if _, err = r.loadBlockReaderMode(uint64(len(blocks[0])), uint64(len(blocks[1])), false); err != nil {
		t.Fatalf("error poisoned scratch reuse: %v", err)
	}
}
