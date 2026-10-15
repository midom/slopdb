package storage

import (
	"bytes"
	"sync"
	"testing"

	"github.com/klauspost/compress/zstd"
)

func TestZstdBlockDecoderReuseConcurrentOwnershipAndErrors(t *testing.T) {
	encoder, err := zstd.NewWriter(nil, zstd.WithEncoderConcurrency(1), zstd.WithWindowSize(1<<20))
	if err != nil {
		t.Fatal(err)
	}
	defer encoder.Close()
	var raw, encoded [][]byte
	pattern := []byte("independently decoded immutable page ")
	for _, size := range []int{512, 4096, 256 << 10, 1 << 20} {
		data := bytes.Repeat(pattern, size/len(pattern)+1)[:size]
		raw = append(raw, data)
		encoded = append(encoded, encoder.EncodeAll(data, nil))
	}
	var wg sync.WaitGroup
	for worker := 0; worker < 100; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			var retained []byte
			for n := 0; n < 12; n++ {
				i := (n + worker) % len(raw)
				data, e := decodeZstdBlock(encoded[i])
				if e != nil {
					t.Error(e)
					return
				}
				if !bytes.Equal(data, raw[i]) {
					t.Error("wrong decoded block")
					return
				}
				if retained != nil && retained[0] != 255 {
					t.Error("decoder reused a caller-owned output")
					return
				}
				data[0] = 255
				retained = data
				if _, e = decodeZstdBlock([]byte("invalid frame")); e == nil {
					t.Error("invalid frame accepted")
					return
				}
			}
		}(worker)
	}
	wg.Wait()
	for i := range encoded {
		data, err := decodeZstdBlock(encoded[i])
		if err != nil || !bytes.Equal(data, raw[i]) {
			t.Fatalf("decoder poisoned by earlier error: %v", err)
		}
	}
}
