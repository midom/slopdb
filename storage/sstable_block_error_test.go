package storage

import (
	"encoding/binary"
	"hash/crc32"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/klauspost/compress/snappy"
	"github.com/klauspost/compress/zstd"
)

func TestLoadBlockReaderPreservesValidationError(t *testing.T) {
	encoder, err := zstd.NewWriter(nil, zstd.WithEncoderConcurrency(1))
	if err != nil {
		t.Fatal(err)
	}
	defer encoder.Close()
	for _, tc := range []struct {
		name string
		data []byte
		want string
	}{
		{"short", []byte{0}, "block too short"},
		{"restart-count", []byte{1, 0, 0, 0}, "invalid restart array"},
	} {
		for _, codec := range []byte{kNoCompression, kSnappyCompression, kZstdCompression} {
			t.Run(tc.name+"/"+string(rune('0'+codec)), func(t *testing.T) {
				payload := tc.data
				switch codec {
				case kSnappyCompression:
					payload = snappy.Encode(nil, payload)
				case kZstdCompression:
					payload = encoder.EncodeAll(payload, nil)
				}
				block := append(append([]byte(nil), payload...), make([]byte, 5)...)
				binary.LittleEndian.PutUint32(block[len(payload):], crc32.ChecksumIEEE(payload))
				block[len(block)-1] = codec
				path := filepath.Join(t.TempDir(), "invalid-block")
				if err := os.WriteFile(path, block, 0600); err != nil {
					t.Fatal(err)
				}
				f, err := os.Open(path)
				if err != nil {
					t.Fatal(err)
				}
				defer f.Close()
				r := &SSTableReader{file: f, fileSize: uint64(len(block))}
				reader, err := r.loadBlockReaderMode(0, uint64(len(block)), false)
				if reader != nil {
					reader.release()
					t.Fatal("invalid decoded block returned a reader")
				}
				if err == nil || !strings.Contains(err.Error(), tc.want) {
					t.Fatalf("validation error lost after reader release: got %v, want %q", err, tc.want)
				}
			})
		}
	}
}
