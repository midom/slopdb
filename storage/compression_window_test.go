package storage

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/klauspost/compress/zstd"
)

// Random quarters have no useful nearby matches. The repeated quarters require
// a 256 KiB back-reference across zstd's separate 128 KiB internal blocks.
func distantCompressionFixture(t *testing.T) []byte {
	t.Helper()
	quarter := make([]byte, 256<<10)
	if _, err := rand.New(rand.NewSource(731)).Read(quarter); err != nil {
		t.Fatal(err)
	}
	return bytes.Repeat(quarter, 4)
}

func TestSSTableZstdLargeWindowDistantMatches(t *testing.T) {
	value := distantCompressionFixture(t)
	options := DefaultOptions()
	options.PageSize = 1 << 20
	options.CompressionType = ZstdCompression
	path := filepath.Join(t.TempDir(), "large.sst")
	writer, err := NewSSTableWriter(path, options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = writer.Close() })
	if err = writer.Add(InternalKey{UserKey: []byte("key"), SeqNum: 1, OpType: kTypeValue}, value); err != nil {
		t.Fatal(err)
	}
	if _, err = writer.Finish(); err != nil {
		t.Fatal(err)
	}
	if err = writer.Close(); err != nil {
		t.Fatal(err)
	}
	reader, err := OpenSSTable(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	index := reader.indexBlock.NewIterator()
	if !index.Valid() {
		t.Fatal("missing data block")
	}
	offset, n := binary.Uvarint(index.Value())
	length, _ := binary.Uvarint(index.Value()[n:])
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	block := data[offset : offset+length]
	if block[len(block)-1] != kZstdCompression {
		t.Fatal("distant matches were not compressed")
	}
	payload := block[:len(block)-5]
	var header zstd.Header
	if err = header.Decode(payload); err != nil {
		t.Fatal(err)
	}
	// Single-segment frames advertise their effective window through the
	// content size, rather than the optional Window_Descriptor field.
	window := header.WindowSize
	if header.SingleSegment {
		window = header.FrameContentSize
	}
	if window < uint64(len(value)) {
		t.Fatalf("frame window %d cannot cover the 1 MiB page", window)
	}
	raw, err := reader.loadBlock(offset, length)
	if err != nil {
		t.Fatal(err)
	}
	limited, err := zstd.NewWriter(nil, zstd.WithEncoderConcurrency(1),
		zstd.WithEncoderLevel(zstd.SpeedDefault), zstd.WithWindowSize(64<<10))
	if err != nil {
		t.Fatal(err)
	}
	defer limited.Close()
	smallWindowBytes := len(limited.EncodeAll(raw, nil))
	if len(payload)*2 >= smallWindowBytes {
		t.Fatalf("large window encoded %d bytes vs 64 KiB window %d; distant matches were lost", len(payload), smallWindowBytes)
	}
	it := NewBlockReader(raw).NewIterator()
	if !it.Valid() || !bytes.Equal(it.Value(), value) {
		t.Fatal("large-window block did not round-trip")
	}
	corrupt := append([]byte(nil), block...)
	corrupt[20] ^= 1
	if _, err = reader.readRawBlock(corrupt); err == nil || !strings.Contains(err.Error(), "CRC mismatch") {
		t.Fatalf("compressed block corruption must fail before decoding: %v", err)
	}
	t.Logf("1 MiB distant-repeat page: %d bytes with large window, %d with 64 KiB window", len(payload), smallWindowBytes)
}

func TestSSTableZstdWindowMatchesBlockTarget(t *testing.T) {
	for _, tc := range []struct {
		name                 string
		page, legacy, window int64
	}{
		{"default", 0, 0, 4 << 10},
		{"512-byte-page", 512, 0, 1 << 10},
		{"16KiB", 16 << 10, 0, 16 << 10},
		{"64KiB", 64 << 10, 0, 64 << 10},
		{"1MiB", 1 << 20, 0, 1 << 20},
		{"2MiB", 2 << 20, 0, 2 << 20},
		{"legacy-tiny", 0, 23, 1 << 10},
		{"legacy-odd", 0, 3000, 4 << 10},
	} {
		t.Run(tc.name, func(t *testing.T) {
			options := DefaultOptions()
			options.CompressionType = ZstdCompression
			options.PageSize, options.BlockSize = tc.page, int(tc.legacy)
			path := filepath.Join(t.TempDir(), "window.sst")
			writer, err := NewSSTableWriter(path, options)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = writer.Close() })
			// An oversized row forces an explicit frame descriptor, demonstrating
			// that SSTable framing overhead cannot enlarge the configured window.
			pattern := []byte("window validation")
			value := bytes.Repeat(pattern, int(tc.window)/len(pattern)+2)
			if err := writer.Add(InternalKey{UserKey: []byte("key"), SeqNum: 1, OpType: kTypeValue}, value); err != nil {
				t.Fatal(err)
			}
			if _, err := writer.Finish(); err != nil {
				t.Fatal(err)
			}
			if err := writer.Close(); err != nil {
				t.Fatal(err)
			}
			reader, err := OpenSSTable(path)
			if err != nil {
				t.Fatal(err)
			}
			defer reader.Close()
			index := reader.indexBlock.NewIterator()
			if !index.Valid() {
				t.Fatal("missing data block")
			}
			offset, n := binary.Uvarint(index.Value())
			length, _ := binary.Uvarint(index.Value()[n:])
			data, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			block := data[offset : offset+length]
			if block[len(block)-1] != kZstdCompression {
				t.Fatal("expected compressed page")
			}
			var header zstd.Header
			if err := header.Decode(block[:len(block)-5]); err != nil {
				t.Fatal(err)
			}
			if header.SingleSegment || header.WindowSize != uint64(tc.window) {
				t.Fatalf("window does not match target: single=%v window=%d, want %d", header.SingleSegment, header.WindowSize, tc.window)
			}
			raw, err := reader.loadBlock(offset, length)
			if err != nil {
				t.Fatal(err)
			}
			it := NewBlockReader(raw).NewIterator()
			if !it.Valid() || !bytes.Equal(it.Value(), value) {
				t.Fatal("oversized page did not round-trip")
			}
		})
	}
}

func TestSSTableZstdWindowLimits(t *testing.T) {
	for _, tc := range []struct{ target, window int64 }{
		{0, DefaultPageSize}, {1, 1024}, {512, 1024}, {1024, 1024},
		{1025, 2048}, {16 << 10, 16 << 10}, {1 << 20, 1 << 20},
		{MaxPageSize - 1, MaxPageSize}, {MaxPageSize, MaxPageSize},
	} {
		if got := SSTableZstdWindowSizeForBlock(tc.target); got != tc.window {
			t.Errorf("target %d: window %d, want %d", tc.target, got, tc.window)
		}
	}
}

func TestSSTableZstdLegacyWindowsReadable(t *testing.T) {
	for _, window := range []int{64 << 10, 8 << 20} {
		t.Run(fmt.Sprint(window), func(t *testing.T) {
			legacy, err := zstd.NewWriter(nil, zstd.WithEncoderConcurrency(1), zstd.WithWindowSize(window))
			if err != nil {
				t.Fatal(err)
			}
			defer legacy.Close()
			raw := distantCompressionFixture(t)
			payload := legacy.EncodeAll(raw, nil)
			block := append(payload, make([]byte, 5)...)
			binary.LittleEndian.PutUint32(block[len(payload):], crc32.ChecksumIEEE(payload))
			block[len(block)-1] = kZstdCompression
			reader := new(SSTableReader)
			defer func() {
				if reader.zstdDecoder != nil {
					reader.zstdDecoder.Close()
				}
			}()
			decoded, err := reader.readRawBlock(block)
			if err != nil || !bytes.Equal(decoded, raw) {
				t.Fatalf("legacy frame did not round-trip: %v", err)
			}
		})
	}
}
