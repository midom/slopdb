package storage

import (
	"bytes"
	"encoding/binary"
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

func TestSSTableZstdExplicitMaximumWindow(t *testing.T) {
	options := DefaultOptions()
	options.CompressionType = ZstdCompression
	writer, err := NewSSTableWriter(filepath.Join(t.TempDir(), "window.sst"), options)
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close()
	// Above the configured maximum, the frame carries an explicit descriptor.
	// This verifies the actual encoder used by SSTableWriter, not just a constant.
	raw := bytes.Repeat([]byte("window validation"), (SSTableZstdWindowSize/(len("window validation")))+1)
	payload := writer.zstdEncoder.EncodeAll(raw, nil)
	var header zstd.Header
	if err = header.Decode(payload); err != nil {
		t.Fatal(err)
	}
	if header.SingleSegment || header.WindowSize != SSTableZstdWindowSize {
		t.Fatalf("unexpected maximum window: single=%v window=%d", header.SingleSegment, header.WindowSize)
	}
	decoder, err := zstd.NewReader(nil, zstd.WithDecoderConcurrency(1))
	if err != nil {
		t.Fatal(err)
	}
	defer decoder.Close()
	decoded, err := decoder.DecodeAll(payload, nil)
	if err != nil || !bytes.Equal(decoded, raw) {
		t.Fatalf("maximum-window frame did not round-trip: %v", err)
	}
}

func TestSSTableZstdLegacySmallWindowReadable(t *testing.T) {
	legacy, err := zstd.NewWriter(nil, zstd.WithEncoderConcurrency(1), zstd.WithWindowSize(64<<10))
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
		t.Fatalf("old small-window frame did not round-trip: %v", err)
	}
}
