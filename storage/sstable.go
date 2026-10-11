package storage

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"runtime"
	"sync"

	"github.com/klauspost/compress/snappy"
	"github.com/klauspost/compress/zstd"
	"github.com/pierrec/lz4/v4"
)

const (
	kTableMagicNumber uint64 = 0xdb4775248b80fb57
	kNoCompression           = 0
	// Stable on-disk codec tags; do not renumber.
	kGzipCompression   = 1
	kZstdCompression   = 2
	kSnappyCompression = 3
	kLZ4Compression    = 4
)

type FileMetaData struct {
	Number          uint64
	FileSize        uint64
	Smallest        InternalKey
	Largest         InternalKey
	AllowedSeeks    int32
	hasRangeDeletes bool // Runtime metadata; MANIFEST encoding stays unchanged.
}

type BlockHandle struct {
	Offset uint64
	Size   uint64
}

func EncodeBlockHandle(h BlockHandle) []byte {
	buf := make([]byte, binary.MaxVarintLen64*2)
	n := binary.PutUvarint(buf, h.Offset)
	n += binary.PutUvarint(buf[n:], h.Size)
	return buf[:n]
}

type SSTableWriter struct {
	file            *os.File
	writer          *bufio.Writer
	offset          uint64
	opt             *Options
	dataBlocks      []BlockHandle
	indexBlock      *BlockBuilder
	keys            [][]byte // for bloom filter
	prevKey         []byte
	curBlock        *BlockBuilder
	compression     CompressionType
	zstdEncoder     *zstd.Encoder
	lz4Encoder      *lz4.Writer
	hasRangeDeletes bool
	maxSeq          uint64
	firstKey        InternalKey
	lastKey         InternalKey
	rangeDeletes    []KeyValue
}

// NewSSTableWriter uses L0 compression for auto; the LSM resolves the destination
// level before constructing writers for compaction output.
func NewSSTableWriter(path string, opt *Options) (*SSTableWriter, error) {
	if opt == nil {
		opt = DefaultOptions()
	}
	options := *opt
	compression, err := options.compressionType(0)
	if err != nil {
		return nil, err
	}
	var encoder *zstd.Encoder
	if compression == ZstdCompression {
		encoder, err = zstd.NewWriter(nil, zstd.WithEncoderConcurrency(1), zstd.WithEncoderLevel(zstd.SpeedDefault))
		if err != nil {
			return nil, fmt.Errorf("zstd encoder: %w", err)
		}
	}
	var lz4Encoder *lz4.Writer
	if compression == LZ4Compression {
		lz4Encoder = lz4.NewWriter(nil)
		if err := lz4Encoder.Apply(lz4.BlockSizeOption(lz4.Block64Kb), lz4.ConcurrencyOption(1)); err != nil {
			return nil, fmt.Errorf("lz4 encoder: %w", err)
		}
	}
	f, err := os.Create(path)
	if err != nil {
		if encoder != nil {
			encoder.Close()
		}
		return nil, err
	}
	return &SSTableWriter{
		file:        f,
		writer:      bufio.NewWriterSize(f, 64*1024),
		opt:         &options,
		indexBlock:  NewBlockBuilder(options.BlockSize),
		compression: compression,
		zstdEncoder: encoder,
		lz4Encoder:  lz4Encoder,
	}, nil
}

func (w *SSTableWriter) Add(key InternalKey, value []byte) error {
	ownedKey := key
	ownedKey.UserKey = append([]byte(nil), key.UserKey...)
	if len(w.keys) == 0 {
		w.firstKey = ownedKey
	}
	w.lastKey = ownedKey
	if key.SeqNum > w.maxSeq {
		w.maxSeq = key.SeqNum
	}
	if key.OpType == kTypeRangeDelete {
		w.rangeDeletes = append(w.rangeDeletes, KeyValue{Key: ownedKey, Value: append([]byte(nil), value...)})
	}
	w.hasRangeDeletes = w.hasRangeDeletes || key.OpType == kTypeRangeDelete
	if w.curBlock == nil {
		w.curBlock = NewBlockBuilder(w.opt.BlockSize)
	}

	ek := EncodeInternalKey(key)
	w.curBlock.Add(ek, value)
	w.keys = append(w.keys, ownedKey.UserKey)
	w.prevKey = append(w.prevKey[:0], key.UserKey...)

	if len(w.curBlock.buf) >= w.opt.BlockSize {
		return w.flushBlock()
	}
	return nil
}

func (w *SSTableWriter) flushBlock() error {
	if w.curBlock == nil || w.curBlock.Empty() {
		return nil
	}

	rawBlock := w.curBlock.Finish()
	if len(rawBlock) >= 4 {
		rawBlock = rawBlock[:len(rawBlock)-4]
	}

	blockData := rawBlock
	compressionType := byte(kNoCompression)

	switch w.compression {
	case GzipCompression:
		var compressed bytes.Buffer
		gw, err := gzip.NewWriterLevel(&compressed, gzip.BestCompression)
		if err != nil {
			return err
		}
		if _, err := gw.Write(rawBlock); err != nil {
			gw.Close()
			return err
		}
		if err := gw.Close(); err != nil {
			return err
		}
		if compressed.Len() < len(rawBlock) {
			blockData = compressed.Bytes()
			compressionType = kGzipCompression
		}
	case ZstdCompression:
		compressed := w.zstdEncoder.EncodeAll(rawBlock, nil)
		if len(compressed) < len(rawBlock) {
			blockData = compressed
			compressionType = kZstdCompression
		}
	case SnappyCompression:
		compressed := snappy.Encode(nil, rawBlock)
		if len(compressed) < len(rawBlock) {
			blockData = compressed
			compressionType = kSnappyCompression
		}
	case LZ4Compression:
		var compressed bytes.Buffer
		w.lz4Encoder.Reset(&compressed)
		if _, err := w.lz4Encoder.Write(rawBlock); err != nil {
			w.lz4Encoder.Reset(nil)
			return fmt.Errorf("lz4 compress: %w", err)
		}
		if err := w.lz4Encoder.Close(); err != nil {
			return fmt.Errorf("lz4 compress: %w", err)
		}
		if compressed.Len() < len(rawBlock) {
			blockData = compressed.Bytes()
			compressionType = kLZ4Compression
		}
	}

	crc := crc32.ChecksumIEEE(blockData)
	dataWithChecksum := make([]byte, len(blockData)+5)
	copy(dataWithChecksum, blockData)
	binary.LittleEndian.PutUint32(dataWithChecksum[len(blockData):], crc)
	dataWithChecksum[len(blockData)+4] = compressionType

	handle := BlockHandle{Offset: w.offset, Size: uint64(len(dataWithChecksum))}

	if _, err := w.writer.Write(dataWithChecksum); err != nil {
		return err
	}
	w.offset += uint64(len(dataWithChecksum))

	w.dataBlocks = append(w.dataBlocks, handle)

	sepKey := append([]byte{}, w.prevKey...)
	// Store the actual last user key; adding a suffix can route a seek
	// into the preceding block and miss the next block.
	w.indexBlock.Add(sepKey, EncodeBlockHandle(handle))

	w.curBlock = NewBlockBuilder(w.opt.BlockSize)
	return nil
}

func (w *SSTableWriter) Finish() (FileMetaData, error) {
	if err := w.flushBlock(); err != nil {
		return FileMetaData{}, err
	}

	bf := NewBloomFilter(w.opt.BloomBitsKey)
	filterBlock := bf.Generate(w.keys)

	filterHandle := BlockHandle{Offset: w.offset, Size: uint64(len(filterBlock))}
	if _, err := w.writer.Write(filterBlock); err != nil {
		return FileMetaData{}, err
	}
	w.offset += uint64(len(filterBlock))

	indexData := w.indexBlock.Finish()
	indexHandle := BlockHandle{Offset: w.offset, Size: uint64(len(indexData))}
	if _, err := w.writer.Write(indexData); err != nil {
		return FileMetaData{}, err
	}
	w.offset += uint64(len(indexData))
	metadata := encodeTableMetadata(w.maxSeq, crc32.ChecksumIEEE(filterBlock), w.firstKey, w.lastKey, w.rangeDeletes)
	if _, err := w.writer.Write(metadata); err != nil {
		return FileMetaData{}, err
	}
	w.offset += uint64(len(metadata))

	footer := make([]byte, 48)
	n1 := EncodeBlockHandle(indexHandle)
	n2 := EncodeBlockHandle(filterHandle)
	copy(footer[:20], make([]byte, 20))
	copy(footer[:len(n1)], n1)
	copy(footer[20:40], make([]byte, 20))
	copy(footer[20:20+len(n2)], n2)
	binary.LittleEndian.PutUint64(footer[40:], kTableMagicNumber)

	if _, err := w.writer.Write(footer); err != nil {
		return FileMetaData{}, err
	}
	w.offset += 48

	if err := w.writer.Flush(); err != nil {
		return FileMetaData{}, err
	}
	if err := w.file.Sync(); err != nil {
		return FileMetaData{}, err
	}

	fileInfo, err := w.file.Stat()
	if err != nil {
		return FileMetaData{}, err
	}

	return FileMetaData{
		hasRangeDeletes: w.hasRangeDeletes,
		FileSize:        uint64(fileInfo.Size()),
		Smallest:        w.firstKey,
		Largest:         w.lastKey,
	}, nil
}

func (w *SSTableWriter) Close() error {
	if w.lz4Encoder != nil {
		w.lz4Encoder.Reset(nil)
	}
	if w.zstdEncoder != nil {
		return errors.Join(w.zstdEncoder.Close(), w.file.Close())
	}
	return w.file.Close()
}

type SSTableReader struct {
	file          *os.File
	fileSize      uint64
	indexHandle   BlockHandle
	indexData     []byte
	indexBlock    *BlockReader
	filterData    []byte
	meta          FileMetaData
	maxSeq        uint64
	rangeDeletes  []KeyValue
	zstdOnce      sync.Once
	zstdDecoder   *zstd.Decoder
	zstdErr       error
	blocks        *blockCache
	cacheID       uint64
	modified      int64
	verifyChanges bool
}

func (r *SSTableReader) loadBlock(offset, size uint64) ([]byte, error) {
	return r.loadBlockMode(offset, size, true)
}

func (r *SSTableReader) loadBlockMode(offset, size uint64, fillCache bool) ([]byte, error) {
	reader, err := r.loadBlockReaderMode(offset, size, fillCache)
	if err != nil {
		return nil, err
	}
	return reader.data, nil
}

func (r *SSTableReader) loadBlockReaderMode(offset, size uint64, fillCache bool) (*BlockReader, error) {
	key := blockCacheKey{table: r.cacheID, offset: offset, size: size, modified: r.modified}
	cacheable := r.blocks != nil
	if cacheable && r.verifyChanges {
		// Tables are immutable. If an external writer changes one, bypass stale
		// blocks so checksum errors remain visible instead of hiding corruption.
		info, err := r.file.Stat()
		if err != nil {
			return nil, err
		}
		cacheable = uint64(info.Size()) == r.fileSize && info.ModTime().UnixNano() == r.modified
	}
	if cacheable {
		if reader, ok := r.blocks.lookupReader(key, fillCache); ok {
			return reader, nil
		}
	}
	data := make([]byte, size)
	if _, err := r.file.ReadAt(data, int64(offset)); err != nil {
		return nil, err
	}
	block, err := r.readRawBlock(data)
	if err != nil {
		return nil, err
	}
	reader := NewBlockReader(block)
	if reader.err != nil {
		return nil, reader.err
	}
	if cacheable && fillCache {
		r.blocks.addReader(key, reader)
	}
	return reader, nil
}

func (r *SSTableReader) readRawBlock(data []byte) ([]byte, error) {
	if len(data) < 5 {
		return nil, fmt.Errorf("block too short")
	}
	compressionType := data[len(data)-1]
	storedCRC := binary.LittleEndian.Uint32(data[len(data)-5 : len(data)-1])
	payload := data[:len(data)-5]

	calcCRC := crc32.ChecksumIEEE(payload)
	if storedCRC != calcCRC {
		return nil, fmt.Errorf("block CRC mismatch")
	}

	if compressionType == kNoCompression {
		return payload, nil
	}
	if compressionType == kSnappyCompression {
		decoded, err := snappy.Decode(nil, payload)
		if err != nil {
			return nil, fmt.Errorf("snappy decompress: %w", err)
		}
		return decoded, nil
	}
	if compressionType == kLZ4Compression {
		decoded, err := io.ReadAll(lz4.NewReader(bytes.NewReader(payload)))
		if err != nil {
			return nil, fmt.Errorf("lz4 decompress: %w", err)
		}
		return decoded, nil
	}
	if compressionType == kZstdCompression {
		// Reuse a decoder per table. DecodeAll is safe for concurrent readers.
		r.zstdOnce.Do(func() {
			r.zstdDecoder, r.zstdErr = zstd.NewReader(nil, zstd.WithDecoderConcurrency(runtime.GOMAXPROCS(0)))
		})
		if r.zstdErr != nil {
			return nil, fmt.Errorf("zstd decoder: %w", r.zstdErr)
		}
		decoded, err := r.zstdDecoder.DecodeAll(payload, nil)
		if err != nil {
			return nil, fmt.Errorf("zstd decompress: %w", err)
		}
		return decoded, nil
	}

	if compressionType != kGzipCompression {
		return nil, fmt.Errorf("unknown compression type: %d", compressionType)
	}
	gr, err := gzip.NewReader(bytes.NewReader(payload))
	if err != nil {
		return nil, fmt.Errorf("gzip decompress: %w", err)
	}
	decompressed, err := io.ReadAll(gr)
	gr.Close()
	if err != nil {
		return nil, fmt.Errorf("gzip read: %w", err)
	}
	return decompressed, nil
}

func OpenSSTable(path string) (*SSTableReader, error) {
	return openSSTable(path, nil, 0)
}

func openSSTable(path string, blocks *blockCache, number uint64) (*SSTableReader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	info, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	fileSize := uint64(info.Size())

	if fileSize < 48 {
		f.Close()
		return nil, fmt.Errorf("file too small")
	}

	footer := make([]byte, 48)
	if _, err := f.ReadAt(footer, int64(fileSize-48)); err != nil {
		f.Close()
		return nil, err
	}

	magic := binary.LittleEndian.Uint64(footer[40:])
	if magic != kTableMagicNumber {
		f.Close()
		return nil, fmt.Errorf("bad magic number")
	}

	decodeHandle := func(data []byte) (BlockHandle, error) {
		off, n := binary.Uvarint(data)
		if n <= 0 {
			return BlockHandle{}, fmt.Errorf("invalid block offset")
		}
		size, n := binary.Uvarint(data[n:])
		if n <= 0 || off > fileSize-48 || size > fileSize-48-off {
			return BlockHandle{}, fmt.Errorf("invalid block handle")
		}
		return BlockHandle{Offset: off, Size: size}, nil
	}
	idxHandle, err := decodeHandle(footer[:20])
	if err != nil {
		f.Close()
		return nil, err
	}
	filtHandle, err := decodeHandle(footer[20:40])
	if err != nil {
		f.Close()
		return nil, err
	}

	indexData := make([]byte, idxHandle.Size)
	if _, err := f.ReadAt(indexData, int64(idxHandle.Offset)); err != nil {
		f.Close()
		return nil, err
	}
	if len(indexData) < 4 || crc32.ChecksumIEEE(indexData[:len(indexData)-4]) != binary.LittleEndian.Uint32(indexData[len(indexData)-4:]) {
		f.Close()
		return nil, fmt.Errorf("index CRC mismatch")
	}
	indexData = indexData[:len(indexData)-4]

	filterData := make([]byte, filtHandle.Size)
	if _, err := f.ReadAt(filterData, int64(filtHandle.Offset)); err != nil {
		f.Close()
		return nil, err
	}

	r := &SSTableReader{file: f, fileSize: fileSize, indexHandle: idxHandle, indexData: indexData, filterData: filterData, modified: info.ModTime().UnixNano(), blocks: blocks, cacheID: number}
	r.indexBlock = NewBlockReader(indexData)
	if r.indexBlock.err != nil {
		r.Close()
		return nil, r.indexBlock.err
	}
	metadataOffset := idxHandle.Offset + idxHandle.Size
	metadataSize := fileSize - 48 - metadataOffset
	data := make([]byte, metadataSize)
	if _, err := f.ReadAt(data, int64(metadataOffset)); err != nil {
		r.Close()
		return nil, err
	}
	if err := r.decodeTableMetadata(data); err != nil {
		r.Close()
		return nil, err
	}
	return r, nil
}

func (r *SSTableReader) Get(key []byte) ([]byte, bool, error) { return r.GetAt(key, ^uint64(0)) }
func (r *SSTableReader) GetAt(key []byte, snapshot uint64) ([]byte, bool, error) {
	kv, ok, rd, err := r.lookupAt(key, snapshot)
	if err != nil || !ok || kv.Key.OpType == kTypeDelete || rd > kv.Key.SeqNum {
		return nil, false, err
	}
	return append([]byte{}, kv.Value...), true, nil
}
func (r *SSTableReader) lookupAt(key []byte, snapshot uint64) (KeyValue, bool, uint64, error) {
	var workspace pointWorkspace
	return r.pointLookup(key, snapshot, &workspace)
}

func (r *SSTableReader) NewIterator() *SSTableIterator {
	return r.newIterator(true, true, false)
}

func (r *SSTableReader) newIterator(eager, fillCache, reuseKeys bool) *SSTableIterator {
	it := &SSTableIterator{
		reader:    r,
		idxIter:   &BlockIterator{block: r.indexBlock, err: r.indexBlock.err},
		fillCache: fillCache,
		reuseKeys: reuseKeys,
	}
	it.idxIter.reuseKey = reuseKeys
	if eager {
		it.idxIter.Next()
	}
	if err := it.idxIter.Err(); err != nil {
		it.err = err
		return it
	}
	if eager && it.idxIter.Valid() {
		it.openCurrentBlock()
	}
	return it
}

func (r *SSTableReader) Close() error {
	if r.zstdDecoder != nil {
		r.zstdDecoder.Close()
	}
	return r.file.Close()
}

type SSTableIterator struct {
	reader    *SSTableReader
	idxIter   *BlockIterator
	blockIter *BlockIterator
	valid     bool
	key       InternalKey
	value     []byte
	err       error
	fillCache bool
	reuseKeys bool
}

func (it *SSTableIterator) Valid() bool { return it.valid }
func (it *SSTableIterator) Err() error  { return it.err }
func (it *SSTableIterator) update() {
	if it.blockIter != nil && it.blockIter.Err() != nil {
		it.err = it.blockIter.Err()
	}
	it.valid = it.blockIter != nil && it.blockIter.Valid()
	if it.valid {
		it.key = ParseInternalKey(it.blockIter.Key())
		it.value = it.blockIter.Value()
	}
}
func (it *SSTableIterator) Next() {
	if !it.valid {
		return
	}
	it.blockIter.Next()
	it.update()
	for !it.valid && it.err == nil {
		it.idxIter.Next()
		if !it.idxIter.Valid() {
			return
		}
		it.openCurrentBlock()
	}
}
func (it *SSTableIterator) openCurrentBlock() {
	it.openBlock(true)
}
func (it *SSTableIterator) openBlock(position bool) {
	it.valid = false
	it.blockIter = nil
	if !it.idxIter.Valid() {
		return
	}
	buf := it.idxIter.Value()
	off, n := binary.Uvarint(buf)
	if n <= 0 {
		it.err = fmt.Errorf("invalid block handle")
		return
	}
	size, n := binary.Uvarint(buf[n:])
	if n <= 0 || off > it.reader.fileSize || size > it.reader.fileSize-off {
		it.err = fmt.Errorf("invalid block handle")
		return
	}
	reader, err := it.reader.loadBlockReaderMode(off, size, it.fillCache)
	if err != nil {
		it.err = err
		return
	}
	if reader.numRestarts == 0 {
		it.err = fmt.Errorf("invalid data block")
		return
	}
	it.blockIter = &BlockIterator{block: reader, err: reader.err, reuseKey: it.reuseKeys}
	if position {
		it.blockIter.Next()
	}
	it.update()
}
func (it *SSTableIterator) Seek(key []byte) {
	it.err = nil
	it.valid = false
	it.blockIter = nil
	it.idxIter.Seek(key)
	for it.idxIter.Valid() {
		it.openBlock(false)
		if it.err != nil {
			return
		}
		if it.blockIter != nil {
			it.blockIter.Seek(key)
			it.update()
			if it.valid {
				return
			}
		}
		// Seek may exhaust a block; continue to the next indexed block.
		it.idxIter.Next()
	}
}
func (it *SSTableIterator) Key() InternalKey {
	if it.valid {
		return it.key
	}
	return InternalKey{}
}
func (it *SSTableIterator) Value() []byte {
	if it.valid {
		return it.value
	}
	return nil
}
func (it *SSTableIterator) Prev() {
	if it.valid {
		it.blockIter.Prev()
		it.update()
		if it.valid {
			return
		}
	}
	// Seek past the end leaves idxIter invalid; Prev restores the final block.
	it.idxIter.Prev()
	for it.idxIter.Valid() {
		it.openBlock(false)
		if it.err != nil {
			return
		}
		if it.blockIter != nil {
			it.blockIter.SeekToLast()
			it.update()
			if it.valid {
				return
			}
		}
		it.idxIter.Prev()
	}
	it.valid = false
}
func (it *SSTableIterator) Close() {}

func (it *SSTableIterator) SeekToLast() {
	it.err = nil
	it.valid = false
	it.blockIter = nil
	it.idxIter.SeekToLast()
	for it.idxIter.Valid() {
		it.openBlock(false)
		if it.err != nil {
			return
		}
		if it.blockIter != nil {
			it.blockIter.SeekToLast()
			it.update()
			if it.valid {
				return
			}
		}
		it.idxIter.Prev()
	}
}
