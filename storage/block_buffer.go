package storage

import "sync"

const pooledBlockReadBytes = 8 << 10

// Encoded input is transient when decompression allocates the decoded block.
// Bound pooled scratch independently of page/cache size; large pages retain
// their existing allocation path. Decode-only states and these buffers have no
// external resources and may be discarded by sync.Pool during collection.
type blockReadBuffer [pooledBlockReadBytes]byte

var blockReadBuffers = sync.Pool{New: func() any { return new(blockReadBuffer) }}

func (r *SSTableReader) readBlockBytes(offset, size uint64) ([]byte, error) {
	compressed := r.hasCompressedBlocks.Load()
	var scratch *blockReadBuffer
	var data []byte
	if compressed && size <= pooledBlockReadBytes {
		scratch = blockReadBuffers.Get().(*blockReadBuffer)
		defer blockReadBuffers.Put(scratch)
		data = scratch[:size]
	} else {
		data = make([]byte, size)
	}
	if _, err := r.file.ReadAt(data, int64(offset)); err != nil {
		return nil, err
	}
	block, err := r.readRawBlock(data)
	if err != nil {
		return nil, err
	}
	if data[len(data)-1] != kNoCompression {
		if !compressed {
			r.hasCompressedBlocks.Store(true)
		}
	} else if scratch != nil {
		// A compressed SSTable can also contain raw pages. Detach their data
		// before returning scratch so readers/cached blocks retain ownership.
		block = append([]byte(nil), block...)
	}
	return block, nil
}
