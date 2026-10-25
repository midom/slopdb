package storage

import "sync"

const pooledBlockReadBytes = 8 << 10

// Encoded input and private decode output use separate bounded pools. Cache
// admission copies output to owned heap storage or an arena before returning
// scratch; bypassed reads keep output until their private reader releases it. Large blocks can
// use an owned Go buffer. Pools own no external resources and may be collected.
type blockReadBuffer [pooledBlockReadBytes]byte

type blockDecodeBuffer [pooledBlockReadBytes]byte

var blockReadBuffers = sync.Pool{New: func() any { return new(blockReadBuffer) }}
var blockDecodeBuffers = sync.Pool{New: func() any { return new(blockDecodeBuffer) }}

func (r *SSTableReader) readBlockBytes(offset, size uint64) ([]byte, *blockDecodeBuffer, error) {
	var counters *blockCacheShard
	if r.blocks != nil {
		counters = r.blocks.shard(blockCacheKey{table: r.cacheID, offset: offset})
		counters.readAttempts.Add(1)
	}
	success := false
	defer func() {
		if counters != nil && !success {
			counters.readErrors.Add(1)
		}
	}()
	compressed := r.hasCompressedBlocks.Load()
	var scratch *blockReadBuffer
	var data []byte
	if size <= pooledBlockReadBytes {
		scratch = blockReadBuffers.Get().(*blockReadBuffer)
		defer blockReadBuffers.Put(scratch)
		data = scratch[:size]
	} else {
		data = make([]byte, size)
	}
	n, readErr := r.file.ReadAt(data, int64(offset))
	if counters != nil {
		counters.readBytes.Add(int64(n))
	}
	if readErr != nil {
		return nil, nil, readErr
	}
	var decoded *blockDecodeBuffer
	var destination []byte
	if size <= pooledBlockReadBytes {
		decoded = blockDecodeBuffers.Get().(*blockDecodeBuffer)
		destination = decoded[:]
	}
	block, err := r.readRawBlockInto(data, destination)
	if err != nil {
		if decoded != nil {
			blockDecodeBuffers.Put(decoded)
		}
		return nil, nil, err
	}
	if decoded != nil && (len(block) == 0 || &block[0] != &decoded[0]) {
		blockDecodeBuffers.Put(decoded)
		decoded = nil
	}
	if data[len(data)-1] != kNoCompression {
		if !compressed {
			r.hasCompressedBlocks.Store(true)
		}
	} else if scratch != nil && decoded == nil {
		// A compressed SSTable can also contain raw pages. Detach their data
		// before returning scratch so readers/cached blocks retain ownership.
		block = append([]byte(nil), block...)
	}
	success = true
	if counters != nil {
		counters.decodedBytes.Add(int64(len(block)))
	}
	return block, decoded, nil
}
