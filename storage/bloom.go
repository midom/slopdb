package storage

import (
	"hash"
	"hash/fnv"
	"math"
)

// The hash-count byte identifies filters using a rotated probe delta.
const bloomRotatedDelta = 0x80

type BloomFilter struct {
	bits       []byte
	numHashes  int
	bitsPerKey int
}

func NewBloomFilter(bitsPerKey int) *BloomFilter {
	k := int(float64(bitsPerKey) * 0.69)
	if k < 1 {
		k = 1
	}
	if k > 30 {
		k = 30
	}
	return &BloomFilter{numHashes: k, bitsPerKey: bitsPerKey}
}

func (bf *BloomFilter) Generate(keys [][]byte) []byte {
	n := len(keys)
	bits := n * bf.bitsPerKey
	if bits < 64 {
		bits = 64
	}
	bytes := (bits + 7) / 8
	bf.bits = make([]byte, bytes, bytes+1)
	bf.bits = append(bf.bits, byte(bf.numHashes|bloomRotatedDelta))
	numBits := uint32(bytes * 8)

	for _, key := range keys {
		h := fnv.New32a()
		h.Write(key)
		hash := h.Sum32()
		delta := (hash >> 17) | (hash << 15)
		for j := 0; j < bf.numHashes; j++ {
			bitPos := hash % numBits
			bf.bits[bitPos/8] |= 1 << (bitPos % 8)
			hash += delta
		}
	}
	return bf.bits
}

func (bf *BloomFilter) MayContain(filter []byte, key []byte) bool {
	if len(filter) < 2 {
		return true
	}
	bits := filter[:len(filter)-1]
	tag := int(filter[len(filter)-1])
	k := tag &^ bloomRotatedDelta
	if k > 30 {
		return true
	}

	h := fnv.New32a()
	h.Write(key)
	hash := h.Sum32()
	delta := (hash >> 17) | (hash << 15)
	for j := 0; j < k; j++ {
		bitPos := hash % uint32(len(bits)*8)
		if bits[bitPos/8]&(1<<(bitPos%8)) == 0 {
			return false
		}
		hash += delta
	}
	return true
}

var _ hash.Hash32 = fnv.New32a()

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

var _ = math.MaxFloat64
