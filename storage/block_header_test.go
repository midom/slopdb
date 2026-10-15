package storage

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"math/rand"
	"testing"
)

func checkBlockHeader(t *testing.T, data []byte) {
	t.Helper()
	var want [3]uint64
	offset := 0
	for i := range want {
		value, n := binary.Uvarint(data[offset:])
		if n <= 0 {
			_, _, _, got := blockEntryHeader(data)
			if got != 0 {
				t.Fatalf("accepted incomplete/overflowed header %x", data)
			}
			return
		}
		want[i] = value
		offset += n
	}
	a, b, c, n := blockEntryHeader(data)
	if [3]uint64{a, b, c} != want || n != offset {
		t.Fatalf("header %x: (%d,%d,%d)/%d, want %v/%d", data, a, b, c, n, want, offset)
	}
}

func TestBlockEntryHeadersBoundariesAndMalformed(t *testing.T) {
	values := []uint64{0, 1, 127, 128, 16383, 16384, math.MaxUint64}
	for _, a := range values {
		for _, b := range values {
			for _, c := range values {
				data := binary.AppendUvarint(nil, a)
				data = binary.AppendUvarint(data, b)
				data = binary.AppendUvarint(data, c)
				for end := 0; end <= len(data); end++ {
					checkBlockHeader(t, data[:end])
				}
			}
		}
	}
	rng := rand.New(rand.NewSource(761))
	for i := 0; i < 10000; i++ {
		data := make([]byte, rng.Intn(40))
		_, _ = rng.Read(data)
		checkBlockHeader(t, data)
	}
}

func FuzzBlockEntryHeader(f *testing.F) {
	for _, seed := range [][]byte{nil, {0, 23, 0x83, 2}, {0, 1, 127}, bytes.Repeat([]byte{255}, 12)} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, data []byte) { checkBlockHeader(t, data) })
}

func TestBlockHeadersLongEntriesAndBidirectionalSeeks(t *testing.T) {
	builder := NewBlockBuilder(4096)
	var keys, values [][]byte
	for n := 0; n < 100; n++ {
		key := []byte(fmt.Sprintf("%08d:%s", n, bytes.Repeat([]byte{'x'}, n*2)))
		value := bytes.Repeat([]byte{byte(n)}, []int{0, 1, 127, 128, 16384}[n%5])
		keys, values = append(keys, key), append(values, value)
		builder.Add(key, value)
	}
	data := builder.Finish()
	r := NewBlockReader(data[:len(data)-4])
	it := r.NewIterator()
	for n := range keys {
		if !it.Valid() || !bytes.Equal(it.Key(), keys[n]) || !bytes.Equal(it.Value(), values[n]) {
			t.Fatalf("forward position %d: %v", n, it.Err())
		}
		it.Next()
	}
	for n := len(keys) - 1; n >= 0; n-- {
		it.Prev()
		if !it.Valid() || !bytes.Equal(it.Key(), keys[n]) || !bytes.Equal(it.Value(), values[n]) {
			t.Fatalf("reverse position %d: %v", n, it.Err())
		}
		it.Seek(keys[n])
		if !it.Valid() || !bytes.Equal(it.Key(), keys[n]) {
			t.Fatalf("seek position %d: %v", n, it.Err())
		}
	}
}
