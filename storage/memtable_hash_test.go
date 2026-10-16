package storage

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"sync"
	"testing"
)

func TestMemTableHashCollisionsSnapshotsAndIterators(t *testing.T) {
	for _, hash := range []bool{false, true} {
		t.Run(fmt.Sprintf("hash=%v", hash), func(t *testing.T) {
			m := newMemTableSized(0, hash)
			// Use real collisions, not merely different adjacent hash buckets.
			var keys [][]byte
			target := memKeyHash([]byte("first")) & (memHashBuckets - 1)
			for n := 0; len(keys) < 3; n++ {
				key := []byte(fmt.Sprintf("collision-%d", n))
				if memKeyHash(key)&(memHashBuckets-1) == target {
					keys = append(keys, key)
				}
			}
			for i, key := range keys {
				m.Put(uint64(i+1), kTypeValue, key, []byte(fmt.Sprintf("old-%d", i)))
			}
			old := m.NewIteratorAt(3)
			defer old.Close()
			// Installation order is deliberately different from sequence order.
			m.Put(9, kTypeValue, keys[0], []byte("new"))
			m.Put(7, kTypeValue, keys[0], []byte("middle"))
			m.Put(8, kTypeDelete, keys[1], nil)
			m.Put(10, kTypeRangeDelete, []byte("collision-"), []byte("collision."))
			for i, key := range keys {
				kv, ok, rd := m.lookupAt(key, 3)
				if !ok || rd != 0 || string(kv.Value) != fmt.Sprintf("old-%d", i) {
					t.Fatalf("old row %d: %+v %v range=%d", i, kv, ok, rd)
				}
			}
			kv, ok, _ := m.lookupAt(keys[0], 8)
			if !ok || string(kv.Value) != "middle" {
				t.Fatal("snapshot did not select newest visible collision version", kv)
			}
			kv, ok, rd := m.lookupAt(keys[0], 10)
			if !ok || string(kv.Value) != "new" || rd != 10 {
				t.Fatal("latest point/range ordering", kv, ok, rd)
			}
			n := 0
			for ; old.Valid(); old.Next() {
				if old.Key().SeqNum > 3 {
					t.Fatal("old iterator saw new row")
				}
				n++
			}
			if n != 3 {
				t.Fatal("old iterator changed", n)
			}
			old.SeekToLast()
			for old.Valid() {
				n--
				old.Prev()
			}
			if n != 0 {
				t.Fatal("reverse iterator lost rows", n)
			}
			it := m.NewIteratorAt(8)
			defer it.Close()
			it.Seek(keys[0])
			if !it.Valid() || !bytes.Equal(it.Key().UserKey, keys[0]) || it.Key().SeqNum != 7 {
				t.Fatal("seek snapshot", it.Key())
			}
			it.SkipUserKey(keys[0])
			if it.Valid() && bytes.Compare(it.Key().UserKey, keys[0]) <= 0 {
				t.Fatal("skip user key failed")
			}
			if len(it.RangeTombstones()) != 0 {
				t.Fatal("future range tombstone entered snapshot")
			}
		})
	}
}

func TestMemTableHashConcurrentPublicationAndStableScan(t *testing.T) {
	for _, hash := range []bool{false, true} {
		t.Run(fmt.Sprintf("hash=%v", hash), func(t *testing.T) {
			m := newMemTableSized(0, hash)
			key := []byte("same-row")
			var value [8]byte
			binary.LittleEndian.PutUint64(value[:], 1)
			m.Put(1, kTypeValue, key, value[:])
			held := m.NewIterator()
			defer held.Close()
			var wg sync.WaitGroup
			stop := make(chan struct{})
			for i := 0; i < 8; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for {
						select {
						case <-stop:
							return
						default:
						}
						snapshot := m.maxSeq.Load()
						kv, ok, _ := m.lookupAt(key, snapshot)
						if !ok || kv.Key.SeqNum > snapshot || binary.LittleEndian.Uint64(kv.Value) != kv.Key.SeqNum {
							t.Error("partial or missing publication")
							return
						}
					}
				}()
			}
			for seq := uint64(2); seq <= 5000; seq++ {
				binary.LittleEndian.PutUint64(value[:], seq)
				m.Put(seq, kTypeValue, key, value[:])
			}
			close(stop)
			wg.Wait()
			if !held.Valid() || held.Key().SeqNum != 1 || binary.LittleEndian.Uint64(held.Value()) != 1 {
				t.Fatal("held iterator value mutated")
			}
			held.Next()
			if held.Valid() {
				t.Fatal("held iterator included later versions")
			}
		})
	}
}

func TestMemTableChunksOwnKeysValuesAndRangeBounds(t *testing.T) {
	for _, hash := range []bool{false, true} {
		t.Run(fmt.Sprintf("hash=%v", hash), func(t *testing.T) {
			m := newMemTableSized(0, hash)
			key, value := []byte("row"), []byte("value")
			m.Put(1, kTypeValue, key, value)
			key[0] = 'x'
			value[0] = 'x'
			got, found := m.Get([]byte("row"))
			if !found || string(got) != "value" {
				t.Fatal("caller mutated memtable row", string(got), found)
			}
			start, end := []byte("a"), []byte("z")
			m.Put(2, kTypeRangeDelete, start, end)
			start[0] = 'z'
			end[0] = 'a'
			if seq, found := m.IsDeletedByRangeSeq([]byte("row")); !found || seq != 2 {
				t.Fatal("caller mutated range bounds", seq, found)
			}
		})
	}
}

func TestMemTableEqualSequenceKeepsLatestInsertionFirst(t *testing.T) {
	for _, hash := range []bool{false, true} {
		t.Run(fmt.Sprintf("hash=%v", hash), func(t *testing.T) {
			m := newMemTableSized(0, hash)
			m.Put(1, kTypeValue, []byte("same"), []byte("old"))
			m.Put(1, kTypeValue, []byte("same"), []byte("new"))
			value, found := m.Get([]byte("same"))
			if !found || string(value) != "new" {
				t.Fatal("point read tie ordering")
			}
			it := m.NewIterator()
			defer it.Close()
			if !it.Valid() || string(it.Value()) != "new" {
				t.Fatal("scan tie ordering")
			}

		})
	}
}
