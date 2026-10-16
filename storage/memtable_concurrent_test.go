package storage

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"sync"
	"testing"
)

func TestConcurrentSkipListVersionsAndAtomicLinks(t *testing.T) {
	m := NewMemTable()
	const writers = 32
	const rounds = 400
	for w := 0; w < writers; w++ {
		m.Put(1, kTypeValue, []byte(fmt.Sprintf("row-%02d", w)), []byte("original"))
	}
	held := m.NewIteratorAt(1)
	defer held.Close()
	var wg sync.WaitGroup
	start := make(chan struct{})
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			<-start
			key := []byte(fmt.Sprintf("row-%02d", w))
			for n := 1; n <= rounds; n++ {
				seq := uint64(n*writers + w + 2)
				var value [8]byte
				binary.LittleEndian.PutUint64(value[:], seq)
				m.Put(seq, kTypeValue, key, value[:])
				got, found := m.Get(key)
				if !found || binary.LittleEndian.Uint64(got) != seq {
					t.Errorf("writer %d lost completed version %d", w, seq)
					return
				}
			}
		}(w)
	}
	close(start)
	wg.Wait()
	if m.entries.Load() != writers*(rounds+1) {
		t.Fatal("lost entry accounting", m.entries.Load())
	}
	for w := 0; w < writers; w++ {
		if !held.Valid() || string(held.Key().UserKey) != fmt.Sprintf("row-%02d", w) || string(held.Value()) != "original" {
			t.Fatal("snapshot changed", w)
		}
		held.SkipUserKey(held.Key().UserKey)
	}
	if held.Valid() {
		t.Fatal("snapshot included later inserts")
	}
	// Verify every node at every level, not only the latest point values.
	for level := 0; level < int(m.height.Load()); level++ {
		var previous *skipNode
		count := 0
		for node := m.head.next[level].Load(); node != nil; node = node.next[level].Load() {
			if previous != nil && previous.key.Compare(node.key) >= 0 {
				t.Fatalf("level %d is out of order", level)
			}
			if len(node.next) <= level {
				t.Fatal("published incomplete tower")
			}
			previous = node
			count++
		}
		if level == 0 && count != writers*(rounds+1) {
			t.Fatal("bottom level lost nodes", count)
		}
	}
	it := m.NewIterator()
	defer it.Close()
	it.SeekToLast()
	count := 0
	for ; it.Valid(); it.Prev() {
		count++
	}
	if count != writers*(rounds+1) {
		t.Fatal("reverse traversal lost nodes", count)
	}
}

func TestConcurrentSkipListPrivateBatchFingers(t *testing.T) {
	m := NewMemTable()
	const writers = 16
	const count = 1000
	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			entries := make([]KeyValue, count)
			for n := range entries {
				key := []byte(fmt.Sprintf("%06d", (count-1-n)*writers+w))
				entries[n] = KeyValue{Key: InternalKey{UserKey: key, SeqNum: uint64(n*writers + w + 1), OpType: kTypeValue}, Value: bytes.Clone(key)}
			}
			m.insertSortedBatch(entries)
		}(w)
	}
	wg.Wait()
	m.maxSeq.Store(writers * count)
	it := m.NewIterator()
	defer it.Close()
	n := 0
	for ; it.Valid(); it.Next() {
		want := fmt.Sprintf("%06d", n)
		if string(it.Key().UserKey) != want || string(it.Value()) != want {
			t.Fatal("concurrent batch lost or changed a row", n)
		}
		n++
	}
	if n != writers*count || m.entries.Load() != writers*count {
		t.Fatal("batch count mismatch", n, m.entries.Load())
	}
}
