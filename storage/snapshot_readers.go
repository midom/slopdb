package storage

import "sync"

const snapshotReaderShards = 64

type snapshotReaderShard struct {
	mu      sync.Mutex
	pins    map[uint64]int
	padding [48]byte // keep independent reader locks on separate cache lines
}

// SnapshotForReader gives a stable reader its own snapshot-registration bucket.
// Pair it with ReleaseSnapshotForReader using the same reader ID. Collisions
// only share bookkeeping; every registered sequence still pins old versions.
func (l *LSMTree) SnapshotForReader(reader uint64) uint64 {
	shard := &l.readerSnapshots[reader&(snapshotReaderShards-1)]
	l.viewMu.RLock()
	shard.mu.Lock()
	if shard.pins == nil {
		shard.pins = make(map[uint64]int)
	}
	seq := l.published.Load()
	shard.pins[seq]++
	shard.mu.Unlock()
	l.viewMu.RUnlock()
	return seq
}
func (l *LSMTree) ReleaseSnapshotForReader(reader, seq uint64) {
	shard := &l.readerSnapshots[reader&(snapshotReaderShards-1)]
	shard.mu.Lock()
	if n := shard.pins[seq]; n > 1 {
		shard.pins[seq] = n - 1
	} else {
		delete(shard.pins, seq)
	}
	shard.mu.Unlock()
}
