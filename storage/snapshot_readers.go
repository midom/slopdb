package storage

import (
	"fmt"
	"sync"
)

const snapshotReaderShards = 64

type snapshotReaderShard struct {
	mu      sync.Mutex
	pins    map[uint64]int
	padding [48]byte // keep independent reader locks on separate cache lines
}

// PromoteReaderSnapshot retains a reader's existing view for a transaction.
// Registration follows the same global-before-shard lock order as collection;
// the reader pin must remain held until this method returns.
func (l *LSMTree) PromoteReaderSnapshot(reader, seq uint64) error {
	l.snapshotMu.Lock()
	defer l.snapshotMu.Unlock()
	shard := &l.readerSnapshots[reader&(snapshotReaderShards-1)]
	shard.mu.Lock()
	defer shard.mu.Unlock()
	if shard.pins[seq] == 0 {
		return fmt.Errorf("reader snapshot is no longer pinned")
	}
	l.snapshots[seq]++
	return nil
}

// SnapshotForReader gives a stable reader its own snapshot-registration bucket.
// Pair it with ReleaseSnapshotForReader using the same reader ID. Collisions
// only share bookkeeping; every registered sequence still pins old versions.
func (l *LSMTree) SnapshotForReader(reader uint64) uint64 {
	shard := &l.readerSnapshots[reader&(snapshotReaderShards-1)]
	// The cutoff walker locks this bucket too. A pin registered after its
	// visit reads a watermark at least as new as the cutoff captured before
	// the walk; a pin with an older watermark is visible to that walk.
	shard.mu.Lock()
	if shard.pins == nil {
		shard.pins = make(map[uint64]int)
	}
	seq := l.published.Load()
	shard.pins[seq]++
	shard.mu.Unlock()
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

// RetainSnapshotForReader copies an existing global Snapshot pin into a
// statement reader's bucket. Keep the global pin until this call returns.
func (l *LSMTree) RetainSnapshotForReader(reader, seq uint64) error {
	l.snapshotMu.Lock()
	defer l.snapshotMu.Unlock()
	if l.snapshots[seq] == 0 {
		return fmt.Errorf("snapshot is no longer pinned")
	}
	shard := &l.readerSnapshots[reader&(snapshotReaderShards-1)]
	shard.mu.Lock()
	defer shard.mu.Unlock()
	if shard.pins == nil {
		shard.pins = make(map[uint64]int)
	}
	shard.pins[seq]++
	return nil
}
