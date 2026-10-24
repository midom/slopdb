package storage

import "fmt"

// ReplicationHooks intercept logical WAL batches before queueing or locking.
// Write must call queued exactly once, after ordering the proposal and before
// waiting for it. Buffers belong to the caller until Write returns.
type ReplicationHooks struct {
	Write    func([]Mutation, func()) error
	PageSize func(int64) error
}

// SetReplicationHooks must be called before admitting SQL clients.
func (l *LSMTree) SetReplicationHooks(h *ReplicationHooks) { l.replication.Store(h) }

// ApplyReplicated atomically installs a committed batch, including its replay
// checkpoint, and synchronizes the native WAL before reporting success.
func (l *LSMTree) ApplyReplicated(batch []Mutation) error {
	return l.writeMutationsLocal(batch, true, false, nil)
}

func (l *LSMTree) ReplicaMode() bool { return l.opt.Replica }

// ApplyReplicatedDeferred records an atomic batch without a per-call WAL fsync.
// Only a replication owner with a separate durable-frontier/recovery protocol
// may use it. Rotation syncs sealed WALs in the background; a subsequent
// ApplyReplicated or SyncReplicationWAL waits for the complete preceding prefix.
// SSTable and MANIFEST publication remain durable.
func (l *LSMTree) ApplyReplicatedDeferred(batch []Mutation) error {
	return l.writeMutationsLocal(batch, false, false, nil)
}

// SyncReplicationWAL synchronizes all WAL records ordered before this call,
// including sealed generations whose background fsync has not yet completed.
func (l *LSMTree) SyncReplicationWAL() error {
	l.writerMu.Lock()
	defer l.writerMu.Unlock()
	l.mu.RLock()
	closed := l.closed
	backgroundErr := l.bgErr
	l.mu.RUnlock()
	if closed {
		return fmt.Errorf("database closed")
	}
	if backgroundErr != nil {
		return backgroundErr
	}
	if l.opt.DisableWAL || l.opt.DisableWALSync {
		return fmt.Errorf("replication sync requires enabled WAL fsync")
	}
	return l.syncWAL()
}
