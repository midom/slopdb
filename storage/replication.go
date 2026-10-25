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

// RestoreSnapshotBatch installs already materialized physical state without
// synthesizing changes from its rows/catalog or rewriting its CDC baseline.
// Use only while constructing a fresh, unexposed snapshot generation. Close
// and reopen it before admission so persisted control state is initialized.
func (l *LSMTree) RestoreSnapshotBatch(batch []Mutation) error {
	return l.writeMutationsLocalMode(batch, true, false, nil, false)
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

// writeWithLocalMutations never sends session-local rows to consensus. The SQL
// session gate excludes every observer of these rows while the durable part is
// committed, so mixed SQL transactions remain atomic to all permitted readers.
// Standalone batches publish both parts at one native sequence watermark.
func (l *LSMTree) writeWithLocalMutations(h *ReplicationHooks, batch []Mutation, queued func()) error {
	var durable, local []Mutation
	for _, w := range batch {
		if w.NoWAL {
			local = append(local, w)
		} else {
			durable = append(durable, w)
		}
	}
	if len(local) == 0 {
		return h.Write(batch, queued)
	}
	if len(durable) > 0 {
		if err := h.Write(durable, queued); err != nil {
			return err
		}
	} else if queued != nil {
		queued()
	}
	if err := l.writeMutationsLocal(local, false, false, nil); err != nil {
		if len(durable) > 0 {
			return fmt.Errorf("durable batch committed but temporary publication failed: %w", err)
		}
		return err
	}
	return nil
}
