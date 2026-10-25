package db

import (
	"context"
	"strconv"
	"time"

	"slopdb/storage"
)

// RaftState is a read-only owner snapshot. Collecting it must not propose log
// entries, request a quorum fence, enable CDC or execute SQL.
type RaftState struct {
	Enabled, Ready, Writable, Recovering, NonVoting, LogsOnly                                                     bool
	NodeID, ClusterID, Address, ControlAddress, Role, LeaderID, LeaderAddress                                     string
	Term, AppliedIndex, AppliedTerm, CommitIndex, LastLogIndex, LastSnapshotIndex, LastSnapshotTerm, DurableIndex uint64
	Durability, ReadBarrier, ReplicatedPosition, DurablePosition, Error, DurabilityError                          string
	DurableReadIndex, ReplicaReadIndex                                                                            uint64
	DurableReadReady, ReplicaReadReady                                                                            bool
	ReplicaReadMaxStaleness, ReplicaReadProofAge                                                                  time.Duration
}

// ServerState separates live owner/storage facts from protocol-specific names.
type ServerState struct {
	Changefeed                ChangefeedState
	ReadOnly                  bool
	Raft                      RaftState
	Storage                   storage.LSMStats
	QueryTimeout, LockTimeout time.Duration
	QueryMemoryBytes          int64
	DeadlockDetection         bool
}

func (d *Database) RaftState(ctx context.Context) (RaftState, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return RaftState{}, err
	}
	if d.closed.Load() {
		return RaftState{}, ErrSessionClosed
	}
	guard := d.replication.Load()
	if guard == nil {
		return RaftState{Role: "Standalone", Ready: true, Writable: true}, nil
	}
	if guard.State != nil {
		v, e := guard.State(ctx)
		if e != nil {
			return RaftState{}, e
		}
		if e = ctx.Err(); e != nil {
			return RaftState{}, e
		}
		v.Enabled = true
		return v, nil
	}
	v := RaftState{Enabled: true, Role: "Unknown"}
	if guard.Check != nil {
		v.Writable = guard.Check(ctx) == nil
	}
	if err := ctx.Err(); err != nil {
		return RaftState{}, err
	}
	v.Ready = v.Writable
	return v, nil
}
func (d *Database) ServerState(ctx context.Context) (ServerState, error) {
	r, e := d.RaftState(ctx)
	if e != nil {
		return ServerState{}, e
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if e = ctx.Err(); e != nil {
		return ServerState{}, e
	}
	// Stats owns the storage metadata locks. No SQL session/catalog gate is
	// acquired here, so expression callbacks cannot re-enter a queued SQL writer.
	stats := d.currentStorage.Load().Stats()
	feed, e := d.ChangefeedState(ctx)
	if e != nil {
		return ServerState{}, e
	}
	memory := d.opt.QueryMemoryBytes
	if memory == 0 {
		memory = 64 << 20
	}
	return ServerState{Changefeed: feed, ReadOnly: !r.Writable, Raft: r, Storage: stats, QueryTimeout: d.opt.QueryTimeout, LockTimeout: d.opt.LockTimeout, QueryMemoryBytes: memory, DeadlockDetection: d.opt.DeadlockDetection}, nil
}
func boolState(v bool) string {
	if v {
		return "ON"
	}
	return "OFF"
}
func uintState(v uint64) string { return strconv.FormatUint(v, 10) }
func (s ServerState) Variables() map[string]string {
	v := map[string]string{
		"slopdb_cdc_enabled": boolState(s.Changefeed.Enabled), "slopdb_cdc_sequence": uintState(s.Changefeed.Sequence), "slopdb_cdc_source_uuid": s.Changefeed.SourceID,
		"read_only": boolState(s.ReadOnly), "super_read_only": boolState(s.ReadOnly),
		"slopdb_storage_engine": "LSM", "slopdb_page_size": strconv.FormatInt(s.Storage.PageSize, 10),
		"slopdb_wal_enabled": boolState(s.Storage.WALActive), "slopdb_wal_fsync": boolState(s.Storage.WALActive && !s.Storage.WALSyncDisabled),
		"slopdb_query_timeout_ms": strconv.FormatInt(s.QueryTimeout.Milliseconds(), 10), "slopdb_lock_wait_timeout_ms": strconv.FormatInt(s.LockTimeout.Milliseconds(), 10),
		"slopdb_query_memory_bytes": strconv.FormatInt(s.QueryMemoryBytes, 10), "slopdb_deadlock_detection": boolState(s.DeadlockDetection),
		"slopdb_memtable_bytes": strconv.Itoa(s.Storage.MemSize), "slopdb_immutable_memtable_bytes": strconv.Itoa(s.Storage.ImmSize),
		"slopdb_storage_sequence": uintState(s.Storage.SeqNum), "slopdb_block_cache_bytes": strconv.FormatInt(s.Storage.BlockCacheBytes, 10),
		"slopdb_block_cache_hits": strconv.FormatInt(s.Storage.BlockCacheHits, 10), "slopdb_block_cache_misses": strconv.FormatInt(s.Storage.BlockCacheMisses, 10),
		"slopdb_flushes": strconv.FormatInt(s.Storage.CompletedFlushes, 10), "slopdb_compactions": strconv.FormatInt(s.Storage.CompletedCompactions, 10),
	}
	var files int
	var bytes int64
	for _, level := range s.Storage.Levels {
		files += level.FileCount
		bytes += level.TotalSize
	}
	v["slopdb_sstable_count"] = strconv.Itoa(files)
	v["slopdb_sstable_bytes"] = strconv.FormatInt(bytes, 10)
	for name, value := range s.Raft.Variables() {
		v["slopdb_raft_"+name] = value
	}
	return v
}
func (r RaftState) Variables() map[string]string {
	return map[string]string{
		"enabled": boolState(r.Enabled), "node_id": r.NodeID, "cluster_id": r.ClusterID, "address": r.Address, "control_address": r.ControlAddress,
		"role": r.Role, "leader_id": r.LeaderID, "leader_address": r.LeaderAddress, "ready": boolState(r.Ready), "writable": boolState(r.Writable),
		"recovering": boolState(r.Recovering), "non_voting": boolState(r.NonVoting), "logs_only": boolState(r.LogsOnly),
		"term": uintState(r.Term), "applied_index": uintState(r.AppliedIndex), "applied_term": uintState(r.AppliedTerm), "commit_index": uintState(r.CommitIndex),
		"last_log_index": uintState(r.LastLogIndex), "last_snapshot_index": uintState(r.LastSnapshotIndex), "last_snapshot_term": uintState(r.LastSnapshotTerm),
		"durable_index": uintState(r.DurableIndex), "durability": r.Durability, "read_barrier": r.ReadBarrier,
		"durable_read_index": uintState(r.DurableReadIndex), "durable_read_ready": boolState(r.DurableReadReady),
		"replica_read_index": uintState(r.ReplicaReadIndex), "replica_read_ready": boolState(r.ReplicaReadReady),
		"replica_read_max_staleness_ms": strconv.FormatInt(r.ReplicaReadMaxStaleness.Milliseconds(), 10), "replica_read_proof_age_ms": strconv.FormatInt(r.ReplicaReadProofAge.Milliseconds(), 10),
		"replicated_position": r.ReplicatedPosition, "durable_position": r.DurablePosition, "error": r.Error, "durability_error": r.DurabilityError,
	}
}
func (d *Database) ReadOnly(ctx context.Context) (bool, error) {
	r, e := d.RaftState(ctx)
	return !r.Writable, e
}
