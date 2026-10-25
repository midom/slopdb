package replication

import (
	"context"
	"strconv"
	"time"

	"slopdb/db"
)

// sqlState reports local atomics and Raft stats without VerifyLeader, proposals
// or GetConfiguration futures. Status queries remain useful during recovery.
func (n *Node) sqlState(ctx context.Context) (db.RaftState, error) {
	if e := ctx.Err(); e != nil {
		return db.RaftState{}, e
	}
	out := db.RaftState{Enabled: true, NodeID: n.config.ID, ClusterID: n.cluster, Address: n.Address(), ControlAddress: n.ControlAddress(), Role: "Recovering", Recovering: true, Durability: string(n.config.Durability), ReadBarrier: string(n.config.ReadBarrier), NonVoting: n.config.NonVoting, LogsOnly: n.config.LogsOnly, DurableIndex: n.durableIndex(), AppliedIndex: n.applied.Load(), AppliedTerm: n.appliedTerm.Load(), ReplicaReadMaxStaleness: n.config.ReplicaReadMaxStaleness, ReplicatedPosition: n.currentPosition()}
	if err := n.failure(); err != nil {
		out.Error = err.Error()
	}
	n.durability.publication.Lock()
	out.DurabilityError = n.durability.lastError
	n.durability.publication.Unlock()
	r := n.raftNode()
	if r == nil {
		return out, nil
	}
	out.Role = r.State().String()
	out.Recovering = n.recovering.Load()
	out.Term = r.CurrentTerm()
	leader, id := r.LeaderWithID()
	out.LeaderID = string(id)
	out.LeaderAddress = string(leader)
	out.Ready = n.readyTerm.Load() != 0 && n.readyTerm.Load() == out.Term && out.Role == "Leader" && !out.Recovering && out.Error == "" && !n.closing.Load()
	out.Writable = out.Ready && !out.LogsOnly && !out.NonVoting
	stats := r.Stats()
	number := func(k string) uint64 { v, _ := strconv.ParseUint(stats[k], 10, 64); return v }
	out.CommitIndex = r.CommitIndex()
	out.LastLogIndex = number("last_log_index")
	out.LastSnapshotIndex = number("last_snapshot_index")
	out.LastSnapshotTerm = number("last_snapshot_term")
	n.durableReads.mu.Lock()
	if v := n.durableReads.latest; v != nil {
		out.DurableReadIndex = v.index
		out.DurableReadReady = v.tree == n.database.ReplicationStorage() && v.index >= out.DurableIndex && n.checkDurableReadHealth(ctx) == nil
	}
	n.durableReads.mu.Unlock()
	n.replicaReads.mu.Lock()
	proof := n.replicaReads.proof
	out.ReplicaReadIndex = proof.fence.Index
	if !proof.since.IsZero() {
		out.ReplicaReadProofAge = time.Since(proof.since)
	}
	out.ReplicaReadReady = n.checkReplicaReadRole(ctx) == nil && n.usableReadProof(proof)
	n.replicaReads.mu.Unlock()
	if n.async() {
		n.durability.publication.Lock()
		if c := n.durability.publishedCertificate; c != nil {
			out.DurablePosition = n.formatPosition(c.Body.Index, c.Body.Term)
		}
		n.durability.publication.Unlock()
	} else {
		out.DurablePosition = out.ReplicatedPosition
	}

	return out, ctx.Err()
}
func (n *Node) checkInspection(ctx context.Context) error {
	if e := ctx.Err(); e != nil {
		return e
	}
	if n.closing.Load() {
		return ErrClosed
	}
	return nil
}
