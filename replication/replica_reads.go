package replication

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/hashicorp/raft"
	"slopdb/db"
	"slopdb/sql"
	"slopdb/storage"
)

var ErrReplicaTooStale = errors.New("replica cannot satisfy the read staleness bound")

type replicaReadFence struct {
	Cluster, Leader, Address string
	Index, Term              uint64
}

type replicaReadProof struct {
	fence replicaReadFence
	since time.Time // local request start, including network and apply latency
}

type replicaReadState struct {
	mu      sync.Mutex
	proof   replicaReadProof
	refresh chan struct{}
	// Catalog and data snapshot acquisition are serialized with FSM application.
	// Query execution/streaming does not hold fsmMu.
	catalog      *sql.Schema
	catalogTree  *storage.LSMTree
	catalogIndex uint64
}

func (n *Node) checkReplicaReadRole(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if n.closing.Load() {
		return ErrClosed
	}
	if err := n.failure(); err != nil {
		return err
	}
	r := n.raftNode()
	if n.recovering.Load() || r == nil {
		return ErrRecovering
	}
	if n.config.ReplicaReadMaxStaleness == 0 || r.State() != raft.Follower || n.readyTerm.Load() != 0 || n.writeTerm.Load() != 0 {
		return n.notLeader()
	}
	return nil
}

func (n *Node) usableReadProof(p replicaReadProof) bool {
	r := n.raftNode()
	if r == nil || p.since.IsZero() || time.Since(p.since) >= n.config.ReplicaReadMaxStaleness {
		return false
	}
	address, leader := r.LeaderWithID()
	return p.fence.Cluster == n.cluster && p.fence.Term == r.CurrentTerm() &&
		p.fence.Leader == string(leader) && p.fence.Address == string(address) &&
		n.applied.Load() >= p.fence.Index
}

func (n *Node) checkReadSQL(ctx context.Context) error {
	if durableBarrier(n.readBarrier(ctx)) {
		return n.checkDurableReadHealth(ctx)
	}
	if n.config.ReplicaReadMaxStaleness == 0 || n.raftNode() != nil && n.raftNode().State() == raft.Leader {
		return n.checkSQL(ctx)
	}
	_, err := n.replicaReadProof(ctx)
	return err
}

// Concurrent readers share a refresh but retain independently cancellable waits.
func (n *Node) replicaReadProof(ctx context.Context) (replicaReadProof, error) {
	for {
		if err := n.checkReplicaReadRole(ctx); err != nil {
			return replicaReadProof{}, err
		}
		n.replicaReads.mu.Lock()
		if proof := n.replicaReads.proof; n.usableReadProof(proof) {
			n.replicaReads.mu.Unlock()
			return proof, nil
		}
		if flight := n.replicaReads.refresh; flight != nil {
			n.replicaReads.mu.Unlock()
			select {
			case <-flight:
				continue
			case <-ctx.Done():
				return replicaReadProof{}, ctx.Err()
			case <-n.ctx.Done():
				return replicaReadProof{}, ErrClosed
			}
		}
		flight := make(chan struct{})
		n.replicaReads.refresh = flight
		n.replicaReads.mu.Unlock()
		proof, err := n.refreshReplicaReadProof(ctx)
		n.replicaReads.mu.Lock()
		if err == nil {
			n.replicaReads.proof = proof
		}
		n.replicaReads.refresh = nil
		close(flight)
		n.replicaReads.mu.Unlock()
		return proof, err
	}
}

// Replica reads must not wait indefinitely for a stalled FSM or RPC. The bound
// starts before the RPC; time spent waiting for application consumes it too.
func (n *Node) refreshReplicaReadProof(ctx context.Context) (replicaReadProof, error) {
	proof := replicaReadProof{since: time.Now()}
	timeout := n.config.ReplicaReadMaxStaleness
	if n.config.OperationTimeout < timeout {
		timeout = n.config.OperationTimeout
	}
	call, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	stop := context.AfterFunc(n.ctx, cancel)
	defer stop()
	_, leader := n.raftNode().LeaderWithID()
	if err := n.lockReadFSM(call); err != nil {
		return proof, fmt.Errorf("%w: %w", ErrReplicaTooStale, err)
	}
	data, found, err := n.database.ReplicationStorage().Get(peerKey(string(leader)))
	n.fsmMu.Unlock()
	if err != nil {
		return proof, err
	}
	var peer Peer
	if !found || json.Unmarshal(data, &peer) != nil || peer.ID != string(leader) || peer.Cluster != n.cluster {
		return proof, fmt.Errorf("%w: leader control endpoint is unavailable", ErrReplicaTooStale)
	}
	reader := Peer{ID: n.config.ID, Address: n.Address(), Cluster: n.cluster}
	if err = n.postJSON(call, peer.Control, "/v1/read-index", reader, &proof.fence); err != nil {
		return proof, fmt.Errorf("%w: %w", ErrReplicaTooStale, err)
	}
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		if err := n.checkReplicaReadRole(call); err != nil {
			return proof, fmt.Errorf("%w: %w", ErrReplicaTooStale, err)
		}
		r := n.raftNode()
		address, id := r.LeaderWithID()
		if proof.fence.Cluster != n.cluster || proof.fence.Leader != string(id) || proof.fence.Address != string(address) || proof.fence.Term != r.CurrentTerm() || proof.fence.Index == 0 {
			return proof, fmt.Errorf("%w: read-index identity or term changed", ErrReplicaTooStale)
		}
		if n.usableReadProof(proof) {
			return proof, nil
		}
		select {
		case <-call.Done():
			return proof, fmt.Errorf("%w: %w", ErrReplicaTooStale, call.Err())
		case <-n.ctx.Done():
			return proof, ErrClosed
		case <-ticker.C:
		}
	}
}

func (n *Node) lockReadFSM(ctx context.Context) error {
	if n.fsmMu.TryLock() {
		return nil
	}
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-n.ctx.Done():
			return ErrClosed
		case <-ticker.C:
			if n.fsmMu.TryLock() {
				return nil
			}
		}
	}
}

func (n *Node) captureReadSnapshot(ctx context.Context, session *sql.Schema) (bool, error) {
	mode := n.readBarrier(ctx)
	if durableBarrier(mode) {
		if db.IsReadStatementSnapshot(ctx) {
			return n.captureDurableRead(ctx, session, mode)
		}
		if n.raftNode() == nil || n.raftNode().State() != raft.Leader {
			if err := n.checkDurableReadHealth(ctx); err != nil {
				return true, err
			}
			return n.captureReplicaControl(ctx, session)
		}
	}
	if n.raftNode() != nil && n.raftNode().State() == raft.Leader {
		// A follower can become leader between read admission and capture.
		// Its old catalog/session cannot be used during leader initialization.
		_, err := n.checkReadyLeader(ctx)
		return false, err
	}
	if n.config.ReplicaReadMaxStaleness == 0 {
		return false, n.notLeader()
	}
	proof, err := n.replicaReadProof(ctx)
	if err != nil {
		return true, err
	}
	call, cancel := context.WithDeadline(ctx, proof.since.Add(n.config.ReplicaReadMaxStaleness))
	defer cancel()
	if err = n.lockReadFSM(call); err != nil {
		return true, fmt.Errorf("%w: %w", ErrReplicaTooStale, err)
	}
	defer n.fsmMu.Unlock()
	if err = n.checkReplicaReadRole(call); err != nil {
		return true, err
	}
	state := &n.replicaReads
	tree, index := n.database.ReplicationStorage(), n.applied.Load()
	if state.catalog == nil || state.catalogTree != tree || state.catalogIndex != index {
		catalog, e := sql.NewSchemaWithOptions(tree, int(tree.PageSize()), n.storageOptions())
		if e != nil {
			return true, e
		}
		state.catalog, state.catalogTree, state.catalogIndex = catalog, tree, index
	}
	if !n.usableReadProof(proof) {
		return true, ErrReplicaTooStale
	}
	err = session.CaptureReplicaStatement(state.catalog, index)
	// Scheduling/catalog loading must not extend a proof past snapshot capture.
	if err == nil && (!n.usableReadProof(proof) || call.Err() != nil) {
		session.EndStatement()
		err = errors.Join(ErrReplicaTooStale, call.Err())
	}
	return true, err
}

// Transaction control in durable mode needs a coherent catalog but reads no
// application data. It must not request an extra "next" barrier itself.
func (n *Node) captureReplicaControl(ctx context.Context, session *sql.Schema) (bool, error) {
	if err := n.lockReadFSM(ctx); err != nil {
		return true, err
	}
	defer n.fsmMu.Unlock()
	state := &n.replicaReads
	tree, index := n.database.ReplicationStorage(), n.applied.Load()
	if state.catalog == nil || state.catalogTree != tree || state.catalogIndex != index {
		catalog, err := sql.NewSchemaWithOptions(tree, int(tree.PageSize()), n.storageOptions())
		if err != nil {
			return true, err
		}
		state.catalog, state.catalogTree, state.catalogIndex = catalog, tree, index
	}
	return true, session.CaptureReplicaStatement(state.catalog, index)
}

func (n *Node) replicaReadHandlers(mux *http.ServeMux) {
	mux.HandleFunc("/v1/read-index", func(w http.ResponseWriter, r *http.Request) {
		if !requireMethod(w, r, http.MethodPost) {
			return
		}
		var reader Peer
		if !decodeJSON(w, r, &reader) {
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), n.config.OperationTimeout)
		defer cancel()
		term, err := n.checkReadyLeader(ctx)
		if err != nil {
			http.Error(w, err.Error(), http.StatusConflict)
			return
		}
		index := n.raftNode().CommitIndex()
		if err = n.checkSQL(ctx); err != nil {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}
		configuration := n.raftNode().GetConfiguration()
		if err = n.wait(ctx, configuration); err != nil {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}
		member := false
		for _, server := range configuration.Configuration().Servers {
			if reader.Cluster == n.cluster && string(server.ID) == reader.ID && string(server.Address) == reader.Address {
				member = true
			}
		}
		current, err := n.checkReadyLeader(ctx)
		if !member || err != nil || term != current {
			http.Error(w, "reader is not a member or leader changed", http.StatusConflict)
			return
		}
		writeJSON(w, replicaReadFence{Cluster: n.cluster, Leader: n.config.ID, Address: n.Address(), Index: index, Term: term})
	})
}
