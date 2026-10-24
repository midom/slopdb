package replication

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sync"

	"github.com/hashicorp/raft"
	"slopdb/db"
	"slopdb/sql"
	"slopdb/storage"
)

var ErrDurableReadUnavailable = errors.New("no retained certified durable read barrier")

type durableReadView struct {
	tree                  *storage.LSMTree
	sequence, index, term uint64
	catalog               *sql.Schema
}
type durableReadViews struct {
	mu      sync.Mutex
	pending map[uint64]*durableReadView
	latest  *durableReadView
	changed chan struct{}
}
type durableReadBarrierReply struct {
	Token       DurabilityToken
	Certificate *durableCertificate
}

// Called while FSM application is excluded, exactly at a barrier checkpoint.
// Keep one published view and a bounded set of in-flight certificates. Failed
// seals or delayed old finalize requests cannot grow the candidate set forever.
func (n *Node) captureDurableView(index, term uint64, published bool) {
	if index == 0 {
		return
	}
	views := &n.durableReads
	views.mu.Lock()
	defer views.mu.Unlock()
	tree := n.database.ReplicationStorage()
	view := &durableReadView{tree: tree, sequence: tree.Snapshot(), index: index, term: term}
	if published {
		views.replaceLatest(view)
	} else {
		if views.pending == nil {
			views.pending = make(map[uint64]*durableReadView)
		}
		if old := views.pending[index]; old != nil {
			old.tree.ReleaseSnapshot(old.sequence)
		}
		views.pending[index] = view
		for len(views.pending) > 8 {
			oldest := ^uint64(0)
			for index := range views.pending {
				if index < oldest {
					oldest = index
				}
			}
			old := views.pending[oldest]
			old.tree.ReleaseSnapshot(old.sequence)
			delete(views.pending, oldest)
		}
	}
	views.notify()
}
func (v *durableReadViews) notify() {
	if v.changed != nil {
		close(v.changed)
	}
	v.changed = make(chan struct{})
}
func (v *durableReadViews) replaceLatest(view *durableReadView) {
	if v.latest != nil {
		v.latest.tree.ReleaseSnapshot(v.latest.sequence)
	}
	v.latest = view
}

// The caller holds views.mu before durability.publication, matching certificate
// publication and preventing a reader from selecting an uncertified candidate.
func (v *durableReadViews) publish(c *durableCertificate) {
	if view := v.pending[c.Body.Index]; view != nil && view.term == c.Body.Term {
		delete(v.pending, c.Body.Index)
		if v.latest == nil || view.index > v.latest.index {
			v.replaceLatest(view)
		} else {
			view.tree.ReleaseSnapshot(view.sequence)
		}
	}
	for index, old := range v.pending {
		if index <= c.Body.Index {
			old.tree.ReleaseSnapshot(old.sequence)
			delete(v.pending, index)
		}
	}
	v.notify()
}
func (n *Node) clearDurableViews() {
	v := &n.durableReads
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.latest != nil {
		v.latest.tree.ReleaseSnapshot(v.latest.sequence)
		v.latest = nil
	}
	for index, old := range v.pending {
		old.tree.ReleaseSnapshot(old.sequence)
		delete(v.pending, index)
	}
	v.notify()
}

func (n *Node) readBarrier(ctx context.Context) db.ReadBarrier {
	if mode := db.ReadBarrierFromContext(ctx); mode != db.ReadBarrierDefault {
		return mode
	}
	return n.config.ReadBarrier
}
func durableBarrier(mode db.ReadBarrier) bool {
	return mode == db.ReadBarrierPrevious || mode == db.ReadBarrierNext
}

func (n *Node) checkDurableReadHealth(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if n.closing.Load() {
		return ErrClosed
	}
	if err := n.failure(); err != nil {
		return err
	}
	if n.recovering.Load() || n.raftNode() == nil {
		return ErrRecovering
	}
	return nil
}

func (n *Node) captureDurableRead(ctx context.Context, session *sql.Schema, mode db.ReadBarrier) (bool, error) {
	if err := n.checkDurableReadHealth(ctx); err != nil {
		return true, err
	}
	var target uint64
	if mode == db.ReadBarrierNext {
		token, err := n.nextReadBarrier(ctx)
		if err != nil {
			return true, err
		}
		target = token.Index
	} else {
		target = n.durableIndex()
	}
	if _, deadline := ctx.Deadline(); !deadline && mode == db.ReadBarrierNext {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, n.config.OperationTimeout)
		defer cancel()
	}
	v := &n.durableReads
	for {
		if err := n.checkDurableReadHealth(ctx); err != nil {
			return true, err
		}
		v.mu.Lock()
		view := v.latest
		if view != nil && view.index >= target && view.tree == n.database.ReplicationStorage() {
			if minimum := db.MinimumPositionFromContext(ctx); minimum != "" {
				token, err := n.parsePosition(minimum)
				if err != nil {
					v.mu.Unlock()
					return true, err
				}
				if token.Index > view.index {
					v.mu.Unlock()
					return true, fmt.Errorf("%w: minimum position is beyond the durable barrier", ErrDurableReadUnavailable)
				}
			}
			if view.catalog == nil {
				catalog, err := sql.NewReplicaSchemaAtSnapshot(view.tree, view.sequence, n.storageOptions())
				if err != nil {
					v.mu.Unlock()
					return true, err
				}
				view.catalog = catalog
			}
			err := session.CaptureReplicaStatementAt(view.catalog, view.index, view.sequence)
			v.mu.Unlock()
			if err == nil && ctx.Err() != nil {
				session.EndStatement()
				err = ctx.Err()
			}
			return true, err
		}
		if mode == db.ReadBarrierPrevious {
			v.mu.Unlock()
			return true, ErrDurableReadUnavailable
		}
		if v.changed == nil {
			v.changed = make(chan struct{})
		}
		changed := v.changed
		v.mu.Unlock()
		select {
		case <-changed:
		case <-ctx.Done():
			return true, ctx.Err()
		case <-n.ctx.Done():
			return true, ErrClosed
		}
	}
}

func (n *Node) nextReadBarrier(ctx context.Context) (DurabilityToken, error) {
	call, cancel := context.WithTimeout(ctx, n.config.OperationTimeout)
	defer cancel()
	if n.raftNode().State() == raft.Leader {
		reply, err := n.makeNextReadBarrier(call)
		return reply.Token, err
	}
	_, id := n.raftNode().LeaderWithID()
	if err := n.lockReadFSM(call); err != nil {
		return DurabilityToken{}, err
	}
	data, found, err := n.database.ReplicationStorage().Get(peerKey(string(id)))
	n.fsmMu.Unlock()
	if err != nil {
		return DurabilityToken{}, err
	}
	if !found {
		return DurabilityToken{}, n.notLeader()
	}
	var peer Peer
	if err = json.Unmarshal(data, &peer); err != nil {
		return DurabilityToken{}, err
	}
	if peer.ID != string(id) || peer.Cluster != n.cluster {
		return DurabilityToken{}, ErrHistory
	}
	var reply durableReadBarrierReply
	if err = n.postJSON(call, peer.Control, "/v1/durability/read-barrier", struct{}{}, &reply); err != nil {
		return reply.Token, err
	}
	token := reply.Token
	if token.Cluster != n.cluster || token.Index == 0 || token.Term == 0 {
		return token, ErrHistory
	}
	if n.async() {
		c := reply.Certificate
		if c == nil || c.Body.Index != token.Index || c.Body.Term != token.Term {
			return token, ErrHistory
		}
		if err = n.receiveCertificate(call, c); err != nil {
			return token, err
		}
		n.advanceDurable(c)
	}
	return token, nil
}
func (n *Node) makeNextReadBarrier(ctx context.Context) (durableReadBarrierReply, error) {
	if err := n.checkSQL(ctx); err != nil {
		return durableReadBarrierReply{}, err
	}
	if n.async() {
		if err := n.sealDurable(ctx, false, true); err != nil {
			return durableReadBarrierReply{}, err
		}
		n.durability.publication.Lock()
		c := n.durability.publishedCertificate
		n.durability.publication.Unlock()
		if c == nil {
			return durableReadBarrierReply{}, ErrDurableReadUnavailable
		}
		return durableReadBarrierReply{DurabilityToken{c.Body.Cluster, c.Body.Index, c.Body.Term}, c}, nil
	}
	// This is a read-only fence: cancellation can abandon its response without
	// creating an unknown user-write outcome. Do not use propose's irrevocable
	// write wait, which can outlive a query deadline while FSM application stalls.
	payload, err := marshalCommand(command{Version: 1, Kind: "fence"})
	if err != nil {
		return durableReadBarrierReply{}, err
	}
	n.proposalMu.RLock()
	if _, err = n.checkReadyLeader(ctx); err != nil {
		n.proposalMu.RUnlock()
		return durableReadBarrierReply{}, err
	}
	future := n.raftNode().Apply(payload, n.config.OperationTimeout)
	n.proposalMu.RUnlock()
	if err = n.wait(ctx, future); err != nil {
		return durableReadBarrierReply{}, err
	}
	if err, ok := future.Response().(error); ok {
		return durableReadBarrierReply{}, err
	}
	n.durableReads.mu.Lock()
	defer n.durableReads.mu.Unlock()
	view := n.durableReads.latest
	if view == nil || view.index < future.Index() {
		return durableReadBarrierReply{}, ErrDurableReadUnavailable
	}
	return durableReadBarrierReply{Token: DurabilityToken{n.cluster, view.index, view.term}}, nil
}

func (n *Node) notifyLearnerDurable(p Peer, c *durableCertificate) {
	n.workMu.Lock()
	if n.closing.Load() {
		n.workMu.Unlock()
		return
	}
	n.workers.Add(1)
	n.workMu.Unlock()
	go func() {
		defer n.workers.Done()
		ctx, cancel := context.WithTimeout(n.ctx, n.config.OperationTimeout)
		defer cancel()
		_ = n.postJSON(ctx, p.Control, "/v1/durability/finalize", c, nil)
	}()
}
func (n *Node) durableReadHandlers(mux *http.ServeMux) {
	mux.HandleFunc("/v1/durability/read-barrier", func(w http.ResponseWriter, r *http.Request) {
		if !requireMethod(w, r, http.MethodPost) {
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), n.config.OperationTimeout)
		defer cancel()
		token, err := n.makeNextReadBarrier(ctx)
		if err != nil {
			http.Error(w, fmt.Sprint(err), http.StatusServiceUnavailable)
			return
		}
		writeJSON(w, token)
	})
}
