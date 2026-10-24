package replication

import (
	"bytes"
	"context"
	"crypto/hmac"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/hashicorp/raft"
	"slopdb/storage"
)

type Peer struct {
	ID, Address, Control, Cluster string
	AutoPromote                   bool
}
type Member struct{ ID, Address, Suffrage string }
type Status struct {
	ReplicatedToken, DurableToken                                 DurabilityToken
	Durability                                                    Durability
	DurableIndex                                                  uint64
	DurabilityError                                               string
	Recovering                                                    bool
	ID, Cluster, Address, Control, State, LeaderID, LeaderAddress string
	Ready                                                         bool
	AppliedIndex, AppliedTerm, Term                               uint64
	Members                                                       []Member
	Error                                                         string
}

func peerKey(id string) []byte { return []byte("\x00\xffslopdb:raft:peer:" + id) }
func (n *Node) Status() Status {
	if n.raftNode() == nil {
		return Status{ID: n.config.ID, Cluster: n.cluster, Address: n.Address(), Control: n.ControlAddress(), State: "Recovering", Durability: n.config.Durability, DurableIndex: n.durableIndex(), Recovering: true, ReplicatedToken: n.CurrentToken(), DurableToken: n.DurableToken()}
	}
	address, id := n.raftNode().LeaderWithID()
	s := Status{ID: n.config.ID, Cluster: n.cluster, Address: n.Address(), Control: n.ControlAddress(), State: n.raftNode().State().String(), LeaderID: string(id), LeaderAddress: string(address), Ready: n.readyTerm.Load() != 0 && n.raftNode().State() == raft.Leader, AppliedIndex: n.applied.Load(), AppliedTerm: n.appliedTerm.Load(), Term: n.raftNode().CurrentTerm()}
	s.ReplicatedToken = n.CurrentToken()
	s.DurableToken = n.DurableToken()
	s.Durability = n.config.Durability
	s.DurableIndex = n.durableIndex()
	n.durability.publication.Lock()
	s.DurabilityError = n.durability.lastError
	n.durability.publication.Unlock()
	future := n.raftNode().GetConfiguration()
	if e := future.Error(); e == nil {
		for _, m := range future.Configuration().Servers {
			s.Members = append(s.Members, Member{string(m.ID), string(m.Address), m.Suffrage.String()})
		}
	} else {
		s.Error = e.Error()
	}
	if e := n.failure(); e != nil {
		s.Error = e.Error()
		s.Ready = false
	}
	return s
}
func (n *Node) getJSON(ctx context.Context, address, path string, dst any) error {
	response, e := n.request(ctx, address, http.MethodGet, path, nil)
	if e != nil {
		return e
	}
	defer response.Body.Close()
	return json.NewDecoder(io.LimitReader(response.Body, 1<<20)).Decode(dst)
}
func (n *Node) postJSON(ctx context.Context, address, path string, value, dst any) error {
	b, e := json.Marshal(value)
	if e != nil {
		return e
	}
	response, e := n.request(ctx, address, http.MethodPost, path, bytes.NewReader(b))
	if e != nil {
		return e
	}
	defer response.Body.Close()
	if dst == nil {
		_, e = io.Copy(io.Discard, io.LimitReader(response.Body, 1<<20))
		return e
	}
	return json.NewDecoder(io.LimitReader(response.Body, 1<<20)).Decode(dst)
}
func (n *Node) validatePeer(ctx context.Context, p Peer) (Status, error) {
	if p.ID == "" || len(p.ID) > 128 || p.ID == n.config.ID || p.Cluster != n.cluster {
		return Status{}, fmt.Errorf("invalid peer identity or cluster")
	}
	if _, e := parseEndpoint(p.Address); e != nil {
		return Status{}, e
	}
	if _, e := parseEndpoint(p.Control); e != nil {
		return Status{}, e
	}
	var status Status
	if e := n.getJSON(ctx, p.Control, "/v1/status", &status); e != nil {
		return status, e
	}
	if status.Durability != n.config.Durability {
		return status, fmt.Errorf("peer durability mode mismatch")
	}
	if status.ID != p.ID || status.Cluster != p.Cluster || status.Address != p.Address || status.Control != p.Control || status.Error != "" {
		return status, fmt.Errorf("peer status identity mismatch or failure")
	}
	return status, nil
}

// Join adds a learner and returns as soon as nonvoting membership is committed.
// Snapshot transfer and WAL catch-up proceed without blocking existing writes.
func (n *Node) Join(ctx context.Context, p Peer) (resultErr error) {
	if e := n.checkSQL(ctx); e != nil {
		return e
	}
	if _, e := n.validatePeer(ctx, p); e != nil {
		return e
	}
	n.membershipMu.Lock()
	defer func() {
		n.membershipMu.Unlock()
		if resultErr == nil && n.async() {
			resultErr = n.flushDurable(ctx, false)
		}
	}()
	future := n.raftNode().GetConfiguration()
	if e := n.wait(ctx, future); e != nil {
		return e
	}
	exists := false
	for _, member := range future.Configuration().Servers {
		if string(member.ID) == p.ID {
			if string(member.Address) != p.Address {
				return fmt.Errorf("node ID is already bound to another address")
			}
			exists = true
		}
		if string(member.Address) == p.Address && string(member.ID) != p.ID {
			return fmt.Errorf("Raft address already belongs to another ID")
		}
	}
	if e := n.propose(command{Version: 1, Kind: "peer", Peer: &p}, nil); e != nil {
		return e
	}
	if !exists {
		if e := n.wait(ctx, n.raftNode().AddNonvoter(raft.ServerID(p.ID), raft.ServerAddress(p.Address), future.Index(), n.config.OperationTimeout)); e != nil {
			return e
		}
	}
	if p.AutoPromote {
		n.startPromotion(p)
	}
	return nil
}
func (n *Node) peer(id string) (Peer, error) {
	n.fsmMu.Lock()
	defer n.fsmMu.Unlock()
	b, ok, e := n.database.ReplicationStorage().Get(peerKey(id))
	if e != nil {
		return Peer{}, e
	}
	if !ok {
		return Peer{}, fmt.Errorf("unknown peer %q", id)
	}
	var p Peer
	e = json.Unmarshal(b, &p)
	return p, e
}

// Promote proves durable FSM catch-up at a committed fence before requesting a
// voting change. Receipt of logs or Raft.AppliedIndex alone is insufficient.
func (n *Node) Promote(ctx context.Context, id string) (resultErr error) {
	if e := n.checkSQL(ctx); e != nil {
		return e
	}
	p, e := n.peer(id)
	if e != nil {
		return e
	}
	term := n.raftNode().CurrentTerm()
	if e = n.propose(command{Version: 1, Kind: "fence"}, nil); e != nil {
		return e
	}
	fence := n.applied.Load()
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		if n.raftNode().State() != raft.Leader || n.raftNode().CurrentTerm() != term {
			return n.notLeader()
		}
		target, e := n.validatePeer(ctx, p)
		if e == nil && target.AppliedIndex >= fence {
			break
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-n.ctx.Done():
			return ErrClosed
		case <-ticker.C:
		}
	}
	if n.async() {
		if e = n.flushDurable(ctx, false); e != nil {
			return e
		}
		certificate := n.currentCertificate()
		if e = n.postJSON(ctx, p.Control, "/v1/durability/finalize", certificate, nil); e != nil {
			return e
		}
	}
	n.membershipMu.Lock()
	defer func() {
		n.membershipMu.Unlock()
		if resultErr == nil && n.async() {
			resultErr = n.flushDurable(ctx, false)
		}
	}()
	future := n.raftNode().GetConfiguration()
	if e = n.wait(ctx, future); e != nil {
		return e
	}
	found := false
	for _, member := range future.Configuration().Servers {
		if string(member.ID) == id {
			if string(member.Address) != p.Address {
				return fmt.Errorf("peer address changed")
			}
			if member.Suffrage == raft.Voter {
				return nil
			}
			found = true
		}
	}
	if !found {
		return fmt.Errorf("peer is no longer a nonvoter")
	}
	if n.raftNode().CurrentTerm() != term {
		return n.notLeader()
	}
	return n.wait(ctx, n.raftNode().AddVoter(raft.ServerID(id), raft.ServerAddress(p.Address), future.Index(), n.config.OperationTimeout))
}
func (n *Node) startPromotion(p Peer) {
	n.workMu.Lock()
	defer n.workMu.Unlock()
	if n.closing.Load() || n.promoting[p.ID] {
		return
	}
	n.promoting[p.ID] = true
	n.workers.Add(1)
	go func() {
		defer n.workers.Done()
		defer func() { n.workMu.Lock(); delete(n.promoting, p.ID); n.workMu.Unlock() }()
		ctx, cancel := context.WithTimeout(n.ctx, 10*time.Minute)
		defer cancel()
		_ = n.Promote(ctx, p.ID)
	}()
}
func (n *Node) resumePromotions() {
	n.fsmMu.Lock()
	it := n.database.ReplicationStorage().NewIterator()
	prefix := peerKey("")
	it.Seek(prefix)
	var peers []Peer
	for it.Valid() && bytes.HasPrefix(it.Key().UserKey, prefix) {
		var p Peer
		if json.Unmarshal(it.Value(), &p) == nil && p.AutoPromote {
			peers = append(peers, p)
		}
		it.Next()
	}
	err := storage.IteratorError(it)
	it.Close()
	n.fsmMu.Unlock()
	if err != nil {
		n.fail(err)
		return
	}
	configuration := n.raftNode().GetConfiguration()
	if configuration.Error() != nil {
		return
	}
	for _, p := range peers {
		for _, m := range configuration.Configuration().Servers {
			if string(m.ID) == p.ID && m.Suffrage == raft.Nonvoter {
				n.startPromotion(p)
			}
		}
	}
}

// Remove changes membership. Transfer leadership before removing the leader.
func (n *Node) Remove(ctx context.Context, id string) (resultErr error) {
	if id == n.config.ID {
		return fmt.Errorf("transfer leadership before removing this node")
	}
	if e := n.checkSQL(ctx); e != nil {
		return e
	}
	n.membershipMu.Lock()
	defer func() {
		n.membershipMu.Unlock()
		if resultErr == nil && n.async() {
			resultErr = n.flushDurable(ctx, false)
		}
	}()
	future := n.raftNode().GetConfiguration()
	if e := n.wait(ctx, future); e != nil {
		return e
	}
	return n.wait(ctx, n.raftNode().RemoveServer(raft.ServerID(id), future.Index(), n.config.OperationTimeout))
}
func (n *Node) TransferLeadership(ctx context.Context, id string) error {
	if n.async() {
		if e := n.flushDurable(ctx, false); e != nil {
			return e
		}
	}
	if e := n.checkSQL(ctx); e != nil {
		return e
	}
	future := n.raftNode().GetConfiguration()
	if e := n.wait(ctx, future); e != nil {
		return e
	}
	for _, member := range future.Configuration().Servers {
		if string(member.ID) == id && member.Suffrage == raft.Voter {
			return n.wait(ctx, n.raftNode().LeadershipTransferToServer(member.ID, member.Address))
		}
	}
	return fmt.Errorf("unknown voting member")
}
func (n *Node) handler() http.Handler {
	mux := http.NewServeMux()
	n.durabilityHandlers(mux)
	mux.HandleFunc("/v1/status", func(w http.ResponseWriter, r *http.Request) {
		if !requireMethod(w, r, http.MethodGet) {
			return
		}
		writeJSON(w, n.Status())
	})
	mux.HandleFunc("/v1/snapshot", func(w http.ResponseWriter, r *http.Request) {
		if !requireMethod(w, r, http.MethodGet) {
			return
		}
		// Snapshot creation and export can exceed a normal RPC timeout. The
		// requesting context and node shutdown still cancel the operation.
		ctx := r.Context()
		if e := n.checkSQL(ctx); e != nil {
			http.Error(w, e.Error(), http.StatusConflict)
			return
		}
		var e error
		if n.async() {
			e = n.takeStableSnapshot(ctx)
		}
		var future raft.SnapshotFuture
		if !n.async() {
			future = n.raftNode().Snapshot()
			e = n.wait(ctx, future)
		}
		if errors.Is(e, raft.ErrNothingNewToSnapshot) {
			e = nil
		}
		if e != nil {
			http.Error(w, e.Error(), http.StatusServiceUnavailable)
			return
		}
		var meta *raft.SnapshotMeta
		var reader io.ReadCloser
		if future != nil && future.Error() == nil {
			meta, reader, e = future.Open()
		} else {
			list, le := n.snapshots.List()
			if le != nil {
				e = le
			} else if len(list) == 0 {
				e = fmt.Errorf("no snapshot available")
			} else {
				meta, reader, e = n.snapshots.Open(list[0].ID)
			}
		}
		if e != nil {
			http.Error(w, e.Error(), http.StatusServiceUnavailable)
			return
		}
		defer reader.Close()
		w.Header().Set("Content-Type", "application/octet-stream")
		_ = writeSnapshotEnvelope(w, meta, reader)
	})
	mux.HandleFunc("/v1/join", func(w http.ResponseWriter, r *http.Request) {
		if !requireMethod(w, r, http.MethodPost) {
			return
		}
		var p Peer
		if !decodeJSON(w, r, &p) {
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), n.config.OperationTimeout)
		defer cancel()
		if e := n.Join(ctx, p); e != nil {
			http.Error(w, e.Error(), http.StatusConflict)
			return
		}
		writeJSON(w, n.Status())
	})
	for _, operation := range []string{"promote", "remove", "transfer"} {
		operation := operation
		mux.HandleFunc("/v1/"+operation, func(w http.ResponseWriter, r *http.Request) {
			if !requireMethod(w, r, http.MethodPost) {
				return
			}
			var p struct{ ID string }
			if !decodeJSON(w, r, &p) {
				return
			}
			ctx, cancel := context.WithTimeout(r.Context(), 10*time.Minute)
			defer cancel()
			var err error
			switch operation {
			case "promote":
				err = n.Promote(ctx, p.ID)
			case "remove":
				err = n.Remove(ctx, p.ID)
			case "transfer":
				err = n.TransferLeadership(ctx, p.ID)
			}
			if err != nil {
				http.Error(w, err.Error(), http.StatusConflict)
				return
			}
			writeJSON(w, n.Status())
		})
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		supplied := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		if !hmac.Equal([]byte(supplied), []byte(n.config.Token)) {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		mux.ServeHTTP(w, r)
	})
}
func requireMethod(w http.ResponseWriter, r *http.Request, method string) bool {
	if r.Method != method {
		w.Header().Set("Allow", method)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return false
	}
	return true
}
func decodeJSON(w http.ResponseWriter, r *http.Request, dst any) bool {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	d := json.NewDecoder(r.Body)
	d.DisallowUnknownFields()
	if e := d.Decode(dst); e != nil {
		http.Error(w, e.Error(), http.StatusBadRequest)
		return false
	}
	var extra any
	if e := d.Decode(&extra); e != io.EOF {
		http.Error(w, "expected one JSON object", http.StatusBadRequest)
		return false
	}
	return true
}
func writeJSON(w http.ResponseWriter, value any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(value)
}
