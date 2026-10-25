package replication

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"time"

	"github.com/hashicorp/raft"
	"slopdb/storage"
)

type Durability string

const (
	DurabilitySync  Durability = "sync"
	DurabilityAsync Durability = "async"
)

var ErrRecovering = errors.New("async replica is recovering; a live quorum or explicit stable-point recovery is required")
var ErrDurabilityLag = errors.New("durable frontier is too old; waiting for a stable quorum")
var ErrHistory = errors.New("replication history or durability certificate mismatch")
var ErrRetired = errors.New("node was retired by stable-point recovery; bootstrap or clone into a new node directory")

// DurabilityToken names a prefix in a cluster incarnation. Tokens from a prior
// disaster-recovery incarnation must never be silently accepted in a new one.
type DurabilityToken struct {
	Cluster     string
	Index, Term uint64
}
type durableBody struct {
	Cluster, Leader    string
	Index, Term        uint64
	Hash               [32]byte
	Configuration      raft.Configuration
	ConfigurationIndex uint64
	Peers              []Peer
}
type durableReceipt struct{ ID, MAC string }
type durableCertificate struct {
	Body     durableBody
	Receipts []durableReceipt
}
type durabilityState struct {
	coordinatorGate      chan struct{}
	certificateWrite     sync.Mutex
	publication          sync.Mutex
	certificate          *durableCertificate
	publishedCertificate *durableCertificate
	committed            uint64
	changed              chan struct{}
	pendingSince         time.Time
	sealingIndex         uint64
	postSealSince        time.Time
	preparedReceipt      durableReceipt // protected by diskStore.mu
	lastError            string
}

var certificateKey = []byte("sDurabilityCertificate")
var preparedKey = []byte("sDurabilityPrepared")

func (n *Node) async() bool { return n.config.Durability == DurabilityAsync }
func receiptMAC(token string, body durableBody, id string) string {
	value, _ := json.Marshal(body)
	h := hmac.New(sha256.New, []byte(token))
	h.Write([]byte("slopdb-stable-prefix-v1\x00"))
	h.Write(value)
	h.Write([]byte("\x00" + id))
	return hex.EncodeToString(h.Sum(nil))
}
func validateCertificate(c *durableCertificate, token, cluster string) error {
	if c == nil || c.Body.Cluster != cluster || c.Body.Index == 0 || c.Body.Term == 0 || c.Body.ConfigurationIndex == 0 || c.Body.ConfigurationIndex > c.Body.Index {
		return ErrHistory
	}
	voters := map[string]bool{}
	ids := map[string]bool{}
	addresses := map[string]bool{}
	peers := map[string]Peer{}
	for _, p := range c.Body.Peers {
		if p.ID == "" || p.Cluster != cluster {
			return ErrHistory
		}
		if _, exists := peers[p.ID]; exists {
			return ErrHistory
		}
		if _, e := parseEndpoint(p.Control); e != nil {
			return e
		}
		peers[p.ID] = p
	}
	for _, s := range c.Body.Configuration.Servers {
		if s.ID == "" || ids[string(s.ID)] || addresses[string(s.Address)] {
			return ErrHistory
		}
		addresses[string(s.Address)] = true
		ids[string(s.ID)] = true
		p, ok := peers[string(s.ID)]
		if !ok || p.Address != string(s.Address) {
			return ErrHistory
		}
		if s.Suffrage == raft.Voter {
			if p.NonVoting {
				return ErrHistory
			}
			if voters[string(s.ID)] {
				return ErrHistory
			}
			voters[string(s.ID)] = true
		}
	}
	if len(voters) == 0 || !voters[c.Body.Leader] || len(peers) != len(ids) {
		return ErrHistory
	}
	if peers[c.Body.Leader].LogsOnly {
		return ErrHistory
	}
	seen := map[string]bool{}
	for _, r := range c.Receipts {
		if !voters[r.ID] || seen[r.ID] || !hmac.Equal([]byte(r.MAC), []byte(receiptMAC(token, c.Body, r.ID))) {
			return ErrHistory
		}
		seen[r.ID] = true
	}
	if len(seen) < len(voters)/2+1 {
		return fmt.Errorf("%w: certificate lacks a voting majority", ErrHistory)
	}
	return nil
}
func (n *Node) currentCertificate() *durableCertificate {
	n.durability.publication.Lock()
	defer n.durability.publication.Unlock()
	return n.durability.certificate
}
func (n *Node) durableIndex() uint64 {
	if !n.async() {
		return n.applied.Load()
	}
	n.durability.publication.Lock()
	defer n.durability.publication.Unlock()
	return n.durability.committed
}
func (n *Node) advanceDurable(certificate *durableCertificate) {
	index := certificate.Body.Index
	n.durableReads.mu.Lock()
	defer n.durableReads.mu.Unlock()
	n.durability.publication.Lock()
	defer n.durability.publication.Unlock()
	if index > n.durability.committed {
		n.durability.committed = index
		n.durability.publishedCertificate = certificate
	}
	n.durableReads.publish(certificate)
	if n.applied.Load() <= index {
		n.durability.pendingSince = time.Time{}
	} else if n.durability.sealingIndex != 0 && index >= n.durability.sealingIndex {
		// Proposals can continue during a seal. The next unstable prefix starts
		// at its first application after the captured fence, not at the first
		// write of the previous prefix or at the time this seal completed.
		n.durability.pendingSince = n.durability.postSealSince
	}
	if index >= n.durability.sealingIndex {
		n.durability.sealingIndex = 0
		n.durability.postSealSince = time.Time{}
	}
	if n.durability.changed != nil {
		close(n.durability.changed)
	}
	n.durability.changed = make(chan struct{})
}
func (n *Node) installCertificate(c *durableCertificate) error {
	if err := validateCertificate(c, n.config.Token, n.cluster); err != nil {
		return err
	}
	n.durability.certificateWrite.Lock()
	defer n.durability.certificateWrite.Unlock()
	n.durability.publication.Lock()
	old := n.durability.certificate
	n.durability.publication.Unlock()
	if old != nil {
		if c.Body.Index < old.Body.Index {
			return nil
		}
		if c.Body.Index == old.Body.Index {
			if c.Body.Hash != old.Body.Hash {
				return ErrHistory
			}
			return nil
		}
	}
	value, _ := json.Marshal(c)
	if err := n.store.tree.ApplyReplicated([]storage.Mutation{{Key: certificateKey, Value: value}}); err != nil {
		return err
	}
	n.durability.publication.Lock()
	defer n.durability.publication.Unlock()
	n.durability.certificate = c

	if n.durability.changed != nil {
		close(n.durability.changed)
	}
	n.durability.changed = make(chan struct{})
	return nil
}
func (n *Node) loadCertificate() error {
	value, found, err := n.store.tree.Get(certificateKey)
	if err != nil || !found {
		return err
	}
	var c durableCertificate
	if err = json.Unmarshal(value, &c); err != nil {
		return err
	}
	if err = validateCertificate(&c, n.config.Token, n.cluster); err != nil {
		return err
	}
	n.durability.certificate = &c
	n.durability.committed = c.Body.Index
	n.durability.publishedCertificate = &c
	return nil
}
func (n *Node) noteUnstable(index uint64) {
	if !n.async() {
		return
	}
	n.durability.publication.Lock()
	defer n.durability.publication.Unlock()
	if (n.durability.certificate == nil || index > n.durability.certificate.Body.Index) && n.durability.pendingSince.IsZero() {
		n.durability.pendingSince = time.Now()
	}
	if n.durability.sealingIndex != 0 && index > n.durability.sealingIndex && n.durability.postSealSince.IsZero() {
		n.durability.postSealSince = time.Now()
	}
}
func (n *Node) checkDurabilityLag() error {
	if !n.async() || n.config.MaxDurabilityLag == 0 {
		return nil
	}
	n.durability.publication.Lock()
	defer n.durability.publication.Unlock()
	if !n.durability.pendingSince.IsZero() && time.Since(n.durability.pendingSince) > n.config.MaxDurabilityLag {
		return ErrDurabilityLag
	}
	return nil
}
func (n *Node) CurrentToken() DurabilityToken {
	n.fsmMu.Lock()
	defer n.fsmMu.Unlock()
	return DurabilityToken{n.cluster, n.applied.Load(), n.appliedTerm.Load()}
}

// DurableToken returns the last published durable prefix, rather than the live
// applied position. Persist this token to require a minimum disaster restore.
func (n *Node) DurableToken() DurabilityToken {
	if !n.async() {
		return n.CurrentToken()
	}
	n.durability.publication.Lock()
	defer n.durability.publication.Unlock()
	if c := n.durability.publishedCertificate; c != nil {
		return DurabilityToken{c.Body.Cluster, c.Body.Index, c.Body.Term}
	}
	return DurabilityToken{Cluster: n.cluster}
}

// WaitDurable makes the requested prefix stable on a voting majority. A returned
// error does not undo an already acknowledged asynchronous transaction.
func (n *Node) WaitDurable(ctx context.Context, token DurabilityToken) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if token.Cluster != n.cluster {
		return ErrHistory
	}
	if token.Index <= n.durableIndex() {
		return nil
	}
	if !n.async() {
		return fmt.Errorf("token is ahead of the applied prefix")
	}
	if err := n.flushDurable(ctx, false); err != nil {
		return err
	}
	if token.Index > n.durableIndex() {
		return fmt.Errorf("token is ahead of the durable prefix")
	}
	return nil
}
func (n *Node) FlushDurable(ctx context.Context) error { return n.WaitDurable(ctx, n.CurrentToken()) }

// localReceipt persists all preceding consensus WAL data plus its evidence. The
// application WAL can be rebuilt from a durable snapshot and this log prefix.
func (n *Node) localReceipt(ctx context.Context, body durableBody) (durableReceipt, error) {
	if !n.async() || n.raftNode() == nil {
		return durableReceipt{}, ErrRecovering
	}
	r := n.raftNode()
	address, id := r.LeaderWithID()
	_ = address
	if r.CurrentTerm() != body.Term || string(id) != body.Leader || body.Cluster != n.cluster {
		return durableReceipt{}, ErrHistory
	}
	member := false
	for _, s := range body.Configuration.Servers {
		if string(s.ID) == n.config.ID {
			member = true
		}
	}
	if !member {
		return durableReceipt{}, ErrHistory
	}
	for n.applied.Load() < body.Index {
		select {
		case <-ctx.Done():
			return durableReceipt{}, ctx.Err()
		case <-n.ctx.Done():
			return durableReceipt{}, ErrClosed
		case <-time.After(time.Millisecond):
		}
	}
	if n.configurationIndex.Load() != body.ConfigurationIndex {
		return durableReceipt{}, ErrHistory
	}
	configuration := r.GetConfiguration()
	if err := n.wait(ctx, configuration); err != nil {
		return durableReceipt{}, err
	}
	if !reflect.DeepEqual(configuration.Configuration(), body.Configuration) {
		return durableReceipt{}, ErrHistory
	}
	n.store.mu.Lock()
	defer n.store.mu.Unlock()
	hash, err := n.store.hashAt(body.Index)
	if err != nil {
		return durableReceipt{}, err
	}
	if hash != body.Hash {
		return durableReceipt{}, ErrHistory
	}
	receipt := durableReceipt{n.config.ID, receiptMAC(n.config.Token, body, n.config.ID)}
	// Publish repeats prepare for the same body. Its receipt already proves
	// this prefix was synchronized in this process incarnation; avoid issuing
	// the same WAL fsync twice. Hash, term and membership were checked above.
	if n.durability.preparedReceipt == receipt {
		return receipt, nil
	}
	value, _ := json.Marshal(struct {
		Body    durableBody
		Receipt durableReceipt
	}{body, receipt})
	if err = n.store.tree.ApplyReplicated([]storage.Mutation{{Key: preparedKey, Value: value}}); err != nil {
		return durableReceipt{}, err
	}
	if r.CurrentTerm() != body.Term || r.State() == raft.Shutdown {
		return durableReceipt{}, ErrHistory
	}
	n.durability.preparedReceipt = receipt
	return receipt, nil
}
func (n *Node) receiveCertificate(ctx context.Context, c *durableCertificate) error {
	if err := validateCertificate(c, n.config.Token, n.cluster); err != nil {
		return err
	}
	if previous := n.currentCertificate(); previous != nil && previous.Body.Index == c.Body.Index && receiptMAC(n.config.Token, previous.Body, n.config.ID) == receiptMAC(n.config.Token, c.Body, n.config.ID) {
		return nil
	}
	if _, err := n.localReceipt(ctx, c.Body); err != nil {
		return err
	}
	return n.installCertificate(c)
}

// flushDurable captures a committed fence, then certifies that fixed prefix
// while later proposals continue. Membership remains fixed throughout the seal;
// snapshot seals also freeze proposals until the snapshot is complete. It never
// takes SQL catalog locks, because SQL commit hooks may be waiting here.
func (n *Node) flushDurable(ctx context.Context, snapshot bool) error {
	return n.sealDurable(ctx, snapshot, false)
}

func (n *Node) sealDurable(ctx context.Context, snapshot, force bool) error {
	if !n.async() {
		if snapshot {
			return n.wait(ctx, n.raftNode().Snapshot())
		}
		return nil
	}
	n.durability.publication.Lock()
	if n.durability.coordinatorGate == nil {
		n.durability.coordinatorGate = make(chan struct{}, 1)
	}
	gate := n.durability.coordinatorGate
	n.durability.publication.Unlock()
	select {
	case gate <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	case <-n.ctx.Done():
		return ErrClosed
	}
	defer func() { <-gate }()
	if err := ctx.Err(); err != nil {
		return err
	}
	if n.raftNode() == nil || n.raftNode().State() != raft.Leader {
		return n.notLeader()
	}
	if !snapshot && !force && n.applied.Load() <= n.durableIndex() {
		return nil
	}
	n.membershipMu.Lock()
	defer n.membershipMu.Unlock()
	n.proposalMu.Lock()
	proposalHeld := true
	defer func() {
		if proposalHeld {
			n.proposalMu.Unlock()
		}
	}()
	r := n.raftNode()
	term := r.CurrentTerm()
	value, _ := json.Marshal(command{Version: 1, Kind: "fence"})
	future := r.Apply(value, n.config.OperationTimeout)
	if err := n.wait(ctx, future); err != nil {
		return err
	}
	if e, ok := future.Response().(error); ok {
		return e
	}
	index := future.Index()
	configuration := r.GetConfiguration()
	if err := n.wait(ctx, configuration); err != nil {
		return err
	}
	hash, err := n.store.historyHash(index)
	if err != nil {
		return err
	}
	body := durableBody{Cluster: n.cluster, Leader: n.config.ID, Index: index, Term: term, Hash: hash, Configuration: configuration.Configuration(), ConfigurationIndex: n.configurationIndex.Load()}
	n.fsmMu.Lock()
	for _, member := range body.Configuration.Servers {
		p := Peer{ID: string(member.ID), Address: string(member.Address), Cluster: n.cluster}
		if p.ID == n.config.ID {
			p.Control = n.ControlAddress()
		} else {
			data, found, e := n.database.ReplicationStorage().Get(peerKey(p.ID))
			if e != nil {
				err = e
				break
			}
			if !found {
				if stable := n.currentCertificate(); stable != nil {
					for _, known := range stable.Body.Peers {
						if known.ID == p.ID && known.Address == p.Address {
							p = known
							found = true
							break
						}
					}
				}
				if !found {
					err = fmt.Errorf("missing control endpoint for %s", p.ID)
					break
				}
				data, _ = json.Marshal(p)
			}
			if e = json.Unmarshal(data, &p); e != nil {
				err = e
				break
			}
		}
		body.Peers = append(body.Peers, p)
	}
	n.fsmMu.Unlock()
	if err != nil {
		return err
	}
	n.durability.publication.Lock()
	n.durability.sealingIndex = index
	n.durability.postSealSince = time.Time{}
	n.durability.publication.Unlock()
	if !snapshot {
		n.proposalMu.Unlock()
		proposalHeld = false
	}
	certificate := &durableCertificate{Body: body}
	voters := 0
	for _, m := range body.Configuration.Servers {
		if m.Suffrage == raft.Voter {
			voters++
		}
	}
	type result struct {
		receipt durableReceipt
		err     error
	}
	responses := make(chan result, len(body.Peers))
	requestCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	for _, member := range body.Configuration.Servers {
		if member.Suffrage != raft.Voter {
			continue
		}
		member := member
		go func() {
			var receipt durableReceipt
			var e error
			if string(member.ID) == n.config.ID {
				receipt, e = n.localReceipt(requestCtx, body)
			} else {
				for _, p := range body.Peers {
					if p.ID == string(member.ID) {
						e = n.postJSON(requestCtx, p.Control, "/v1/durability/prepare", body, &receipt)
						break
					}
				}
			}
			responses <- result{receipt, e}
		}()
	}
	for received := 0; received < voters; received++ {
		select {
		case response := <-responses:
			if response.err == nil {
				certificate.Receipts = append(certificate.Receipts, response.receipt)
			}
		case <-ctx.Done():
			return ctx.Err()
		case <-n.ctx.Done():
			return ErrClosed
		}
		if len(certificate.Receipts) >= voters/2+1 {
			break
		}
	}
	if err = validateCertificate(certificate, n.config.Token, n.cluster); err != nil {
		return err
	}
	// First persist locally. Remote publication verifies and synchronizes its own
	// prefix as well; nonvoters also receive certificates for future recovery.
	if err = n.installCertificate(certificate); err != nil {
		return err
	}
	successes := 1
	published := make(chan error, len(body.Peers))
	remoteVoters := 0
	for _, member := range body.Configuration.Servers {
		if string(member.ID) == n.config.ID {
			continue
		}
		member := member
		for _, p := range body.Peers {
			if p.ID == string(member.ID) {
				p := p
				if member.Suffrage == raft.Voter {
					remoteVoters++
					go func() { published <- n.postJSON(requestCtx, p.Control, "/v1/durability/publish", certificate, nil) }()
				} else {
					go func() { _ = n.postJSON(requestCtx, p.Control, "/v1/durability/publish", certificate, nil) }()
				}
				break
			}
		}
	}
	for received := 0; successes < voters/2+1 && received < remoteVoters; received++ {
		select {
		case e := <-published:
			if e == nil {
				successes++
			}
		case <-ctx.Done():
			return ctx.Err()
		case <-n.ctx.Done():
			return ErrClosed
		}
	}
	if successes < voters/2+1 {
		return fmt.Errorf("durability certificate was not stored on a majority")
	}
	if r.State() != raft.Leader || r.CurrentTerm() != term {
		return n.notLeader()
	}
	n.advanceDurable(certificate)
	n.notifyLearnersDurable(certificate)
	finalized := make(chan error, len(body.Peers))
	finalVoters := 0
	for _, member := range body.Configuration.Servers {
		if member.Suffrage != raft.Voter || string(member.ID) == n.config.ID {
			continue
		}
		for _, p := range body.Peers {
			if p.ID == string(member.ID) {
				p := p
				finalVoters++
				go func() { finalized <- n.postJSON(requestCtx, p.Control, "/v1/durability/finalize", certificate, nil) }()
				break
			}
		}
	}
	finalSuccess := 1
	for received := 0; received < finalVoters && finalSuccess < voters/2+1; received++ {
		select {
		case e := <-finalized:
			if e == nil {
				finalSuccess++
			}
		case <-ctx.Done():
			return ctx.Err()
		case <-n.ctx.Done():
			return ErrClosed
		}
	}
	// The quorum certificate is already durable. Failure to notify a follower
	// cannot undo it; a later seal or re-clone supplies the notification.
	if snapshot {
		if err = n.wait(ctx, r.Snapshot()); err != nil && !errors.Is(err, raft.ErrNothingNewToSnapshot) {
			return err
		}
	}
	return nil
}
func (n *Node) durabilityLoop() {
	defer n.workers.Done()
	ticker := time.NewTicker(n.config.DurabilityInterval)
	defer ticker.Stop()
	for {
		select {
		case <-n.ctx.Done():
			return
		case <-ticker.C:
		}
		if n.raftNode() == nil || n.readyTerm.Load() == 0 || n.raftNode().State() != raft.Leader || n.applied.Load() <= n.durableIndex() {
			continue
		}
		ctx, cancel := context.WithTimeout(n.ctx, n.config.OperationTimeout)
		err := n.flushDurable(ctx, false)
		cancel()
		n.durability.publication.Lock()
		if err != nil {
			n.durability.lastError = err.Error()
		} else {
			n.durability.lastError = ""
		}
		n.durability.publication.Unlock()
	}
}
func (n *Node) durabilityHandlers(mux *http.ServeMux) {
	mux.HandleFunc("/v1/durability/prepare", func(w http.ResponseWriter, r *http.Request) {
		if !requireMethod(w, r, http.MethodPost) {
			return
		}
		var body durableBody
		if !decodeJSON(w, r, &body) {
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), n.config.OperationTimeout)
		defer cancel()
		receipt, e := n.localReceipt(ctx, body)
		if e != nil {
			http.Error(w, e.Error(), http.StatusConflict)
			return
		}
		writeJSON(w, receipt)
	})
	mux.HandleFunc("/v1/durability/publish", func(w http.ResponseWriter, r *http.Request) {
		if !requireMethod(w, r, http.MethodPost) {
			return
		}
		var c durableCertificate
		if !decodeJSON(w, r, &c) {
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), n.config.OperationTimeout)
		defer cancel()
		if e := n.receiveCertificate(ctx, &c); e != nil {
			http.Error(w, e.Error(), http.StatusConflict)
			return
		}
		writeJSON(w, n.CurrentToken())
	})
	mux.HandleFunc("/v1/durability/finalize", func(w http.ResponseWriter, r *http.Request) {
		if !requireMethod(w, r, http.MethodPost) {
			return
		}
		var c durableCertificate
		if !decodeJSON(w, r, &c) {
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), n.config.OperationTimeout)
		defer cancel()
		if e := n.receiveCertificate(ctx, &c); e != nil {
			http.Error(w, e.Error(), http.StatusConflict)
			return
		}
		n.advanceDurable(&c)
		writeJSON(w, n.CurrentToken())
	})
	mux.HandleFunc("/v1/durability/wait", func(w http.ResponseWriter, r *http.Request) {
		if !requireMethod(w, r, http.MethodPost) {
			return
		}
		var token DurabilityToken
		if !decodeJSON(w, r, &token) {
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), n.config.OperationTimeout)
		defer cancel()
		if e := n.WaitDurable(ctx, token); e != nil {
			http.Error(w, e.Error(), http.StatusConflict)
			return
		}
		writeJSON(w, n.Status())
	})
}
func retiredNode(directory string) error {
	if _, err := os.Stat(filepath.Join(directory, "RETIRED")); err == nil {
		return ErrRetired
	} else if !os.IsNotExist(err) {
		return err
	}
	return nil
}
