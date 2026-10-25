package replication

import (
	"context"
	"time"

	"github.com/hashicorp/raft"
)

type learnerDurableNotice struct {
	peer        Peer
	certificate *durableCertificate
}
type learnerDurableDelivery struct {
	term    uint64
	ctx     context.Context
	cancel  context.CancelFunc
	done    chan struct{}
	pending map[string]*learnerDurableNotice // Node.workMu owns this map
}

// One worker per leader coalesces the latest certificate for each current
// learner. A failed notification must not require a later write or seal to
// publish an already certified read barrier. Voter quorum completion is separate.
func (n *Node) notifyLearnersDurable(c *durableCertificate) {
	if c == nil || c.Body.Leader != n.config.ID {
		return
	}
	pending := map[string]*learnerDurableNotice{}
	for _, member := range c.Body.Configuration.Servers {
		if member.Suffrage != raft.Nonvoter {
			continue
		}
		for _, peer := range c.Body.Peers {
			if peer.ID == string(member.ID) {
				pending[peer.ID] = &learnerDurableNotice{peer, c}
				break
			}
		}
	}
	n.workMu.Lock()
	defer n.workMu.Unlock()
	if n.closing.Load() {
		return
	}
	state := n.learnerDurable
	if state != nil && state.term != c.Body.Term {
		state.cancel()
		n.learnerDurable = nil
		state = nil
	}
	if len(pending) == 0 {
		if state != nil {
			state.cancel()
			n.learnerDurable = nil
		}
		return
	}
	if state != nil {
		// Replace the configured peer set, retiring removed learners and keeping one
		// latest immutable certificate per peer, even during overlapping deliveries.
		for id, notice := range pending {
			if old := state.pending[id]; old != nil && old.certificate.Body.Index > notice.certificate.Body.Index {
				pending[id] = old
			}
		}
		state.pending = pending
		return
	}
	ctx, cancel := context.WithCancel(n.ctx)
	state = &learnerDurableDelivery{term: c.Body.Term, ctx: ctx, cancel: cancel, done: make(chan struct{}), pending: pending}
	n.learnerDurable = state
	n.workers.Add(1)
	go n.deliverLearnerDurable(state)
}

// Monitor calls this before waiting for SQL drain on a role/term transition.
// Canceling also interrupts an outstanding HTTP request, not just its backoff.
func (n *Node) stopStaleLearnerDurable(term uint64) {
	n.workMu.Lock()
	if state := n.learnerDurable; state != nil && state.term != term {
		state.cancel()
		n.learnerDurable = nil
	}
	n.workMu.Unlock()
}
func (n *Node) deliverLearnerDurable(state *learnerDurableDelivery) {
	defer n.workers.Done()
	defer state.cancel()
	defer close(state.done)
	defer func() {
		n.workMu.Lock()
		state.pending = nil
		if n.learnerDurable == state {
			n.learnerDurable = nil
		}
		n.workMu.Unlock()
	}()
	delay := 50 * time.Millisecond
	for {
		if state.ctx.Err() != nil {
			return
		}
		r := n.raftNode()
		if r == nil || r.State() != raft.Leader || r.CurrentTerm() != state.term {
			return
		}
		n.workMu.Lock()
		batch := make([]*learnerDurableNotice, 0, len(state.pending))
		for _, notice := range state.pending {
			batch = append(batch, notice)
		}
		if len(batch) == 0 {
			if n.learnerDurable == state {
				n.learnerDurable = nil
			}
			n.workMu.Unlock()
			return
		}
		n.workMu.Unlock()
		failed := false
		for _, notice := range batch {
			if state.ctx.Err() != nil {
				return
			}
			r := n.raftNode()
			if r == nil || r.State() != raft.Leader || r.CurrentTerm() != state.term {
				return
			}
			n.workMu.Lock()
			current := state.pending[notice.peer.ID]
			n.workMu.Unlock()
			if current != notice {
				continue
			}
			ctx, cancel := context.WithTimeout(state.ctx, n.config.OperationTimeout)
			err := n.postJSON(ctx, notice.peer.Control, "/v1/durability/finalize", notice.certificate, nil)
			cancel()
			if err != nil {
				failed = true
			}
			if err == nil {
				n.workMu.Lock()
				if state.pending[notice.peer.ID] == notice {
					delete(state.pending, notice.peer.ID)
				}
				n.workMu.Unlock()
			}
		}
		if !failed {
			delay = 50 * time.Millisecond
			continue
		}
		// New certificates replace pending state without bypassing failure backoff.
		// Rapid seals during an outage must not turn into an HTTP retry storm.
		timer := time.NewTimer(delay)
		select {
		case <-state.ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
			if delay < time.Second {
				delay *= 2
				if delay > time.Second {
					delay = time.Second
				}
			}
		}
	}
}
