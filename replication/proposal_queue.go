package replication

import (
	"sync"

	"slopdb/storage"
)

const maxWALProposalBatches = 64
const maxGroupedWALBytes = 1 << 20
const smallWALProposalBytes = 16 << 10

type walProposal struct {
	writes []storage.Mutation
	queued func()
	ready  sync.WaitGroup
	lead   bool
	bytes  int
	err    error
}

type walProposalQueue struct {
	mu      sync.Mutex
	active  bool
	pending []*walProposal
}

var walProposals = sync.Pool{New: func() any { return new(walProposal) }}

// Independent SQL commits may share one ordered Raft WAL command. Every caller
// retains its row locks and mutation buffers until the whole proposal applies.
// No batching timer or acknowledgement-before-application is introduced.
func (n *Node) proposeWAL(writes []storage.Mutation, queued func()) error {
	if !n.async() {
		return n.propose(command{Version: 1, Kind: "wal", Writes: writes}, queued)
	}
	size := 0
	for _, write := range writes {
		size += len(write.Key) + len(write.Value) + 64
		if size > smallWALProposalBytes {
			return n.propose(command{Version: 1, Kind: "wal", Writes: writes}, queued)
		}
	}
	request := walProposals.Get().(*walProposal)
	request.writes, request.queued, request.bytes = writes, queued, size
	request.err, request.lead = nil, false
	request.ready.Add(1)
	defer func() { request.writes = nil; request.queued = nil; request.err = nil; walProposals.Put(request) }()
	// Every queued request owns a shared gate until Raft has accepted its group
	// and its catalog-release callback has completed. Exclusive seals therefore
	// cannot overtake a proposal still waiting in this queue.
	n.proposalMu.RLock()
	q := &n.walProposals
	q.mu.Lock()
	q.pending = append(q.pending, request)
	lead := !q.active
	q.active = true
	q.mu.Unlock()
	if !lead {
		request.ready.Wait()
		if !request.lead {
			return request.err
		}
	} else {
		// This collector did not wait on its own ready signal.
		request.ready.Done()
	}
	var group [maxWALProposalBatches]*walProposal
	q.mu.Lock()
	count, total := 0, 0
	for count < len(q.pending) && count < len(group) {
		candidate := q.pending[count]
		if count > 0 && total+candidate.bytes > maxGroupedWALBytes {
			break
		}
		group[count] = candidate
		total += candidate.bytes
		count++
	}
	copy(q.pending, q.pending[count:])
	for i := len(q.pending) - count; i < len(q.pending); i++ {
		q.pending[i] = nil
	}
	q.pending = q.pending[:len(q.pending)-count]
	q.mu.Unlock()
	batch := group[0].writes
	if count > 1 {
		mutations := 0
		for _, member := range group[:count] {
			mutations += len(member.writes)
		}
		batch = make([]storage.Mutation, 0, mutations)
		for _, member := range group[:count] {
			batch = append(batch, member.writes...)
		}
	}
	release := func() {
		for _, member := range group[:count] {
			if member.queued != nil {
				member.queued()
			}
			n.proposalMu.RUnlock()
		}
		// Once the group is in Raft, hand collection to the next producer. Its
		// consensus round can overlap this group's replication and application.
		q.mu.Lock()
		q.handoffLocked()
		q.mu.Unlock()
	}
	// The shared gates are already held. Reacquiring one here could deadlock
	// behind a waiting exclusive seal. The small group is independently bounded.
	err := n.proposeWithGate(command{Version: 1, Kind: "wal", Writes: batch}, release, true)
	for _, member := range group[:count] {
		if member != request {
			member.err = err
			member.ready.Done()
		}
	}
	return err
}

func (q *walProposalQueue) handoffLocked() {
	if len(q.pending) > 0 {
		next := q.pending[0]
		next.lead = true
		next.ready.Done()
	} else {
		q.active = false
	}
}
