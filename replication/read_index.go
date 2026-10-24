package replication

import (
	"context"
	"time"

	"github.com/hashicorp/raft"
)

type applyNotice struct{ done chan struct{} }

// Raft.AppliedIndex only means an entry was queued to the FSM. Wait for our
// checkpoint, which advances after the atomic native batch becomes visible.
// The usual already-applied case uses only an atomic load; notification channels
// are allocated only when a reader actually encounters an application backlog.
func (n *Node) waitApplied(ctx context.Context, index, term uint64) error {
	if n.applied.Load() >= index {
		return nil
	}
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for n.applied.Load() < index {
		if n.readyTerm.Load() != term || n.raftNode().CurrentTerm() != term || n.raftNode().State() != raft.Leader {
			return n.notLeader()
		}
		notice := n.applyNotice.Load()
		if notice == nil {
			notice = &applyNotice{done: make(chan struct{})}
			if !n.applyNotice.CompareAndSwap(nil, notice) {
				continue
			}
		}
		// Publication can race waiter registration. Recheck after registration so
		// an already completed batch cannot leave us waiting for another write.
		if n.applied.Load() >= index {
			return nil
		}
		select {
		case <-notice.done:
		case <-ticker.C:
		case <-ctx.Done():
			return ctx.Err()
		case <-n.ctx.Done():
			if err := n.failure(); err != nil {
				return err
			}
			return ErrClosed
		}
	}
	return nil
}

func (n *Node) notifyApplied() {
	if n.applyNotice.Load() == nil {
		return
	}
	if notice := n.applyNotice.Swap(nil); notice != nil {
		close(notice.done)
	}
}
