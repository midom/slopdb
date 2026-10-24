package replication

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"slopdb/storage"
)

func waitWALProposals(t *testing.T, n *Node, count int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		n.walProposals.mu.Lock()
		actual := len(n.walProposals.pending)
		n.walProposals.mu.Unlock()
		if actual == count {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("queued proposals %d, want %d", actual, count)
		}
		time.Sleep(time.Millisecond)
	}
}

func startQueuedWALProposals(n *Node) {
	n.walProposals.mu.Lock()
	first := n.walProposals.pending[0]
	first.lead = true
	first.ready.Done()
	n.walProposals.mu.Unlock()
}

func TestWALProposalGroupPublicationAndSealOrdering(t *testing.T) {
	c := asyncConfig(t, "group", "tcp")
	c.Bootstrap = true
	n := openTestNode(t, c)
	ready(t, n)
	n.fsmMu.Lock()
	held := true
	defer func() {
		if held {
			n.fsmMu.Unlock()
		}
	}()
	// Pause collection to make a full group deterministic, without scheduling
	// sleeps or assumptions about how quickly individual clients reach Raft.
	n.walProposals.mu.Lock()
	n.walProposals.active = true
	n.walProposals.mu.Unlock()
	const count = 64
	var callbacks atomic.Int64
	done := make(chan error, count)
	before := n.raftNode().LastIndex()
	for i := 0; i < count; i++ {
		go func(i int) {
			done <- n.proposeWAL([]storage.Mutation{{Key: []byte(fmt.Sprintf("queue:%03d", i)), Value: []byte("value")}}, func() { callbacks.Add(1) })
		}(i)
	}
	started := false
	defer func() {
		if !started {
			n.walProposals.mu.Lock()
			if len(n.walProposals.pending) > 0 {
				first := n.walProposals.pending[0]
				first.lead = true
				first.ready.Done()
			} else {
				n.walProposals.active = false
			}
			n.walProposals.mu.Unlock()
		}
	}()
	waitWALProposals(t, n, count)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	sealed := make(chan error, 1)
	go func() { sealed <- n.sealDurable(ctx, false, true) }()
	deadline := time.Now().Add(5 * time.Second)
	for n.proposalMu.TryRLock() {
		n.proposalMu.RUnlock()
		if time.Now().After(deadline) {
			t.Fatal("exclusive seal did not wait behind queued writes")
		}
		time.Sleep(time.Millisecond)
	}
	startQueuedWALProposals(n)
	started = true
	for callbacks.Load() != count {
		if time.Now().After(deadline) {
			t.Fatal("collector blocked behind waiting exclusive seal")
		}
		time.Sleep(time.Millisecond)
	}
	select {
	case err := <-done:
		t.Fatalf("commit returned before native application: %v", err)
	default:
	}
	n.fsmMu.Unlock()
	held = false
	for i := 0; i < count; i++ {
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	}
	if err := <-sealed; err != nil {
		t.Fatal(err)
	}
	if callbacks.Load() != count {
		t.Fatal("enqueue callback did not run exactly once")
	}
	if got := n.raftNode().LastIndex() - before; got != 2 {
		t.Fatalf("group and seal appended %d entries, want 2", got)
	}
	for i := 0; i < count; i++ {
		value, found, err := n.Database().ReplicationStorage().Get([]byte(fmt.Sprintf("queue:%03d", i)))
		if err != nil || !found || string(value) != "value" {
			t.Fatalf("row %d: %q %v %v", i, value, found, err)
		}
	}
	if n.durableIndex() < before+2 {
		t.Fatal("seal did not cover grouped commits")
	}
	if err := n.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestWALProposalGroupFailureReleasesEveryCaller(t *testing.T) {
	n := &Node{config: Config{Durability: DurabilityAsync}}
	n.walProposals.active = true
	const count = 128
	var callbacks atomic.Int64
	done := make(chan error, count)
	for i := 0; i < count; i++ {
		go func() {
			done <- n.proposeWAL([]storage.Mutation{{Key: []byte("key"), Value: []byte("value")}}, func() { callbacks.Add(1) })
		}()
	}
	started := false
	defer func() {
		if !started {
			n.walProposals.mu.Lock()
			if len(n.walProposals.pending) > 0 {
				first := n.walProposals.pending[0]
				first.lead = true
				first.ready.Done()
			} else {
				n.walProposals.active = false
			}
			n.walProposals.mu.Unlock()
		}
	}()
	waitWALProposals(t, n, count)
	startQueuedWALProposals(n)
	started = true
	for i := 0; i < count; i++ {
		select {
		case err := <-done:
			if !errors.Is(err, ErrNotLeader) {
				t.Fatalf("unexpected error %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("failed proposal left a blocked caller")
		}
	}
	if callbacks.Load() != count {
		t.Fatalf("callbacks %d, want %d", callbacks.Load(), count)
	}
	if !n.proposalMu.TryLock() {
		t.Fatal("failed group leaked proposal gates")
	}
	n.proposalMu.Unlock()
	n.walProposals.mu.Lock()
	defer n.walProposals.mu.Unlock()
	if len(n.walProposals.pending) != 0 || n.walProposals.active {
		t.Fatal("failed queue did not drain")
	}
}
