package replication

import (
	"bytes"
	"context"
	"testing"
	"time"

	"slopdb/storage"
)

func TestIndependentProposalsAndExclusiveDurabilityFence(t *testing.T) {
	config := asyncConfig(t, "source", "tcp")
	config.Bootstrap = true
	node := openTestNode(t, config)
	ready(t, node)
	entered, release := make(chan struct{}), make(chan struct{})
	released := false
	defer func() {
		if !released {
			close(release)
		}
	}()
	firstDone := make(chan error, 1)
	go func() {
		firstDone <- node.propose(command{Version: 1, Kind: "fence"}, func() {
			close(entered)
			<-release
		})
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("first proposal was not enqueued")
	}
	// An independent proposal can reach Raft while the first enqueue's callback
	// is paused. Neither request is acknowledged until its own FSM application.
	secondDone := make(chan error, 1)
	go func() { secondDone <- node.propose(command{Version: 1, Kind: "fence"}, nil) }()
	select {
	case err := <-secondDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("independent proposal was serialized behind a callback")
	}
	priorD := node.durableIndex()
	sealDone := make(chan error, 1)
	go func() { sealDone <- node.FlushDurable(ctx) }()
	// Observe actual writer registration, rather than sleeping to guess when
	// the seal started. RWMutex writer preference blocks new shared entrants.
	for node.proposalMu.TryRLock() {
		node.proposalMu.RUnlock()
		select {
		case err := <-sealDone:
			t.Fatalf("seal crossed an unfinished enqueue callback: %v", err)
		case <-ctx.Done():
			t.Fatal("durability fence did not register")
		case <-time.After(time.Millisecond):
		}
	}
	if node.durableIndex() != priorD {
		t.Fatal("durable prefix advanced across an unfinished enqueue")
	}
	close(release)
	released = true
	if err := <-firstDone; err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-sealDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("durability fence did not finish")
	}
	if node.durableIndex() != node.applied.Load() {
		t.Fatal("durability fence missed an earlier proposal")
	}
}

func TestLargeProposalBoundAllowsSmallEnqueues(t *testing.T) {
	config := asyncConfig(t, "source", "tcp")
	config.Bootstrap = true
	node := openTestNode(t, config)
	ready(t, node)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	node.fsmMu.Lock()
	held := true
	defer func() {
		if held {
			node.fsmMu.Unlock()
		}
	}()
	large := bytes.Repeat([]byte{'x'}, 64<<10)
	firstQueued, secondQueued, smallQueued := make(chan struct{}), make(chan struct{}), make(chan struct{})
	firstDone, secondDone, smallDone := make(chan error, 1), make(chan error, 1), make(chan error, 1)
	go func() {
		firstDone <- node.propose(command{Version: 1, Kind: "wal", Writes: []storage.Mutation{{Key: []byte("test:a"), Value: large}}}, func() { close(firstQueued) })
	}()
	select {
	case <-firstQueued:
	case <-ctx.Done():
		t.Fatal("large command was not enqueued")
	}
	go func() {
		secondDone <- node.propose(command{Version: 1, Kind: "wal", Writes: []storage.Mutation{{Key: []byte("test:b"), Value: large}}}, func() { close(secondQueued) })
	}()
	go func() { smallDone <- node.propose(command{Version: 1, Kind: "fence"}, func() { close(smallQueued) }) }()
	select {
	case <-smallQueued:
	case <-ctx.Done():
		t.Fatal("pending large command blocked small enqueues")
	}
	select {
	case <-secondQueued:
		t.Fatal("multiple large commands became unresolved together")
	default:
	}
	node.fsmMu.Unlock()
	held = false
	for _, done := range []chan error{firstDone, secondDone, smallDone} {
		select {
		case err := <-done:
			if err != nil {
				t.Fatal(err)
			}
		case <-ctx.Done():
			t.Fatal("proposal did not finish")
		}
	}
	for _, key := range []string{"test:a", "test:b"} {
		value, found, err := node.Database().ReplicationStorage().Get([]byte(key))
		if err != nil || !found || !bytes.Equal(value, large) {
			t.Fatalf("large mutation %s was not applied: %v", key, err)
		}
	}
}
