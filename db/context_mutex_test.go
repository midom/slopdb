package db

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

type observedDoneContext struct {
	context.Context
	once sync.Once
	read chan struct{}
}

func (c *observedDoneContext) Done() <-chan struct{} {
	c.once.Do(func() { close(c.read) })
	return c.Context.Done()
}

func TestContextMutexAvoidsDoneUntilContendedAndHonorsCancellation(t *testing.T) {
	parent, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx := &observedDoneContext{Context: parent, read: make(chan struct{})}
	var gate contextMutex
	if err := gate.LockContext(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case <-ctx.read:
		t.Fatal("uncontended lock materialized cancellation channel")
	default:
	}
	done := make(chan error, 1)
	go func() { done <- gate.LockContext(ctx) }()
	select {
	case <-ctx.read:
	case <-time.After(5 * time.Second):
		gate.Unlock()
		t.Fatal("queued acquisition did not observe cancellation")
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("waiter: %v", err)
		}
	case <-time.After(5 * time.Second):
		gate.Unlock()
		t.Fatal("cancelled waiter did not return")
	}
	gate.Unlock()
	if err := gate.LockContext(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled uncontended acquisition: %v", err)
	}
	if err := gate.LockContext(context.Background()); err != nil {
		t.Fatal(err)
	}
	gate.Unlock()
}
