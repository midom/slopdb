package db

import (
	"context"
	"fmt"
	"runtime"
	"slopdb/sql"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestExecutionGateMutualExclusion(t *testing.T) {
	var g executionGate
	var readers atomic.Int64
	var writer atomic.Bool
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for n := 0; n < 500; n++ {
				if err := g.RLockContext(context.Background()); err != nil {
					t.Error(err)
					return
				}
				readers.Add(1)
				if writer.Load() {
					t.Error("reader overlapped exclusive operation")
				}
				runtime.Gosched()
				readers.Add(-1)
				g.RUnlock()
			}
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for n := 0; n < 100; n++ {
			g.Lock()
			writer.Store(true)
			if readers.Load() != 0 {
				t.Error("exclusive operation overlapped reader")
			}
			runtime.Gosched()
			writer.Store(false)
			g.Unlock()
		}
	}()
	wg.Wait()
	if g.readers.Load() != 0 || g.pending.Load() {
		t.Fatal("execution gate leaked ownership")
	}
}

func TestExecutionGateWriterPreferenceAndCancellation(t *testing.T) {
	var g executionGate
	if err := g.RLockContext(context.Background()); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- g.LockContext(ctx) }()
	deadline := time.Now().Add(time.Second)
	for !g.pending.Load() && time.Now().Before(deadline) {
		runtime.Gosched()
	}
	if !g.pending.Load() {
		t.Fatal("writer did not queue")
	}
	queuedCtx, queuedCancel := context.WithCancel(context.Background())
	queuedCancel()
	if err := g.RLockContext(queuedCtx); err != context.Canceled {
		t.Fatal("queued reader ignored cancellation", err)
	}
	cancel()
	if err := <-done; err != context.Canceled {
		t.Fatal(err)
	}
	if g.pending.Load() {
		t.Fatal("cancelled writer blocked future readers")
	}
	if err := g.RLockContext(context.Background()); err != nil {
		t.Fatal(err)
	}
	g.RUnlock()
	entered := make(chan struct{})
	release := make(chan struct{})
	go func() { g.Lock(); close(entered); <-release; g.Unlock() }()
	for !g.pending.Load() && time.Now().Before(deadline) {
		runtime.Gosched()
	}
	if !g.pending.Load() {
		t.Fatal("second writer did not queue")
	}
	nextReader := make(chan struct{})
	go func() { _ = g.RLockContext(context.Background()); close(nextReader); g.RUnlock() }()
	g.RUnlock()
	<-entered
	select {
	case <-nextReader:
		t.Fatal("reader bypassed pending exclusive operation")
	default:
	}
	close(release)
	<-nextReader
}

// A busy resource unrelated to the wait graph must not stall cycle detection.
func TestDeadlockDetectionIgnoresUnrelatedShard(t *testing.T) {
	m := newLockManager()
	names := make(map[int]string)
	for i := 0; len(names) < 3; i++ {
		name := fmt.Sprintf("row:test-%d", i)
		names[m.shardIndex(name)] = name
	}
	var resource []string
	for _, name := range names {
		resource = append(resource, name)
	}
	a, b := new(sql.Schema), new(sql.Schema)
	ra := sql.LockRequest{Resource: resource[0], Exclusive: true}
	rb := sql.LockRequest{Resource: resource[1], Exclusive: true}
	if err := m.acquire(a, ra); err != nil {
		t.Fatal(err)
	}
	if err := m.acquire(b, rb); err != nil {
		t.Fatal(err)
	}
	register := func(owner *sql.Schema, r sql.LockRequest) {
		m.graphMu.Lock()
		shard := m.shards[m.shardIndex(r.Resource)]
		shard.mu.Lock()
		q := queuedLock{request: r, order: m.order.Add(1)}
		m.waiting[owner] = q
		shard.waiting[owner] = q
		shard.mu.Unlock()
		m.graphMu.Unlock()
	}
	register(a, rb)
	register(b, ra)
	unrelated := m.shards[m.shardIndex(resource[2])]
	unrelated.mu.Lock()
	done := make(chan bool, 1)
	go func() { done <- m.deadlocked(a) }()
	select {
	case cycle := <-done:
		unrelated.mu.Unlock()
		if !cycle {
			t.Fatal("missed transaction cycle")
		}
	case <-time.After(time.Second):
		unrelated.mu.Unlock()
		<-done
		t.Fatal("unrelated resource stalled detection")
	}
}
