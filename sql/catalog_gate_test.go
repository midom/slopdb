package sql

import (
	"runtime"
	"sync"
	"testing"
	"time"
)

func TestCatalogGateCollidingAndLegacyReadersExcludeWriters(t *testing.T) {
	var gate catalogGate
	var left, right int
	var wg sync.WaitGroup
	start := make(chan struct{})
	for worker := 0; worker < 256; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			<-start
			for n := 0; n < 100; n++ {
				if worker%11 == 0 {
					gate.RLock()
				} else {
					gate.RLockForReader(uint64(worker % 8))
				}
				if left != right {
					t.Error("reader overlapped catalog publication")
				}
				if worker%11 == 0 {
					gate.RUnlock()
				} else {
					gate.RUnlockForReader(uint64(worker % 8))
				}
			}
		}(worker)
	}
	for writer := 0; writer < 4; writer++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for n := 0; n < 100; n++ {
				gate.Lock()
				left++
				runtime.Gosched()
				right++
				gate.Unlock()
			}
		}()
	}
	close(start)
	wg.Wait()
	if left != 400 || right != 400 || gate.hasReaders() {
		t.Fatalf("lost publications/readers: %d %d", left, right)
	}
}

func TestCatalogGateQueuedWriterPrecedesNewReader(t *testing.T) {
	var gate catalogGate
	gate.RLockForReader(1)
	initial := true
	release := make(chan struct{})
	var releaseOnce sync.Once
	defer func() {
		releaseOnce.Do(func() { close(release) })
		if initial {
			gate.RUnlockForReader(1)
		}
	}()
	writer := make(chan struct{})
	writerDone := make(chan struct{})
	go func() {
		gate.Lock()
		close(writer)
		<-release
		gate.Unlock()
		close(writerDone)
	}()
	deadline := time.Now().Add(5 * time.Second)
	for !gate.pending.Load() {
		if time.Now().After(deadline) {
			t.Fatal("writer never registered")
		}
		runtime.Gosched()
	}
	reader := make(chan struct{})
	go func() { gate.RLockForReader(129); close(reader); gate.RUnlockForReader(129) }()
	gate.RUnlockForReader(1)
	initial = false
	select {
	case <-writer:
	case <-time.After(5 * time.Second):
		t.Fatal("writer was starved")
	}
	select {
	case <-reader:
		t.Fatal("reader entered before queued writer completed")
	default:
	}
	releaseOnce.Do(func() { close(release) })
	select {
	case <-reader:
	case <-time.After(5 * time.Second):
		t.Fatal("reader never resumed")
	}
	<-writerDone
}
