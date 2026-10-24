package storage

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// Gate the real file operation, rather than relying on a slow filesystem or a
// throughput threshold. The buffered writer still writes to the actual WAL.
type gatedWALFile struct {
	walFile
	started, release, closed chan struct{}
	startOnce, releaseOnce   sync.Once
	failure                  error
}

func (f *gatedWALFile) Sync() error {
	f.startOnce.Do(func() { close(f.started) })
	<-f.release
	if f.failure != nil {
		return f.failure
	}
	return f.walFile.Sync()
}

func (f *gatedWALFile) Close() error {
	err := f.walFile.Close()
	close(f.closed)
	return err
}

func (f *gatedWALFile) unblock() { f.releaseOnce.Do(func() { close(f.release) }) }

func gateCurrentWAL(t *testing.T, tree *LSMTree, failure error) *gatedWALFile {
	t.Helper()
	tree.writerMu.Lock()
	f := &gatedWALFile{walFile: tree.wal.file, started: make(chan struct{}), release: make(chan struct{}), closed: make(chan struct{}), failure: failure}
	tree.wal.file = f
	tree.writerMu.Unlock()
	t.Cleanup(f.unblock) // release gates before the fallback database Close
	return f
}

func waitWALSignal(t *testing.T, signal <-chan struct{}) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(10 * time.Second):
		t.Fatal("WAL operation did not reach its synchronization point")
	}
}

func waitWALResult(t *testing.T, result <-chan error) error {
	t.Helper()
	select {
	case err := <-result:
		return err
	case <-time.After(10 * time.Second):
		t.Fatal("WAL operation did not finish")
		return nil
	}
}

func waitWALFlush(t *testing.T, tree *LSMTree) {
	t.Helper()
	done := make(chan struct{})
	go func() { tree.flushWg.Wait(); close(done) }()
	waitWALSignal(t, done)
}

func walSyncTestOptions() *Options {
	o := DefaultOptions()
	o.WriteBufferSize = 1024
	o.CompactionL0Trigger = 1000
	return o
}

func openWALSyncTest(t *testing.T) (*LSMTree, string) {
	t.Helper()
	dir := t.TempDir()
	tree, err := NewLSMTree(dir, walSyncTestOptions())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = tree.Close() })
	return tree, dir
}

func writeDeferredWAL(t *testing.T, tree *LSMTree, key string, value []byte) {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- tree.ApplyReplicatedDeferred([]Mutation{{Key: []byte(key), Value: value}}) }()
	if err := waitWALResult(t, done); err != nil {
		t.Fatal(err)
	}
}

func TestRotatedWALDurableBarrierCoversEveryGeneration(t *testing.T) {
	for _, barrier := range []string{"explicit_sync", "durable_write", "flush"} {
		t.Run(barrier, func(t *testing.T) {
			tree, dir := openWALSyncTest(t)
			payload := bytes.Repeat([]byte("v"), 2048)
			writeDeferredWAL(t, tree, "first", payload)
			first := gateCurrentWAL(t, tree, nil)
			writeDeferredWAL(t, tree, "second", payload) // rotates first
			waitWALSignal(t, first.started)
			// Neither foreground writes/reads nor durable SST publication may
			// wait for a rotated WAL's background fsync.
			waitWALFlush(t, tree)
			second := gateCurrentWAL(t, tree, nil)
			writeDeferredWAL(t, tree, "third", []byte("three")) // rotates second
			waitWALSignal(t, second.started)
			second.unblock()
			waitWALSignal(t, second.closed) // younger generation finishes first
			waitWALFlush(t, tree)
			writeDeferredWAL(t, tree, "fourth", []byte("four"))
			value, found, err := tree.Get([]byte("first"))
			if err != nil || !found || !bytes.Equal(value, payload) {
				t.Fatalf("read during background sync: %q %v %v", value, found, err)
			}
			current := gateCurrentWAL(t, tree, nil)
			current.unblock()
			done := make(chan error, 1)
			go func() {
				switch barrier {
				case "durable_write":
					done <- tree.ApplyReplicated([]Mutation{{Key: []byte("receipt"), Value: []byte("stable")}})
				case "flush":
					done <- tree.Flush()
				default:
					done <- tree.SyncReplicationWAL()
				}
			}()
			waitWALSignal(t, current.started)
			select {
			case err := <-done:
				t.Fatalf("barrier ignored the oldest unsynced WAL: %v", err)
			case <-time.After(100 * time.Millisecond):
			}
			first.unblock()
			if err := waitWALResult(t, done); err != nil {
				t.Fatal(err)
			}
			if err := tree.Close(); err != nil {
				t.Fatal(err)
			}
			reopened, err := NewLSMTree(dir, walSyncTestOptions())
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = reopened.Close() })
			want := map[string][]byte{"first": payload, "second": payload, "third": []byte("three"), "fourth": []byte("four")}
			if barrier == "durable_write" {
				want["receipt"] = []byte("stable")
			}
			for key, expected := range want {
				value, found, err := reopened.Get([]byte(key))
				if err != nil || !found || !bytes.Equal(value, expected) {
					t.Fatalf("reopened %s: %q %v %v", key, value, found, err)
				}
			}
			if err := reopened.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestRotatedWALSyncFailureRejectsDurableWrite(t *testing.T) {
	tree, _ := openWALSyncTest(t)
	writeDeferredWAL(t, tree, "first", bytes.Repeat([]byte("v"), 2048))
	injected := errors.New("injected rotated WAL fsync failure")
	first := gateCurrentWAL(t, tree, injected)
	writeDeferredWAL(t, tree, "second", []byte("two"))
	waitWALSignal(t, first.started)
	current := gateCurrentWAL(t, tree, nil)
	current.unblock()
	done := make(chan error, 1)
	go func() {
		done <- tree.ApplyReplicated([]Mutation{{Key: []byte("receipt"), Value: []byte("must not publish")}})
	}()
	waitWALSignal(t, current.started)
	first.unblock()
	if err := waitWALResult(t, done); !errors.Is(err, injected) {
		t.Fatalf("durable write ignored predecessor failure: %v", err)
	}
	if _, found, err := tree.Get([]byte("receipt")); err != nil || found {
		t.Fatalf("failed durable write published: %v %v", found, err)
	}
	if err := tree.SyncReplicationWAL(); !errors.Is(err, injected) {
		t.Fatalf("explicit barrier ignored background error: %v", err)
	}
	if err := tree.ApplyReplicatedDeferred([]Mutation{{Key: []byte("later"), Value: []byte("no")}}); !errors.Is(err, injected) {
		t.Fatalf("later write ignored background error: %v", err)
	}
	if err := tree.Close(); !errors.Is(err, injected) {
		t.Fatalf("Close ignored background error: %v", err)
	}
}

func TestRotatedWALFlushDoesNotDeadlockOnSyncError(t *testing.T) {
	tree, _ := openWALSyncTest(t)
	writeDeferredWAL(t, tree, "first", bytes.Repeat([]byte("v"), 2048))
	injected := errors.New("rotated WAL failed during Flush")
	first := gateCurrentWAL(t, tree, injected)
	writeDeferredWAL(t, tree, "second", []byte("two"))
	waitWALSignal(t, first.started)
	waitWALFlush(t, tree)
	current := gateCurrentWAL(t, tree, nil)
	current.unblock()
	done := make(chan error, 1)
	go func() { done <- tree.Flush() }()
	waitWALSignal(t, current.started)
	first.unblock()
	if err := waitWALResult(t, done); !errors.Is(err, injected) {
		t.Fatalf("Flush ignored rotated WAL error: %v", err)
	}
	if err := tree.Close(); !errors.Is(err, injected) {
		t.Fatalf("Close ignored rotated WAL error: %v", err)
	}
}

func TestRotatedWALYoungerFailureStopsWritesBeforeOlderSyncFinishes(t *testing.T) {
	tree, _ := openWALSyncTest(t)
	payload := bytes.Repeat([]byte("v"), 2048)
	writeDeferredWAL(t, tree, "first", payload)
	first := gateCurrentWAL(t, tree, nil)
	writeDeferredWAL(t, tree, "second", payload)
	waitWALSignal(t, first.started)
	waitWALFlush(t, tree)
	injected := errors.New("younger WAL failed")
	second := gateCurrentWAL(t, tree, injected)
	writeDeferredWAL(t, tree, "third", []byte("three"))
	waitWALSignal(t, second.started)
	second.unblock()
	failed := make(chan error, 1)
	go func() {
		tree.mu.Lock()
		for tree.bgErr == nil {
			tree.cond.Wait()
		}
		err := tree.bgErr
		tree.mu.Unlock()
		failed <- err
	}()
	if err := waitWALResult(t, failed); !errors.Is(err, injected) {
		t.Fatal(err)
	}
	if err := tree.ApplyReplicatedDeferred([]Mutation{{Key: []byte("later"), Value: []byte("no")}}); !errors.Is(err, injected) {
		t.Fatalf("write ignored younger WAL failure: %v", err)
	}
	if err := tree.SyncReplicationWAL(); !errors.Is(err, injected) {
		t.Fatalf("barrier ignored younger WAL failure: %v", err)
	}
	first.unblock()
	if err := tree.Close(); !errors.Is(err, injected) {
		t.Fatalf("Close ignored younger WAL failure: %v", err)
	}
}

func TestRotatedWALCloseWaitsForBackgroundSync(t *testing.T) {
	tree, _ := openWALSyncTest(t)
	writeDeferredWAL(t, tree, "first", bytes.Repeat([]byte("v"), 2048))
	first := gateCurrentWAL(t, tree, nil)
	writeDeferredWAL(t, tree, "second", []byte("two"))
	waitWALSignal(t, first.started)
	done := make(chan error, 1)
	go func() { done <- tree.Close() }()
	select {
	case err := <-done:
		t.Fatalf("Close left a background WAL writer running: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	first.unblock()
	if err := waitWALResult(t, done); err != nil {
		t.Fatal(err)
	}
	waitWALSignal(t, first.closed)
}

func TestRotatedWALDisabledSyncDoesNotStartBackgroundFsync(t *testing.T) {
	o := walSyncTestOptions()
	o.DisableWALSync = true
	tree, err := NewLSMTree(t.TempDir(), o)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = tree.Close() })
	writeDeferredWAL(t, tree, "first", bytes.Repeat([]byte("v"), 2048))
	first := gateCurrentWAL(t, tree, errors.New("fsync must be disabled"))
	first.unblock()
	writeDeferredWAL(t, tree, "second", []byte("two"))
	if err := tree.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-first.started:
		t.Fatal("rotation performed fsync with DisableWALSync")
	default:
	}
	waitWALSignal(t, first.closed)
}

func TestRotatedWALBarrierCrashRecovery(t *testing.T) {
	if dir := os.Getenv("SLOPDB_ROTATED_WAL_CRASH_DIR"); dir != "" {
		tree, err := NewLSMTree(dir, walSyncTestOptions())
		if err != nil {
			t.Fatal(err)
		}
		for i := 0; i < 32; i++ {
			writeDeferredWAL(t, tree, fmt.Sprintf("key-%02d", i), bytes.Repeat([]byte{byte(i)}, 2048))
		}
		if err := tree.SyncReplicationWAL(); err != nil {
			t.Fatal(err)
		}
		os.Exit(81) // deliberately skip Close/flush after the durable barrier
	}
	dir := filepath.Join(t.TempDir(), "db")
	child := exec.Command(os.Args[0], "-test.run=^TestRotatedWALBarrierCrashRecovery$", "-test.count=1")
	child.Env = append(os.Environ(), "SLOPDB_ROTATED_WAL_CRASH_DIR="+dir)
	output, err := child.CombinedOutput()
	var exit *exec.ExitError
	if !errors.As(err, &exit) || exit.ExitCode() != 81 {
		t.Fatalf("crash child: %v\n%s", err, output)
	}
	tree, err := NewLSMTree(dir, walSyncTestOptions())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = tree.Close() })
	for i := 0; i < 32; i++ {
		value, found, err := tree.Get([]byte(fmt.Sprintf("key-%02d", i)))
		if err != nil || !found || !bytes.Equal(value, bytes.Repeat([]byte{byte(i)}, 2048)) {
			t.Fatalf("recovered key %d: found=%v err=%v", i, found, err)
		}
	}
	if err := tree.Close(); err != nil {
		t.Fatal(err)
	}
}
