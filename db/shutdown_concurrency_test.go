package db

import (
	"context"
	"errors"
	"runtime"
	"testing"
	"time"

	"slopdb/storage"
)

// Adapted from the original shutdown regression. Observe the actual registered
// lock wait rather than assuming a one-millisecond sleep schedules a goroutine.
func TestConcurrentShutdown(t *testing.T) {
	for i := 0; i < 30; i++ {
		opt := storage.DefaultOptions()
		opt.DisableWALSync = true
		d, err := Open(t.TempDir(), opt)
		if err != nil {
			t.Fatal(err)
		}
		objectExec(t, d, "CREATE TABLE t(id INTEGER PRIMARY KEY,v INT,INDEX iv(v));INSERT INTO t VALUES(1,0)")
		owner, err := d.Begin()
		if err != nil {
			t.Fatal(err)
		}
		if _, err = owner.Exec("UPDATE t SET v=1 WHERE id=1"); err != nil {
			t.Fatal(err)
		}
		waiter, err := d.Begin()
		if err != nil {
			t.Fatal(err)
		}
		done := make(chan error, 1)
		go func() { _, err := waiter.Exec("UPDATE t SET v=2 WHERE id=1"); done <- err }()
		deadline := time.Now().Add(5 * time.Second)
		registered := false
		for time.Now().Before(deadline) {
			d.locks.graphMu.Lock()
			_, registered = d.locks.waiting[waiter.session.schema]
			d.locks.graphMu.Unlock()
			if registered {
				break
			}
			runtime.Gosched()
		}
		if !registered {
			_ = d.Close()
			t.Fatal("lock waiter did not register")
		}
		if i%2 == 0 {
			if err = waiter.Rollback(); err != nil {
				t.Fatal(err)
			}
			if err = <-done; !errors.Is(err, context.Canceled) {
				t.Fatalf("rollback active waiter: %v", err)
			}
			if err = owner.Rollback(); err != nil {
				t.Fatal(err)
			}
			if err = d.Close(); err != nil {
				t.Fatal(err)
			}
		} else {
			if err = d.Close(); err != nil {
				t.Fatal(err)
			}
			select {
			case err = <-done:
				if err == nil {
					t.Fatal("closed waiter committed")
				}
			case <-time.After(time.Second):
				t.Fatal("Close stuck waiting for logical lock")
			}
		}
		if err = d.Close(); err != nil {
			t.Fatal(err)
		}
	}
}
