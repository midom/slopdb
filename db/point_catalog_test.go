package db

import (
	"context"
	"runtime"
	"sync"
	"testing"
	"time"

	"slopdb/sql"
)

func TestPointCatalogReadersExcludePublication(t *testing.T) {
	m := newLockManager()
	var left, right int
	var wg sync.WaitGroup
	start := make(chan struct{})
	for id := 0; id < 200; id++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			owner := new(sql.Schema)
			<-start
			for n := 0; n < 80; n++ {
				for {
					grant, err := m.acquirePointCatalog(owner)
					if err != nil {
						runtime.Gosched()
						continue
					}
					if left != right {
						t.Error("catalog publication overlapped point reader")
					}
					m.release(owner)
					m.releasePointCatalog(grant)
					break
				}
			}
		}()
	}
	for id := 0; id < 4; id++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			owner := new(sql.Schema)
			<-start
			for n := 0; n < 80; n++ {
				for m.acquire(owner, sql.LockRequest{Resource: "catalog", Exclusive: true}) != nil {
					runtime.Gosched()
				}
				left++
				runtime.Gosched()
				right++
				m.release(owner)
			}
		}()
	}
	close(start)
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(15 * time.Second):
		t.Fatal("catalog grants did not finish")
	}
	if left != 320 || right != 320 {
		t.Fatal(left, right)
	}
}

func TestPointCatalogQueuedDDLAndDiagnostics(t *testing.T) {
	d, err := Open(t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	objectExec(t, d, "CREATE TABLE t(id INTEGER PRIMARY KEY,v INTEGER); INSERT INTO t VALUES(1,0)")
	owner, err := d.NewSession()
	if err != nil {
		t.Fatal(err)
	}
	defer owner.Close()
	grant, err := d.locks.acquirePointCatalog(owner.schema)
	if err != nil || grant == nil {
		t.Fatal("no point grant", err)
	}
	var release sync.Once
	defer release.Do(func() { d.locks.releasePointCatalog(grant) })
	ddl, err := d.NewSession()
	if err != nil {
		t.Fatal(err)
	}
	defer ddl.Close()
	ddlDone := make(chan error, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	go func() { _, err := ddl.ExecContext(ctx, "CREATE TABLE other(id INTEGER PRIMARY KEY)"); ddlDone <- err }()
	wait := waitForReportedLock(t, d)
	if wait.ResourceKind != "catalog" || len(wait.BlockingSessionIDs) != 1 || wait.BlockingSessionIDs[0] != owner.SessionID() {
		t.Fatalf("missing point blocker: %+v", wait)
	}
	update, err := d.NewSession()
	if err != nil {
		t.Fatal(err)
	}
	defer update.Close()
	updateDone := make(chan error, 1)
	go func() { _, err := update.ExecContext(ctx, "UPDATE t SET v=v+1 WHERE id=1"); updateDone <- err }()
	waitCatalogRequest(t, d, false)
	release.Do(func() { d.locks.releasePointCatalog(grant) })
	if err := <-ddlDone; err != nil {
		t.Fatal(err)
	}
	if err := <-updateDone; err != nil {
		t.Fatal(err)
	}
	r := objectExec(t, d, "SELECT v FROM t WHERE id=1")
	if r.Rows[0]["v"].Int != 1 {
		t.Fatal(r)
	}
	if d.locks.pointCatalog.pending.Load() {
		t.Fatal("catalog intent remained after publication")
	}
}
