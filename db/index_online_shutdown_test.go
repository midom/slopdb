package db

import (
	"context"
	"errors"
	"strings"
	"testing"
)

func TestOnlineIndexPublicationWaitCancellationAndShutdown(t *testing.T) {
	for _, shutdown := range []bool{false, true} {
		name := "cancel"
		if shutdown {
			name = "close"
		}
		t.Run(name, func(t *testing.T) {
			d, dir, opt := onlineTestDatabase(t)
			objectExec(t, d, "CREATE TABLE t(id INTEGER PRIMARY KEY,v TEXT); INSERT INTO t VALUES(1,'before')")
			before := objectData(t, d)
			owner, err := d.Begin()
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = owner.Rollback() })
			if _, err := owner.Exec("UPDATE t SET v='uncommitted' WHERE id=1"); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			t.Cleanup(cancel)
			done := onlineStart(t, d, ctx, "CREATE INDEX iv ON t(v)")
			waitCatalogRequest(t, d, true)
			if shutdown {
				if err := d.Close(); err != nil {
					t.Fatal(err)
				}
			} else {
				cancel()
			}
			err = onlineWait(t, done)
			if err == nil || (!errors.Is(err, context.Canceled) && !strings.Contains(err.Error(), "database closed")) {
				t.Fatalf("publication wait was not interrupted: %v", err)
			}
			if !shutdown {
				if err := owner.Rollback(); err != nil {
					t.Fatal(err)
				}
				if id := objectIndexID(t, d, "iv"); id != 0 {
					t.Fatal("cancelled publication exposed an index", id)
				}
				if len(objectData(t, d)) != len(before) {
					t.Fatal("cancelled publication retained staged entries")
				}
				if err := d.Close(); err != nil {
					t.Fatal(err)
				}
			}
			reopened, err := Open(dir, opt)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = reopened.Close() })
			if _, err := reopened.CheckIntegrity(); err != nil {
				t.Fatal(err)
			}
			if id := objectIndexID(t, reopened, "iv"); id != 0 || len(objectData(t, reopened)) != len(before) {
				t.Fatal("interrupted publication survived recovery", id)
			}
			r := objectExec(t, reopened, "SELECT v FROM t WHERE id=1")
			if len(r.Rows) != 1 || r.Rows[0]["v"].Text != "before" {
				t.Fatal("shutdown committed the lock owner's private update", r)
			}
			objectExec(t, reopened, "CREATE INDEX iv ON t(v)")
			if _, err := reopened.CheckIntegrity(); err != nil {
				t.Fatal(err)
			}
			if err := reopened.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}
