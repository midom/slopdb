package db

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
)

func TestRuntimeStatusNativeAndPrepared(t *testing.T) {
	d, e := Open(t.TempDir(), nil)
	if e != nil {
		t.Fatal(e)
	}
	defer d.Close()
	for _, q := range []string{"SHOW RAFT STATUS", "SHOW GLOBAL VARIABLES LIKE 'slopdb_raft_%'"} {
		r, e := d.Exec(q)
		if e != nil || len(r.Rows) == 0 || len(r.Columns) != 2 {
			t.Fatalf("%s %+v %v", q, r, e)
		}
		p, e := d.Prepare(q)
		if e != nil {
			t.Fatal(e)
		}
		r, e = p.Exec()
		p.Close()
		if e != nil || len(r.Rows) == 0 {
			t.Fatalf("prepared runtime: %+v %v", r, e)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, e = d.ServerState(ctx); !errors.Is(e, context.Canceled) {
		t.Fatalf("canceled status %v", e)
	}
	if d.ReplicationStorage().ChangeLogEnabled() {
		t.Fatal("native state enabled CDC")
	}
}

func TestLegacyRaftStateDoesNotInvokeActiveAdmission(t *testing.T) {
	d, e := Open(t.TempDir(), nil)
	if e != nil {
		t.Fatal(e)
	}
	defer d.Close()
	var called atomic.Int64
	d.SetReplicationGuard(&ReplicationGuard{Check: func(context.Context) error { called.Add(1); return nil }})
	state, e := d.RaftState(context.Background())
	if e != nil {
		t.Fatal(e)
	}
	if called.Load() != 0 || state.Role != "Unknown" || !state.Enabled || state.Writable {
		t.Fatalf("legacy state invoked active guard: %+v checks=%d", state, called.Load())
	}
}
