package db

import (
	"context"
	"math"
	"sync"
	"testing"
)

func TestActivityBackendIDsSurviveReplicaCatalogReload(t *testing.T) {
	d, err := Open(t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = d.Close() })
	s, err := d.NewSession()
	if err != nil {
		t.Fatal(err)
	}
	before, err := d.ReserveBackendID()
	if err != nil {
		t.Fatal(err)
	}
	if err = d.ReloadReplicaCatalog(); err != nil {
		t.Fatal(err)
	}
	if _, err = s.Exec("SELECT 1"); err == nil {
		t.Fatal("old session survived catalog replacement")
	}
	after, err := d.ReserveBackendID()
	if err != nil {
		t.Fatal(err)
	}
	if after <= before {
		t.Fatalf("backend ID reused: before %d after %d", before, after)
	}
	activity, err := d.Activity(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(activity) != 1 {
		t.Fatalf("old session records survived reload: %+v", activity)
	}
	if activity[0].ID <= uint64(before) {
		t.Fatalf("replacement default session reused public ID: %+v", activity[0])
	}
}

func TestActivityBackendIDsConcurrentAndExhaustion(t *testing.T) {
	d, err := Open(t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = d.Close() })
	const count = 128
	ids := make(chan uint32, count)
	failures := make(chan error, count)
	var group sync.WaitGroup
	for i := 0; i < count; i++ {
		group.Add(1)
		go func() {
			defer group.Done()
			id, err := d.ReserveBackendID()
			if err != nil {
				failures <- err
				return
			}
			ids <- id
		}()
	}
	group.Wait()
	close(ids)
	close(failures)
	for err := range failures {
		t.Fatal(err)
	}
	seen := make(map[uint32]bool, count)
	for id := range ids {
		if id == 0 || seen[id] {
			t.Fatalf("invalid or duplicate backend ID %d", id)
		}
		seen[id] = true
	}
	if len(seen) != count {
		t.Fatalf("got %d IDs", len(seen))
	}
	d.activityIDs.Store(math.MaxInt32)
	for i := 0; i < 2; i++ {
		if id, err := d.ReserveBackendID(); err == nil || id != 0 {
			t.Fatalf("exhaustion returned %d, %v", id, err)
		}
	}
}
