package sql

import (
	"testing"
	"time"

	"slopdb/storage"
)

func TestIndexCleanupIdleSessionDoesNotLockCatalog(t *testing.T) {
	opt := storage.DefaultOptions()
	opt.DisableWALSync = true
	lsm, err := storage.NewLSMTree(t.TempDir(), opt)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = lsm.Close() })
	root, err := NewSchema(lsm, 4096)
	if err != nil {
		t.Fatal(err)
	}
	session := root.NewSession()
	// A held catalog reader permits ordinary statement/commit reads but would
	// block the old unconditional cleanup writer, even with no garbage present.
	root.commitMu.RLock()
	done := make(chan error, 1)
	go func() { done <- session.CleanupPendingIndexGarbage() }()
	select {
	case err := <-done:
		root.commitMu.RUnlock()
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		root.commitMu.RUnlock()
		<-done
		t.Fatal("idle session cleanup waited for the catalog writer lock")
	}
}
