package storage

import (
	"os"
	"path/filepath"
	"testing"
)

func TestBlockCachePolicyValidationBeforeCreate(t *testing.T) {
	path := filepath.Join(t.TempDir(), "missing")
	o := DefaultOptions()
	o.BlockCachePolicy = "unknown"
	if d, err := NewLSMTree(path, o); err == nil {
		d.Close()
		t.Fatal("invalid policy accepted")
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("invalid policy created database: %v", err)
	}
}

func TestBlockCachePolicyDefaultAndReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "database")
	o := DefaultOptions()
	o.BlockCachePolicy = ""
	d, err := NewLSMTree(path, o)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if d != nil {
			_ = d.Close()
		}
	})
	if d.Stats().BlockCachePolicy != BlockCacheLRU {
		t.Fatal("empty policy did not default to LRU")
	}
	if err = d.Put([]byte("key"), []byte("value")); err != nil {
		t.Fatal(err)
	}
	if err = d.Close(); err != nil {
		t.Fatal(err)
	}
	for _, policy := range []BlockCachePolicy{BlockCacheCLOCK, BlockCacheLRU} {
		o.BlockCachePolicy = policy
		d, err = NewLSMTree(path, o)
		if err != nil {
			t.Fatal(err)
		}
		value, found, err := d.Get([]byte("key"))
		if err != nil || !found || string(value) != "value" {
			t.Fatalf("policy %s changed persisted rows: %q %v %v", policy, value, found, err)
		}
		if d.Stats().BlockCachePolicy != policy {
			t.Fatalf("policy %s not effective", policy)
		}
		if err = d.Close(); err != nil {
			t.Fatal(err)
		}
	}
}
