package storage

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

type cancelDuringBackup struct {
	context.Context
	cancel context.CancelFunc
	checks int
}

func (c *cancelDuringBackup) Err() error {
	c.checks++
	if c.checks == 10 {
		c.cancel()
	}
	return c.Context.Err()
}
func TestBackupCancellationRemovesStaging(t *testing.T) {
	source, e := NewLSMTree(filepath.Join(t.TempDir(), "source"), nil)
	if e != nil {
		t.Fatal(e)
	}
	defer source.Close()
	entries := make([]Mutation, 100)
	for i := range entries {
		entries[i] = Mutation{Key: []byte{1, byte(i)}, Value: []byte("value")}
	}
	if e = source.WriteBatch(entries); e != nil {
		t.Fatal(e)
	}
	contextValue, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx := &cancelDuringBackup{Context: contextValue, cancel: cancel}
	parent := t.TempDir()
	target := filepath.Join(parent, "backup")
	if e = source.Backup(ctx, target); !errors.Is(e, context.Canceled) {
		t.Fatal(e)
	}
	files, e := os.ReadDir(parent)
	if e != nil {
		t.Fatal(e)
	}
	if len(files) != 0 {
		t.Fatal("cancelled backup leaked target, staging or lock", files)
	}
}
