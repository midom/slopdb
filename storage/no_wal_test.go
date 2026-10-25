package storage

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestMutationNoWALRemainsLocalMetadata(t *testing.T) {
	mutation := Mutation{Key: []byte("key"), Value: []byte("value")}
	durable, err := json.Marshal(mutation)
	if err != nil {
		t.Fatal(err)
	}
	mutation.NoWAL = true
	local, err := json.Marshal(mutation)
	if err != nil || !bytes.Equal(durable, local) {
		t.Fatalf("local publication flag changed persisted mutation format: %s %s %v", durable, local, err)
	}
	var decoded Mutation
	if err := json.Unmarshal([]byte(`{"Key":"a2V5","Value":"dmFsdWU=","NoWAL":true}`), &decoded); err != nil || decoded.NoWAL || string(decoded.Key) != "key" {
		t.Fatalf("persisted mutation adopted a local publication flag: %+v %v", decoded, err)
	}
}

func TestAtomicBatchOmitsLocalMutationsFromWAL(t *testing.T) {
	dir := t.TempDir()
	tree, err := NewLSMTree(dir, DefaultOptions())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = tree.Close() })
	local := []byte{3, 1, 'k'}
	if err = tree.WriteBatch([]Mutation{{Key: []byte("durable-a"), Value: []byte("a")}, {Key: local, Value: []byte("never-in-wal"), NoWAL: true}, {Key: []byte("durable-z"), Value: []byte("z")}}); err != nil {
		t.Fatal(err)
	}
	for _, key := range [][]byte{[]byte("durable-a"), local, []byte("durable-z")} {
		if _, ok, e := tree.Get(key); e != nil || !ok {
			t.Fatalf("atomic publication %q: %v %v", key, ok, e)
		}
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	records := 0
	for _, entry := range entries {
		if !strings.HasSuffix(entry.Name(), ".log") {
			continue
		}
		data, e := os.ReadFile(filepath.Join(dir, entry.Name()))
		if e != nil {
			t.Fatal(e)
		}
		if bytes.Contains(data, []byte("never-in-wal")) || bytes.Contains(data, local) {
			t.Fatal("temporary bytes recorded in WAL")
		}
		if bytes.Contains(data, []byte("durable-a")) {
			records++
		}
	}
	if records != 1 {
		t.Fatalf("durable WAL records=%d", records)
	}
	before := tree.Snapshot()
	defer tree.ReleaseSnapshot(before)
	if err = tree.WriteBatch([]Mutation{{Key: local, Delete: true, NoWAL: true}}); err != nil {
		t.Fatal(err)
	}
	if _, ok, e := tree.Get(local); e != nil || ok {
		t.Fatalf("local delete: %v %v", ok, e)
	}
	if v, ok, e := tree.GetAt(local, before); e != nil || !ok || string(v) != "never-in-wal" {
		t.Fatalf("local MVCC: %q %v %v", v, ok, e)
	}
}

func TestLocalMutationsNeverEnterReplication(t *testing.T) {
	tree, err := NewLSMTree(t.TempDir(), DefaultOptions())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = tree.Close() })
	calls := 0
	reject := false
	tree.SetReplicationHooks(&ReplicationHooks{Write: func(batch []Mutation, queued func()) error {
		calls++
		for _, w := range batch {
			if w.NoWAL || len(w.Key) > 0 && w.Key[0] == 3 {
				t.Fatal("local mutation reached replication")
			}
		}
		if queued != nil {
			queued()
		}
		if reject {
			return errors.New("proposal rejected")
		}
		return tree.ApplyReplicated(batch)
	}})
	queued := 0
	batch := []Mutation{{Key: []byte("persistent"), Value: []byte("one")}, {Key: []byte{3, 'a'}, Value: []byte("local"), NoWAL: true}}
	if err = tree.WriteBatchWithEnqueue(batch, func() { queued++ }); err != nil {
		t.Fatal(err)
	}
	if calls != 1 || queued != 1 {
		t.Fatal(calls, queued)
	}
	if _, ok, e := tree.Get([]byte{3, 'a'}); e != nil || !ok {
		t.Fatal(ok, e)
	}
	if err = tree.WriteBatch([]Mutation{{Key: []byte{3, 'b'}, Value: []byte("private"), NoWAL: true}}); err != nil {
		t.Fatal(err)
	}
	if calls != 1 {
		t.Fatal("pure local batch entered consensus")
	}
	reject = true
	batch[0].Value = []byte("two")
	batch[1].Value = []byte("changed")
	if err = tree.WriteBatch(batch); err == nil {
		t.Fatal("rejected proposal succeeded")
	}
	if v, _, _ := tree.Get([]byte{3, 'a'}); string(v) != "local" {
		t.Fatal("local data published for rejected transaction")
	}
}
