package replication

import (
	"bytes"
	"context"
	"encoding/json"
	"path/filepath"
	"testing"

	"github.com/hashicorp/raft"
	"slopdb/db"
	"slopdb/storage"
)

func batchTestNode(t *testing.T, mode Durability) *Node {
	t.Helper()
	options := storage.DefaultOptions()
	options.Replica = true
	directory := filepath.Join(t.TempDir(), "data")
	database, err := db.Open(directory, options)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	node := &Node{config: Config{Directory: directory, Durability: mode}, database: database, cluster: "batch-test", ctx: ctx, cancel: cancel}
	t.Cleanup(func() { _ = node.Close() })
	return node
}

func batchTestLog(t *testing.T, index uint64, c command) *raft.Log {
	t.Helper()
	value, err := json.Marshal(c)
	if err != nil {
		t.Fatal(err)
	}
	return &raft.Log{Index: index, Term: 1, Type: raft.LogCommand, Data: value}
}

func TestApplyBatchOrderingReplayAndReopen(t *testing.T) {
	for _, mode := range []Durability{DurabilitySync, DurabilityAsync} {
		t.Run(string(mode), func(t *testing.T) {
			node := batchTestNode(t, mode)
			wal := func(index uint64, writes ...storage.Mutation) *raft.Log {
				return batchTestLog(t, index, command{Version: 1, Kind: "wal", Writes: writes})
			}
			logs := []*raft.Log{
				wal(1, storage.Mutation{Key: []byte("a"), Value: []byte("old")}, storage.Mutation{Key: []byte("b"), Value: []byte("old")}),
				wal(2, storage.Mutation{Key: []byte("a"), Value: []byte("c"), RangeDelete: true}),
				wal(3, storage.Mutation{Key: []byte("b"), Value: []byte("new")}),
				batchTestLog(t, 4, command{Version: 1, Kind: "page-size", PageSize: 8192}),
				{Index: 5, Term: 1, Type: raft.LogConfiguration, Data: raft.EncodeConfiguration(raft.Configuration{Servers: []raft.Server{{ID: "one", Address: "tcp://127.0.0.1:1", Suffrage: raft.Voter}}})},
				wal(6, storage.Mutation{Key: []byte("d"), Value: []byte("last")}),
				batchTestLog(t, 7, command{Version: 1, Kind: "fence"}),
			}
			check := func(tree *storage.LSMTree) {
				t.Helper()
				for key, want := range map[string]string{"a": "", "b": "new", "d": "last"} {
					value, found, err := tree.Get([]byte(key))
					if err != nil || found != (want != "") || string(value) != want {
						t.Fatalf("key=%s value=%q found=%v err=%v", key, value, found, err)
					}
				}
				cp, err := readCheckpoint(tree, node.cluster)
				if err != nil || cp.Index != 7 || cp.Term != 1 || tree.PageSize() != 8192 {
					t.Fatalf("checkpoint=%+v page size=%d err=%v", cp, tree.PageSize(), err)
				}
			}
			for _, response := range node.ApplyBatch(logs) {
				if response != nil {
					t.Fatal(response)
				}
			}
			if node.configurationIndex.Load() != 5 {
				t.Fatal("batch did not publish its configuration checkpoint")
			}
			check(node.Database().ReplicationStorage())
			sequence := node.Database().ReplicationStorage().Snapshot()
			node.Database().ReplicationStorage().ReleaseSnapshot(sequence)
			// Replayed committed entries, including configuration/setting changes,
			// must perform no additional storage write.
			node.ApplyBatch(logs)
			after := node.Database().ReplicationStorage().Snapshot()
			node.Database().ReplicationStorage().ReleaseSnapshot(after)
			if after != sequence {
				t.Fatal("batch replay changed native state")
			}
			directory := node.config.Directory
			if err := node.Close(); err != nil {
				t.Fatal(err)
			}
			tree, err := storage.NewLSMTree(directory, storage.DefaultOptions())
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = tree.Close() })
			check(tree)
			if err = tree.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestApplyBatchFailurePreservesValidPrefix(t *testing.T) {
	node := batchTestNode(t, DurabilitySync)
	logs := []*raft.Log{
		batchTestLog(t, 1, command{Version: 1, Kind: "wal", Writes: []storage.Mutation{{Key: []byte("a"), Value: []byte("valid")}}}),
		batchTestLog(t, 2, command{Version: 1, Kind: "wal", Writes: []storage.Mutation{{Key: appliedKey, Value: []byte("invalid checkpoint override")}}}),
		batchTestLog(t, 3, command{Version: 1, Kind: "wal", Writes: []storage.Mutation{{Key: []byte("b"), Value: []byte("must not apply")}}}),
	}
	responses := node.ApplyBatch(logs)
	if len(responses) != 3 || responses[0] != nil || responses[1] == nil || responses[2] == nil {
		t.Fatalf("batch failure responses: %+v", responses)
	}
	tree := node.Database().ReplicationStorage()
	value, found, err := tree.Get([]byte("a"))
	if err != nil || !found || string(value) != "valid" {
		t.Fatalf("valid prefix lost: %q %v %v", value, found, err)
	}
	if _, found, err = tree.Get([]byte("b")); err != nil || found {
		t.Fatal("unapplied suffix became visible")
	}
	cp, err := readCheckpoint(tree, node.cluster)
	if err != nil || cp.Index != 1 || node.Err() == nil {
		t.Fatalf("failure checkpoint=%+v error=%v fatal=%v", cp, err, node.Err())
	}
	if err = node.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestApplyBatchLargeAtomicCommand(t *testing.T) {
	node := batchTestNode(t, DurabilitySync)
	large := bytes.Repeat([]byte{0xab}, maxApplyBatchBytes+1)
	logs := []*raft.Log{
		batchTestLog(t, 1, command{Version: 1, Kind: "wal", Writes: []storage.Mutation{{Key: []byte("large"), Value: large}}}),
		batchTestLog(t, 2, command{Version: 1, Kind: "wal", Writes: []storage.Mutation{{Key: []byte("last"), Value: []byte("tail")}}}),
	}
	for _, response := range node.ApplyBatch(logs) {
		if response != nil {
			t.Fatal(response)
		}
	}
	tree := node.Database().ReplicationStorage()
	value, found, err := tree.Get([]byte("large"))
	if err != nil || !found || !bytes.Equal(value, large) {
		t.Fatal("large transaction was split or truncated")
	}
	value, found, err = tree.Get([]byte("last"))
	if err != nil || !found || string(value) != "tail" {
		t.Fatal("large transaction lost following entry")
	}
	if cp, err := readCheckpoint(tree, node.cluster); err != nil || cp.Index != 2 {
		t.Fatalf("large transaction checkpoint=%+v err=%v", cp, err)
	}
	if err = node.Close(); err != nil {
		t.Fatal(err)
	}
}
