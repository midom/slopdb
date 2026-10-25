package replication

import (
	"bytes"
	"context"
	"os"
	"testing"

	"slopdb/storage"
)

func TestSnapshotRestoresExactChangeLogAndExcludesTemporaryRows(t *testing.T) {
	config := testConfig(t, "cdc-snapshot", "tcp")
	config.Bootstrap = true
	node := openTestNode(t, config)
	ready(t, node)
	exec(t, node, "CREATE TABLE items(id INTEGER PRIMARY KEY,v TEXT);INSERT INTO items VALUES(1,'before activation')")
	if _, err := node.Database().EnableCDC(context.Background()); err != nil {
		t.Fatal(err)
	}
	exec(t, node, "ALTER TABLE items ADD COLUMN extra INTEGER;UPDATE items SET v='changed'")
	session, err := node.Database().NewSession()
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	if _, err = session.Exec("CREATE TEMPORARY TABLE scratch(id INTEGER PRIMARY KEY,v TEXT);INSERT INTO scratch VALUES(1,'private snapshot payload')"); err != nil {
		t.Fatal(err)
	}
	snapshot, err := node.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.Release()
	var payload bytes.Buffer
	if err = snapshot.(*stateSnapshot).write(&payload); err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(payload.Bytes(), []byte("private snapshot payload")) {
		t.Fatal("replica snapshot contains session-local rows")
	}
	dir, _, err := node.decodeSnapshot(bytes.NewReader(payload.Bytes()))
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	restored, err := storage.NewLSMTree(dir, node.storageOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer restored.Close()
	original := node.Database().ReplicationStorage()
	a, err := original.ReadChanges(context.Background(), 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	b, err := restored.ReadChanges(context.Background(), 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(a) != len(b) {
		t.Fatal("restoring snapshot synthesized journal entries", len(a), len(b))
	}
	for i := range a {
		if a[i].Sequence != b[i].Sequence || a[i].Time != b[i].Time || len(a[i].Mutations) != len(b[i].Mutations) {
			t.Fatal(a[i], b[i])
		}
		for j := range a[i].Mutations {
			x, y := a[i].Mutations[j], b[i].Mutations[j]
			if !bytes.Equal(x.Key, y.Key) || !bytes.Equal(x.Before, y.Before) || !bytes.Equal(x.After, y.After) || x.BeforeExists != y.BeforeExists || x.Delete != y.Delete {
				t.Fatal(x, y)
			}
		}
	}
	ah, err := original.ChangeLogHeader()
	if err != nil {
		t.Fatal(err)
	}
	bh, err := restored.ChangeLogHeader()
	if err != nil {
		t.Fatal(err)
	}
	if ah.SourceID != bh.SourceID || !bytes.Equal(ah.Catalog["\x00__schema__"], bh.Catalog["\x00__schema__"]) {
		t.Fatal("snapshot changed CDC baseline", ah, bh)
	}
	it := restored.NewIteratorRange([]byte{3}, []byte{4}, false)
	defer it.Close()
	if it.Valid() {
		t.Fatal("restored temporary range is not empty")
	}
}
