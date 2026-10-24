package replication

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"slopdb/db"
	"slopdb/storage"
)

func TestDurableBarrierRecoversRotatedJournalWALs(t *testing.T) {
	options := storage.DefaultOptions()
	options.WriteBufferSize = 4096
	options.CompactionL0Trigger = 1000
	c := asyncConfig(t, "primary", "tcp")
	c.Bootstrap, c.StorageOptions = true, options
	c.RaftConfig.ElectionTimeout = time.Second
	leader := openTestNode(t, c)
	ready(t, leader)
	exec(t, leader, "CREATE TABLE items(id INTEGER PRIMARY KEY,v TEXT)")
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	nodes := []*Node{leader}
	paths := []string{c.Directory}
	for i := 1; i <= 2; i++ {
		peer := asyncConfig(t, fmt.Sprintf("journal-%d", i), "tcp")
		peer.Join, peer.LogsOnly, peer.StorageOptions = leader.ControlAddress(), true, options
		peer.RaftConfig.ElectionTimeout = time.Second
		node := openTestNode(t, peer)
		if err := leader.Promote(ctx, peer.ID); err != nil {
			t.Fatal(err)
		}
		nodes = append(nodes, node)
		paths = append(paths, peer.Directory)
	}
	// Consensus stores retain their default 4 MiB buffers independently of
	// application StorageOptions. Cross a consensus WAL rotation too.
	payload := strings.Repeat("rotated WAL payload ", 8192)
	for i := 1; i <= 32; i++ {
		exec(t, leader, fmt.Sprintf("BEGIN; INSERT INTO items VALUES(%d,'%s'); COMMIT ASYNC", i, payload))
	}
	target := leader.CurrentToken()
	if err := leader.WaitDurable(ctx, target); err != nil {
		t.Fatal(err)
	}
	stable := leader.DurableToken()
	if stable.Index < target.Index {
		t.Fatal("durable barrier did not cover the writes", stable, target)
	}
	certificate := leader.currentCertificate()
	for _, node := range nodes[1:] {
		if err := leader.postJSON(ctx, node.ControlAddress(), "/v1/durability/finalize", certificate, nil); err != nil {
			t.Fatal(err)
		}
		assertLogsOnlyStore(t, node)
	}
	for _, node := range nodes {
		stats := node.store.tree.Stats()
		if stats.CompletedFlushes == 0 && stats.ImmSize == 0 {
			t.Fatal("fixture did not rotate consensus WALs", node.config.ID, stats)
		}
	}
	for i := len(nodes) - 1; i >= 0; i-- {
		if err := nodes[i].Close(); err != nil {
			t.Fatal(err)
		}
	}
	// Require recovery from journal payloads, retaining the primary's identity
	// evidence but removing its consensus log and snapshot data.
	for _, name := range []string{"raft-log", "snapshots"} {
		if err := os.RemoveAll(filepath.Join(c.Directory, name)); err != nil {
			t.Fatal(err)
		}
	}
	destination := filepath.Join(t.TempDir(), "recovered")
	if _, err := RecoverStable(ctx, RecoveryConfig{Sources: paths, Destination: destination, Token: testToken, Fenced: true, Minimum: &stable}); err != nil {
		t.Fatal(err)
	}
	restored, err := db.Open(destination, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = restored.Close() })
	rows, err := restored.Exec("SELECT id,v FROM items ORDER BY id")
	if err != nil {
		t.Fatal(err)
	}
	if len(rows.Rows) != 32 {
		t.Fatal("recovery lost rows", len(rows.Rows))
	}
	for i, row := range rows.Rows {
		if row["id"].Int != int64(i+1) || row["v"].Text != payload {
			t.Fatal("recovery changed a row", i, row)
		}
	}
	if _, err := restored.CheckIntegrity(); err != nil {
		t.Fatal(err)
	}
	if err := restored.Close(); err != nil {
		t.Fatal(err)
	}
}
