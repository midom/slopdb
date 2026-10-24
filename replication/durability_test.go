package replication

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slopdb/db"
	"slopdb/storage"
	"testing"
	"time"

	"github.com/hashicorp/raft"
)

func TestStableRecoveryRejectsBrokenLogChain(t *testing.T) {
	config := asyncConfig(t, "broken-log", "tcp")
	config.Bootstrap = true
	node := openTestNode(t, config)
	ready(t, node)
	exec(t, node, "CREATE TABLE items(id INTEGER PRIMARY KEY,v TEXT); BEGIN; INSERT INTO items VALUES(1,'stable'); COMMIT DURABLE")
	stable := node.DurableToken()
	if err := node.Close(); err != nil {
		t.Fatal(err)
	}
	// Damage a checksummed log payload through storage so this exercises the
	// independent consensus hash rather than the native WAL CRC detector.
	tree, err := storage.NewLSMTree(filepath.Join(config.Directory, "raft-log"), storage.DefaultOptions())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = tree.Close() })
	store := &diskStore{tree: tree, async: true}
	var log raft.Log
	if err = store.GetLog(stable.Index, &log); err != nil {
		t.Fatal(err)
	}
	original := bytes.Clone(log.Data)
	log.Data = []byte(`{"Version":1,"Kind":"fence","extra":"corrupt history"}`)
	value, _ := json.Marshal(log)
	if err = tree.ApplyReplicated([]storage.Mutation{{Key: logKey(stable.Index), Value: value}}); err != nil {
		t.Fatal(err)
	}
	if err = tree.Close(); err != nil {
		t.Fatal(err)
	}
	destination := filepath.Join(t.TempDir(), "recovered")
	recovery := RecoveryConfig{Sources: []string{config.Directory}, Destination: destination, Token: testToken, Fenced: true}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err = RecoverStable(ctx, recovery); !errors.Is(err, ErrHistory) {
		t.Fatalf("broken log chain accepted: %v", err)
	}
	for _, path := range []string{destination, filepath.Join(config.Directory, "RETIRED")} {
		if _, err = os.Stat(path); !os.IsNotExist(err) {
			t.Fatalf("failed recovery published or retired %s: %v", path, err)
		}
	}
	tree, err = storage.NewLSMTree(filepath.Join(config.Directory, "raft-log"), storage.DefaultOptions())
	if err != nil {
		t.Fatal(err)
	}
	log.Data = original
	value, _ = json.Marshal(log)
	if err = tree.ApplyReplicated([]storage.Mutation{{Key: logKey(stable.Index), Value: value}}); err != nil {
		t.Fatal(err)
	}
	if err = tree.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err = RecoverStable(ctx, recovery); err != nil {
		t.Fatalf("failed recovery damaged its evidence: %v", err)
	}
}

func asyncConfig(t *testing.T, id, network string) Config {
	t.Helper()
	config := testConfig(t, id, network)
	config.Durability = DurabilityAsync
	config.DurabilityInterval = time.Hour
	config.MaxDurabilityLag = 2 * time.Hour
	return config
}
func TestAsyncAndDurableCommits(t *testing.T) {
	config := asyncConfig(t, "first", "tcp")
	config.Bootstrap = true
	leader := openTestNode(t, config)
	ready(t, leader)
	stable := leader.durableIndex()
	exec(t, leader, "CREATE TABLE items(id INTEGER PRIMARY KEY,v TEXT); INSERT INTO items VALUES(1,'async')")
	token := leader.CurrentToken()
	if token.Index <= stable || leader.durableIndex() != stable {
		t.Fatalf("async write unexpectedly stable: token=%+v D=%d prior=%d", token, leader.durableIndex(), stable)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if e := leader.WaitDurable(ctx, token); e != nil {
		t.Fatal(e)
	}
	if leader.durableIndex() < token.Index {
		t.Fatal("durable wait did not advance D")
	}
	tx, e := leader.Database().Begin()
	if e != nil {
		t.Fatal(e)
	}
	if _, e = tx.Exec("INSERT INTO items VALUES(2,'durable')"); e != nil {
		t.Fatal(e)
	}
	if e = tx.CommitDurableContext(ctx); e != nil {
		t.Fatal(e)
	}
	if leader.durableIndex() != leader.applied.Load() {
		t.Fatalf("COMMIT DURABLE returned before seal: D=%d R=%d", leader.durableIndex(), leader.applied.Load())
	}
	if e = leader.WaitDurable(ctx, DurabilityToken{Cluster: "old-incarnation", Index: 1}); !errors.Is(e, ErrHistory) {
		t.Fatalf("foreign token accepted: %v", e)
	}
	assertCount(t, leader, 2)
}
func TestAsyncReplicaReclonesBeforeVoting(t *testing.T) {
	for _, network := range []string{"tcp", "unix"} {
		t.Run(network, func(t *testing.T) {
			config := asyncConfig(t, "first", network)
			config.Bootstrap = true
			source := openTestNode(t, config)
			ready(t, source)
			exec(t, source, "CREATE TABLE items(id INTEGER PRIMARY KEY,v TEXT); INSERT INTO items VALUES(1,'base')")
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			var replicas []*Node
			var configs []Config
			for i := 2; i <= 3; i++ {
				c := asyncConfig(t, fmt.Sprintf("node%d", i), network)
				c.Join = source.ControlAddress()
				node := openTestNode(t, c)
				if e := source.Promote(ctx, c.ID); e != nil {
					t.Fatal(e)
				}
				c.Listen = node.Address()
				c.ControlListen = node.ControlAddress()
				c.Join = ""
				configs = append(configs, c)
				replicas = append(replicas, node)
			}
			target := replicas[0]
			old := target.dataDirectory
			if e := target.Close(); e != nil {
				t.Fatal(e)
			}
			exec(t, source, "INSERT INTO items VALUES(2,'while offline')")
			restarted := openTestNode(t, configs[0])
			deadline := time.NewTimer(15 * time.Second)
			defer deadline.Stop()
			for restarted.raftNode() == nil {
				select {
				case <-deadline.C:
					t.Fatalf("rejoin failed: source=%+v target=%+v", source.Status(), restarted.Status())
				case <-time.After(10 * time.Millisecond):
				}
			}
			if restarted.dataDirectory == old {
				t.Fatal("recovered node reused its volatile application state")
			}
			if e := source.TransferLeadership(ctx, configs[0].ID); e != nil {
				t.Fatal(e)
			}
			ready(t, restarted)
			assertCount(t, restarted, 2)
			if restarted.raftNode().State() != raft.Leader {
				t.Fatal("recovered voter could not become leader")
			}
		})
	}
}

func TestStableRecoveryDropsOnlyUnstableSuffix(t *testing.T) {
	config := asyncConfig(t, "one", "tcp")
	config.Bootstrap = true
	leader := openTestNode(t, config)
	ready(t, leader)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	nodes := []*Node{leader}
	configs := []Config{config}
	for _, id := range []string{"two", "three"} {
		c := asyncConfig(t, id, "tcp")
		c.Join = leader.ControlAddress()
		node := openTestNode(t, c)
		if e := leader.Promote(ctx, id); e != nil {
			t.Fatal(e)
		}
		nodes = append(nodes, node)
		configs = append(configs, c)
	}
	exec(t, leader, "CREATE TABLE items(id INTEGER PRIMARY KEY,v TEXT); CREATE INDEX item_values ON items(v); BEGIN; INSERT INTO items VALUES(1,'stable'); COMMIT DURABLE")
	stable := leader.CurrentToken()
	if stable.Index != leader.durableIndex() {
		t.Fatal("durable commit did not seal its prefix")
	}
	exec(t, leader, "BEGIN; INSERT INTO items VALUES(2,'lost after cluster failure'); UPDATE items SET v='unstable rewrite' WHERE id=1; DROP INDEX item_values; COMMIT ASYNC")
	if leader.applied.Load() <= stable.Index {
		t.Fatal("no unstable suffix")
	}
	paths := []string{}
	for i, node := range nodes {
		paths = append(paths, node.config.Directory)
		configs[i].Listen = node.Address()
		configs[i].ControlListen = node.ControlAddress()
		configs[i].Join = ""
		configs[i].Bootstrap = false
		configs[i].SeedDirectory = ""
	}
	for i := len(nodes) - 1; i >= 0; i-- {
		if e := nodes[i].Close(); e != nil {
			t.Fatal(e)
		}
	}
	// Model loss of every volatile consensus suffix and application generation.
	for _, path := range paths {
		options := storage.DefaultOptions()
		tree, e := storage.NewLSMTree(filepath.Join(path, "raft-log"), options)
		if e != nil {
			t.Fatal(e)
		}
		store := &diskStore{tree: tree, async: true}
		last, e := store.LastIndex()
		if e != nil {
			t.Fatal(e)
		}
		if last > stable.Index {
			if e = store.DeleteRange(stable.Index+1, last); e != nil {
				t.Fatal(e)
			}
		}
		if e = tree.Close(); e != nil {
			t.Fatal(e)
		}
		directory, e := generation(path)
		if e != nil {
			t.Fatal(e)
		}
		if e = os.RemoveAll(directory); e != nil {
			t.Fatal(e)
		}
	}
	cold := []*Node{}
	for _, c := range configs {
		cold = append(cold, openTestNode(t, c))
	}
	blocked, stop := context.WithTimeout(context.Background(), 400*time.Millisecond)
	if e := cold[0].WaitReady(blocked); !errors.Is(e, context.DeadlineExceeded) {
		stop()
		t.Fatalf("cold cluster started voting: %v", e)
	}
	stop()
	for _, node := range cold {
		if node.raftNode() != nil || !node.Status().Recovering {
			t.Fatal("cold async node started a Raft election")
		}
		if _, e := node.Database().Exec("SELECT 1"); !errors.Is(e, ErrRecovering) {
			t.Fatalf("cold node served SQL: %v", e)
		}
		if e := node.Close(); e != nil {
			t.Fatal(e)
		}
	}
	destination := filepath.Join(t.TempDir(), "recovered")
	recovery := RecoveryConfig{Sources: paths, Destination: destination, Token: testToken, Fenced: true}
	missing := recovery
	missing.Sources = paths[:2]
	if _, e := RecoverStable(ctx, missing); e == nil {
		t.Fatal("recovery accepted incomplete voter set")
	}
	minimum := stable
	minimum.Index++
	ahead := recovery
	ahead.Minimum = &minimum
	if _, e := RecoverStable(ctx, ahead); e == nil {
		t.Fatal("recovery silently lost a required durable token")
	}
	result, e := RecoverStable(ctx, recovery)
	if e != nil {
		t.Fatal(e)
	}
	if result.Stable != stable {
		t.Fatalf("recovered wrong prefix: %+v want %+v", result.Stable, stable)
	}
	restored, e := db.Open(destination, nil)
	if e != nil {
		t.Fatal(e)
	}
	rows, e := restored.Exec("SELECT id,v FROM items ORDER BY id")
	if e != nil || len(rows.Rows) != 1 || rows.Rows[0]["id"].Int != 1 || rows.Rows[0]["v"].Text != "stable" {
		t.Fatalf("recovered rows=%+v error=%v", rows, e)
	}
	foundIndex := false
	for _, index := range restored.GetIndexList() {
		if index == "item_values" {
			foundIndex = true
		}
	}
	if !foundIndex {
		t.Fatal("recovery did not restore the stable secondary index")
	}
	if _, found, e := restored.ReplicationStorage().Get(appliedKey); e != nil || found {
		t.Fatalf("old epoch metadata copied into standalone seed: %v %v", found, e)
	}
	if e = restored.Close(); e != nil {
		t.Fatal(e)
	}
	if retry, e := RecoverStable(ctx, recovery); e != nil || retry.RecoveryID != result.RecoveryID {
		t.Fatalf("recovery retirement could not resume: %+v %v", retry, e)
	}
	if _, e := Open(ctx, configs[0]); !errors.Is(e, ErrRetired) {
		t.Fatalf("old epoch restarted: %v", e)
	}
	fresh := asyncConfig(t, "new-one", "tcp")
	fresh.Bootstrap = true
	fresh.SeedDirectory = destination
	next := openTestNode(t, fresh)
	ready(t, next)
	assertCount(t, next, 1)
	if next.cluster == stable.Cluster {
		t.Fatal("recovery reused old consensus epoch")
	}
	if e = next.WaitDurable(ctx, stable); !errors.Is(e, ErrHistory) {
		t.Fatalf("old epoch token accepted: %v", e)
	}
	modified, e := db.Open(destination, nil)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { _ = modified.Close() })
	if _, e = modified.Exec("INSERT INTO items VALUES(9,'changed seed')"); e != nil {
		t.Fatal(e)
	}
	if e = modified.Close(); e != nil {
		t.Fatal(e)
	}
	if _, e = RecoverStable(ctx, recovery); e == nil {
		t.Fatal("recovery resumed against a modified export")
	}
}

func TestAsyncPeriodicFlushAndLagGuard(t *testing.T) {
	periodic := asyncConfig(t, "periodic", "tcp")
	periodic.Bootstrap = true
	periodic.DurabilityInterval = 20 * time.Millisecond
	periodic.MaxDurabilityLag = time.Second
	node := openTestNode(t, periodic)
	ready(t, node)
	exec(t, node, "CREATE TABLE items(id INTEGER PRIMARY KEY,v TEXT); INSERT INTO items VALUES(1,'periodic')")
	token := node.CurrentToken()
	deadline := time.NewTimer(3 * time.Second)
	defer deadline.Stop()
	for node.durableIndex() < token.Index {
		select {
		case <-deadline.C:
			t.Fatalf("periodic flush did not advance D: %+v", node.Status())
		case <-time.After(5 * time.Millisecond):
		}
	}
	manual := asyncConfig(t, "lagged", "tcp")
	manual.Bootstrap = true
	lagged := openTestNode(t, manual)
	ready(t, lagged)
	exec(t, lagged, "CREATE TABLE items(id INTEGER PRIMARY KEY,v TEXT); INSERT INTO items VALUES(1,'pending')")
	lagged.durability.publication.Lock()
	lagged.durability.pendingSince = time.Now().Add(-3 * time.Hour)
	lagged.durability.publication.Unlock()
	if _, e := lagged.Database().Exec("INSERT INTO items VALUES(2,'too old')"); !errors.Is(e, ErrDurabilityLag) {
		t.Fatalf("lag guard admitted a write: %v", e)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if e := lagged.FlushDurable(ctx); e != nil {
		t.Fatal(e)
	}
	exec(t, lagged, "INSERT INTO items VALUES(2,'after seal')")
	assertCount(t, lagged, 2)
}
func TestDurabilityCertificateValidation(t *testing.T) {
	config := asyncConfig(t, "first", "tcp")
	config.Bootstrap = true
	node := openTestNode(t, config)
	ready(t, node)
	original := node.currentCertificate()
	value, e := json.Marshal(original)
	if e != nil {
		t.Fatal(e)
	}
	corrupt := func() *durableCertificate {
		var c durableCertificate
		if e := json.Unmarshal(value, &c); e != nil {
			t.Fatal(e)
		}
		return &c
	}
	for _, change := range []func(*durableCertificate){func(c *durableCertificate) { c.Body.Hash[0] ^= 1 }, func(c *durableCertificate) { c.Body.Cluster = "other" }, func(c *durableCertificate) { c.Receipts = nil }, func(c *durableCertificate) { c.Receipts = append(c.Receipts, c.Receipts[0]) }, func(c *durableCertificate) { c.Body.Configuration.Servers[0].Address = "tcp://127.0.0.1:1" }} {
		c := corrupt()
		change(c)
		if e := validateCertificate(c, testToken, node.cluster); e == nil {
			t.Fatal("corrupt certificate accepted")
		}
	}
	if e := validateCertificate(original, "wrong token that is long enough", node.cluster); e == nil {
		t.Fatal("foreign certificate authenticated")
	}
	// A timed-out caller waiting behind another durability barrier must return
	// promptly, even before the held coordinator is released.
	node.durability.publication.Lock()
	if node.durability.coordinatorGate == nil {
		node.durability.coordinatorGate = make(chan struct{}, 1)
	}
	gate := node.durability.coordinatorGate
	node.durability.publication.Unlock()
	gate <- struct{}{}
	defer func() { <-gate }()
	exec(t, node, "CREATE TABLE pending(id INTEGER PRIMARY KEY)")
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	began := time.Now()
	if e := node.FlushDurable(ctx); !errors.Is(e, context.DeadlineExceeded) {
		t.Fatalf("barrier ignored cancellation: %v", e)
	}
	if time.Since(began) > time.Second {
		t.Fatal("barrier cancellation was not prompt")
	}
}
func TestAsyncMutualTLS(t *testing.T) {
	server, client := nodeTLS(t)
	config := asyncConfig(t, "first", "tcp")
	config.Bootstrap = true
	config.TLSConfig = server
	config.ClientTLSConfig = client
	source := openTestNode(t, config)
	ready(t, source)
	other := asyncConfig(t, "second", "tcp")
	other.TLSConfig = server
	other.ClientTLSConfig = client
	other.Join = source.ControlAddress()
	replica := openTestNode(t, other)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if e := source.Promote(ctx, "second"); e != nil {
		t.Fatal(e)
	}
	exec(t, source, "CREATE TABLE items(id INTEGER PRIMARY KEY,v TEXT); BEGIN; INSERT INTO items VALUES(1,'mutual TLS'); COMMIT DURABLE")
	token := source.DurableToken()
	if e := source.TransferLeadership(ctx, "second"); e != nil {
		t.Fatal(e)
	}
	ready(t, replica)
	if e := replica.WaitDurable(ctx, token); e != nil {
		t.Fatal(e)
	}
	assertCount(t, replica, 1)
}
