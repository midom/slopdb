package replication

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	"slopdb/db"
	"slopdb/storage"
)

func assertLogsOnlyStore(t testing.TB, n *Node) {
	t.Helper()
	n.fsmMu.Lock()
	defer n.fsmMu.Unlock()
	it := n.Database().ReplicationStorage().NewIterator()
	defer it.Close()
	for it.Seek(nil); it.Valid(); it.Next() {
		if !replicationMetadata(it.Key().UserKey) {
			t.Fatalf("logs-only node materialized SQL key %x", it.Key().UserKey)
		}
	}
	if err := storage.IteratorError(it); err != nil {
		t.Fatal(err)
	}
	if n.currentPosition() != "" {
		t.Fatal("logs-only node advertised a SQL visibility position")
	}
	n.durableReads.mu.Lock()
	defer n.durableReads.mu.Unlock()
	if n.durableReads.latest != nil || len(n.durableReads.pending) != 0 {
		t.Fatal("logs-only node retained SQL read snapshots")
	}
}

func waitMember(t *testing.T, n *Node, id, suffrage string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for {
		for _, m := range n.Status().Members {
			if m.ID == id && m.Suffrage == suffrage {
				return
			}
		}
		select {
		case <-ctx.Done():
			t.Fatalf("member %s did not become %s: %+v", id, suffrage, n.Status())
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func TestPermanentNonVotingReplica(t *testing.T) {
	for _, mode := range []Durability{DurabilitySync, DurabilityAsync} {
		for _, network := range []string{"tcp", "unix"} {
			t.Run(string(mode)+"/"+network, func(t *testing.T) {
				c := testConfig(t, "primary", network)
				c.Bootstrap, c.Durability = true, mode
				source := openTestNode(t, c)
				ready(t, source)
				exec(t, source, "CREATE TABLE items(id INTEGER PRIMARY KEY,v TEXT); INSERT INTO items VALUES(1,'one')")
				r := testConfig(t, "reader", network)
				r.Join, r.NonVoting, r.AutoPromote, r.ReplicaReadMaxStaleness = source.ControlAddress(), true, true, time.Second
				replica := openTestNode(t, r)
				ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
				defer cancel()
				waitMember(t, source, r.ID, "Nonvoter")
				if replica.config.AutoPromote {
					t.Fatal("permanent nonvoter kept auto promotion enabled")
				}
				if err := source.Promote(ctx, r.ID); !errors.Is(err, ErrNonVoting) {
					t.Fatalf("manual promotion: %v", err)
				}
				exec(t, source, "INSERT INTO items VALUES(2,'two')")
				waitApplied(t, replica, source.applied.Load())
				assertCount(t, replica, 2)
				// A disconnected nonvoter cannot enlarge the commit quorum.
				r.Listen, r.ControlListen, r.Join = replica.Address(), replica.ControlAddress(), ""
				if err := replica.Close(); err != nil {
					t.Fatal(err)
				}
				exec(t, source, "INSERT INTO items VALUES(3,'three')")
				restarted := openTestNode(t, r)
				deadline := time.Now().Add(10 * time.Second)
				for restarted.recovering.Load() || restarted.raftNode() == nil {
					if time.Now().After(deadline) {
						t.Fatal("nonvoter did not rejoin")
					}
					time.Sleep(10 * time.Millisecond)
				}
				waitApplied(t, restarted, source.applied.Load())
				for {
					result, err := restarted.Database().ExecContext(ctx, "SELECT COUNT(*) AS n FROM items")
					if errors.Is(err, context.Canceled) && ctx.Err() == nil {
						continue // A late InstallSnapshot cancels the old generation.
					}
					if err != nil || len(result.Rows) != 1 || result.Rows[0]["n"].Int != 3 {
						t.Fatalf("restarted nonvoter read: %+v %v", result, err)
					}
					break
				}
				if !restarted.Status().NonVoting {
					t.Fatal("nonvoter role lost on restart")
				}
				if err := source.TransferLeadership(ctx, r.ID); err == nil {
					t.Fatal("nonvoter accepted leadership transfer")
				}
				for _, member := range source.Status().Members {
					if member.ID == r.ID && (member.Suffrage != "Nonvoter" || !member.NonVoting) {
						t.Fatalf("nonvoter role lost: %+v", member)
					}
				}
			})
		}
	}
}

func TestLogsOnlyVoterQuorumAndFailover(t *testing.T) {
	for _, mode := range []Durability{DurabilitySync, DurabilityAsync} {
		for _, network := range []string{"tcp", "unix"} {
			t.Run(string(mode)+"/"+network, func(t *testing.T) {
				c := testConfig(t, "primary", network)
				c.Bootstrap, c.Durability = true, mode
				source := openTestNode(t, c)
				ready(t, source)
				exec(t, source, "CREATE TABLE items(id INTEGER PRIMARY KEY,v TEXT); INSERT INTO items VALUES(1,'base')")
				ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
				defer cancel()
				full := testConfig(t, "full", network)
				full.Join = source.ControlAddress()
				other := openTestNode(t, full)
				if err := source.Promote(ctx, full.ID); err != nil {
					t.Fatal(err)
				}
				wc := testConfig(t, "journal", network)
				wc.Join, wc.LogsOnly, wc.AutoPromote = source.ControlAddress(), true, true
				// Role enforcement must override callers disabling pre-vote.
				wc.RaftConfig.PreVoteDisabled = true
				witness := openTestNode(t, wc)
				waitMember(t, source, wc.ID, "Voter")
				exec(t, source, "INSERT INTO items VALUES(2,'tail')")
				waitApplied(t, witness, source.applied.Load())
				waitApplied(t, other, source.applied.Load())
				assertLogsOnlyStore(t, witness)
				if !witness.Status().LogsOnly {
					t.Fatal("missing logs-only status")
				}
				for _, q := range []string{"SELECT 1", "CREATE TABLE forbidden(id INTEGER PRIMARY KEY)", "BEGIN"} {
					if _, err := witness.Database().Exec(q); !errors.Is(err, ErrLogsOnly) {
						t.Fatalf("SQL %q admitted: %v", q, err)
					}
				}
				if _, err := witness.Database().ExecWithOptions("SELECT 1", db.ExecOptions{ReadBarrier: db.ReadBarrierPrevious}); !errors.Is(err, ErrLogsOnly) {
					t.Fatal(err)
				}
				if err := witness.WaitPosition(ctx, source.currentPosition()); !errors.Is(err, ErrLogsOnly) {
					t.Fatal(err)
				}
				if _, err := witness.Snapshot(); !errors.Is(err, ErrLogsOnly) {
					t.Fatal("logs-only node generated an incomplete SQL snapshot")
				}
				if err := source.TransferLeadership(ctx, wc.ID); !errors.Is(err, ErrLogsOnly) {
					t.Fatalf("leadership transfer: %v", err)
				}
				// Also reject direct Raft TimeoutNow, beyond the control API.
				err := source.transport.TimeoutNow(raft.ServerID(wc.ID), raft.ServerAddress(witness.Address()), &raft.TimeoutNowRequest{RPCHeader: raft.RPCHeader{ProtocolVersion: raft.ProtocolVersionMax}}, &raft.TimeoutNowResponse{})
				if err == nil {
					t.Fatal("direct TimeoutNow admitted")
				}
				// Election eligibility still requires an up-to-date full log.
				waitApplied(t, other, source.applied.Load())
				term := source.raftNode().CurrentTerm()
				c.Listen, c.ControlListen, c.Bootstrap = source.Address(), source.ControlAddress(), false
				if err := source.Close(); err != nil {
					t.Fatal(err)
				}
				ready(t, other) // The remaining full copy elects with the journal vote.
				if other.raftNode().CurrentTerm() <= term {
					t.Fatal("failover did not elect a new term")
				}
				exec(t, other, "INSERT INTO items VALUES(3,'after failover'); BEGIN; COMMIT DURABLE")
				assertCount(t, other, 3)
				waitApplied(t, witness, other.applied.Load())
				assertLogsOnlyStore(t, witness)
				// Restore the failed full voter before restarting another voter:
				// async restarts need a live quorum to certify their fresh clone.
				c.Join = other.ControlAddress()
				returned := openTestNode(t, c)
				deadline := time.Now().Add(10 * time.Second)
				for returned.recovering.Load() || returned.raftNode() == nil {
					if time.Now().After(deadline) {
						t.Fatal("full voter failed to rejoin")
					}
					time.Sleep(10 * time.Millisecond)
				}
				waitApplied(t, returned, other.applied.Load())
				// Sync restart restores the opaque full base into metadata only;
				// async restart must re-clone before it votes again.
				wc.Listen, wc.ControlListen, wc.Join = witness.Address(), witness.ControlAddress(), ""
				if err := witness.Close(); err != nil {
					t.Fatal(err)
				}
				restarted := openTestNode(t, wc)
				deadline = time.Now().Add(10 * time.Second)
				for restarted.recovering.Load() || restarted.raftNode() == nil {
					if time.Now().After(deadline) {
						t.Fatal("logs-only node failed to rejoin")
					}
					time.Sleep(10 * time.Millisecond)
				}
				// The restarted journal rejoins the same voting configuration.
				ready(t, other)
				exec(t, other, "INSERT INTO items VALUES(4,'after journal restart'); BEGIN; COMMIT DURABLE")
				waitApplied(t, restarted, other.applied.Load())
				assertLogsOnlyStore(t, restarted)
				if restarted.raftNode().State() == raft.Leader {
					t.Fatal("logs-only replica became leader")
				}
				assertCount(t, other, 4)
			})
		}
	}
}

func TestLogsOnlyApplyBatchAndValidation(t *testing.T) {
	n := batchTestNode(t, DurabilityAsync)
	n.config.LogsOnly = true
	p := Peer{ID: "peer", Address: "tcp://127.0.0.1:1", Control: "tcp://127.0.0.1:2", Cluster: n.cluster}
	logs := []*raft.Log{
		batchTestLog(t, 1, command{Version: 1, Kind: "wal", Writes: []storage.Mutation{{Key: []byte("row"), Value: []byte("secret")}}}),
		batchTestLog(t, 2, command{Version: 1, Kind: "peer", Peer: &p}),
		batchTestLog(t, 3, command{Version: 1, Kind: "fence"}),
	}
	for _, r := range n.ApplyBatch(logs) {
		if r != nil {
			t.Fatal(r)
		}
	}
	assertLogsOnlyStore(t, n)
	if _, err := n.peer(p.ID); err != nil {
		t.Fatal(err)
	}
	bad := batchTestLog(t, 4, command{Version: 1, Kind: "wal", Writes: []storage.Mutation{{Key: appliedKey, Value: []byte("forged")}}})
	if err := n.Apply(bad); err == nil {
		t.Fatal("logs-only node skipped WAL validation")
	}
}

func TestReplicaRoleIdentity(t *testing.T) {
	for _, role := range []string{"non-voting", "logs-only"} {
		t.Run(role, func(t *testing.T) {
			c := testConfig(t, "source", "tcp")
			c.Bootstrap = true
			source := openTestNode(t, c)
			ready(t, source)
			c = testConfig(t, "target", "tcp")
			c.Join = source.ControlAddress()
			c.NonVoting, c.LogsOnly = role == "non-voting", role == "logs-only"
			target := openTestNode(t, c)
			value, err := os.ReadFile(filepath.Join(c.Directory, "node.json"))
			if err != nil {
				t.Fatal(err)
			}
			var meta nodeMetadata
			if err = json.Unmarshal(value, &meta); err != nil || meta.Version != 2 {
				t.Fatalf("restricted role must reject older binaries: %+v %v", meta, err)
			}
			c.Listen, c.ControlListen, c.Join = target.Address(), target.ControlAddress(), ""
			if err := target.Close(); err != nil {
				t.Fatal(err)
			}
			c.NonVoting, c.LogsOnly = false, false
			if n, err := Open(context.Background(), c); err == nil {
				n.Close()
				t.Fatal("persisted role changed on restart")
			}
		})
	}
	for _, role := range []string{"non-voting", "logs-only"} {
		c := testConfig(t, role, "tcp")
		c.Bootstrap = true
		c.NonVoting, c.LogsOnly = role == "non-voting", role == "logs-only"
		if n, err := Open(context.Background(), c); err == nil {
			n.Close()
			t.Fatal("bootstrapped restricted role")
		}
	}
}

func TestLogsOnlyRecoveryFromJournal(t *testing.T) {
	c := asyncConfig(t, "primary", "tcp")
	c.Bootstrap = true
	source := openTestNode(t, c)
	ready(t, source)
	exec(t, source, "CREATE TABLE items(id INTEGER PRIMARY KEY,v TEXT); INSERT INTO items VALUES(1,'base')")
	wc := asyncConfig(t, "journal", "tcp")
	wc.LogsOnly, wc.Join = true, source.ControlAddress()
	journal := openTestNode(t, wc)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := source.Promote(ctx, wc.ID); err != nil {
		t.Fatal(err)
	}
	exec(t, source, "INSERT INTO items VALUES(2,'tail'); BEGIN; COMMIT DURABLE")
	stable := source.DurableToken()
	// The journal must retain a certified base and the WAL tail, with no SQL rows.
	assertLogsOnlyStore(t, journal)
	if err := source.Close(); err != nil {
		t.Fatal(err)
	}
	if err := journal.Close(); err != nil {
		t.Fatal(err)
	}
	// Keep identity/fencing evidence but make the primary's payload unusable.
	for _, name := range []string{"raft-log", "snapshots"} {
		if err := os.RemoveAll(filepath.Join(c.Directory, name)); err != nil {
			t.Fatal(err)
		}
	}
	destination := filepath.Join(t.TempDir(), "recovered")
	result, err := RecoverStable(ctx, RecoveryConfig{Sources: []string{c.Directory, wc.Directory}, Destination: destination, Token: testToken, Fenced: true, Minimum: &stable})
	if err != nil {
		t.Fatal(err)
	}
	if result.Stable.Index != stable.Index {
		t.Fatalf("recovered %+v want %+v", result.Stable, stable)
	}
	restored, err := db.Open(destination, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = restored.Close() })
	rows, err := restored.Exec("SELECT id,v FROM items ORDER BY id")
	if err != nil || len(rows.Rows) != 2 || rows.Rows[1]["v"].Text != "tail" {
		t.Fatalf("journal recovery: %+v %v", rows, err)
	}
	if err = restored.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestLogsOnlyCannotCampaign(t *testing.T) {
	c := testConfig(t, "primary", "tcp")
	c.Bootstrap = true
	source := openTestNode(t, c)
	ready(t, source)
	wc := testConfig(t, "journal", "tcp")
	wc.Join, wc.LogsOnly = source.ControlAddress(), true
	witness := openTestNode(t, wc)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := source.Promote(ctx, wc.ID); err != nil {
		t.Fatal(err)
	}
	term := witness.raftNode().CurrentTerm()
	if err := source.Close(); err != nil {
		t.Fatal(err)
	}
	// Observe several election deadlines: local pre-votes must neither win
	// leadership nor increase the term and disrupt a later full-node election.
	deadline := time.Now().Add(6 * wc.RaftConfig.ElectionTimeout)
	for time.Now().Before(deadline) {
		if state := witness.raftNode().State(); state == raft.Leader {
			t.Fatal("journal elected itself")
		}
		if got := witness.raftNode().CurrentTerm(); got != term {
			t.Fatalf("journal increased term: %d -> %d", term, got)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestNonVotingLogsOnlyReplica(t *testing.T) {
	c := asyncConfig(t, "primary", "tcp")
	c.Bootstrap = true
	source := openTestNode(t, c)
	ready(t, source)
	exec(t, source, "CREATE TABLE items(id INTEGER PRIMARY KEY); INSERT INTO items VALUES(1)")
	wc := asyncConfig(t, "journal", "tcp")
	wc.Join, wc.LogsOnly, wc.NonVoting, wc.AutoPromote = source.ControlAddress(), true, true, true
	journal := openTestNode(t, wc)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := source.Promote(ctx, wc.ID); !errors.Is(err, ErrNonVoting) {
		t.Fatalf("promoted logs-only nonvoter: %v", err)
	}
	exec(t, source, "INSERT INTO items VALUES(2); BEGIN; COMMIT DURABLE")
	waitApplied(t, journal, source.applied.Load())
	assertLogsOnlyStore(t, journal)
	if err := journal.Close(); err != nil {
		t.Fatal(err)
	}
	exec(t, source, "INSERT INTO items VALUES(3); BEGIN; COMMIT DURABLE")
	assertCount(t, source, 3)
}

func TestReplicaRolesCertificateValidation(t *testing.T) {
	c := asyncConfig(t, "primary", "tcp")
	c.Bootstrap = true
	n := openTestNode(t, c)
	ready(t, n)
	for _, role := range []string{"non-voting", "logs-only"} {
		certificate := *n.currentCertificate()
		certificate.Body.Peers = append([]Peer(nil), certificate.Body.Peers...)
		certificate.Receipts = append([]durableReceipt(nil), certificate.Receipts...)
		certificate.Body.Peers[0].NonVoting = role == "non-voting"
		certificate.Body.Peers[0].LogsOnly = role == "logs-only"
		for i := range certificate.Receipts {
			certificate.Receipts[i].MAC = receiptMAC(testToken, certificate.Body, certificate.Receipts[i].ID)
		}
		if err := validateCertificate(&certificate, testToken, n.cluster); !errors.Is(err, ErrHistory) {
			t.Fatalf("accepted %s certificate leader: %v", role, err)
		}
	}
}

func TestLogsOnlyMutualTLS(t *testing.T) {
	for _, network := range []string{"tcp", "unix"} {
		t.Run(network, func(t *testing.T) {
			server, client := nodeTLS(t)
			c := asyncConfig(t, "primary", network)
			c.Bootstrap, c.TLSConfig, c.ClientTLSConfig = true, server, client
			source := openTestNode(t, c)
			ready(t, source)
			exec(t, source, "CREATE TABLE items(id INTEGER PRIMARY KEY); INSERT INTO items VALUES(1)")
			wc := asyncConfig(t, "journal", network)
			wc.LogsOnly, wc.Join, wc.TLSConfig, wc.ClientTLSConfig = true, source.ControlAddress(), server, client
			journal := openTestNode(t, wc)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			if err := source.Promote(ctx, wc.ID); err != nil {
				t.Fatal(err)
			}
			exec(t, source, "INSERT INTO items VALUES(2); BEGIN; COMMIT DURABLE")
			waitApplied(t, journal, source.applied.Load())
			if err := journal.WaitDurable(ctx, source.DurableToken()); err != nil {
				t.Fatal(err)
			}
			assertLogsOnlyStore(t, journal)
		})
	}
}
