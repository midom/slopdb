package replication

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hashicorp/raft"

	"slopdb/db"
)

func readReplicaPair(t *testing.T, mode Durability, network string, bound time.Duration) (*Node, *Node) {
	t.Helper()
	config := asyncConfig(t, "source", network)
	config.Durability, config.Bootstrap = mode, true
	source := openTestNode(t, config)
	ready(t, source)
	exec(t, source, "CREATE TABLE items(id INTEGER PRIMARY KEY,v TEXT); INSERT INTO items VALUES(1,'one'),(2,'two')")
	other := asyncConfig(t, "reader", network)
	other.Durability, other.Join, other.ReplicaReadMaxStaleness = mode, source.ControlAddress(), bound
	replica := openTestNode(t, other)
	waitApplied(t, replica, source.applied.Load())
	return source, replica
}

func TestReplicaReadsSQLAndCatalogSnapshots(t *testing.T) {
	for _, mode := range []Durability{DurabilitySync, DurabilityAsync} {
		for _, network := range []string{"tcp", "unix"} {
			t.Run(string(mode)+"/"+network, func(t *testing.T) {
				source, replica := readReplicaPair(t, mode, network, 2*time.Second)
				before := source.raftNode().LastIndex()
				session, err := replica.Database().NewSession()
				if err != nil {
					t.Fatal(err)
				}
				defer session.Close()
				prepared, err := session.Prepare("SELECT v FROM items WHERE id=?")
				if err != nil {
					t.Fatal(err)
				}
				defer prepared.Close()
				for i := 0; i < 2; i++ {
					result, err := prepared.Exec(int64(1))
					if err != nil || len(result.Rows) != 1 || result.Rows[0]["v"].Text != "one" {
						t.Fatalf("prepared replica read: %+v %v", result, err)
					}
				}
				for _, query := range []string{
					"SELECT COUNT(*) AS n FROM items",
					"WITH q AS (SELECT id FROM items) SELECT id FROM q UNION ALL SELECT 3",
					"PRAGMA table_info('items')", "PRAGMA page_size", "SHOW TABLES", "EXPLAIN ANALYZE SELECT * FROM items",
					"BEGIN; SAVEPOINT s; SELECT * FROM items; ROLLBACK TO s; RELEASE s; COMMIT",
				} {
					exec(t, replica, query)
				}
				if source.raftNode().LastIndex() != before {
					t.Fatal("replica reads appended consensus entries")
				}
				for _, query := range []string{
					"INSERT INTO items VALUES(3,'bad')", "UPDATE items SET v='bad'", "DELETE FROM items",
					"CREATE TABLE bad(id INTEGER)", "ALTER TABLE items RENAME TO bad", "VACUUM", "ANALYZE",
					"PRAGMA user_version=4", "PRAGMA page_size=8192", "PRAGMA collation='NFC'",
					"SELECT * FROM items FOR UPDATE", "SELECT * FROM items FOR SHARE",
					"SELECT (SELECT id FROM items WHERE id=1 FOR UPDATE)",
					"WITH q AS (SELECT id FROM items FOR UPDATE) SELECT * FROM q",
					"EXPLAIN ANALYZE UPDATE items SET v='bad'", "COMMIT DURABLE",
					"SELECT 1; UPDATE items SET v='bad'",
				} {
					if _, err = session.Exec(query); !errors.Is(err, ErrNotLeader) {
						t.Fatalf("replica accepted %q: %v", query, err)
					}
				}
				if source.raftNode().LastIndex() != before {
					t.Fatal("rejected writes appended consensus entries")
				}
				exec(t, source, "CREATE VIEW locked_view AS SELECT * FROM items FOR UPDATE")
				waitApplied(t, replica, source.applied.Load())
				if _, err = session.Exec("SELECT * FROM locked_view"); err == nil || !strings.Contains(err.Error(), "locking reads require") {
					t.Fatalf("replica executed locking stored view: %v", err)
				}
				stream, err := replica.Database().Query("SELECT id,v FROM items ORDER BY id")
				if err != nil {
					t.Fatal(err)
				}
				defer stream.Close()
				if !stream.Next() {
					t.Fatalf("empty stream: %v", stream.Err())
				}
				// An idle stream must neither block FSM application nor mix a later
				// rename/new table's catalog and rows into its pinned snapshot.
				exec(t, source, "ALTER TABLE items RENAME TO old_items; CREATE TABLE items(id INTEGER PRIMARY KEY,v TEXT); INSERT INTO items VALUES(1,'replacement')")
				waitApplied(t, replica, source.applied.Load())
				var rows []string
				for {
					var id int64
					var value string
					if err = stream.Scan(&id, &value); err != nil {
						t.Fatal(err)
					}
					rows = append(rows, fmt.Sprintf("%d:%s", id, value))
					if !stream.Next() {
						break
					}
				}
				if err = stream.Err(); err != nil || fmt.Sprint(rows) != "[1:one 2:two]" {
					t.Fatalf("stream crossed snapshots: %v %v", rows, err)
				}
				if err = stream.Close(); err != nil {
					t.Fatal(err)
				}
				result, err := prepared.Exec(int64(1))
				if err != nil || len(result.Rows) != 1 || result.Rows[0]["v"].Text != "replacement" {
					t.Fatalf("cached plan reused old table: %+v %v", result, err)
				}
				exec(t, source, "ALTER TABLE items ADD COLUMN extra TEXT DEFAULT 'new'; CREATE VIEW item_view AS SELECT extra FROM items")
				waitApplied(t, replica, source.applied.Load())
				result, err = session.Exec("SELECT extra FROM item_view")
				if err != nil || len(result.Rows) != 1 || result.Rows[0]["extra"].Text != "new" {
					t.Fatalf("replicated catalog not refreshed: %+v %v", result, err)
				}
				// The API transaction remains read-only and sees fresh committed data.
				tx, err := replica.Database().Begin()
				if err != nil {
					t.Fatal(err)
				}
				defer tx.Rollback()
				if _, err = tx.Exec("SELECT * FROM items"); err != nil {
					t.Fatal(err)
				}
				exec(t, source, "UPDATE items SET extra='later' WHERE id=1")
				waitApplied(t, replica, source.applied.Load())
				result, err = tx.Exec("SELECT extra FROM items")
				if err != nil || result.Rows[0]["extra"].Text != "later" {
					t.Fatalf("READ COMMITTED replica transaction: %+v %v", result, err)
				}
				if err = tx.Commit(); err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}

func TestReplicaReadProofExpiresWithoutControlConnectivity(t *testing.T) {
	source, replica := readReplicaPair(t, DurabilitySync, "tcp", 500*time.Millisecond)
	exec(t, replica, "SELECT * FROM items")
	status := replica.Status()
	if !status.ReplicaReadReady || status.ReplicaReadIndex == 0 {
		t.Fatalf("read proof missing from status: %+v", status)
	}
	// Keep Raft heartbeats alive while preventing fresh quorum proofs.
	if err := source.httpServer.Close(); err != nil {
		t.Fatal(err)
	}
	exec(t, replica, "SELECT * FROM items")
	replica.replicaReads.mu.Lock()
	expires := replica.replicaReads.proof.since.Add(replica.config.ReplicaReadMaxStaleness)
	replica.replicaReads.mu.Unlock()
	timer := time.NewTimer(time.Until(expires) + 20*time.Millisecond)
	defer timer.Stop()
	<-timer.C
	if _, err := replica.Database().Exec("SELECT * FROM items"); !errors.Is(err, ErrReplicaTooStale) {
		t.Fatalf("expired proof served SQL: %v", err)
	}
	if replica.Status().ReplicaReadReady {
		t.Fatal("expired proof reported ready")
	}
}

func TestReplicaReadSnapshotWaitHonorsCancellationAndExpiry(t *testing.T) {
	_, replica := readReplicaPair(t, DurabilitySync, "tcp", time.Second)
	exec(t, replica, "SELECT * FROM items")
	replica.fsmMu.Lock()
	held := true
	defer func() {
		if held {
			replica.fsmMu.Unlock()
		}
	}()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { _, err := replica.Database().ExecContext(ctx, "SELECT * FROM items"); done <- err }()
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("read snapshot wait ignored cancellation: %v", err)
	}
	// A cached proof is rechecked after waiting for snapshot capture. Holding
	// FSM application cannot extend that proof into an unbounded stale read.
	go func() { _, err := replica.Database().Exec("SELECT * FROM items"); done <- err }()
	select {
	case err := <-done:
		if !errors.Is(err, ErrReplicaTooStale) {
			t.Fatalf("expired snapshot wait: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("snapshot acquisition exceeded its proof deadline")
	}
	replica.fsmMu.Unlock()
	held = false
	exec(t, replica, "SELECT * FROM items")
}

func TestReplicaReadsConcurrentWithCatalogChanges(t *testing.T) {
	source, replica := readReplicaPair(t, DurabilitySync, "tcp", 2*time.Second)
	var readers sync.WaitGroup
	errorsFound := make(chan error, 8)
	for i := 0; i < 8; i++ {
		session, err := replica.Database().NewSession()
		if err != nil {
			t.Fatal(err)
		}
		readers.Add(1)
		go func(s *db.Session) {
			defer readers.Done()
			defer s.Close()
			for j := 0; j < 30; j++ {
				result, err := s.Exec("SELECT v FROM items WHERE id=1")
				if err != nil || len(result.Rows) != 1 || result.Rows[0]["v"].Text != "one" {
					errorsFound <- fmt.Errorf("concurrent replica snapshot: %+v %v", result, err)
					return
				}
			}
		}(session)
	}
	for i := 0; i < 10; i++ {
		exec(t, source, "CREATE TABLE transient(id INTEGER PRIMARY KEY); DROP TABLE transient")
	}
	readers.Wait()
	close(errorsFound)
	for err := range errorsFound {
		t.Error(err)
	}
}

func TestReplicaReadWaitsForConfirmedPrefix(t *testing.T) {
	var source *Node
	var published atomic.Pointer[Node]
	var future atomic.Bool
	future.Store(true)
	entered := make(chan struct{}, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/read-index" || r.Header.Get("Authorization") != "Bearer "+testToken {
			http.Error(w, "invalid read request", http.StatusBadRequest)
			return
		}
		node := published.Load()
		fence := replicaReadFence{Cluster: node.cluster, Leader: node.config.ID, Address: node.Address(), Term: node.raftNode().CurrentTerm(), Index: node.applied.Load()}
		if future.Load() {
			fence.Index += 100
		}
		writeJSON(w, fence)
		select {
		case entered <- struct{}{}:
		default:
		}
	}))
	defer server.Close()
	c := testConfig(t, "source", "tcp")
	c.Bootstrap = true
	c.ControlListen = "unix://" + filepath.Join(t.TempDir(), "control.sock")
	c.ControlAdvertise = strings.Replace(server.URL, "http://", "tcp://", 1)
	source = openTestNode(t, c)
	published.Store(source)
	ready(t, source)
	exec(t, source, "CREATE TABLE items(id INTEGER PRIMARY KEY); INSERT INTO items VALUES(1)")
	other := testConfig(t, "reader", "tcp")
	other.Join, other.ReplicaReadMaxStaleness = c.ControlListen, 2*time.Second
	replica := openTestNode(t, other)
	waitApplied(t, replica, source.applied.Load())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { _, err := replica.Database().ExecContext(ctx, "SELECT * FROM items"); done <- err }()
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("read did not request a progress fence")
	}
	select {
	case err := <-done:
		t.Fatalf("read ran ahead of confirmed native application: %v", err)
	default:
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("application wait cancellation: %v", err)
	}
	if replica.Status().ReplicaReadReady {
		t.Fatal("unapplied prefix cached as readable")
	}
	future.Store(false)
	exec(t, replica, "SELECT * FROM items")
}

func TestReplicaReadsAfterRestartAndLeadershipTransfer(t *testing.T) {
	source, replica := readReplicaPair(t, DurabilitySync, "tcp", 2*time.Second)
	position, err := source.AckedPosition(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	exec(t, replica, "SELECT * FROM items")
	c := replica.config
	c.Listen, c.ControlListen, c.Join = replica.Address(), replica.ControlAddress(), ""
	if err := replica.Close(); err != nil {
		t.Fatal(err)
	}
	replica = openTestNode(t, c)
	deadline := time.Now().Add(5 * time.Second)
	for {
		if _, err := replica.Database().Exec("SELECT * FROM items"); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("read discovery after restart: %+v", replica.Status())
		}
		time.Sleep(10 * time.Millisecond)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := source.Promote(ctx, replica.config.ID); err != nil {
		t.Fatal(err)
	}
	old, err := replica.Database().NewSession()
	if err != nil {
		t.Fatal(err)
	}
	defer old.Close()
	if err := source.TransferLeadership(ctx, replica.config.ID); err != nil {
		t.Fatal(err)
	}
	ready(t, replica)
	if err = replica.WaitPosition(ctx, position); err != nil {
		t.Fatalf("commit position did not survive restart and leadership transfer: %v", err)
	}
	if _, err = old.Exec("SELECT * FROM items"); !errors.Is(err, db.ErrSessionClosed) {
		t.Fatalf("old replica session survived leader admission: %v", err)
	}
	exec(t, replica, "INSERT INTO items VALUES(3,'leader')")
	assertCount(t, replica, 3)
}

func TestReplicaReadCannotRefreshWithoutQuorum(t *testing.T) {
	source, replica := readReplicaPair(t, DurabilitySync, "tcp", 500*time.Millisecond)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := source.Promote(ctx, replica.config.ID); err != nil {
		t.Fatal(err)
	}
	third := testConfig(t, "third", "tcp")
	third.Join = source.ControlAddress()
	other := openTestNode(t, third)
	if err := source.Promote(ctx, third.ID); err != nil {
		t.Fatal(err)
	}
	waitApplied(t, replica, source.applied.Load())
	exec(t, replica, "SELECT * FROM items")
	if err := source.Close(); err != nil {
		t.Fatal(err)
	}
	if err := other.Close(); err != nil {
		t.Fatal(err)
	}
	// Even if this isolated node still remembers its old leader, expired
	// progress cannot be renewed from a minority. It must never self-serve.
	replica.replicaReads.mu.Lock()
	replica.replicaReads.proof.since = time.Now().Add(-time.Second)
	replica.replicaReads.mu.Unlock()
	if _, err := replica.Database().Exec("SELECT * FROM items"); err == nil {
		t.Fatal("isolated replica renewed progress")
	}
	if replica.Status().ReplicaReadReady {
		t.Fatal("minority reported replica reads ready")
	}
}

func TestReplicaReadConfigurationAndDefault(t *testing.T) {
	c := testConfig(t, "invalid", "tcp")
	c.ReplicaReadMaxStaleness = -time.Second
	if _, err := Open(context.Background(), c); err == nil {
		t.Fatal("negative read bound accepted")
	}
	_, replica := readReplicaPair(t, DurabilitySync, "tcp", 0)
	if _, err := replica.Database().Exec("SELECT * FROM items"); !errors.Is(err, ErrNotLeader) {
		t.Fatalf("default follower reads enabled: %v", err)
	}
	if _, err := replica.Database().NewSession(); !errors.Is(err, ErrNotLeader) {
		t.Fatalf("default follower session enabled: %v", err)
	}
	if replica.raftNode().State() != raft.Follower {
		t.Fatal("test replica unexpectedly became leader")
	}
}

func TestReplicaReadSnapshotRejectsUnreadyLeader(t *testing.T) {
	c := testConfig(t, "initializing", "tcp")
	c.Bootstrap, c.ReplicaReadMaxStaleness = true, time.Second
	node := openTestNode(t, c)
	ready(t, node)
	// Keep any re-initialization fence from applying while testing the role
	// transition between SQL admission and statement snapshot acquisition.
	node.fsmMu.Lock()
	term := node.readyTerm.Swap(0)
	_, err := node.captureReadSnapshot(context.Background(), nil)
	node.readyTerm.Store(term)
	node.fsmMu.Unlock()
	if !errors.Is(err, ErrNotLeader) {
		t.Fatalf("unready leader captured an ordinary SQL snapshot: %v", err)
	}
}
