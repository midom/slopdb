package replication

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	"slopdb/db"
)

const testToken = "replication-test-token-32-bytes-long"

func testConfig(t *testing.T, id, network string) Config {
	t.Helper()
	root := t.TempDir()
	listen, control := "tcp://127.0.0.1:0", "tcp://127.0.0.1:0"
	if network == "unix" {
		listen = "unix://" + filepath.Join(root, "r.sock")
		control = "unix://" + filepath.Join(root, "c.sock")
	}
	rc := raft.DefaultConfig()
	rc.HeartbeatTimeout = 200 * time.Millisecond
	rc.ElectionTimeout = 200 * time.Millisecond
	rc.LeaderLeaseTimeout = 100 * time.Millisecond
	rc.CommitTimeout = 10 * time.Millisecond
	rc.SnapshotInterval = 20 * time.Second
	return Config{Directory: filepath.Join(root, "node"), ID: id, Listen: listen, ControlListen: control, Token: testToken, RaftConfig: rc, OperationTimeout: 3 * time.Second}
}
func openTestNode(t *testing.T, c Config) *Node {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	n, e := Open(ctx, c)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() {
		if e := n.Close(); e != nil {
			t.Error(e)
		}
	})
	return n
}
func ready(t *testing.T, n *Node) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if e := n.WaitReady(ctx); e != nil {
		t.Fatalf("ready: %v status=%+v", e, n.Status())
	}
}
func exec(t *testing.T, n *Node, q string) {
	t.Helper()
	if _, e := n.Database().Exec(q); e != nil {
		t.Fatalf("%s: %v status=%+v", q, e, n.Status())
	}
}
func waitApplied(t *testing.T, n *Node, index uint64) {
	t.Helper()
	deadline := time.NewTimer(15 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		if n.applied.Load() >= index {
			return
		}
		select {
		case <-deadline.C:
			t.Fatalf("replica not caught up: %+v want=%d", n.Status(), index)
		case <-ticker.C:
		}
	}
}
func assertCount(t *testing.T, n *Node, want int64) {
	t.Helper()
	result, e := n.Database().Exec("SELECT COUNT(*) AS n FROM items")
	if e != nil {
		t.Fatal(e)
	}
	if got := result.Rows[0]["n"].Int; got != want {
		t.Fatalf("count=%v want=%v", got, want)
	}
}
func TestRaftSnapshotLearnerPromotionAndFailover(t *testing.T) {
	for _, network := range []string{"tcp", "unix"} {
		t.Run(network, func(t *testing.T) {
			seedDir := filepath.Join(t.TempDir(), "seed")
			seed, e := db.Open(seedDir, nil)
			if e != nil {
				t.Fatal(e)
			}
			if _, e = seed.Exec("CREATE TABLE items (id INTEGER PRIMARY KEY, v TEXT); INSERT INTO items VALUES (1, 'seed')"); e != nil {
				t.Fatal(e)
			}
			if e = seed.Close(); e != nil {
				t.Fatal(e)
			}
			c1 := testConfig(t, "n1", network)
			c1.Bootstrap = true
			c1.SeedDirectory = seedDir
			n1 := openTestNode(t, c1)
			ready(t, n1)
			c2 := testConfig(t, "n2", network)
			c2.Join = n1.ControlAddress()
			n2 := openTestNode(t, c2)
			waitApplied(t, n2, n1.applied.Load())
			s := n1.Status()
			found := false
			for _, m := range s.Members {
				if m.ID == "n2" {
					found = true
					if m.Suffrage != "Nonvoter" {
						t.Fatalf("joined as %s", m.Suffrage)
					}
				}
			}
			if !found {
				t.Fatal("missing learner")
			}
			if _, e = n2.Database().Exec("INSERT INTO items VALUES (99,'bad')"); !errors.Is(e, ErrNotLeader) {
				t.Fatalf("follower accepted SQL: %v", e)
			}
			for i := 2; i <= 10; i++ {
				exec(t, n1, fmt.Sprintf("INSERT INTO items VALUES (%d, 'wal')", i))
			}
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			if e = n1.Promote(ctx, "n2"); e != nil {
				t.Fatal(e)
			}
			c3 := testConfig(t, "n3", network)
			c3.Join = n1.ControlAddress()
			c3.AutoPromote = true
			n3 := openTestNode(t, c3)
			deadline := time.Now().Add(15 * time.Second)
			for {
				voters := 0
				for _, m := range n1.Status().Members {
					if m.Suffrage == "Voter" {
						voters++
					}
				}
				if voters == 3 {
					break
				}
				if time.Now().After(deadline) {
					t.Fatalf("auto promotion failed: %+v", n1.Status())
				}
				time.Sleep(20 * time.Millisecond)
			}
			exec(t, n1, "BEGIN; UPDATE items SET v='committed' WHERE id=1; COMMIT")
			if e = n1.Close(); e != nil {
				t.Fatal(e)
			}
			var leader *Node
			for time.Now().Before(deadline) {
				for _, n := range []*Node{n2, n3} {
					if n.readyTerm.Load() != 0 {
						leader = n
						break
					}
				}
				if leader != nil {
					break
				}
				time.Sleep(20 * time.Millisecond)
			}
			if leader == nil {
				t.Fatalf("no elected leader: n2=%+v n3=%+v", n2.Status(), n3.Status())
			}
			ready(t, leader)
			assertCount(t, leader, 10)
			exec(t, leader, "INSERT INTO items VALUES (11,'after election')")
			assertCount(t, leader, 11)
		})
	}
}
func TestDurableStoreAndMixedRangeCheckpoint(t *testing.T) {
	c := testConfig(t, "one", "tcp")
	c.Bootstrap = true
	n := openTestNode(t, c)
	ready(t, n)
	exec(t, n, "CREATE TABLE items (id INTEGER PRIMARY KEY, v TEXT); INSERT INTO items VALUES(1,'a'),(2,'b'); CREATE INDEX items_v ON items(v); DROP INDEX items_v")
	assertCount(t, n, 2)
	address, control := n.Address(), n.ControlAddress()
	if e := n.Close(); e != nil {
		t.Fatal(e)
	}
	c.Listen = address
	c.ControlListen = control
	c.Bootstrap = false
	restarted := openTestNode(t, c)
	ready(t, restarted)
	assertCount(t, restarted, 2)
}

func TestSnapshotValidationAndIdempotentApply(t *testing.T) {
	c := testConfig(t, "snapshot", "tcp")
	c.Bootstrap = true
	n := openTestNode(t, c)
	ready(t, n)
	exec(t, n, "CREATE TABLE items(id INTEGER PRIMARY KEY,v TEXT); INSERT INTO items VALUES(1,'snapshot bytes')")
	snapshot, e := n.Snapshot()
	if e != nil {
		t.Fatal(e)
	}
	var data bytes.Buffer
	if e = snapshot.(*stateSnapshot).write(&data); e != nil {
		t.Fatal(e)
	}
	snapshot.Release()
	original := n.dataDirectory
	payload := data.Bytes()
	for _, cut := range []int{0, 1, len(snapshotMagic), len(payload) / 2, len(payload) - 1} {
		t.Run(fmt.Sprint(cut), func(t *testing.T) {
			if directory, _, e := n.decodeSnapshot(bytes.NewReader(payload[:cut])); e == nil {
				os.RemoveAll(directory)
				t.Fatal("truncated snapshot accepted")
			}
		})
	}
	bad := bytes.Clone(payload)
	bad[len(bad)-1] ^= 1
	if directory, _, e := n.decodeSnapshot(bytes.NewReader(bad)); e == nil {
		os.RemoveAll(directory)
		t.Fatal("bad snapshot hash accepted")
	}
	if n.dataDirectory != original {
		t.Fatal("failed snapshot replaced active generation")
	}
	assertCount(t, n, 1)
	seq := n.Database().ReplicationStorage().Snapshot()
	n.Database().ReplicationStorage().ReleaseSnapshot(seq)
	if e := n.Apply(&raft.Log{Index: n.applied.Load(), Term: n.appliedTerm.Load(), Data: []byte("invalid replay must be skipped")}); e != nil {
		t.Fatal(e)
	}
	after := n.Database().ReplicationStorage().Snapshot()
	n.Database().ReplicationStorage().ReleaseSnapshot(after)
	if after != seq {
		t.Fatal("duplicate Raft index wrote a second native WAL batch")
	}
}

func TestQuorumLossAndLeadershipTransfer(t *testing.T) {
	c1 := testConfig(t, "leader", "tcp")
	c1.Bootstrap = true
	n1 := openTestNode(t, c1)
	ready(t, n1)
	exec(t, n1, "CREATE TABLE items(id INTEGER PRIMARY KEY,v TEXT); INSERT INTO items VALUES(1,'a')")
	var followers []*Node
	for _, id := range []string{"second", "third"} {
		c := testConfig(t, id, "tcp")
		c.Join = n1.ControlAddress()
		n := openTestNode(t, c)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		if e := n1.Promote(ctx, id); e != nil {
			cancel()
			t.Fatal(e)
		}
		cancel()
		followers = append(followers, n)
	}
	session, e := n1.Database().NewSession()
	if e != nil {
		t.Fatal(e)
	}
	if _, e = session.Exec("BEGIN; INSERT INTO items VALUES(2,'private')"); e != nil {
		t.Fatal(e)
	}
	prepared, e := n1.Database().Prepare("SELECT COUNT(*) AS n FROM items")
	if e != nil {
		t.Fatal(e)
	}
	defer prepared.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if e = n1.TransferLeadership(ctx, "second"); e != nil {
		t.Fatal(e)
	}
	n2 := followers[0]
	ready(t, n2)
	assertCount(t, n2, 1)
	if _, e = session.Exec("COMMIT"); e == nil {
		t.Fatal("old session survived leadership loss")
	}
	_ = session.Close()
	exec(t, n2, "PRAGMA page_size = 8192; INSERT INTO items VALUES(3,'new term')")
	if e = n2.TransferLeadership(ctx, "leader"); e != nil {
		t.Fatal(e)
	}
	ready(t, n1)
	assertCount(t, n1, 2)
	if _, e = prepared.Exec(); !errors.Is(e, db.ErrStmtClosed) {
		t.Fatalf("old prepared plan was not invalidated: %v", e)
	}
	if got := n1.Database().ReplicationStorage().PageSize(); got != 8192 {
		t.Fatalf("replicated page size = %d", got)
	}
	for _, n := range followers {
		if e = n.Close(); e != nil {
			t.Fatal(e)
		}
	}
	callCtx, stop := context.WithTimeout(context.Background(), 3*time.Second)
	defer stop()
	if _, e = n1.Database().ExecContext(callCtx, "INSERT INTO items VALUES(4,'no quorum')"); e == nil {
		t.Fatal("minority acknowledged a write")
	}
	readCtx, stopRead := context.WithTimeout(context.Background(), time.Second)
	defer stopRead()
	if _, e = n1.Database().ExecContext(readCtx, "SELECT COUNT(*) FROM items"); e == nil {
		t.Fatal("minority served a SQL read without a quorum")
	}
}

func TestAuthenticatedCloneAndParallelWrites(t *testing.T) {
	c := testConfig(t, "source", "unix")
	c.Bootstrap = true
	n := openTestNode(t, c)
	ready(t, n)
	exec(t, n, "CREATE TABLE items(id INTEGER PRIMARY KEY,v TEXT)")
	wrong := &Node{config: Config{Token: "different-token-at-least-16"}}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if response, e := wrong.request(ctx, n.ControlAddress(), http.MethodGet, "/v1/snapshot", nil); e == nil {
		response.Body.Close()
		t.Fatal("unauthorized clone succeeded")
	}
	stopped := make(chan struct{})
	failures := make(chan error, 1)
	go func() {
		defer close(stopped)
		for i := 1; i <= 40; i++ {
			if _, e := n.Database().Exec(fmt.Sprintf("INSERT INTO items VALUES(%d,'concurrent')", i)); e != nil {
				failures <- e
				return
			}
		}
	}()
	targetConfig := testConfig(t, "target", "unix")
	targetConfig.Join = n.ControlAddress()
	target := openTestNode(t, targetConfig)
	<-stopped
	select {
	case e := <-failures:
		t.Fatal(e)
	default:
	}
	if e := n.Promote(ctx, "target"); e != nil {
		t.Fatal(e)
	}
	if e := n.TransferLeadership(ctx, "target"); e != nil {
		t.Fatal(e)
	}
	ready(t, target)
	assertCount(t, target, 40)
	report, e := target.Database().CheckIntegrity()
	if e != nil {
		t.Fatal(e)
	}
	_ = report
}

func TestOfflineLearnerInstallsNewRaftSnapshot(t *testing.T) {
	c1 := testConfig(t, "source", "tcp")
	c1.Bootstrap = true
	source := openTestNode(t, c1)
	ready(t, source)
	exec(t, source, "CREATE TABLE items(id INTEGER PRIMARY KEY,v TEXT); INSERT INTO items VALUES(1,'base')")
	c2 := testConfig(t, "offline", "tcp")
	c2.Join = source.ControlAddress()
	target := openTestNode(t, c2)
	waitApplied(t, target, source.applied.Load())
	c2.Listen = target.Address()
	c2.ControlListen = target.ControlAddress()
	c2.Join = ""
	oldGeneration := target.dataDirectory
	if e := target.Close(); e != nil {
		t.Fatal(e)
	}
	short, stop := context.WithTimeout(context.Background(), 100*time.Millisecond)
	if e := source.Promote(short, "offline"); e == nil {
		stop()
		t.Fatal("offline learner was promoted")
	}
	stop()
	for _, member := range source.Status().Members {
		if member.ID == "offline" && member.Suffrage != "Nonvoter" {
			t.Fatal("offline learner entered quorum")
		}
	}
	for i := 2; i <= 30; i++ {
		exec(t, source, fmt.Sprintf("INSERT INTO items VALUES(%d,'missed')", i))
	}
	if e := source.raft.Snapshot().Error(); e != nil {
		t.Fatal(e)
	}
	restarted := openTestNode(t, c2)
	waitApplied(t, restarted, source.applied.Load())
	if restarted.dataDirectory == oldGeneration {
		t.Fatal("lagging learner did not install a snapshot generation")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if e := source.Promote(ctx, "offline"); e != nil {
		t.Fatal(e)
	}
	if e := source.TransferLeadership(ctx, "offline"); e != nil {
		t.Fatal(e)
	}
	ready(t, restarted)
	assertCount(t, restarted, 30)
}

func TestSnapshotReaderSurvivesGenerationRestore(t *testing.T) {
	c := testConfig(t, "source", "tcp")
	c.Bootstrap = true
	source := openTestNode(t, c)
	ready(t, source)
	exec(t, source, "CREATE TABLE items(id INTEGER PRIMARY KEY,v TEXT); INSERT INTO items VALUES(1,'pinned')")
	targetConfig := testConfig(t, "reader", "tcp")
	targetConfig.Join = source.ControlAddress()
	target := openTestNode(t, targetConfig)
	waitApplied(t, target, source.applied.Load())
	snapshot, e := target.Snapshot()
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(snapshot.Release)
	held := snapshot.(*stateSnapshot)
	var original bytes.Buffer
	if e = held.write(&original); e != nil {
		t.Fatal(e)
	}
	oldDirectory := target.dataDirectory
	stopStats := make(chan struct{})
	statsDone := make(chan struct{})
	go func() {
		defer close(statsDone)
		for {
			select {
			case <-stopStats:
				return
			default:
			}
			_ = target.Database().PageSize()
			_ = target.Database().Stats()
		}
	}()
	defer func() { close(stopStats); <-statsDone }()
	handle := target.Database()
	restored := make(chan error, 1)
	go func() { restored <- target.Restore(io.NopCloser(bytes.NewReader(original.Bytes()))) }()
	select {
	case e = <-restored:
		if e != nil {
			t.Fatal(e)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Restore waited for an outstanding snapshot reader")
	}
	if target.Database() != handle {
		t.Fatal("Restore replaced the public database handle")
	}
	if _, e = os.Stat(oldDirectory); e != nil {
		t.Fatalf("pinned generation removed: %v", e)
	}
	held.iterator.Seek(nil)
	var again bytes.Buffer
	if e = held.write(&again); e != nil {
		t.Fatal(e)
	}
	if !bytes.Equal(original.Bytes(), again.Bytes()) {
		t.Fatal("restore changed pinned snapshot bytes")
	}
	snapshot.Release()
	if _, e = os.Stat(oldDirectory); !os.IsNotExist(e) {
		t.Fatalf("retired generation not removed after Release: %v", e)
	}
}

func TestSnapshotEnvelopeMetadataChecksum(t *testing.T) {
	meta := &raft.SnapshotMeta{Version: 1, Index: 10, Term: 2, ConfigurationIndex: 1}
	var stream bytes.Buffer
	if e := writeSnapshotEnvelope(&stream, meta, strings.NewReader("payload")); e != nil {
		t.Fatal(e)
	}
	encoded := bytes.Clone(stream.Bytes())
	if _, e := readSnapshotEnvelope(bytes.NewReader(encoded)); e != nil {
		t.Fatal(e)
	}
	encoded[5] ^= 1
	if _, e := readSnapshotEnvelope(bytes.NewReader(encoded)); e == nil {
		t.Fatal("corrupt snapshot metadata accepted")
	}
}

func TestConcurrentReplicatedAutoIncrementAndIndex(t *testing.T) {
	c := testConfig(t, "writer", "tcp")
	c.Bootstrap = true
	source := openTestNode(t, c)
	ready(t, source)
	exec(t, source, "CREATE TABLE items(id INTEGER PRIMARY KEY AUTOINCREMENT,v TEXT)")
	targetConfig := testConfig(t, "copy", "tcp")
	targetConfig.Join = source.ControlAddress()
	target := openTestNode(t, targetConfig)
	statement, e := source.Database().Prepare("INSERT INTO items(v) VALUES(?)")
	if e != nil {
		t.Fatal(e)
	}
	defer statement.Close()
	failures := make(chan error, 9)
	var writers sync.WaitGroup
	for worker := 0; worker < 8; worker++ {
		worker := worker
		writers.Add(1)
		go func() {
			defer writers.Done()
			session, e := source.Database().NewSession()
			if e != nil {
				failures <- e
				return
			}
			defer session.Close()
			for i := 0; i < 10; i++ {
				if _, e = session.ExecPrepared(statement, fmt.Sprintf("%d/%d", worker, i)); e != nil {
					failures <- e
					return
				}
			}
		}()
	}
	writers.Add(1)
	go func() {
		defer writers.Done()
		if _, e := source.Database().Exec("CREATE INDEX item_values ON items(v)"); e != nil {
			failures <- e
		}
	}()
	writers.Wait()
	close(failures)
	for e := range failures {
		t.Error(e)
	}
	if t.Failed() {
		return
	}
	if e = source.propose(command{Version: 1, Kind: "fence"}, nil); e != nil {
		t.Fatal(e)
	}
	waitApplied(t, target, source.applied.Load())
	dump := func(n *Node) []byte {
		t.Helper()
		snapshot, e := n.Snapshot()
		if e != nil {
			t.Fatal(e)
		}
		defer snapshot.Release()
		var output bytes.Buffer
		if e = snapshot.(*stateSnapshot).write(&output); e != nil {
			t.Fatal(e)
		}
		return output.Bytes()
	}
	if !bytes.Equal(dump(source), dump(target)) {
		t.Fatal("replicas differ in raw rows, indexes, catalog or durable counters")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if e = source.Promote(ctx, "copy"); e != nil {
		t.Fatal(e)
	}
	if e = source.TransferLeadership(ctx, "copy"); e != nil {
		t.Fatal(e)
	}
	ready(t, target)
	assertCount(t, target, 80)
	exec(t, target, "INSERT INTO items(v) VALUES('after promotion')")
	result, e := target.Database().Exec("SELECT MAX(id) AS m FROM items")
	if e != nil || result.Rows[0]["m"].Int != 81 {
		t.Fatalf("automatic counter after promotion: %+v %v", result, e)
	}
	if _, e = target.Database().CheckIntegrity(); e != nil {
		t.Fatal(e)
	}
}
