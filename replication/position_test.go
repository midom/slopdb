package replication

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"sync"
	"testing"
	"time"

	"slopdb/db"
)

func TestCommitPositionsAndSQLWait(t *testing.T) {
	for _, mode := range []Durability{DurabilitySync, DurabilityAsync} {
		for _, network := range []string{"tcp", "unix"} {
			t.Run(string(mode)+"/"+network, func(t *testing.T) {
				source, replica := readReplicaPair(t, mode, network, 2*time.Second)
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				session, err := source.Database().NewSession()
				if err != nil {
					t.Fatal(err)
				}
				defer session.Close()
				tx, err := source.Database().Begin()
				if err != nil {
					t.Fatal(err)
				}
				defer tx.Rollback()
				result, err := tx.Exec("UPDATE items SET v='committed' WHERE id=1")
				if err != nil || result.CommitPosition != "" {
					t.Fatalf("uncommitted position: %+v %v", result, err)
				}
				before := source.applied.Load()
				position, err := tx.CommitWithPosition(ctx)
				if err != nil || position == "" {
					t.Fatalf("commit position: %q %v", position, err)
				}
				token, err := source.parsePosition(position)
				if err != nil || token.Index <= before {
					t.Fatalf("position does not cover commit: %+v %v", token, err)
				}
				if err = replica.WaitPosition(ctx, position); err != nil {
					t.Fatal(err)
				}
				exec(t, replica, "SELECT * FROM items")
				wait, err := replica.Database().Prepare("WAIT FOR POSITION ? TIMEOUT ?")
				if err != nil {
					t.Fatal(err)
				}
				defer wait.Close()
				index := source.raftNode().LastIndex()
				if _, err = wait.Exec(position, "1s"); err != nil {
					t.Fatal(err)
				}
				current, err := source.Database().Exec("SHOW REPLICATION POSITION")
				if err != nil {
					t.Fatal(err)
				}
				parsed, err := source.parsePosition(current.Rows[0]["Position"].Text)
				if err != nil || parsed.Index < token.Index {
					t.Fatalf("master position went backwards: %+v %v", parsed, err)
				}
				if _, err = replica.Database().Exec("SHOW REPLICATION POSITION"); !errors.Is(err, ErrNotLeader) {
					t.Fatalf("follower supplied master position: %v", err)
				}
				if source.raftNode().LastIndex() != index {
					t.Fatal("position query/wait appended Raft entries")
				}
				result, err = session.Exec("UPDATE items SET v='session commit' WHERE id=2")
				if err != nil || result.CommitPosition == "" {
					t.Fatalf("autocommit position missing: %+v %v", result, err)
				}
				last, err := session.Exec("SHOW LAST COMMIT POSITION")
				if err != nil || last.Rows[0]["Position"].Text != result.CommitPosition {
					t.Fatalf("session lost its position: %+v %v", last, err)
				}
				if _, err = session.Exec("BEGIN; UPDATE items SET v='rolled back'; ROLLBACK"); err != nil {
					t.Fatal(err)
				}
				last, err = session.Exec("SHOW LAST COMMIT POSITION")
				if err != nil || last.Rows[0]["Position"].Text != result.CommitPosition {
					t.Fatalf("rollback published position: %+v %v", last, err)
				}
				if err = source.takeStableSnapshot(ctx); err != nil {
					t.Fatal(err)
				}
				if err = replica.WaitPosition(ctx, position); err != nil {
					t.Fatalf("old token rejected after snapshot/compaction: %v", err)
				}
			})
		}
	}
}

func TestPositionWaitAndMinimumReadWaitForNativeApplication(t *testing.T) {
	source, replica := readReplicaPair(t, DurabilityAsync, "tcp", 2*time.Second)
	exec(t, replica, "SELECT * FROM items") // warm the proof before holding application
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
	replica.fsmMu.Lock()
	held := true
	defer func() {
		if held {
			replica.fsmMu.Unlock()
		}
	}()
	result, err := source.Database().Exec("INSERT INTO items VALUES(3,'visible after wait')")
	if err != nil {
		t.Fatal(err)
	}
	position := result.CommitPosition
	token, err := source.parsePosition(position)
	if err != nil || replica.applied.Load() >= token.Index {
		t.Fatalf("application hold failed: %+v %v", token, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err = replica.WaitPosition(ctx, position); !errors.Is(err, context.Canceled) {
		t.Fatalf("wait ignored cancellation: %v", err)
	}
	if _, err = replica.Database().Exec("WAIT FOR POSITION '" + position + "' TIMEOUT '10ms'"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("SQL wait ignored timeout: %v", err)
	}
	ctx, stop := context.WithTimeout(context.Background(), 5*time.Second)
	defer stop()
	done := make(chan error, 2)
	go func() { done <- replica.Database().WaitPosition(ctx, position) }()
	go func() {
		result, err := prepared.ExecContextWithOptions(ctx, db.ExecOptions{MinimumPosition: position}, int64(3))
		if err == nil && (len(result.Rows) != 1 || result.Rows[0]["v"].Text != "visible after wait") {
			err = fmt.Errorf("read snapshot preceded minimum: %+v", result)
		}
		done <- err
	}()
	select {
	case err := <-done:
		t.Fatalf("wait/read completed before application: %v", err)
	default:
	}
	replica.fsmMu.Unlock()
	held = false
	for i := 0; i < 2; i++ {
		if err = <-done; err != nil {
			t.Fatal(err)
		}
	}
	rows, err := replica.Database().QueryContext(db.WithMinimumPosition(ctx, position), "SELECT v FROM items WHERE id=3")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	if !rows.Next() {
		t.Fatalf("stream omitted required commit: %v", rows.Err())
	}
	var value string
	if err = rows.Scan(&value); err != nil || value != "visible after wait" {
		t.Fatalf("stream minimum: %q %v", value, err)
	}
}

func TestPositionValidationAndQuorumIndependentWait(t *testing.T) {
	source, replica := readReplicaPair(t, DurabilitySync, "tcp", 2*time.Second)
	position, err := source.AckedPosition(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	waitApplied(t, replica, source.applied.Load())
	for _, bad := range []string{"", "1", position + "x", strings.Replace(position, source.cluster, "other", 1), position[:len(position)-1] + "g"} {
		if err = replica.WaitPosition(context.Background(), bad); !errors.Is(err, db.ErrInvalidPosition) {
			t.Fatalf("accepted %q: %v", bad, err)
		}
		if _, err = replica.Database().ExecWithOptions("SELECT * FROM items", db.ExecOptions{MinimumPosition: bad + "invalid"}); !errors.Is(err, db.ErrInvalidPosition) {
			t.Fatalf("invalid minimum ignored: %v", err)
		}
	}
	// A valid token is evidence of committed history. Waiting for local
	// application doesn't need to obtain another read proof from the leader.
	if err = source.httpServer.Close(); err != nil {
		t.Fatal(err)
	}
	replica.replicaReads.mu.Lock()
	replica.replicaReads.proof.since = time.Now().Add(-time.Hour)
	replica.replicaReads.mu.Unlock()
	if _, err = replica.Database().Exec("WAIT FOR POSITION '" + position + "' TIMEOUT '0s'"); err != nil {
		t.Fatal(err)
	}
	if _, err = replica.Database().Exec("SELECT * FROM items"); !errors.Is(err, ErrReplicaTooStale) {
		t.Fatalf("wait bypassed bounded-read admission: %v", err)
	}
	other := &Node{cluster: source.cluster, config: source.config}
	other.publishPosition(math.MaxUint64, math.MaxUint64)
	token, err := source.parsePosition(other.currentPosition())
	if err != nil || token.Index != math.MaxUint64 || token.Term != math.MaxUint64 {
		t.Fatalf("position lost uint64 precision: %+v %v", token, err)
	}
	other.cluster = "new-incarnation"
	if _, err = other.parsePosition(position); !errors.Is(err, db.ErrInvalidPosition) {
		t.Fatalf("old epoch accepted: %v", err)
	}
}

func TestConcurrentCommitPositionsCoverTheirTransactions(t *testing.T) {
	source, replica := readReplicaPair(t, DurabilityAsync, "tcp", 2*time.Second)
	var workers sync.WaitGroup
	errorsFound := make(chan error, 8)
	for i := 10; i < 18; i++ {
		session, err := source.Database().NewSession()
		if err != nil {
			t.Fatal(err)
		}
		workers.Add(1)
		go func(id int, session *db.Session) {
			defer workers.Done()
			defer session.Close()
			result, err := session.Exec(fmt.Sprintf("INSERT INTO items VALUES(%d,'own commit')", id))
			if err == nil {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				err = replica.WaitPosition(ctx, result.CommitPosition)
				if err == nil {
					result, err = replica.Database().ExecWithOptions(fmt.Sprintf("SELECT v FROM items WHERE id=%d", id), db.ExecOptions{MinimumPosition: result.CommitPosition})
					if err == nil && len(result.Rows) != 1 {
						err = fmt.Errorf("token omitted own transaction %d", id)
					}
				}
			}
			if err != nil {
				errorsFound <- err
			}
		}(i, session)
	}
	workers.Wait()
	close(errorsFound)
	for err := range errorsFound {
		t.Error(err)
	}
}
