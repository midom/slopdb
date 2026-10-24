//go:build linux

package regression

import (
	"context"
	gosql "database/sql"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	_ "github.com/lib/pq"
	"slopdb/db"
	"slopdb/replication"
)

func TestCLIRaftAsyncCommitAndOfflineRecovery(t *testing.T) {
	env := newRegressionEnv(t)
	root := t.TempDir()
	nodeDirectory := filepath.Join(root, "node")
	socket := filepath.Join(root, ".s.PGSQL.5432")
	ctx, cancel := context.WithTimeout(context.Background(), 35*time.Second)
	defer cancel()
	arguments := []string{"--raft-id=one", "--raft-bootstrap", "--raft-durability=async", "--raft-durability-interval=1h", "--raft-max-durability-lag=2h", "--raft-listen=tcp://127.0.0.1:0", "--raft-control=unix://" + filepath.Join(root, "control.sock"), "--raft-token-env=SLOPDB_TEST_ASYNC_TOKEN", "--postgres-socket=" + socket, nodeDirectory}
	command := exec.CommandContext(ctx, env.cli(), arguments...)
	command.Env = append(os.Environ(), "SLOPDB_TEST_ASYNC_TOKEN=permanent-async-durability-test-token")
	if e := command.Start(); e != nil {
		t.Fatal(e)
	}
	finished := false
	t.Cleanup(func() {
		if !finished {
			_ = command.Process.Kill()
			_ = command.Wait()
		}
	})
	client, e := gosql.Open("postgres", "host="+root+" port=5432 user=slopdb dbname=slopdb sslmode=disable connect_timeout=1")
	if e != nil {
		t.Fatal(e)
	}
	defer client.Close()
	client.SetMaxOpenConns(1)
	for {
		if e = client.PingContext(ctx); e == nil {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatalf("async CLI did not start: %v", e)
		case <-time.After(25 * time.Millisecond):
		}
	}
	connection, e := client.Conn(ctx)
	if e != nil {
		t.Fatal(e)
	}
	for _, query := range []string{"CREATE TABLE items(id INTEGER PRIMARY KEY,v TEXT)", "BEGIN", "INSERT INTO items VALUES(1,'durable')", "COMMIT DURABLE", "BEGIN", "INSERT INTO items VALUES(2,'async')", "COMMIT ASYNC"} {
		if _, e = connection.ExecContext(ctx, query); e != nil {
			t.Fatalf("%s: %v", query, e)
		}
	}
	_ = connection.Close()
	_ = client.Close()
	if e = command.Process.Kill(); e != nil {
		t.Fatal(e)
	}
	var exit *exec.ExitError
	if e = command.Wait(); !errors.As(e, &exit) {
		t.Fatalf("expected killed node: %v", e)
	}
	finished = true
	destination := filepath.Join(root, "recovered")
	restore := func(fenced bool) ([]byte, error) {
		args := []string{"--raft-recover-to=" + destination, "--raft-token-env=SLOPDB_TEST_ASYNC_TOKEN"}
		if fenced {
			args = append(args, "--raft-recovery-fenced")
		}
		args = append(args, nodeDirectory)
		child := exec.CommandContext(ctx, env.cli(), args...)
		child.Env = command.Env
		return child.CombinedOutput()
	}
	if output, e := restore(false); e == nil {
		t.Fatalf("recovery accepted missing fencing assertion: %s", output)
	}
	output, e := restore(true)
	if e != nil {
		t.Fatalf("offline CLI recovery: %v\n%s", e, output)
	}
	var result replication.RecoveryResult
	if e = json.Unmarshal(output, &result); e != nil {
		t.Fatalf("recovery output: %v\n%s", e, output)
	}
	if result.Stable.Index == 0 || result.Destination != destination {
		t.Fatalf("missing recovery lineage: %+v", result)
	}
	restored, e := db.Open(destination, nil)
	if e != nil {
		t.Fatal(e)
	}
	defer restored.Close()
	rows, e := restored.Exec("SELECT id,v FROM items ORDER BY id")
	if e != nil || len(rows.Rows) != 1 || rows.Rows[0]["id"].Int != 1 || rows.Rows[0]["v"].Text != "durable" {
		t.Fatalf("recovered wrong SQL prefix: %+v %v", rows, e)
	}
	if e = restored.Close(); e != nil {
		t.Fatal(e)
	}
	if _, e = os.Stat(filepath.Join(nodeDirectory, "RETIRED")); e != nil {
		t.Fatalf("old node incarnation not retired: %v", e)
	}
}
