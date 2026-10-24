//go:build linux

package regression

import (
	"bufio"
	"context"
	gosql "database/sql"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	mysql "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
)

func TestCLIRaftProtocolsAndCrashRecovery(t *testing.T) {
	for _, protocol := range []string{"mysql", "postgres"} {
		t.Run(protocol, func(t *testing.T) {
			env := newRegressionEnv(t)
			root := t.TempDir()
			nodeDir := filepath.Join(root, "node")
			socket := filepath.Join(root, "sql.sock")
			if protocol == "postgres" {
				socket = filepath.Join(root, ".s.PGSQL.5432")
			}
			ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
			defer cancel()
			args := []string{"--raft-id=n1", "--raft-listen=unix://" + filepath.Join(root, "raft.sock"), "--raft-control=unix://" + filepath.Join(root, "control.sock"), "--raft-token-env=SLOPDB_TEST_RAFT_TOKEN", "--" + protocol + "-socket=" + socket}
			start := func(bootstrap bool) (*exec.Cmd, <-chan string) {
				t.Helper()
				actual := append([]string(nil), args...)
				if bootstrap {
					actual = append(actual, "--raft-bootstrap")
				}
				actual = append(actual, nodeDir)
				command := exec.CommandContext(ctx, env.cli(), actual...)
				command.Env = append(os.Environ(), "SLOPDB_TEST_RAFT_TOKEN=permanent-regression-raft-token")
				stderr, e := command.StderrPipe()
				if e != nil {
					t.Fatal(e)
				}
				if e = command.Start(); e != nil {
					t.Fatal(e)
				}
				logs := make(chan string, 1)
				go func() {
					var b strings.Builder
					s := bufio.NewScanner(stderr)
					for s.Scan() {
						fmt.Fprintln(&b, s.Text())
					}
					logs <- b.String()
				}()
				t.Cleanup(func() {
					if command.ProcessState == nil {
						_ = command.Process.Kill()
						_ = command.Wait()
					}
				})
				return command, logs
			}
			connect := func(logs <-chan string) *gosql.DB {
				t.Helper()
				var client *gosql.DB
				var e error
				if protocol == "mysql" {
					config := mysql.NewConfig()
					config.User = "slopdb"
					config.DBName = "slopdb"
					config.Net = "unix"
					config.Addr = socket
					config.Timeout = time.Second
					config.ReadTimeout = 5 * time.Second
					connector, ce := mysql.NewConnector(config)
					if ce != nil {
						t.Fatal(ce)
					}
					client = gosql.OpenDB(connector)
				} else {
					client, e = gosql.Open("postgres", fmt.Sprintf("host=%s port=5432 user=slopdb dbname=slopdb sslmode=disable connect_timeout=1", root))
					if e != nil {
						t.Fatal(e)
					}
				}
				client.SetMaxOpenConns(1)
				t.Cleanup(func() { _ = client.Close() })
				ticker := time.NewTicker(25 * time.Millisecond)
				defer ticker.Stop()
				for {
					if e = client.PingContext(ctx); e == nil {
						return client
					}
					select {
					case output := <-logs:
						t.Fatalf("server exited before SQL was ready:\n%s", output)
					case <-ctx.Done():
						t.Fatalf("SQL never ready: %v", e)
					case <-ticker.C:
					}
				}
			}
			command, logs := start(true)
			client := connect(logs)
			if _, e := client.ExecContext(ctx, "CREATE TABLE items(id INTEGER PRIMARY KEY,v TEXT)"); e != nil {
				t.Fatal(e)
			}
			if _, e := client.ExecContext(ctx, "INSERT INTO items VALUES(1,'durable')"); e != nil {
				t.Fatal(e)
			}
			tx, e := client.BeginTx(ctx, nil)
			if e != nil {
				t.Fatal(e)
			}
			if _, e = tx.ExecContext(ctx, "INSERT INTO items VALUES(2,'uncommitted')"); e != nil {
				t.Fatal(e)
			}
			if e = command.Process.Kill(); e != nil {
				t.Fatal(e)
			}
			var exit *exec.ExitError
			if e = command.Wait(); !errors.As(e, &exit) {
				t.Fatalf("expected killed child, got %v", e)
			}
			<-logs
			_ = tx.Rollback()
			_ = client.Close()
			command, logs = start(false)
			client = connect(logs)
			var count int
			if e = client.QueryRowContext(ctx, "SELECT COUNT(*) FROM items").Scan(&count); e != nil || count != 1 {
				t.Fatalf("crash replay count=%d error=%v", count, e)
			}
			for _, path := range []string{socket, filepath.Join(root, "raft.sock"), filepath.Join(root, "control.sock")} {
				info, e := os.Stat(path)
				if e != nil || info.Mode().Perm() != 0600 {
					t.Fatalf("socket mode %s: %v %v", path, info, e)
				}
			}
			_ = client.Close()
			if e = command.Process.Signal(syscall.SIGTERM); e != nil {
				t.Fatal(e)
			}
			if e = command.Wait(); e != nil {
				t.Fatalf("shutdown: %v\n%s", e, <-logs)
			}
			<-logs
		})
	}
}

func TestCLIRaftRejectsUnsafeDurability(t *testing.T) {
	env := newRegressionEnv(t)
	for _, flag := range []string{"--no-wal", "--no-wal-fsync", "--no-autoincrement-persistence"} {
		command := exec.Command(env.cli(), "--raft-id=n1", "--raft-bootstrap", "--raft-listen=tcp://127.0.0.1:0", "--raft-control=tcp://127.0.0.1:0", "--raft-token-env=SLOPDB_TEST_RAFT_TOKEN", flag, filepath.Join(t.TempDir(), "node"))
		command.Env = append(os.Environ(), "SLOPDB_TEST_RAFT_TOKEN=permanent-regression-raft-token")
		output, e := command.CombinedOutput()
		if e == nil || !strings.Contains(string(output), "replication requires durable WAL") {
			t.Fatalf("%s: %v %s", flag, e, output)
		}
	}
}
