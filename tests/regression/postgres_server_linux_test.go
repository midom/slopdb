//go:build linux

package regression

import (
	"bufio"
	"context"
	"crypto/tls"
	"crypto/x509"
	gosql "database/sql"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	mysql "github.com/go-sql-driver/mysql"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
	"net/url"
	"slopdb/internal/testcert"
)

func TestCLIPostgresTCPAndUnixPersistence(t *testing.T) {
	for _, mutual := range []bool{false, true} {
		name := "plaintext"
		if mutual {
			name = "mutual_TLS"
		}
		t.Run(name, func(t *testing.T) { testCLIPostgresTCPAndUnixPersistence(t, mutual) })
	}
}
func testCLIPostgresTCPAndUnixPersistence(t *testing.T, mutual bool) {
	env := newRegressionEnv(t)
	dir := filepath.Join(t.TempDir(), "db")
	socket := filepath.Join(t.TempDir(), ".s.PGSQL.5432")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	args := []string{"--postgres-tcp=127.0.0.1:0", "--postgres-socket=" + socket, "--postgres-password-env=SLOPDB_TEST_POSTGRES_PASSWORD", "--no-wal"}
	var clientTLS *tls.Config
	if mutual {
		serverCA, clientCA := testcert.NewCA(t, "server CA"), testcert.NewCA(t, "client CA")
		now := time.Now()
		serverCert := serverCA.Issue(t, "server", x509.ExtKeyUsageServerAuth, now.Add(-time.Hour), now.Add(time.Hour))
		clientCert := clientCA.Issue(t, "client", x509.ExtKeyUsageClientAuth, now.Add(-time.Hour), now.Add(time.Hour))
		cert, key := testcert.WriteKeyPair(t, t.TempDir(), "server", serverCert)
		ca := clientCA.Write(t, t.TempDir(), "client-ca.pem")
		args = append(args, "--postgres-tls-cert="+cert, "--postgres-tls-key="+key, "--postgres-tls-ca="+ca)
		clientTLS = &tls.Config{RootCAs: serverCA.Pool(), ServerName: "localhost", Certificates: []tls.Certificate{clientCert}, MinVersion: tls.VersionTLS12}
	}
	command := exec.CommandContext(ctx, env.cli(), append(args, dir)...)
	command.Env = append(os.Environ(), "SLOPDB_TEST_POSTGRES_PASSWORD=secret")
	stderr, err := command.StderrPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	finished := false
	t.Cleanup(func() {
		if !finished {
			_ = command.Process.Kill()
			_ = command.Wait()
		}
	})
	addresses := make(chan string, 2)
	logs := make(chan string, 1)
	go func() {
		var output strings.Builder
		scanner := bufio.NewScanner(stderr)
		for scanner.Scan() {
			line := scanner.Text()
			fmt.Fprintln(&output, line)
			fields := strings.Fields(line)
			if len(fields) > 4 && strings.HasPrefix(line, "PostgreSQL listening on tcp ") {
				addresses <- fields[4]
			}
			if strings.HasPrefix(line, "PostgreSQL listening on unix ") {
				addresses <- socket
			}
		}
		if err := scanner.Err(); err != nil {
			fmt.Fprintln(&output, err)
		}
		logs <- output.String()
	}()
	var address string
	for i := 0; i < 2; i++ {
		select {
		case a := <-addresses:
			if a != socket {
				address = a
			}
		case <-ctx.Done():
			t.Fatal("server did not start")
		}
	}
	if mode, err := os.Stat(socket); err != nil || mode.Mode().Perm() != 0600 {
		t.Fatalf("socket permissions %v %v", mode, err)
	}
	open := func(network, address string) *gosql.DB {
		t.Helper()
		endpoint := url.URL{Scheme: "postgres", Host: address, User: url.UserPassword("slopdb", "secret"), Path: "/slopdb"}
		q := url.Values{"sslmode": {"disable"}, "connect_timeout": {"3"}}
		if network == "unix" {
			endpoint.Host = ""
			q.Set("host", filepath.Dir(address))
			q.Set("port", "5432")
		}
		endpoint.RawQuery = q.Encode()
		config, err := pgx.ParseConfig(endpoint.String())
		if err != nil {
			t.Fatal(err)
		}
		config.TLSConfig = clientTLS
		config.Fallbacks = nil
		client := stdlib.OpenDB(*config)
		t.Cleanup(func() { _ = client.Close() })
		return client
	}
	tcp := open("tcp", address)
	unix := open("unix", socket)
	for _, q := range []string{`CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)`, `INSERT INTO t VALUES(1,'PostgreSQL string')`} {
		if _, err := tcp.Exec(q); err != nil {
			t.Fatal(err)
		}
	}
	var value string
	if err := unix.QueryRow("SELECT v FROM t WHERE id=$1", 1).Scan(&value); err != nil || value != "PostgreSQL string" {
		t.Fatalf("shared TCP/Unix database %q %v", value, err)
	}
	tx, err := unix.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec("INSERT INTO t VALUES(2,'uncommitted')"); err != nil {
		t.Fatal(err)
	}
	if err := command.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	err = command.Wait()
	finished = true
	output := <-logs
	if err != nil || strings.Contains(output, "DATA RACE") {
		t.Fatalf("server shutdown %v: %s", err, output)
	}
	_ = tx.Rollback()
	if _, err := os.Stat(socket); !os.IsNotExist(err) {
		t.Fatalf("socket not removed: %v", err)
	}
	database, err := env.openDatabase(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	result, err := database.Exec("SELECT id,v FROM t ORDER BY id")
	if err != nil || len(result.Rows) != 1 || result.Rows[0]["id"].Int != 1 || result.Rows[0]["v"].Text != "PostgreSQL string" {
		t.Fatalf("clean shutdown persistence: %v %v", result, err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
}
func TestCLIPostgresStartupFailurePreservesSocketPath(t *testing.T) {
	env := newRegressionEnv(t)
	socket := filepath.Join(t.TempDir(), "existing")
	if err := os.WriteFile(socket, []byte("keep"), 0600); err != nil {
		t.Fatal(err)
	}
	command := exec.Command(env.cli(), "--postgres-tcp=127.0.0.1:0", "--postgres-socket="+socket, filepath.Join(t.TempDir(), "db"))
	if output, err := command.CombinedOutput(); err == nil || !strings.Contains(string(output), "PostgreSQL server:") {
		t.Fatalf("startup should fail: %v %s", err, output)
	}
	if data, err := os.ReadFile(socket); err != nil || string(data) != "keep" {
		t.Fatalf("existing path changed %q %v", data, err)
	}
}

func TestCLIPostgresTLSInvalidConfiguration(t *testing.T) {
	env := newRegressionEnv(t)
	binary := env.cli()
	for _, args := range [][]string{
		{"--postgres-tcp=127.0.0.1:0", "--postgres-require-tls"},
		{"--postgres-tcp=127.0.0.1:0", "--postgres-tls-ca=missing.pem"},
		{"--postgres-tcp=127.0.0.1:0", "--postgres-tls-cert=missing.pem"},
		{"--postgres-tls-cert=missing.pem", "--postgres-tls-key=missing.key"},
	} {
		path := filepath.Join(t.TempDir(), "database")
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		command := exec.CommandContext(ctx, binary, append(args, path)...)
		output, err := command.CombinedOutput()
		timedOut := ctx.Err()
		cancel()
		if timedOut != nil {
			t.Fatalf("invalid TLS flags did not fail promptly: %v %s", args, output)
		}
		if err == nil || !strings.Contains(string(output), "PostgreSQL") {
			t.Fatalf("invalid TLS flags accepted: %v %s", err, output)
		}
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Fatalf("invalid TLS startup created a database: %v", err)
		}
	}
}

func TestCLIDualFrontendsShareDatabaseAndQuoting(t *testing.T) {
	env := newRegressionEnv(t)
	binary := env.cli()
	path := filepath.Join(t.TempDir(), "database")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, binary, "--postgres-tcp=127.0.0.1:0", "--mysql-tcp=127.0.0.1:0", "--mysql-quoting", path)
	stderr, e := command.StderrPipe()
	if e != nil {
		t.Fatal(e)
	}
	if e = command.Start(); e != nil {
		t.Fatal(e)
	}
	finished := false
	t.Cleanup(func() {
		if !finished {
			_ = command.Process.Kill()
			_ = command.Wait()
		}
	})
	type endpoint struct{ protocol, address string }
	addresses := make(chan endpoint, 2)
	logs := make(chan string, 1)
	go func() {
		var output strings.Builder
		scanner := bufio.NewScanner(stderr)
		for scanner.Scan() {
			line := scanner.Text()
			fmt.Fprintln(&output, line)
			fields := strings.Fields(line)
			if len(fields) > 4 && (strings.HasPrefix(line, "MySQL listening on tcp ") || strings.HasPrefix(line, "PostgreSQL listening on tcp ")) {
				addresses <- endpoint{fields[0], fields[4]}
			}
		}
		if err := scanner.Err(); err != nil {
			fmt.Fprintln(&output, err)
		}
		logs <- output.String()
	}()
	endpoints := map[string]string{}
	for len(endpoints) < 2 {
		select {
		case item := <-addresses:
			endpoints[item.protocol] = item.address
		case <-ctx.Done():
			t.Fatal("dual frontend startup timed out")
		}
	}
	pg, e := pgx.Connect(ctx, "postgres://slopdb@"+endpoints["PostgreSQL"]+"/slopdb?sslmode=disable")
	if e != nil {
		t.Fatal(e)
	}
	defer pg.Close(ctx)
	configuration := mysql.NewConfig()
	configuration.User = "slopdb"
	configuration.DBName = "slopdb"
	configuration.Net = "tcp"
	configuration.Addr = endpoints["MySQL"]
	configuration.Timeout = 3 * time.Second
	configuration.ReadTimeout = 5 * time.Second
	connector, e := mysql.NewConnector(configuration)
	if e != nil {
		t.Fatal(e)
	}
	my := gosql.OpenDB(connector)
	defer my.Close()
	if _, e = pg.Exec(ctx, `CREATE TABLE dual_frontend(id BIGINT PRIMARY KEY,msg TEXT);INSERT INTO dual_frontend VALUES(1,'PostgreSQL literal')`); e != nil {
		t.Fatal(e)
	}
	if _, e = my.ExecContext(ctx, `INSERT INTO dual_frontend VALUES(2,"MySQL literal")`); e != nil {
		t.Fatal(e)
	}
	var text string
	if e = my.QueryRowContext(ctx, `SELECT msg FROM dual_frontend WHERE id=1`).Scan(&text); e != nil || text != "PostgreSQL literal" {
		t.Fatal(text, e)
	}
	if e = pg.QueryRow(ctx, `SELECT "msg" FROM dual_frontend WHERE id=2`).Scan(&text); e != nil || text != "MySQL literal" {
		t.Fatal(text, e)
	}
	if e = pg.Close(ctx); e != nil {
		t.Fatal(e)
	}
	if e = my.Close(); e != nil {
		t.Fatal(e)
	}
	if e = command.Process.Signal(syscall.SIGTERM); e != nil {
		t.Fatal(e)
	}
	e = command.Wait()
	finished = true
	output := <-logs
	if e != nil || strings.Contains(output, "DATA RACE") {
		t.Fatalf("dual frontend shutdown: %v %s", e, output)
	}
	reopened, e := env.openDatabase(path, nil)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { reopened.Close() })
	r, e := reopened.Exec("SELECT id,msg FROM dual_frontend ORDER BY id")
	if e != nil || len(r.Rows) != 2 || r.Rows[0]["msg"].Text != "PostgreSQL literal" || r.Rows[1]["msg"].Text != "MySQL literal" {
		t.Fatal(r, e)
	}
	if e = reopened.Close(); e != nil {
		t.Fatal(e)
	}
}
