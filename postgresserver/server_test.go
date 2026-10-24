package postgresserver

import (
	"bytes"
	"context"
	gosql "database/sql"
	"errors"
	"net"
	"net/url"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/lib/pq"
	"slopdb/db"
	"slopdb/storage"
)

type testServer struct {
	server           *Server
	database         *db.Database
	network, address string
}

func startTestServer(t *testing.T, network string, config Config) *testServer {
	t.Helper()
	options := storage.DefaultOptions()
	options.DisableWALSync = true
	return startTestServerOptions(t, network, config, options)
}
func startTestServerOptions(t *testing.T, network string, config Config, options *storage.Options) *testServer {
	t.Helper()
	database, err := db.Open(t.TempDir(), options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := database.Close(); err != nil {
			t.Error(err)
		}
	})
	server, err := New(database, config)
	if err != nil {
		t.Fatal(err)
	}
	address := "127.0.0.1:0"
	if network == "unix" {
		if runtime.GOOS == "windows" {
			t.Skip("Unix sockets")
		}
		address = filepath.Join(t.TempDir(), ".s.PGSQL.5432")
	}
	listener, err := net.Listen(network, address)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- server.Serve(listener) }()
	t.Cleanup(func() {
		_ = server.Close()
		if err := <-done; err != nil && !errors.Is(err, net.ErrClosed) {
			t.Error(err)
		}
	})
	return &testServer{server, database, network, listener.Addr().String()}
}
func (s *testServer) dsn(user, password, database string) string {
	u := url.URL{Scheme: "postgres", Host: s.address, User: url.UserPassword(user, password), Path: "/" + database}
	q := url.Values{"sslmode": {"disable"}, "connect_timeout": {"3"}}
	if s.network == "unix" {
		u.Host = ""
		q.Set("host", filepath.Dir(s.address))
		q.Set("port", "5432")
	}
	u.RawQuery = q.Encode()
	return u.String()
}
func (s *testServer) open(t *testing.T, password string) *gosql.DB {
	t.Helper()
	connector, err := pq.NewConnector(s.dsn("slopdb", password, "slopdb"))
	if err != nil {
		t.Fatal(err)
	}
	client := gosql.OpenDB(connector)
	client.SetMaxOpenConns(8)
	t.Cleanup(func() { _ = client.Close() })
	return client
}
func mustExec(t *testing.T, c interface {
	Exec(string, ...any) (gosql.Result, error)
}, query string, args ...any) gosql.Result {
	t.Helper()
	result, err := c.Exec(query, args...)
	if err != nil {
		t.Fatalf("%s: %v", query, err)
	}
	return result
}
func TestLibPQTCPAndUnix(t *testing.T) {
	for _, network := range []string{"tcp", "unix"} {
		t.Run(network, func(t *testing.T) {
			s := startTestServer(t, network, Config{Password: "secret"})
			client := s.open(t, "secret")
			if err := client.Ping(); err != nil {
				t.Fatal(err)
			}
			mustExec(t, client, `CREATE TABLE items(id INTEGER PRIMARY KEY, label TEXT, payload BLOB, amount REAL, optional TEXT)`)
			result := mustExec(t, client, `INSERT INTO items VALUES($1,$2,$3,$4,$5)`, int64(9223372036854775807), "héllo", []byte{0, 255, 1}, 1.2345678901234567, nil)
			if n, err := result.RowsAffected(); err != nil || n != 1 {
				t.Fatalf("affected %d %v", n, err)
			}
			stmt, err := client.Prepare(`SELECT id,label,payload,amount,optional FROM items WHERE id=$1`)
			if err != nil {
				t.Fatal(err)
			}
			defer stmt.Close()
			for i := 0; i < 2; i++ {
				var id int64
				var label string
				var blob []byte
				var amount float64
				var optional gosql.NullString
				if err := stmt.QueryRow(int64(9223372036854775807)).Scan(&id, &label, &blob, &amount, &optional); err != nil {
					t.Fatal(err)
				}
				if id != 9223372036854775807 || label != "héllo" || !bytes.Equal(blob, []byte{0, 255, 1}) || amount != 1.2345678901234567 || optional.Valid {
					t.Fatalf("row %d %q %x %.17g %v", id, label, blob, amount, optional)
				}
			}
			var text string
			if err := client.QueryRow(`SELECT label FROM items`).Scan(&text); err != nil || text != "héllo" {
				t.Fatalf("simple query %q %v", text, err)
			}
			var a, b int64
			if err := client.QueryRow(`SELECT $2::bigint AS a, $1::bigint+$2::bigint AS b`, int64(3), int64(7)).Scan(&a, &b); err != nil || a != 7 || b != 10 {
				t.Fatalf("parameter positions %d %d %v", a, b, err)
			}
			var literal string
			if err := client.QueryRow(`SELECT '$1 ?' AS label`).Scan(&literal); err != nil || literal != "$1 ?" {
				t.Fatalf("quoted parameter markers %q %v", literal, err)
			}
			var database string
			if err := client.QueryRow(`SELECT current_database()`).Scan(&database); err != nil || database != "slopdb" {
				t.Fatalf("database %q %v", database, err)
			}
		})
	}
}
func TestPGXExtendedProtocol(t *testing.T) {
	s := startTestServer(t, "tcp", Config{Password: "secret"})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, s.dsn("slopdb", "secret", "slopdb"))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(context.Background())
	if _, err := conn.Exec(ctx, `CREATE TABLE numbers(id INTEGER PRIMARY KEY, label TEXT, b BLOB, amount REAL)`); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if _, err := conn.Exec(ctx, `INSERT INTO numbers VALUES($1,$2,$3,$4)`, int64(i), "cached", []byte{0, byte(i)}, 1.25); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 3; i++ {
		var id int64
		var label string
		var blob []byte
		var real float64
		if err := conn.QueryRow(ctx, `SELECT id,label,b,amount FROM numbers WHERE id=$1`, int64(i)).Scan(&id, &label, &blob, &real); err != nil {
			t.Fatal(err)
		}
		if id != int64(i) || label != "cached" || !bytes.Equal(blob, []byte{0, byte(i)}) || real != 1.25 {
			t.Fatalf("pgx row %d %q %x %v", id, label, blob, real)
		}
	}
	var total int64
	if err := conn.QueryRow(ctx, `SELECT $1::bigint+$2::bigint AS total`, int64(10), int64(20)).Scan(&total); err != nil || total != 30 {
		t.Fatalf("cast parameters %d %v", total, err)
	}
}
func TestAuthenticationAndDatabase(t *testing.T) {
	for _, password := range []string{"", "secret", "café秘密"} {
		t.Run(password, func(t *testing.T) {
			s := startTestServer(t, "tcp", Config{Password: password})
			for _, tc := range []struct{ user, password, database, code string }{{"slopdb", password, "slopdb", ""}, {"wrong", password, "slopdb", "28P01"}, {"slopdb", "wrong", "slopdb", "28P01"}, {"slopdb", password, "other", "3D000"}} {
				// Trust/empty-password mode intentionally ignores supplied password, like PostgreSQL trust.
				if password == "" && tc.user == "slopdb" && tc.database == "slopdb" {
					tc.code = ""
				}
				connector, err := pq.NewConnector(s.dsn(tc.user, tc.password, tc.database))
				if err != nil {
					t.Fatal(err)
				}
				client := gosql.OpenDB(connector)
				err = client.Ping()
				_ = client.Close()
				if tc.code == "" {
					if err != nil {
						t.Fatal(err)
					}
				} else {
					var pgErr *pq.Error
					if !errors.As(err, &pgErr) || string(pgErr.Code) != tc.code {
						t.Fatalf("authentication %q/%q: %v", tc.user, tc.database, err)
					}
				}
			}
		})
	}
}
func TestTransactionsAndSimpleBatch(t *testing.T) {
	s := startTestServer(t, "tcp", Config{})
	client := s.open(t, "")
	mustExec(t, client, `CREATE TABLE accounts(id INTEGER PRIMARY KEY, n INTEGER); INSERT INTO accounts VALUES(1,100);`)
	tx, err := client.Begin()
	if err != nil {
		t.Fatal(err)
	}
	mustExec(t, tx, `UPDATE accounts SET n=$1 WHERE id=$2`, 200, 1)
	var n int
	if err := client.QueryRow(`SELECT n FROM accounts WHERE id=1`).Scan(&n); err != nil || n != 100 {
		t.Fatalf("dirty read %d %v", n, err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	tx, err = client.Begin()
	if err != nil {
		t.Fatal(err)
	}
	mustExec(t, tx, `SAVEPOINT a`)
	if _, err := tx.Exec(`INSERT INTO accounts VALUES(1,999)`); err == nil {
		t.Fatal("duplicate accepted")
	}
	if _, err := tx.Exec(`UPDATE accounts SET n=999 WHERE id=1`); err == nil {
		t.Fatal("failed transaction accepted a statement")
	}
	mustExec(t, tx, `ROLLBACK TO a`)
	mustExec(t, tx, `UPDATE accounts SET n=300 WHERE id=1`)
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if _, err := client.Exec(`INSERT INTO accounts VALUES(2,1); INSERT INTO accounts VALUES(1,999);`); err == nil {
		t.Fatal("failed batch accepted")
	}
	if err := client.QueryRow(`SELECT COUNT(*) FROM accounts`).Scan(&n); err != nil || n != 1 {
		t.Fatalf("batch was not atomic: %d %v", n, err)
	}
	if err := client.QueryRow(`SELECT n FROM accounts WHERE id=1`).Scan(&n); err != nil || n != 300 {
		t.Fatalf("commit %d %v", n, err)
	}
	if _, err := client.Exec(`SELECT 1/0`); err == nil {
		t.Fatal("zero division accepted")
	}
	if err := client.Ping(); err != nil {
		t.Fatal(err)
	}
}
func TestNoExecutionDuringPrepare(t *testing.T) {
	s := startTestServer(t, "tcp", Config{})
	client := s.open(t, "")
	mustExec(t, client, `CREATE TABLE t(id INTEGER PRIMARY KEY)`)
	stmt, err := client.Prepare(`INSERT INTO t VALUES($1) RETURNING id`)
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Close()
	var count int
	if err := client.QueryRow(`SELECT COUNT(*) FROM t`).Scan(&count); err != nil || count != 0 {
		t.Fatalf("prepare executed INSERT: %d %v", count, err)
	}
	var id int64
	if err := stmt.QueryRow(7).Scan(&id); err != nil || id != 7 {
		t.Fatalf("RETURNING %d %v", id, err)
	}
	if err := client.QueryRow(`SELECT COUNT(*) FROM t`).Scan(&count); err != nil || count != 1 {
		t.Fatalf("executed more than once: %d %v", count, err)
	}
}

func TestPreparedMetadataAndSchemaChanges(t *testing.T) {
	s := startTestServer(t, "tcp", Config{})
	client := s.open(t, "")
	mustExec(t, client, `CREATE TABLE t(id INTEGER PRIMARY KEY,n INTEGER,label TEXT)`)
	mustExec(t, client, `INSERT INTO t VALUES(1,5,'one')`)
	for _, q := range []string{
		`SELECT n FROM t WHERE id=$1`,
		`SELECT a.n FROM t AS a JOIN t AS b ON a.id=b.id WHERE b.id=$1`,
		`WITH chosen AS (SELECT id,n FROM t WHERE id=$1) SELECT n FROM chosen`,
		`SELECT n FROM (SELECT id,n FROM t) AS source WHERE id=$1`,
	} {
		var n int64
		if err := client.QueryRow(q, 1).Scan(&n); err != nil || n != 5 {
			t.Fatalf("metadata %s: %d %v", q, n, err)
		}
	}
	mustExec(t, client, `CREATE VIEW names(id,title) AS SELECT id,label FROM t`)
	var title string
	if err := client.QueryRow(`SELECT title FROM names WHERE id=$1`, 1).Scan(&title); err != nil || title != "one" {
		t.Fatalf("view metadata %q %v", title, err)
	}
	stmt, err := client.Prepare(`UPDATE t SET n=n+1 WHERE id=$1 RETURNING *`)
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Close()
	mustExec(t, client, `ALTER TABLE t ADD COLUMN extra TEXT`)
	rows, err := stmt.Query(1)
	if err == nil {
		for rows.Next() {
		}
		err = rows.Err()
		_ = rows.Close()
	}
	if err == nil {
		t.Fatal("stale RETURNING descriptor accepted")
	}
	var n int64
	if err := client.QueryRow(`SELECT n FROM t WHERE id=1`).Scan(&n); err != nil || n != 5 {
		t.Fatalf("descriptor failure committed a write: %d %v", n, err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	pg, err := pgx.Connect(ctx, s.dsn("slopdb", "", "slopdb"))
	if err != nil {
		t.Fatal(err)
	}
	defer pg.Close(context.Background())
	if err := pg.QueryRow(ctx, `SELECT $1+1 AS value`, int64(9)).Scan(&n); err != nil || n != 10 {
		t.Fatalf("arithmetic parameter inference: %d %v", n, err)
	}
}

func TestConcurrentSCRAMSessions(t *testing.T) {
	s := startTestServer(t, "tcp", Config{Password: "secret"})
	client := s.open(t, "secret")
	mustExec(t, client, "CREATE TABLE counts(id INTEGER PRIMARY KEY,n INTEGER)")
	for i := 0; i < 8; i++ {
		mustExec(t, client, "INSERT INTO counts VALUES($1,0)", i)
	}
	start := make(chan struct{})
	var workers sync.WaitGroup
	for i := 0; i < 8; i++ {
		workers.Add(1)
		go func(id int) {
			defer workers.Done()
			<-start
			for j := 0; j < 10; j++ {
				if _, err := client.Exec("UPDATE counts SET n=n+1 WHERE id=$1", id); err != nil {
					t.Error(err)
					return
				}
			}
		}(i)
	}
	close(start)
	workers.Wait()
	var total int64
	if err := client.QueryRow("SELECT SUM(n) FROM counts").Scan(&total); err != nil || total != 80 {
		t.Fatalf("concurrent total %d %v", total, err)
	}
}
