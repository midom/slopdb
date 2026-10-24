package postgresserver

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	gosql "database/sql"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"net/url"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/lib/pq"
	"slopdb/internal/testcert"
)

type tlsFixture struct {
	serverCA, clientCA                   *testcert.CA
	serverCertificate, clientCertificate tls.Certificate
}

func newTLSFixture(t *testing.T) *tlsFixture {
	t.Helper()
	serverCA, clientCA := testcert.NewCA(t, "server CA"), testcert.NewCA(t, "client CA")
	now := time.Now()
	return &tlsFixture{serverCA, clientCA, serverCA.Issue(t, "server", x509.ExtKeyUsageServerAuth, now.Add(-time.Hour), now.Add(time.Hour)), clientCA.Issue(t, "client", x509.ExtKeyUsageClientAuth, now.Add(-time.Hour), now.Add(time.Hour))}
}
func (f *tlsFixture) serverConfig(mutual bool) *tls.Config {
	cfg := &tls.Config{Certificates: []tls.Certificate{f.serverCertificate}}
	if mutual {
		cfg.ClientAuth = tls.RequireAndVerifyClientCert
		cfg.ClientCAs = f.clientCA.Pool()
	}
	return cfg
}
func (f *tlsFixture) clientConfig(mutual bool) *tls.Config {
	cfg := &tls.Config{RootCAs: f.serverCA.Pool(), ServerName: "localhost", MinVersion: tls.VersionTLS12}
	if mutual {
		cfg.Certificates = []tls.Certificate{f.clientCertificate}
	}
	return cfg
}
func (s *testServer) connectTLS(t *testing.T, cfg *tls.Config, password string) (*pgx.Conn, error) {
	t.Helper()
	config, err := pgx.ParseConfig(s.dsn("slopdb", password, "slopdb"))
	if err != nil {
		t.Fatal(err)
	}
	config.TLSConfig = cfg
	config.Fallbacks = nil
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	conn, err := pgx.ConnectConfig(ctx, config)
	if err == nil {
		t.Cleanup(func() { _ = conn.Close(context.Background()) })
	}
	return conn, err
}
func TestTLSOnBothTransports(t *testing.T) {
	f := newTLSFixture(t)
	for _, network := range []string{"tcp", "unix"} {
		t.Run(network, func(t *testing.T) {
			for _, version := range []uint16{tls.VersionTLS12, tls.VersionTLS13} {
				t.Run(tls.VersionName(version), func(t *testing.T) {
					s := startTestServer(t, network, Config{Password: "secret", TLSConfig: f.serverConfig(true)})
					cfg := f.clientConfig(true)
					cfg.MinVersion = version
					cfg.MaxVersion = version
					conn, err := s.connectTLS(t, cfg, "secret")
					if err != nil {
						t.Fatal(err)
					}
					ctx := context.Background()
					if _, err := conn.Exec(ctx, `CREATE TABLE secure(id INTEGER PRIMARY KEY, label TEXT)`); err != nil {
						t.Fatal(err)
					}
					if _, err := conn.Exec(ctx, `INSERT INTO secure VALUES($1,$2)`, int64(7), "TLS"); err != nil {
						t.Fatal(err)
					}
					var label string
					if err := conn.QueryRow(ctx, `SELECT label FROM secure WHERE id=$1`, int64(7)).Scan(&label); err != nil || label != "TLS" {
						t.Fatalf("encrypted data %q %v", label, err)
					}
					if err := s.open(t, "secret").Ping(); err == nil {
						t.Fatal("plaintext bypassed client certificate authentication")
					}
					if _, err := s.connectTLS(t, f.clientConfig(true), "wrong"); err == nil {
						t.Fatal("client certificate bypassed password")
					}
				})
			}
		})
	}
}
func TestTLSClientCertificateValidation(t *testing.T) {
	f := newTLSFixture(t)
	other := testcert.NewCA(t, "other CA")
	now := time.Now()
	for _, network := range []string{"tcp", "unix"} {
		t.Run(network, func(t *testing.T) {
			s := startTestServer(t, network, Config{TLSConfig: f.serverConfig(true)})
			for _, tc := range []struct {
				name        string
				certificate *tls.Certificate
			}{
				{"missing", nil},
				{"untrusted", ptrCert(other.Issue(t, "untrusted", x509.ExtKeyUsageClientAuth, now.Add(-time.Hour), now.Add(time.Hour)))},
				{"server_CA_does_not_trust_client", ptrCert(f.serverCA.Issue(t, "wrong", x509.ExtKeyUsageClientAuth, now.Add(-time.Hour), now.Add(time.Hour)))},
				{"expired", ptrCert(f.clientCA.Issue(t, "expired", x509.ExtKeyUsageClientAuth, now.Add(-2*time.Hour), now.Add(-time.Hour)))},
				{"wrong_usage", ptrCert(f.clientCA.Issue(t, "server only", x509.ExtKeyUsageServerAuth, now.Add(-time.Hour), now.Add(time.Hour)))},
			} {
				t.Run(tc.name, func(t *testing.T) {
					cfg := f.clientConfig(false)
					if tc.certificate != nil {
						cert := *tc.certificate
						cfg.GetClientCertificate = func(*tls.CertificateRequestInfo) (*tls.Certificate, error) { return &cert, nil }
					}
					if _, err := s.connectTLS(t, cfg, ""); err == nil {
						t.Fatal("invalid client certificate authenticated")
					}
				})
			}
			wrongName := f.clientConfig(true)
			wrongName.ServerName = "wrong.invalid"
			if _, err := s.connectTLS(t, wrongName, ""); err == nil {
				t.Fatal("server identity mismatch accepted")
			}
			wrongRoot := f.clientConfig(true)
			wrongRoot.RootCAs = other.Pool()
			if _, err := s.connectTLS(t, wrongRoot, ""); err == nil {
				t.Fatal("untrusted server accepted")
			}
			if _, err := s.connectTLS(t, f.clientConfig(true), ""); err != nil {
				t.Fatal(err)
			}
		})
	}
}
func ptrCert(c tls.Certificate) *tls.Certificate { return &c }
func TestTLSOptionalAndRequired(t *testing.T) {
	f := newTLSFixture(t)
	for _, required := range []bool{false, true} {
		s := startTestServer(t, "tcp", Config{TLSConfig: f.serverConfig(false), RequireTLS: required})
		if _, err := s.connectTLS(t, f.clientConfig(false), ""); err != nil {
			t.Fatal(err)
		}
		err := s.open(t, "").Ping()
		if required && err == nil {
			t.Fatal("TLS requirement bypassed")
		}
		if !required && err != nil {
			t.Fatal(err)
		}
		old := f.clientConfig(false)
		old.MinVersion = tls.VersionTLS11
		old.MaxVersion = tls.VersionTLS11
		if _, err := s.connectTLS(t, old, ""); err == nil {
			t.Fatal("TLS below 1.2 accepted")
		}
	}
}

func TestRequiredTLSCancellation(t *testing.T) {
	f := newTLSFixture(t)
	s := startTestServer(t, "tcp", Config{TLSConfig: f.serverConfig(true)})
	holder, err := s.connectTLS(t, f.clientConfig(true), "")
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if _, err := holder.Exec(ctx, "CREATE TABLE t(id INTEGER PRIMARY KEY,n INTEGER)"); err != nil {
		t.Fatal(err)
	}
	if _, err := holder.Exec(ctx, "INSERT INTO t VALUES(1,0)"); err != nil {
		t.Fatal(err)
	}
	tx, err := holder.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, "UPDATE t SET n=1 WHERE id=1"); err != nil {
		t.Fatal(err)
	}
	waiting, err := s.connectTLS(t, f.clientConfig(true), "")
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { _, err := waiting.Exec(ctx, "UPDATE t SET n=2 WHERE id=1"); done <- err }()
	waitForLock(t, s)
	request := binary.BigEndian.AppendUint32(nil, 16)
	request = binary.BigEndian.AppendUint32(request, cancelRequest)
	request = binary.BigEndian.AppendUint32(request, waiting.PgConn().PID())
	request = binary.BigEndian.AppendUint32(request, waiting.PgConn().SecretKey())
	// Even possession of the cancellation token cannot bypass required TLS.
	plain, err := net.Dial("tcp", s.address)
	if err != nil {
		t.Fatal(err)
	}
	_ = plain.SetDeadline(time.Now().Add(3 * time.Second))
	if err := writeAll(plain, request); err != nil {
		t.Fatal(err)
	}
	reply, err := pgproto3.NewFrontend(plain, plain).Receive()
	_ = plain.Close()
	if err != nil {
		t.Fatal(err)
	}
	if e, ok := reply.(*pgproto3.ErrorResponse); !ok || e.Code != "28000" {
		t.Fatalf("plaintext cancellation not rejected: %+v", reply)
	}
	secureRaw, err := net.Dial("tcp", s.address)
	if err != nil {
		t.Fatal(err)
	}
	defer secureRaw.Close()
	_ = secureRaw.SetDeadline(time.Now().Add(3 * time.Second))
	ssl := binary.BigEndian.AppendUint32(nil, 8)
	ssl = binary.BigEndian.AppendUint32(ssl, sslRequest)
	if err := writeAll(secureRaw, ssl); err != nil {
		t.Fatal(err)
	}
	var accept [1]byte
	if _, err := io.ReadFull(secureRaw, accept[:]); err != nil || accept[0] != 'S' {
		t.Fatalf("SSLRequest %q %v", accept, err)
	}
	connection := tls.Client(secureRaw, f.clientConfig(true))
	if err := connection.Handshake(); err != nil {
		t.Fatal(err)
	}
	if err := writeAll(connection, request); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		var pgErr *pgconn.PgError
		if !errors.As(err, &pgErr) || pgErr.Code != "57014" {
			t.Fatalf("encrypted cancellation %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("TLS cancellation timed out")
	}
	if err := waiting.Ping(ctx); err != nil {
		t.Fatalf("connection not reusable: %v", err)
	}
}

func TestLibPQMutualTLS(t *testing.T) {
	f := newTLSFixture(t)
	s := startTestServer(t, "tcp", Config{Password: "secret", TLSConfig: f.serverConfig(true)})
	dir := t.TempDir()
	root := f.serverCA.Write(t, dir, "server-ca.pem")
	cert, key := testcert.WriteKeyPair(t, dir, "client", f.clientCertificate)
	endpoint, err := url.Parse(s.dsn("slopdb", "secret", "slopdb"))
	if err != nil {
		t.Fatal(err)
	}
	options := endpoint.Query()
	options.Set("sslmode", "verify-full")
	options.Set("sslrootcert", root)
	options.Set("sslcert", cert)
	options.Set("sslkey", key)
	endpoint.RawQuery = options.Encode()
	connector, err := pq.NewConnector(endpoint.String())
	if err != nil {
		t.Fatal(err)
	}
	client := gosql.OpenDB(connector)
	defer client.Close()
	var n int64
	if err := client.QueryRow("SELECT $1::bigint AS n", int64(17)).Scan(&n); err != nil || n != 17 {
		t.Fatalf("lib/pq mutual TLS %d %v", n, err)
	}
}
