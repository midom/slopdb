package mysqlserver

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	gosql "database/sql"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	driver "github.com/go-sql-driver/mysql"
	"slopdb/internal/testcert"
)

type tlsFixture struct {
	serverCA, clientCA                   *testcert.CA
	serverCertificate, clientCertificate tls.Certificate
}

func newTLSFixture(t *testing.T) *tlsFixture {
	t.Helper()
	serverCA := testcert.NewCA(t, "server CA")
	clientCA := testcert.NewCA(t, "client CA")
	now := time.Now()
	return &tlsFixture{serverCA, clientCA, serverCA.Issue(t, "server", x509.ExtKeyUsageServerAuth, now.Add(-time.Hour), now.Add(time.Hour)), clientCA.Issue(t, "client", x509.ExtKeyUsageClientAuth, now.Add(-time.Hour), now.Add(time.Hour))}
}
func (f *tlsFixture) serverConfig(mutual bool) *tls.Config {
	config := &tls.Config{Certificates: []tls.Certificate{f.serverCertificate}}
	if mutual {
		config.ClientCAs = f.clientCA.Pool()
		config.ClientAuth = tls.RequireAndVerifyClientCert
	}
	return config
}
func (f *tlsFixture) clientConfig(mutual bool) *tls.Config {
	config := &tls.Config{RootCAs: f.serverCA.Pool(), ServerName: "localhost", MinVersion: tls.VersionTLS12}
	if mutual {
		config.Certificates = []tls.Certificate{f.clientCertificate}
	}
	return config
}
func (s *testServer) openTLS(t *testing.T, config *tls.Config, password string) *gosql.DB {
	t.Helper()
	cfg := driver.NewConfig()
	cfg.User = "slopdb"
	cfg.Passwd = password
	cfg.DBName = "slopdb"
	cfg.Net = s.network
	cfg.Addr = s.address
	cfg.TLS = config
	cfg.Timeout = 3 * time.Second
	cfg.ReadTimeout = 3 * time.Second
	cfg.WriteTimeout = 3 * time.Second
	connector, err := driver.NewConnector(cfg)
	if err != nil {
		t.Fatal(err)
	}
	client := gosql.OpenDB(connector)
	t.Cleanup(func() { _ = client.Close() })
	return client
}
func TestTLSDriverTCPAndUnix(t *testing.T) {
	f := newTLSFixture(t)
	for _, network := range []string{"tcp", "unix"} {
		t.Run(network, func(t *testing.T) {
			for _, version := range []uint16{tls.VersionTLS12, tls.VersionTLS13} {
				t.Run(tls.VersionName(version), func(t *testing.T) {
					s := startTestServer(t, network, Config{TLSConfig: f.serverConfig(true), Password: "secret"})
					cfg := f.clientConfig(true)
					cfg.MinVersion = version
					cfg.MaxVersion = version
					client := s.openTLS(t, cfg, "secret")
					if err := client.Ping(); err != nil {
						t.Fatal(err)
					}
					mustExec(t, client, `CREATE TABLE secure(id INTEGER PRIMARY KEY, v TEXT)`)
					tx, err := client.Begin()
					if err != nil {
						t.Fatal(err)
					}
					mustExec(t, tx, `INSERT INTO secure VALUES(?,?)`, 1, "encrypted")
					if err := tx.Commit(); err != nil {
						t.Fatal(err)
					}
					var value string
					if err := client.QueryRow(`SELECT v FROM secure WHERE id=?`, 1).Scan(&value); err != nil || value != "encrypted" {
						t.Fatalf("TLS query: %q %v", value, err)
					}
				})
			}
		})
	}
}
func TestTLSClientCertificateRejections(t *testing.T) {
	f := newTLSFixture(t)
	now := time.Now()
	otherCA := testcert.NewCA(t, "untrusted CA")
	for _, network := range []string{"tcp", "unix"} {
		t.Run(network, func(t *testing.T) {
			s := startTestServer(t, network, Config{TLSConfig: f.serverConfig(true), Password: "secret"})
			for _, tc := range []struct {
				name        string
				certificate *tls.Certificate
			}{
				{"missing", nil},
				{"untrusted", ptrCert(otherCA.Issue(t, "untrusted", x509.ExtKeyUsageClientAuth, now.Add(-time.Hour), now.Add(time.Hour)))},
				{"server_CA_is_not_client_CA", ptrCert(f.serverCA.Issue(t, "wrong CA", x509.ExtKeyUsageClientAuth, now.Add(-time.Hour), now.Add(time.Hour)))},
				{"expired", ptrCert(f.clientCA.Issue(t, "expired", x509.ExtKeyUsageClientAuth, now.Add(-2*time.Hour), now.Add(-time.Hour)))},
				{"not_yet_valid", ptrCert(f.clientCA.Issue(t, "future", x509.ExtKeyUsageClientAuth, now.Add(time.Hour), now.Add(2*time.Hour)))},
				{"wrong_usage", ptrCert(f.clientCA.Issue(t, "server only", x509.ExtKeyUsageServerAuth, now.Add(-time.Hour), now.Add(time.Hour)))},
			} {
				t.Run(tc.name, func(t *testing.T) {
					cfg := f.clientConfig(false)
					if tc.certificate != nil {
						cert := *tc.certificate
						cfg.GetClientCertificate = func(*tls.CertificateRequestInfo) (*tls.Certificate, error) { return &cert, nil }
					}
					client := s.openTLS(t, cfg, "secret")
					if err := client.Ping(); err == nil {
						t.Fatal("invalid client certificate authenticated")
					}
				})
			}
			if err := s.open(t, "slopdb", "secret", "slopdb").Ping(); err == nil {
				t.Fatal("plaintext bypassed client certificate authentication")
			} else {
				var protocol *driver.MySQLError
				if !errors.As(err, &protocol) || protocol.Number != 3159 {
					t.Fatalf("plaintext error: %v", err)
				}
			}
			if err := s.openTLS(t, f.clientConfig(true), "wrong").Ping(); err == nil {
				t.Fatal("certificate bypassed password authentication")
			}
			badHost := f.clientConfig(true)
			badHost.ServerName = "wrong.invalid"
			if err := s.openTLS(t, badHost, "secret").Ping(); err == nil {
				t.Fatal("server hostname mismatch accepted")
			}
			badTrust := f.clientConfig(true)
			badTrust.RootCAs = otherCA.Pool()
			if err := s.openTLS(t, badTrust, "secret").Ping(); err == nil {
				t.Fatal("untrusted server certificate accepted")
			}
			if err := s.openTLS(t, f.clientConfig(true), "secret").Ping(); err != nil {
				t.Fatalf("failed clients damaged server: %v", err)
			}
		})
	}
}
func ptrCert(cert tls.Certificate) *tls.Certificate { return &cert }
func TestTLSOptionalAndRequired(t *testing.T) {
	f := newTLSFixture(t)
	for _, required := range []bool{false, true} {
		name := "optional"
		if required {
			name = "required"
		}
		t.Run(name, func(t *testing.T) {
			s := startTestServer(t, "tcp", Config{TLSConfig: f.serverConfig(false), RequireTLS: required})
			if err := s.openTLS(t, f.clientConfig(false), "").Ping(); err != nil {
				t.Fatal(err)
			}
			err := s.open(t, "slopdb", "", "slopdb").Ping()
			if required && err == nil {
				t.Fatal("required TLS allowed plaintext")
			}
			if !required && err != nil {
				t.Fatal(err)
			}
			old := f.clientConfig(false)
			old.MinVersion = tls.VersionTLS11
			old.MaxVersion = tls.VersionTLS11
			if err := s.openTLS(t, old, "").Ping(); err == nil {
				t.Fatal("TLS below 1.2 accepted")
			}
		})
	}
	// The private TLS config and CA pool cannot be weakened by caller mutation.
	config := f.serverConfig(true)
	s := startTestServer(t, "tcp", Config{TLSConfig: config})
	other := testcert.NewCA(t, "new untrusted CA")
	config.ClientCAs.AddCert(other.Certificate)
	config.ClientAuth = tls.NoClientCert
	client := f.clientConfig(false)
	cert := other.Issue(t, "other", x509.ExtKeyUsageClientAuth, time.Now().Add(-time.Hour), time.Now().Add(time.Hour))
	client.GetClientCertificate = func(*tls.CertificateRequestInfo) (*tls.Certificate, error) { return &cert, nil }
	if err := s.openTLS(t, client, "").Ping(); err == nil {
		t.Fatal("caller mutation changed server CA policy")
	}
}
func TestTLSConfigValidation(t *testing.T) {
	f := newTLSFixture(t)
	s := startTestServer(t, "tcp", Config{})
	for _, config := range []Config{
		{RequireTLS: true}, {TLSConfig: &tls.Config{}},
		{TLSConfig: &tls.Config{Certificates: []tls.Certificate{f.serverCertificate}, MaxVersion: tls.VersionTLS11}},
		{TLSConfig: &tls.Config{Certificates: []tls.Certificate{f.serverCertificate}, ClientAuth: tls.RequireAndVerifyClientCert}},
		{TLSConfig: &tls.Config{Certificates: []tls.Certificate{f.serverCertificate}, ClientAuth: tls.RequireAnyClientCert, ClientCAs: f.clientCA.Pool()}},
		{TLSConfig: &tls.Config{Certificates: []tls.Certificate{f.serverCertificate}, GetConfigForClient: func(*tls.ClientHelloInfo) (*tls.Config, error) { return f.serverConfig(false), nil }}},
	} {
		if server, err := New(s.database, config); err == nil {
			_ = server.Close()
			t.Fatal("invalid TLS configuration accepted")
		}
	}
}

// Send SSLRequest separately to exercise cancellation before the TLS handshake
// and validation of the subsequent encrypted MySQL response.
func startTLSRequest(t *testing.T, s *testServer) net.Conn {
	t.Helper()
	conn, err := net.Dial(s.network, s.address)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	_ = conn.SetDeadline(time.Now().Add(3 * time.Second))
	greeting, _, err := readPacket(conn, 0, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	d := decoder{data: greeting}
	d.u8()
	d.nul()
	d.u32()
	d.take(8)
	d.u8()
	if d.u16()&uint16(clientSSL) == 0 {
		t.Fatal("TLS capability not advertised")
	}
	p := append32(nil, clientProtocol41|clientSSL|clientSecureConnection)
	p = append32(p, 1<<20)
	p = append(p, 45)
	p = append(p, make([]byte, 23)...)
	writer := packetWriter{conn: conn, sequence: 1}
	if err := writer.write(p); err != nil {
		t.Fatal(err)
	}
	return conn
}
func TestTLSHandshakeCancellationAndMalformedResponse(t *testing.T) {
	f := newTLSFixture(t)
	t.Run("shutdown", func(t *testing.T) {
		s := startTestServer(t, "tcp", Config{TLSConfig: f.serverConfig(false)})
		conn := startTLSRequest(t, s)
		done := make(chan struct{})
		go func() { _ = s.server.Close(); close(done) }()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatal("shutdown blocked on TLS handshake")
		}
		var b [1]byte
		if _, err := conn.Read(b[:]); err == nil {
			t.Fatal("TLS handshake remained open")
		}
	})
	t.Run("timeout", func(t *testing.T) {
		s := startTestServer(t, "tcp", Config{TLSConfig: f.serverConfig(false), HandshakeTimeout: 100 * time.Millisecond})
		conn := startTLSRequest(t, s)
		var b [1]byte
		if _, err := conn.Read(b[:]); !errors.Is(err, io.EOF) {
			t.Fatalf("TLS handshake timeout: %v", err)
		}
	})
	for _, missingSSL := range []bool{false, true} {
		t.Run("encrypted_response", func(t *testing.T) {
			s := startTestServer(t, "tcp", Config{TLSConfig: f.serverConfig(false)})
			conn := startTLSRequest(t, s)
			secure := tls.Client(conn, f.clientConfig(false))
			if err := secure.HandshakeContext(context.Background()); err != nil {
				t.Fatal(err)
			}
			flags := clientProtocol41 | clientSSL | clientSecureConnection
			if missingSSL {
				flags &^= clientSSL
			}
			p := append32(nil, flags)
			p = append32(p, 1<<20)
			p = append(p, 45)
			p = append(p, make([]byte, 23)...)
			if missingSSL {
				p = append(p, []byte("slopdb\x00\x00")...)
			} // Otherwise repeat SSLRequest without a username.
			w := packetWriter{conn: secure, sequence: 2}
			if err := w.write(p); err != nil {
				t.Fatal(err)
			}
			reply, _, err := readPacket(secure, 3, 1<<20)
			if err != nil {
				t.Fatal(err)
			}
			expectCode(t, reply, 1043)
		})
	}
}

func TestTLSMalformedSSLRequest(t *testing.T) {
	f := newTLSFixture(t)
	s := startTestServer(t, "tcp", Config{TLSConfig: f.serverConfig(false)})
	conn, err := net.Dial(s.network, s.address)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(3 * time.Second))
	if _, _, err := readPacket(conn, 0, 1<<20); err != nil {
		t.Fatal(err)
	}
	p := append32(nil, clientProtocol41|clientSSL)
	p = append32(p, 1<<20)
	p = append(p, 45)
	p = append(p, make([]byte, 24)...)
	w := packetWriter{conn: conn, sequence: 1}
	if err := w.write(p); err != nil {
		t.Fatal(err)
	}
	reply, _, err := readPacket(conn, 2, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	expectCode(t, reply, 1043)
}
