package replication

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"math/big"
	"net"
	"net/http"
	"slopdb/db"
	"testing"
	"time"
)

func nodeTLS(t *testing.T) (*tls.Config, *tls.Config) {
	t.Helper()
	_, key, e := ed25519.GenerateKey(rand.Reader)
	if e != nil {
		t.Fatal(e)
	}
	now := time.Now()
	ca := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "test cluster CA"}, NotBefore: now.Add(-time.Hour), NotAfter: now.Add(time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageCRLSign}
	caDER, e := x509.CreateCertificate(rand.Reader, ca, ca, key.Public(), key)
	if e != nil {
		t.Fatal(e)
	}
	ca, e = x509.ParseCertificate(caDER)
	if e != nil {
		t.Fatal(e)
	}
	node := &x509.Certificate{SerialNumber: big.NewInt(2), Subject: pkix.Name{CommonName: "node"}, DNSNames: []string{"localhost"}, IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}, NotBefore: now.Add(-time.Hour), NotAfter: now.Add(time.Hour), KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth}}
	nodeDER, e := x509.CreateCertificate(rand.Reader, node, ca, key.Public(), key)
	if e != nil {
		t.Fatal(e)
	}
	certificate := tls.Certificate{Certificate: [][]byte{nodeDER, caDER}, PrivateKey: key}
	roots := x509.NewCertPool()
	roots.AddCert(ca)
	return &tls.Config{Certificates: []tls.Certificate{certificate}, ClientCAs: roots, ClientAuth: tls.RequireAndVerifyClientCert, MinVersion: tls.VersionTLS12}, &tls.Config{Certificates: []tls.Certificate{certificate}, RootCAs: roots, MinVersion: tls.VersionTLS12}
}
func TestMutualTLSReplicationTCPAndUnix(t *testing.T) {
	for _, network := range []string{"tcp", "unix"} {
		t.Run(network, func(t *testing.T) {
			server, client := nodeTLS(t)
			c := testConfig(t, "tls-source", network)
			c.Bootstrap = true
			c.TLSConfig = server
			c.ClientTLSConfig = client
			source := openTestNode(t, c)
			ready(t, source)
			exec(t, source, "CREATE TABLE items(id INTEGER PRIMARY KEY,v TEXT); INSERT INTO items VALUES(1,'tls')")
			noCert := client.Clone()
			noCert.Certificates = nil
			attacker := &Node{config: Config{Token: testToken, ClientTLSConfig: noCert}}
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			if response, e := attacker.request(ctx, source.ControlAddress(), http.MethodGet, "/v1/status", nil); e == nil {
				response.Body.Close()
				t.Fatal("client without cluster certificate authenticated")
			}
			_, foreignClient := nodeTLS(t)
			foreign := &Node{config: Config{Token: testToken, ClientTLSConfig: foreignClient}}
			if response, e := foreign.request(ctx, source.ControlAddress(), http.MethodGet, "/v1/snapshot", nil); e == nil {
				response.Body.Close()
				t.Fatal("foreign CA authenticated")
			}
			targetConfig := testConfig(t, "tls-target", network)
			targetConfig.Join = source.ControlAddress()
			targetConfig.TLSConfig = server
			targetConfig.ClientTLSConfig = client
			targetConfig.ReplicaReadMaxStaleness = 2 * time.Second
			target := openTestNode(t, targetConfig)
			waitApplied(t, target, source.applied.Load())
			assertCount(t, target, 1)
			if _, e := target.Database().ExecWithOptions("SELECT * FROM items", db.ExecOptions{ReadBarrier: db.ReadBarrierNext}); e != nil {
				t.Fatalf("durable read over mutual TLS: %v", e)
			}
			if e := source.Promote(ctx, "tls-target"); e != nil {
				t.Fatal(e)
			}
			if e := source.TransferLeadership(ctx, "tls-target"); e != nil {
				t.Fatal(e)
			}
			ready(t, target)
			assertCount(t, target, 1)
		})
	}
}
