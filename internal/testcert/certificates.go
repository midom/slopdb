// Package testcert creates temporary X.509 fixtures for protocol and CLI tests.
package testcert

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"
)

type CA struct {
	Certificate *x509.Certificate
	key         *ecdsa.PrivateKey
}

func serial(t testing.TB) *big.Int {
	t.Helper()
	n, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		t.Fatal(err)
	}
	return n.Add(n, big.NewInt(1))
}
func privateKey(t testing.TB) *ecdsa.PrivateKey {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	return key
}
func NewCA(t testing.TB, name string) *CA {
	t.Helper()
	key := privateKey(t)
	now := time.Now()
	template := &x509.Certificate{SerialNumber: serial(t), Subject: pkix.Name{CommonName: name}, NotBefore: now.Add(-time.Hour), NotAfter: now.Add(48 * time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageCRLSign}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	return &CA{cert, key}
}
func (ca *CA) Issue(t testing.TB, name string, usage x509.ExtKeyUsage, notBefore, notAfter time.Time) tls.Certificate {
	t.Helper()
	key := privateKey(t)
	template := &x509.Certificate{SerialNumber: serial(t), Subject: pkix.Name{CommonName: name}, NotBefore: notBefore, NotAfter: notAfter, KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{usage}, BasicConstraintsValid: true}
	if usage == x509.ExtKeyUsageServerAuth {
		template.DNSNames = []string{"localhost"}
		template.IPAddresses = []net.IP{net.ParseIP("127.0.0.1"), net.ParseIP("::1")}
	}
	der, err := x509.CreateCertificate(rand.Reader, template, ca.Certificate, &key.PublicKey, ca.key)
	if err != nil {
		t.Fatal(err)
	}
	leaf, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	return tls.Certificate{Certificate: [][]byte{der, ca.Certificate.Raw}, PrivateKey: key, Leaf: leaf}
}
func (ca *CA) Pool() *x509.CertPool {
	pool := x509.NewCertPool()
	pool.AddCert(ca.Certificate)
	return pool
}
func (ca *CA) Write(t testing.TB, dir, name string) string {
	t.Helper()
	path := filepath.Join(dir, name)
	data := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: ca.Certificate.Raw})
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	return path
}
func WriteKeyPair(t testing.TB, dir, name string, cert tls.Certificate) (string, string) {
	t.Helper()
	var certificates []byte
	for _, der := range cert.Certificate {
		certificates = append(certificates, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})...)
	}
	key, err := x509.MarshalPKCS8PrivateKey(cert.PrivateKey)
	if err != nil {
		t.Fatal(err)
	}
	certFile, keyFile := filepath.Join(dir, name+".pem"), filepath.Join(dir, name+".key")
	if err := os.WriteFile(certFile, certificates, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(keyFile, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: key}), 0600); err != nil {
		t.Fatal(err)
	}
	return certFile, keyFile
}
