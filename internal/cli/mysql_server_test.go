package cli

import (
	"crypto/tls"
	"crypto/x509"
	"os"
	"path/filepath"
	"testing"
	"time"

	"slopdb/internal/testcert"
)

func TestLoadMySQLTLS(t *testing.T) {
	ca := testcert.NewCA(t, "client CA")
	now := time.Now()
	server := ca.Issue(t, "server", x509.ExtKeyUsageServerAuth, now.Add(-time.Hour), now.Add(time.Hour))
	dir := t.TempDir()
	cert, key := testcert.WriteKeyPair(t, dir, "server", server)
	caFile := ca.Write(t, dir, "ca.pem")
	invalid := filepath.Join(dir, "invalid.pem")
	if err := os.WriteFile(invalid, []byte("not a certificate"), 0600); err != nil {
		t.Fatal(err)
	}
	missing := filepath.Join(dir, "missing.pem")
	for _, tc := range []struct {
		name, cert, key, ca string
		required            bool
		wantError           bool
	}{
		{name: "disabled"}, {name: "server_TLS", cert: cert, key: key}, {name: "required_TLS", cert: cert, key: key, required: true}, {name: "mutual_TLS", cert: cert, key: key, ca: caFile},
		{name: "require_without_certificate", required: true, wantError: true}, {name: "CA_without_certificate", ca: caFile, wantError: true},
		{name: "missing_key_flag", cert: cert, wantError: true}, {name: "missing_cert_flag", key: key, wantError: true},
		{name: "missing_certificate", cert: missing, key: key, wantError: true}, {name: "invalid_key", cert: cert, key: invalid, wantError: true},
		{name: "mismatched_key", cert: caFile, key: key, wantError: true}, {name: "missing_CA", cert: cert, key: key, ca: missing, wantError: true}, {name: "invalid_CA", cert: cert, key: key, ca: invalid, wantError: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg, err := loadMySQLTLS(tc.cert, tc.key, tc.ca, tc.required)
			if tc.wantError {
				if err == nil {
					t.Fatal("invalid TLS configuration accepted")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if tc.cert == "" {
				if cfg != nil {
					t.Fatal("TLS enabled without configuration")
				}
				return
			}
			if len(cfg.Certificates) != 1 || cfg.MinVersion != tls.VersionTLS12 {
				t.Fatalf("TLS configuration: %+v", cfg)
			}
			if tc.ca != "" {
				if cfg.ClientAuth != tls.RequireAndVerifyClientCert || cfg.ClientCAs == nil || len(cfg.ClientCAs.Subjects()) != 1 {
					t.Fatal("client CA did not require verified certificates")
				}
			} else if cfg.ClientAuth != tls.NoClientCert {
				t.Fatal("client certificates required without CA")
			}
		})
	}
}
