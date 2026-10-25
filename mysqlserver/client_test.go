package mysqlserver

import (
	"bytes"
	"context"
	"net"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"

	"slopdb/internal/testcert"
)

// Optional client interoperability, in addition to the always-on driver tests.
func TestMySQLCLI(t *testing.T) {
	binary, err := exec.LookPath("mysql")
	if err != nil {
		t.Skip("mysql client is not installed")
	}
	for _, network := range []string{"tcp", "unix"} {
		for _, compressed := range []bool{false, true} {
			name := network
			if compressed {
				name += "_compressed"
			}
			t.Run(name, func(t *testing.T) {
				s := startTestServer(t, network, Config{})
				args := []string{"--no-defaults", "--batch", "--skip-column-names", "--user=slopdb", "--skip-password", "--database=slopdb"}
				if compressed {
					args = append(args, "--compress")
				}
				if network == "tcp" {
					host, port, err := net.SplitHostPort(s.address)
					if err != nil {
						t.Fatal(err)
					}
					args = append(args, "--protocol=TCP", "--host="+host, "--port="+port)
				} else {
					args = append(args, "--protocol=SOCKET", "--socket="+s.address)
				}
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				cmd := exec.CommandContext(ctx, binary, args...)
				large := strings.Repeat("compressible text ", 1024)
				cmd.Stdin = strings.NewReader("CREATE TABLE native_client(id INTEGER PRIMARY KEY);\nINSERT INTO native_client VALUES(42);\nSELECT id FROM native_client;\nSELECT '" + large + "';\nSELECT 43;\n")
				var stderr bytes.Buffer
				cmd.Stderr = &stderr
				output, err := cmd.Output()
				if err != nil || string(output) != "42\n"+large+"\n43\n" {
					t.Fatalf("mysql client: %v %s %s", err, output, stderr.String())
				}
			})
		}
	}
}

func TestMySQLCLIMutualTLS(t *testing.T) {
	binary, err := exec.LookPath("mysql")
	if err != nil {
		t.Skip("mysql client is not installed")
	}
	version, err := exec.Command(binary, "--no-defaults", "--version").Output()
	_, number, found := strings.Cut(string(version), "Ver ")
	majorText, _, _ := strings.Cut(number, ".")
	major, parseErr := strconv.Atoi(majorText)
	if err != nil || !found || parseErr != nil || major < 8 || strings.Contains(string(version), "MariaDB") {
		t.Skip("requires MySQL 8 or newer for TLS 1.3 and --ssl-mode support")
	}
	f := newTLSFixture(t)
	s := startTestServer(t, "tcp", Config{TLSConfig: f.serverConfig(true)})
	host, port, err := net.SplitHostPort(s.address)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	caFile := f.serverCA.Write(t, dir, "server-ca.pem")
	certFile, keyFile := testcert.WriteKeyPair(t, dir, "client", f.clientCertificate)
	for _, version := range []string{"TLSv1.2", "TLSv1.3"} {
		t.Run(version, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, binary, "--no-defaults", "--compress", "--batch", "--skip-column-names", "--user=slopdb", "--skip-password", "--database=slopdb", "--protocol=TCP", "--host="+host, "--port="+port, "--ssl-mode=VERIFY_IDENTITY", "--ssl-ca="+caFile, "--ssl-cert="+certFile, "--ssl-key="+keyFile, "--tls-version="+version)
			cmd.Stdin = strings.NewReader("SELECT 42;\n")
			var stderr bytes.Buffer
			cmd.Stderr = &stderr
			output, err := cmd.Output()
			if err != nil || strings.TrimSpace(string(output)) != "42" {
				t.Fatalf("mysql mutual TLS: %v %s %s", err, output, stderr.String())
			}
		})
	}
}
