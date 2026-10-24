package cli

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"

	"slopdb/db"
	"slopdb/mysqlserver"
)

func loadMySQLTLS(certFile, keyFile, caFile string, requireTLS bool) (*tls.Config, error) {
	return loadServerTLS("mysql", certFile, keyFile, caFile, requireTLS)
}
func loadServerTLS(protocol, certFile, keyFile, caFile string, requireTLS bool) (*tls.Config, error) {
	if certFile == "" && keyFile == "" {
		if caFile != "" || requireTLS {
			return nil, fmt.Errorf("a server certificate and key are required")
		}
		return nil, nil
	}
	if certFile == "" || keyFile == "" {
		return nil, fmt.Errorf("--%s-tls-cert and --%s-tls-key must be supplied together", protocol, protocol)
	}
	certificate, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, fmt.Errorf("load server certificate/key: %w", err)
	}
	config := &tls.Config{Certificates: []tls.Certificate{certificate}, MinVersion: tls.VersionTLS12}
	if caFile != "" {
		pem, err := os.ReadFile(caFile)
		if err != nil {
			return nil, fmt.Errorf("load client CA: %w", err)
		}
		roots := x509.NewCertPool()
		if !roots.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("client CA file contains no valid PEM certificates")
		}
		config.ClientCAs = roots
		config.ClientAuth = tls.RequireAndVerifyClientCert
	}
	return config, nil
}

func runMySQLServer(database *db.Database, tcp, socket string, config mysqlserver.Config) error {
	server, err := mysqlserver.New(database, config)
	if err != nil {
		return err
	}
	return runWireServer(server, "MySQL", tcp, socket, config.Database, config.Username)
}
