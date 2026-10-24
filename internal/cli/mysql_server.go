package cli

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net"
	"os"
	"os/signal"
	"syscall"

	"slopdb/db"
	"slopdb/mysqlserver"
)

func loadMySQLTLS(certFile, keyFile, caFile string, requireTLS bool) (*tls.Config, error) {
	if certFile == "" && keyFile == "" {
		if caFile != "" || requireTLS {
			return nil, fmt.Errorf("a server certificate and key are required")
		}
		return nil, nil
	}
	if certFile == "" || keyFile == "" {
		return nil, fmt.Errorf("--mysql-tls-cert and --mysql-tls-key must be supplied together")
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
	defer server.Close()
	var listeners []net.Listener
	defer func() {
		for _, listener := range listeners {
			_ = listener.Close()
		}
	}()
	if tcp != "" {
		listener, err := net.Listen("tcp", tcp)
		if err != nil {
			return err
		}
		listeners = append(listeners, listener)
	}
	if socket != "" {
		listener, err := net.Listen("unix", socket)
		if err != nil {
			return err
		}
		listeners = append(listeners, listener)
		if err := os.Chmod(socket, 0600); err != nil {
			return err
		}
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	results := make(chan error, len(listeners))
	for _, listener := range listeners {
		fmt.Fprintf(os.Stderr, "MySQL listening on %s %s (database %s, user %s)\n", listener.Addr().Network(), listener.Addr(), config.Database, config.Username)
		go func(listener net.Listener) { results <- server.Serve(listener) }(listener)
	}
	remaining := len(listeners)
	select {
	case <-ctx.Done():
	case err = <-results:
		remaining--
	}
	_ = server.Close()
	for ; remaining > 0; remaining-- {
		<-results
	}
	return err
}
