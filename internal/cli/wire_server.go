package cli

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/signal"
	"slopdb/internal/wire"
	"syscall"
)

type protocolServer interface {
	Serve(net.Listener) error
	Close() error
}

func runWireServer(server protocolServer, protocol, tcp, socket, databaseName, username string) error {
	var err error
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
		listener, err := wire.Listen("unix", socket)
		if err != nil {
			return err
		}
		listeners = append(listeners, listener)
		if err := os.Chmod(socket, 0600); err != nil {
			return err
		}
	}
	if len(listeners) == 0 {
		return fmt.Errorf("%s server requires a listener", protocol)
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	results := make(chan error, len(listeners))
	for _, listener := range listeners {
		fmt.Fprintf(os.Stderr, "%s listening on %s %s (database %s, user %s)\n", protocol, listener.Addr().Network(), listener.Addr(), databaseName, username)
		go func(listener net.Listener) { results <- server.Serve(listener) }(listener)
	}
	var clusterDone <-chan struct{}
	if currentNode != nil {
		clusterDone = currentNode.Done()
	}
	remaining := len(listeners)
	select {
	case <-clusterDone:
		err = currentNode.Err()
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
