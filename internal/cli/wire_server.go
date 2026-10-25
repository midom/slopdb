package cli

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"os/signal"
	"slopdb/internal/wire"
	"sync"
	"syscall"
)

type protocolServer interface {
	Serve(net.Listener) error
	Close() error
}

func runWireServer(server protocolServer, protocol, tcp, socket, databaseName, username string) error {
	return runWireServers([]wireEndpoint{{server, protocol, tcp, socket, databaseName, username}})
}

type wireEndpoint struct {
	server                                        protocolServer
	protocol, tcp, socket, databaseName, username string
}

func runWireServers(endpoints []wireEndpoint) error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	return serveWireServers(ctx, endpoints)
}

func serveWireServers(ctx context.Context, endpoints []wireEndpoint) error {
	if len(endpoints) == 0 {
		return fmt.Errorf("at least one SQL endpoint is required")
	}
	var err error
	var closeOnce sync.Once
	closeServers := func() {
		closeOnce.Do(func() {
			var wg sync.WaitGroup
			for _, endpoint := range endpoints {
				wg.Add(1)
				go func(s protocolServer) { defer wg.Done(); _ = s.Close() }(endpoint.server)
			}
			wg.Wait()
		})
	}
	defer closeServers()
	var listeners []net.Listener
	defer func() {
		for _, listener := range listeners {
			_ = listener.Close()
		}
	}()
	results := make(chan error, len(endpoints)*2)
	for _, endpoint := range endpoints {
		server, protocol, tcp, socket, databaseName, username := endpoint.server, endpoint.protocol, endpoint.tcp, endpoint.socket, endpoint.databaseName, endpoint.username
		var ownListeners []net.Listener
		if tcp != "" {
			listener, err := net.Listen("tcp", tcp)
			if err != nil {
				return err
			}
			listeners = append(listeners, listener)
			ownListeners = append(ownListeners, listener)
		}
		if socket != "" {
			listener, err := wire.Listen("unix", socket)
			if err != nil {
				return err
			}
			listeners = append(listeners, listener)
			ownListeners = append(ownListeners, listener)
			if err := os.Chmod(socket, 0600); err != nil {
				return err
			}
		}
		if len(ownListeners) == 0 {
			return fmt.Errorf("%s server requires a listener", protocol)
		}
		for _, listener := range ownListeners {
			fmt.Fprintf(os.Stderr, "%s listening on %s %s (database %s, user %s)\n", protocol, listener.Addr().Network(), listener.Addr(), databaseName, username)
			go func(listener net.Listener) { results <- server.Serve(listener) }(listener)
		}
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
	closeServers()
	for ; remaining > 0; remaining-- {
		<-results
	}
	if errors.Is(err, net.ErrClosed) && ctx.Err() != nil {
		return nil
	}
	return err
}
