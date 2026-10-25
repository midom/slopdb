// Package mysqlserver exposes a SlopDB database over the MySQL 4.1 wire
// protocol. Serve accepts any net.Listener, including TCP and Unix sockets.
package mysqlserver

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"slopdb/db"
	"slopdb/internal/wire"
)

// Config describes a single database and login. Empty Password permits a
// passwordless login for Username. TLS and client certificate checks supplement
// the configured username/password authentication.
type Config struct {
	Username string
	Password string
	Database string
	// TLSConfig enables MySQL's SSLRequest upgrade on both TCP and Unix sockets.
	// The server clones the configuration and ClientCAs pool and enforces TLS 1.2
	// or newer. Certificate contents and callbacks must not be mutated after New.
	// GetConfigForClient is unsupported; GetCertificate may select certificates.
	TLSConfig *tls.Config
	// RequireTLS rejects plaintext logins. RequireAndVerifyClientCert in TLSConfig
	// also implies RequireTLS, and requires an explicit ClientCAs trust pool.
	RequireTLS bool
	// MaxPacketBytes bounds incoming logical packets and long parameter data.
	// Zero uses 64 MiB. MaxPreparedStatements defaults to 1024 per connection.
	MaxPacketBytes        int
	MaxPreparedStatements int
	// HandshakeTimeout defaults to 10 seconds.
	HandshakeTimeout time.Duration
}

// Server owns listeners and client sessions, but never closes the Database.
// Multiple Serve calls can expose the same database on TCP and Unix sockets.
type Server struct {
	database   *db.Database
	config     Config
	mu         sync.Mutex
	closed     bool
	listeners  map[net.Listener]struct{}
	clients    map[net.Conn]context.CancelFunc
	wg         sync.WaitGroup
	done       chan struct{}
	nextID     atomic.Uint32
	namedLocks namedLocks
	accounts   serverAccounts
	closeError error
}

func New(database *db.Database, config Config) (*Server, error) {
	if database == nil {
		return nil, errors.New("nil database")
	}
	if config.Username == "" {
		config.Username = "slopdb"
	}
	if config.Database == "" {
		config.Database = "slopdb"
	}
	if strings.ContainsRune(config.Username, 0) || strings.ContainsRune(config.Database, 0) {
		return nil, errors.New("login and database names cannot contain NUL")
	}
	if config.MaxPacketBytes == 0 {
		config.MaxPacketBytes = 64 << 20
	}
	if config.MaxPreparedStatements == 0 {
		config.MaxPreparedStatements = 1024
	}
	if config.HandshakeTimeout == 0 {
		config.HandshakeTimeout = 10 * time.Second
	}
	if config.MaxPacketBytes < 1024 || config.MaxPreparedStatements < 1 || config.HandshakeTimeout < 0 {
		return nil, errors.New("invalid MySQL server limits")
	}
	var err error
	config.TLSConfig, config.RequireTLS, err = wire.TLSConfig(config.TLSConfig, config.RequireTLS)
	if err != nil {
		return nil, fmt.Errorf("MySQL TLS: %w", err)
	}
	server := &Server{database: database, config: config, listeners: make(map[net.Listener]struct{}), clients: make(map[net.Conn]context.CancelFunc), done: make(chan struct{})}
	if err := server.initAccounts(); err != nil {
		return nil, err
	}
	return server, nil
}

// Serve takes ownership of listener, closing it when Serve returns or Close is
// called. It returns nil on server shutdown. A failed listener does not stop
// other listeners; callers should close the server if that is desired.
func (s *Server) Serve(listener net.Listener) error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		_ = listener.Close()
		return net.ErrClosed
	}
	s.listeners[listener] = struct{}{}
	s.mu.Unlock()
	defer func() { _ = listener.Close(); s.mu.Lock(); delete(s.listeners, listener); s.mu.Unlock() }()
	for {
		conn, err := listener.Accept()
		if err != nil {
			s.mu.Lock()
			closed := s.closed
			s.mu.Unlock()
			if closed {
				return nil
			}
			return fmt.Errorf("MySQL accept: %w", err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		s.mu.Lock()
		if s.closed {
			s.mu.Unlock()
			cancel()
			_ = conn.Close()
			return nil
		}
		s.clients[conn] = cancel
		s.wg.Add(1)
		s.mu.Unlock()
		go func() {
			defer s.wg.Done()
			defer func() { cancel(); _ = conn.Close(); s.mu.Lock(); delete(s.clients, conn); s.mu.Unlock() }()
			c := &client{server: s, conn: conn, writer: packetWriter{conn: conn}, ctx: ctx, cancel: cancel, id: s.nextID.Add(1), autocommit: true, statements: make(map[uint32]*prepared)}
			c.run()
		}()
	}
}

// Close stops accepting, cancels running SQL, disconnects clients and waits
// for every session to roll back. Concurrent and repeated calls are safe.
func (s *Server) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		<-s.done
		return s.closeError
	}
	s.closed = true
	for listener := range s.listeners {
		_ = listener.Close()
	}
	for conn, cancel := range s.clients {
		cancel()
		_ = conn.Close()
	}
	s.mu.Unlock()
	s.wg.Wait()
	for name, database := range s.accounts.databases {
		if name != s.config.Database {
			s.closeError = errors.Join(s.closeError, database.Close())
		}
	}
	close(s.done)
	return s.closeError
}
