// Package postgresserver exposes a SlopDB database over the PostgreSQL 3.0 wire
// protocol. Serve accepts any net.Listener, including TCP and Unix sockets.
package postgresserver

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/xdg-go/scram"
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
	// TLSConfig enables PostgreSQL's SSLRequest upgrade on both TCP and Unix sockets.
	// The server clones the configuration and ClientCAs pool and enforces TLS 1.2
	// or newer. Certificate contents and callbacks must not be mutated after New.
	// GetConfigForClient is unsupported; GetCertificate may select certificates.
	TLSConfig *tls.Config
	// RequireTLS rejects plaintext logins. RequireAndVerifyClientCert in TLSConfig
	// also implies RequireTLS, and requires an explicit ClientCAs trust pool.
	RequireTLS bool
	// MaxPacketBytes bounds startup packets and frontend message bodies.
	// Zero uses 64 MiB. Prepared statements and portals default to 1024 per connection.
	MaxPacketBytes        int
	MaxPreparedStatements int
	MaxPortals            int
	// HandshakeTimeout defaults to 10 seconds.
	HandshakeTimeout time.Duration
}

// Server owns listeners and client sessions, but never closes the Database.
// Multiple Serve calls can expose the same database on TCP and Unix sockets.
type Server struct {
	database  *db.Database
	config    Config
	mu        sync.Mutex
	closed    bool
	listeners map[net.Listener]struct{}
	clients   map[net.Conn]context.CancelFunc
	wg        sync.WaitGroup
	done      chan struct{}
	scram     *scram.Server
	backends  map[uint32]*client
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
	if config.MaxPortals == 0 {
		config.MaxPortals = 1024
	}
	if config.MaxPreparedStatements == 0 {
		config.MaxPreparedStatements = 1024
	}
	if config.HandshakeTimeout == 0 {
		config.HandshakeTimeout = 10 * time.Second
	}
	if config.MaxPacketBytes < 1024 || config.MaxPreparedStatements < 1 || config.MaxPortals < 1 || config.HandshakeTimeout < 0 {
		return nil, errors.New("invalid PostgreSQL server limits")
	}
	var err error
	config.TLSConfig, config.RequireTLS, err = wire.TLSConfig(config.TLSConfig, config.RequireTLS)
	if err != nil {
		return nil, fmt.Errorf("PostgreSQL TLS: %w", err)
	}
	var auth *scram.Server
	if config.Password != "" {
		var salt [16]byte
		if _, err := rand.Read(salt[:]); err != nil {
			return nil, err
		}
		credentialClient, err := scram.SHA256.NewClient("", config.Password, "")
		// PostgreSQL falls back to the original bytes when SASLprep is inapplicable.
		if err != nil {
			credentialClient, err = scram.SHA256.NewClientUnprepped("", config.Password, "")
		}
		if err != nil {
			return nil, errors.New("could not configure SCRAM authentication")
		}
		credentials := credentialClient.GetStoredCredentials(scram.KeyFactors{Salt: string(salt[:]), Iters: 4096})
		auth, err = scram.SHA256.NewServer(func(string) (scram.StoredCredentials, error) { return credentials, nil })
		if err != nil {
			return nil, err
		}
	}
	config.Password = "" // Retain the verifier, not the configured plaintext password.
	return &Server{database: database, config: config, scram: auth, backends: make(map[uint32]*client), listeners: make(map[net.Listener]struct{}), clients: make(map[net.Conn]context.CancelFunc), done: make(chan struct{})}, nil
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
			return fmt.Errorf("PostgreSQL accept: %w", err)
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
			backendID, err := s.database.ReserveBackendID()
			if err != nil {
				return
			}
			c := &client{startedAt: time.Now(), server: s, conn: conn, ctx: ctx, cancel: cancel, id: backendID, statements: make(map[string]*prepared), portals: make(map[string]*portal)}
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
		return nil
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
	close(s.done)
	return nil
}

// CancelRequest uses the backend key, not a database session. As in PostgreSQL,
// only the currently active operation is cancelled; the connection stays reusable.
func (s *Server) cancelRequest(id, secret uint32) {
	s.mu.Lock()
	c := s.backends[id]
	s.mu.Unlock()
	if c == nil || subtle.ConstantTimeEq(int32(secret), int32(c.secret)) != 1 {
		return
	}
	c.activeMu.Lock()
	if c.activeCancel != nil {
		c.activeCancel()
	}
	c.activeMu.Unlock()
}
