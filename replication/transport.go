package replication

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/hashicorp/raft"
	"slopdb/internal/wire"
)

type endpoint struct{ network, address string }

func parseEndpoint(s string) (endpoint, error) {
	u, e := url.Parse(s)
	if e != nil {
		return endpoint{}, e
	}
	if u.RawQuery != "" || u.Fragment != "" {
		return endpoint{}, fmt.Errorf("endpoint may not contain a query or fragment")
	}
	switch u.Scheme {
	case "tcp":
		if u.Host == "" || u.Path != "" || u.User != nil {
			return endpoint{}, fmt.Errorf("invalid TCP endpoint")
		}
		if _, _, e = net.SplitHostPort(u.Host); e != nil {
			return endpoint{}, e
		}
		return endpoint{"tcp", u.Host}, nil
	case "unix":
		if u.Host != "" || !strings.HasPrefix(u.Path, "/") {
			return endpoint{}, fmt.Errorf("Unix endpoint requires an absolute path")
		}
		return endpoint{"unix", u.Path}, nil
	}
	return endpoint{}, fmt.Errorf("endpoint must use tcp://host:port or unix:///absolute/path")
}
func (e endpoint) String() string {
	if e.network == "unix" {
		return "unix://" + e.address
	}
	return "tcp://" + e.address
}
func (e endpoint) Network() string { return e.network }
func listenEndpoint(address, advertise string) (net.Listener, endpoint, error) {
	e, err := parseEndpoint(address)
	if err != nil {
		return nil, e, err
	}
	l, err := wire.Listen(e.network, e.address)
	if err != nil {
		return nil, e, err
	}
	fail := func(err error) (net.Listener, endpoint, error) { l.Close(); return nil, e, err }
	if e.network == "unix" {
		if err = os.Chmod(e.address, 0600); err != nil {
			return fail(err)
		}
	} else {
		e.address = l.Addr().String()
		host, _, _ := net.SplitHostPort(e.address)
		if advertise == "" && (net.ParseIP(host) == nil || net.ParseIP(host).IsUnspecified()) {
			return fail(fmt.Errorf("wildcard listener requires an advertised endpoint"))
		}
	}
	if advertise != "" {
		e, err = parseEndpoint(advertise)
		if err != nil {
			return fail(err)
		}
	}
	return l, e, nil
}
func dialEndpoint(ctx context.Context, e endpoint, config *tls.Config) (net.Conn, error) {
	c, err := (&net.Dialer{}).DialContext(ctx, e.network, e.address)
	if err != nil {
		return nil, err
	}
	if config == nil {
		return c, nil
	}
	conf := config.Clone()
	if conf.ServerName == "" {
		conf.ServerName = "localhost"
		if e.network == "tcp" {
			conf.ServerName, _, _ = net.SplitHostPort(e.address)
		}
	}
	t := tls.Client(c, conf)
	if err = t.HandshakeContext(ctx); err != nil {
		c.Close()
		return nil, err
	}
	return t, nil
}
func tag(token, role, cluster string, a, b []byte) []byte {
	m := hmac.New(sha256.New, []byte(token))
	m.Write([]byte(role + ":" + cluster))
	m.Write(a)
	m.Write(b)
	return m.Sum(nil)
}
func authenticate(c net.Conn, token, cluster string, server bool) error {
	var a, b [32]byte
	if server {
		if _, e := rand.Read(a[:]); e != nil {
			return e
		}
		if _, e := c.Write(a[:]); e != nil {
			return e
		}
		var request [64]byte
		if _, e := io.ReadFull(c, request[:]); e != nil {
			return e
		}
		copy(b[:], request[:32])
		if !hmac.Equal(request[32:], tag(token, "client", cluster, a[:], b[:])) {
			return fmt.Errorf("Raft authentication failed")
		}
		_, e := c.Write(tag(token, "server", cluster, a[:], b[:]))
		return e
	}
	if _, e := io.ReadFull(c, a[:]); e != nil {
		return e
	}
	if _, e := rand.Read(b[:]); e != nil {
		return e
	}
	if _, e := c.Write(append(b[:], tag(token, "client", cluster, a[:], b[:])...)); e != nil {
		return e
	}
	var reply [32]byte
	if _, e := io.ReadFull(c, reply[:]); e != nil {
		return e
	}
	if !hmac.Equal(reply[:], tag(token, "server", cluster, a[:], b[:])) {
		return fmt.Errorf("Raft authentication failed")
	}
	return nil
}

// Authentication runs concurrently so a stalled peer cannot block heartbeats.
type streamLayer struct {
	listener       net.Listener
	address        endpoint
	token, cluster string
	clientTLS      *tls.Config
	done           chan struct{}
	accepted       chan net.Conn
	once           sync.Once
	wg             sync.WaitGroup
	mu             sync.Mutex
	pending        map[net.Conn]struct{}
}

func newStream(l net.Listener, e endpoint, token, cluster string, serverTLS, clientTLS *tls.Config) *streamLayer {
	if serverTLS != nil {
		l = tls.NewListener(l, serverTLS)
	}
	s := &streamLayer{listener: l, address: e, token: token, cluster: cluster, clientTLS: clientTLS, done: make(chan struct{}), accepted: make(chan net.Conn), pending: make(map[net.Conn]struct{})}
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		slots := make(chan struct{}, 64)
		for {
			c, err := l.Accept()
			if err != nil {
				return
			}
			select {
			case slots <- struct{}{}:
			default:
				c.Close()
				continue
			}
			s.mu.Lock()
			select {
			case <-s.done:
				s.mu.Unlock()
				c.Close()
				<-slots
				return
			default:
			}
			s.pending[c] = struct{}{}
			s.mu.Unlock()
			s.wg.Add(1)
			go func() {
				defer s.wg.Done()
				defer func() { <-slots; s.mu.Lock(); delete(s.pending, c); s.mu.Unlock() }()
				_ = c.SetDeadline(time.Now().Add(5 * time.Second))
				if err := authenticate(c, token, cluster, true); err != nil {
					c.Close()
					return
				}
				_ = c.SetDeadline(time.Time{})
				select {
				case s.accepted <- c:
				case <-s.done:
					c.Close()
				}
			}()
		}
	}()
	return s
}
func (s *streamLayer) Accept() (net.Conn, error) {
	select {
	case c := <-s.accepted:
		return c, nil
	case <-s.done:
		return nil, net.ErrClosed
	}
}
func (s *streamLayer) Addr() net.Addr { return s.address }
func (s *streamLayer) Close() error {
	s.once.Do(func() {
		close(s.done)
		s.listener.Close()
		s.mu.Lock()
		for c := range s.pending {
			c.Close()
		}
		s.mu.Unlock()
		s.wg.Wait()
	})
	return nil
}
func (s *streamLayer) Dial(a raft.ServerAddress, timeout time.Duration) (net.Conn, error) {
	e, err := parseEndpoint(string(a))
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	c, err := dialEndpoint(ctx, e, s.clientTLS)
	if err != nil {
		return nil, err
	}
	_ = c.SetDeadline(time.Now().Add(timeout))
	if err = authenticate(c, s.token, s.cluster, false); err != nil {
		c.Close()
		return nil, err
	}
	_ = c.SetDeadline(time.Time{})
	return c, nil
}
func (n *Node) request(ctx context.Context, address, method, path string, body io.Reader) (*http.Response, error) {
	e, err := parseEndpoint(address)
	if err != nil {
		return nil, err
	}
	tr := &http.Transport{DisableKeepAlives: true}
	tr.DialContext = func(ctx context.Context, _, _ string) (net.Conn, error) {
		return dialEndpoint(ctx, e, n.config.ClientTLSConfig)
	}
	// TLS is performed by DialContext for TCP and Unix sockets alike.
	req, err := http.NewRequestWithContext(ctx, method, "http://replication"+path, body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+n.config.Token)
	response, err := (&http.Client{Transport: tr}).Do(req)
	if err != nil {
		tr.CloseIdleConnections()
		return nil, err
	}
	if response.StatusCode != http.StatusOK {
		defer response.Body.Close()
		b, _ := io.ReadAll(io.LimitReader(response.Body, 4096))
		return nil, fmt.Errorf("replication control %s: %s", response.Status, strings.TrimSpace(string(b)))
	}
	return response, nil
}
