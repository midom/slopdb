package replication

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// Hold an actual remote flush request, rather than relying on a slow disk or a
// throughput threshold. Later SQL must commit while the fixed prefix is being
// sealed, and only that prefix may become durable when the request is released.
func TestAsyncWritesContinueDuringQuorumFlush(t *testing.T) {
	config := asyncConfig(t, "source", "tcp")
	config.Bootstrap = true
	source := openTestNode(t, config)
	ready(t, source)
	exec(t, source, "CREATE TABLE items(id INTEGER PRIMARY KEY,v TEXT)")
	socket := filepath.Join(t.TempDir(), "control.sock")
	targetURL, _ := url.Parse("http://replication")
	proxy := httputil.NewSingleHostReverseProxy(targetURL)
	transport := &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "unix", socket)
	}}
	proxy.Transport = transport
	defer transport.CloseIdleConnections()
	var pause atomic.Bool
	entered, release := make(chan struct{}, 1), make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if pause.Load() && r.URL.Path == "/v1/durability/prepare" {
			select {
			case entered <- struct{}{}:
			default:
			}
			select {
			case <-release:
			case <-r.Context().Done():
				return
			}
		}
		proxy.ServeHTTP(w, r)
	}))
	defer server.Close()
	released := false
	defer func() {
		if !released {
			close(release)
		}
	}()
	other := asyncConfig(t, "target", "tcp")
	other.ControlListen = "unix://" + socket
	other.ControlAdvertise = strings.Replace(server.URL, "http://", "tcp://", 1)
	other.Join = source.ControlAddress()
	openTestNode(t, other)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := source.Promote(ctx, other.ID); err != nil {
		t.Fatal(err)
	}
	exec(t, source, "INSERT INTO items VALUES(1,'in the captured prefix')")
	priorD := source.durableIndex()
	pause.Store(true)
	flushDone := make(chan error, 1)
	go func() { flushDone <- source.FlushDurable(ctx) }()
	select {
	case <-entered:
	case err := <-flushDone:
		t.Fatalf("flush completed before remote prepare: %v", err)
	case <-ctx.Done():
		t.Fatal("remote prepare was not issued")
	}
	source.durability.publication.Lock()
	sealedIndex := source.durability.sealingIndex
	source.durability.publication.Unlock()
	writeCtx, stop := context.WithTimeout(ctx, 3*time.Second)
	defer stop()
	if _, err := source.Database().ExecContext(writeCtx, "INSERT INTO items VALUES(2,'after the captured prefix')"); err != nil {
		t.Fatalf("flush blocked later SQL: %v", err)
	}
	if source.applied.Load() <= sealedIndex || source.durableIndex() != priorD {
		t.Fatal("live and durable prefixes did not remain separate during flush")
	}
	source.durability.publication.Lock()
	nextSince := source.durability.postSealSince
	source.durability.publication.Unlock()
	if nextSince.IsZero() {
		t.Fatal("later unstable prefix has no admission-lag timestamp")
	}
	close(release)
	released = true
	if err := <-flushDone; err != nil {
		t.Fatal(err)
	}
	if source.durableIndex() != sealedIndex {
		t.Fatal("seal certified writes beyond its captured fence")
	}
	source.durability.publication.Lock()
	pendingSince := source.durability.pendingSince
	source.durability.publication.Unlock()
	if pendingSince != nextSince {
		t.Fatal("seal did not retain the age of the next unstable prefix")
	}
	assertCount(t, source, 2)
	if err := source.FlushDurable(ctx); err != nil {
		t.Fatal(err)
	}
	if source.durableIndex() != source.applied.Load() {
		t.Fatal("second seal did not cover the remaining prefix")
	}
}
