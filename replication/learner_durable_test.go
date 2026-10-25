package replication

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
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

	"github.com/hashicorp/raft"
)

// Proxy a real learner's advertised control endpoint without disturbing its
// independent Raft transport. Tests can lose or block exactly one control RPC.
func learnerDeliveryPair(t *testing.T, intercept func(http.ResponseWriter, *http.Request) bool) (*Node, *Node) {
	t.Helper()
	config := asyncConfig(t, "delivery-source", "tcp")
	config.Bootstrap = true
	source := openTestNode(t, config)
	ready(t, source)
	exec(t, source, "CREATE TABLE items(id INTEGER PRIMARY KEY,v TEXT);INSERT INTO items VALUES(1,'one')")
	socket := filepath.Join(t.TempDir(), "control.sock")
	target, _ := url.Parse("http://replication")
	proxy := httputil.NewSingleHostReverseProxy(target)
	transport := &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "unix", socket)
	}}
	proxy.Transport = transport
	t.Cleanup(transport.CloseIdleConnections)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if intercept != nil && intercept(w, r) {
			return
		}
		proxy.ServeHTTP(w, r)
	}))
	t.Cleanup(server.Close)
	other := asyncConfig(t, "delivery-reader", "tcp")
	other.Join = source.ControlAddress()
	other.ControlListen = "unix://" + socket
	other.ControlAdvertise = strings.Replace(server.URL, "http://", "tcp://", 1)
	replica := openTestNode(t, other)
	waitApplied(t, replica, source.applied.Load())
	return source, replica
}
func readLearnerCertificate(t *testing.T, r *http.Request) (durableCertificate, bool) {
	t.Helper()
	if r.URL.Path != "/v1/durability/finalize" {
		return durableCertificate{}, false
	}
	data, e := io.ReadAll(r.Body)
	if e != nil {
		return durableCertificate{}, false
	}
	r.Body = io.NopCloser(bytes.NewReader(data))
	var c durableCertificate
	e = json.Unmarshal(data, &c)
	return c, e == nil
}
func waitLearnerSignal(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(5 * time.Second):
		t.Fatal("learner control request did not arrive")
	}
}

func TestLearnerDurableRetriesDroppedFinalizeWithoutAnotherWrite(t *testing.T) {
	var threshold atomic.Uint64
	var dropped atomic.Bool
	var attempts atomic.Int64
	first := make(chan struct{})
	source, replica := learnerDeliveryPair(t, func(w http.ResponseWriter, r *http.Request) bool {
		c, ok := readLearnerCertificate(t, r)
		if !ok || threshold.Load() == 0 || c.Body.Index <= threshold.Load() {
			return false
		}
		attempts.Add(1)
		if dropped.CompareAndSwap(false, true) {
			http.Error(w, "injected lost first finalize", http.StatusServiceUnavailable)
			close(first)
			return true
		}
		return false
	})
	threshold.Store(source.durableIndex())
	exec(t, source, "UPDATE items SET v='certified change' WHERE id=1")
	if e := source.FlushDurable(context.Background()); e != nil {
		t.Fatal(e)
	}
	target := source.durableIndex()
	lastLog := source.raftNode().LastIndex()
	waitLearnerSignal(t, first)
	waitDurableReadView(t, replica, target)
	if attempts.Load() < 2 {
		t.Fatal("lost request was not retried")
	}
	if source.raftNode().LastIndex() != lastLog {
		t.Fatal("delivery retried by appending another barrier/write")
	}
	if replica.durableIndex() != target {
		t.Fatalf("replica certificate=%d target=%d", replica.durableIndex(), target)
	}
}

func TestLearnerDurableCoalescesNewestCertificate(t *testing.T) {
	var paused atomic.Bool
	entered := make(chan struct{}, 8)
	source, replica := learnerDeliveryPair(t, func(w http.ResponseWriter, r *http.Request) bool {
		if r.URL.Path == "/v1/durability/finalize" && paused.Load() {
			select {
			case entered <- struct{}{}:
			default:
			}
			http.Error(w, "temporary learner outage", http.StatusServiceUnavailable)
			return true
		}
		return false
	})
	paused.Store(true)
	exec(t, source, "UPDATE items SET v='first' WHERE id=1")
	if e := source.FlushDurable(context.Background()); e != nil {
		t.Fatal(e)
	}
	waitLearnerSignal(t, entered)
	source.workMu.Lock()
	worker := source.learnerDurable
	source.workMu.Unlock()
	if worker == nil {
		t.Fatal("failed delivery not retained")
	}
	for _, value := range []string{"second", "latest"} {
		exec(t, source, "UPDATE items SET v='"+value+"' WHERE id=1")
		if e := source.FlushDurable(context.Background()); e != nil {
			t.Fatal(e)
		}
	}
	target := source.durableIndex()
	source.workMu.Lock()
	same := source.learnerDurable == worker
	count := len(worker.pending)
	notice := worker.pending[replica.config.ID]
	source.workMu.Unlock()
	if !same || count != 1 || notice == nil || notice.certificate.Body.Index != target {
		t.Fatalf("unbounded/stale delivery state: same=%v count=%d notice=%v target=%d", same, count, notice, target)
	}
	paused.Store(false)
	lastLog := source.raftNode().LastIndex()
	waitDurableReadView(t, replica, target)
	if source.raftNode().LastIndex() != lastLog {
		t.Fatal("retry appended a new seal")
	}
}

func TestLearnerDurableShutdownCancelsOutstandingRequest(t *testing.T) {
	var paused atomic.Bool
	entered, cancelled := make(chan struct{}, 1), make(chan struct{}, 1)
	source, _ := learnerDeliveryPair(t, func(w http.ResponseWriter, r *http.Request) bool {
		if r.URL.Path != "/v1/durability/finalize" || !paused.Load() {
			return false
		}
		io.Copy(io.Discard, r.Body)
		select {
		case entered <- struct{}{}:
		default:
		}
		<-r.Context().Done()
		select {
		case cancelled <- struct{}{}:
		default:
		}
		return true
	})
	paused.Store(true)
	exec(t, source, "UPDATE items SET v='waiting' WHERE id=1")
	if e := source.FlushDurable(context.Background()); e != nil {
		t.Fatal(e)
	}
	waitLearnerSignal(t, entered)
	source.workMu.Lock()
	worker := source.learnerDurable
	source.workMu.Unlock()
	if worker == nil {
		t.Fatal("missing delivery worker")
	}
	done := make(chan error, 1)
	go func() { done <- source.Close() }()
	select {
	case e := <-done:
		if e != nil {
			t.Fatal(e)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("close did not cancel learner notification")
	}
	waitLearnerSignal(t, cancelled)
	if worker.ctx.Err() == nil {
		t.Fatal("worker context survived close")
	}
	source.workMu.Lock()
	pending := len(worker.pending)
	active := source.learnerDurable
	source.workMu.Unlock()
	if active != nil || pending != 0 {
		t.Fatal("closed worker retained certificates")
	}
}

func TestLearnerDurableLeadershipChangeCancelsOutstandingRequest(t *testing.T) {
	var paused atomic.Bool
	entered, cancelled := make(chan struct{}, 1), make(chan struct{}, 1)
	source, replica := learnerDeliveryPair(t, func(w http.ResponseWriter, r *http.Request) bool {
		if r.URL.Path != "/v1/durability/finalize" || !paused.Load() {
			return false
		}
		io.Copy(io.Discard, r.Body)
		select {
		case entered <- struct{}{}:
		default:
		}
		<-r.Context().Done()
		select {
		case cancelled <- struct{}{}:
		default:
		}
		return true
	})
	if e := source.FlushDurable(context.Background()); e != nil {
		t.Fatal(e)
	}
	learnerCertificate := source.currentCertificate()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if e := source.Promote(ctx, replica.config.ID); e != nil {
		t.Fatal(e)
	}
	// A previously certified learner notice can still be outstanding when its
	// peer is promoted. Its worker must retire when this leader steps down.
	paused.Store(true)
	source.notifyLearnersDurable(learnerCertificate)
	waitLearnerSignal(t, entered)
	source.workMu.Lock()
	worker := source.learnerDurable
	source.workMu.Unlock()
	if worker == nil {
		t.Fatal("missing delivery worker")
	}
	future := source.raftNode().LeadershipTransferToServer(raft.ServerID(replica.config.ID), raft.ServerAddress(replica.Address()))
	if e := source.wait(ctx, future); e != nil {
		t.Fatal(e)
	}
	waitLearnerSignal(t, cancelled)
	select {
	case <-worker.done:
	case <-time.After(5 * time.Second):
		t.Fatal("stepped-down leader retained its delivery worker")
	}
	source.workMu.Lock()
	active := source.learnerDurable
	pending := len(worker.pending)
	source.workMu.Unlock()
	if worker.ctx.Err() == nil || active != nil || pending != 0 {
		t.Fatal("leadership transition retained learner certificates")
	}
}
