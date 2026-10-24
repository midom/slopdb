package replication

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"slopdb/db"
)

func waitDurableReadView(t *testing.T, n *Node, index uint64) {
	t.Helper()
	deadline := time.Now().Add(8 * time.Second)
	for {
		n.durableReads.mu.Lock()
		view := n.durableReads.latest
		ready := view != nil && view.index >= index
		n.durableReads.mu.Unlock()
		if ready {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("durable view did not reach %d: %+v", index, n.Status())
		}
		time.Sleep(time.Millisecond)
	}
}

func TestDurableReadPolicies(t *testing.T) {
	for _, network := range []string{"tcp", "unix"} {
		t.Run(network, func(t *testing.T) {
			source, replica := readReplicaPair(t, DurabilityAsync, network, 2*time.Second)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			if err := source.FlushDurable(ctx); err != nil {
				t.Fatal(err)
			}
			waitDurableReadView(t, replica, source.durableIndex())
			reader, err := replica.Database().NewSession()
			if err != nil {
				t.Fatal(err)
			}
			defer reader.Close()
			exec(t, source, "UPDATE items SET v='unstable' WHERE id=1; INSERT INTO items VALUES(3,'unstable'); ALTER TABLE items ADD COLUMN extra TEXT DEFAULT 'new'")
			waitApplied(t, replica, source.applied.Load())
			if _, err = reader.Exec("SET READ BARRIER PREVIOUS"); err != nil {
				t.Fatal(err)
			}
			result, err := reader.Exec("SELECT * FROM items ORDER BY id")
			if err != nil || len(result.Rows) != 2 || len(result.Columns) != 2 || result.Rows[0]["v"].Text != "one" {
				t.Fatalf("previous exposed unstable data/catalog: %+v %v", result, err)
			}
			if _, err = reader.Exec("SELECT extra FROM items"); err == nil {
				t.Fatal("previous exposed later column")
			}
			if _, err = reader.Exec("SET READ BARRIER DEFAULT"); err != nil {
				t.Fatal(err)
			}
			result, err = reader.Exec("SELECT * FROM items ORDER BY id")
			if err != nil || len(result.Rows) != 3 || len(result.Columns) != 3 {
				t.Fatalf("default didn't restore committed reads: %+v %v", result, err)
			}
			if _, err = reader.Exec("SET READ BARRIER NEXT"); err != nil {
				t.Fatal(err)
			}
			before := source.durableIndex()
			result, err = reader.Exec("SELECT * FROM items ORDER BY id")
			if err != nil || len(result.Rows) != 3 || result.Rows[0]["v"].Text != "unstable" || source.durableIndex() <= before {
				t.Fatalf("next failed to certify new prefix: %+v %v", result, err)
			}
			before = source.durableIndex()
			if _, err = reader.Exec("SELECT COUNT(*) FROM items"); err != nil {
				t.Fatal(err)
			}
			if source.durableIndex() <= before {
				t.Fatal("idle NEXT reused previous barrier")
			}
			// One streaming statement requests one barrier, including the
			// materialized fallback for aggregate queries.
			beforeLog := source.raftNode().LastIndex()
			rows, err := reader.Query("SELECT COUNT(*) AS n FROM items")
			if err != nil {
				t.Fatal(err)
			}
			if !rows.Next() {
				t.Fatalf("durable stream empty: %v", rows.Err())
			}
			var count int64
			if err = rows.Scan(&count); err != nil || count != 3 {
				t.Fatalf("durable stream: %d %v", count, err)
			}
			if err = rows.Close(); err != nil {
				t.Fatal(err)
			}
			if source.raftNode().LastIndex() != beforeLog+1 {
				t.Fatal("one streaming NEXT statement requested multiple barriers")
			}
		})
	}
}

func TestDurableReadExactBarrier(t *testing.T) {
	c := asyncConfig(t, "source", "tcp")
	c.Bootstrap = true
	source := openTestNode(t, c)
	ready(t, source)
	exec(t, source, "CREATE TABLE items(id INTEGER PRIMARY KEY,v TEXT); INSERT INTO items VALUES(1,'before')")
	socket := filepath.Join(t.TempDir(), "c.sock")
	upstream, _ := url.Parse("http://replication")
	proxy := httputil.NewSingleHostReverseProxy(upstream)
	transport := &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "unix", socket)
	}}
	proxy.Transport = transport
	defer transport.CloseIdleConnections()
	var paused atomic.Bool
	entered, release := make(chan struct{}, 1), make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if paused.Load() && r.URL.Path == "/v1/durability/prepare" {
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
	var releaseOnce sync.Once
	releaseBarrier := func() { releaseOnce.Do(func() { close(release) }) }
	defer releaseBarrier()
	other := asyncConfig(t, "target", "tcp")
	other.ControlListen, other.ControlAdvertise, other.Join = "unix://"+socket, strings.Replace(server.URL, "http://", "tcp://", 1), source.ControlAddress()
	openTestNode(t, other)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := source.Promote(ctx, other.ID); err != nil {
		t.Fatal(err)
	}
	previous := source.durableIndex()
	exec(t, source, "UPDATE items SET v='at barrier' WHERE id=1")
	reader, err := source.Database().NewSession()
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	paused.Store(true)
	type answer struct {
		result *db.ExecResult
		err    error
	}
	done := make(chan answer, 1)
	go func() {
		result, err := reader.ExecContextWithOptions(ctx, "SELECT v FROM items WHERE id=1", db.ExecOptions{ReadBarrier: db.ReadBarrierNext})
		done <- answer{result, err}
	}()
	select {
	case <-entered:
	case a := <-done:
		t.Fatalf("NEXT completed before certification: %+v", a)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	source.durability.publication.Lock()
	fence := source.durability.sealingIndex
	source.durability.publication.Unlock()
	if fence <= previous {
		t.Fatal("next barrier did not advance")
	}
	// Catalog and data change after the captured fence, while its certificate
	// is held. Compaction must retain both historical rows and metadata.
	exec(t, source, "ALTER TABLE items RENAME TO old_items; CREATE TABLE items(id INTEGER PRIMARY KEY,v TEXT); INSERT INTO items VALUES(1,'after barrier'); VACUUM")
	late, err := source.AckedPosition(ctx)
	if err != nil {
		t.Fatal(err)
	}
	old, err := source.Database().ExecWithOptions("SELECT v FROM items WHERE id=1", db.ExecOptions{ReadBarrier: db.ReadBarrierPrevious})
	if err != nil || old.Rows[0]["v"].Text != "before" {
		t.Fatalf("PREVIOUS exposed candidate/tail: %+v %v", old, err)
	}
	select {
	case a := <-done:
		t.Fatalf("NEXT completed while certificate held: %+v", a)
	default:
	}
	paused.Store(false)
	// Closing the gate here also releases the deferred cleanup exactly once.
	releaseBarrier()
	a := <-done
	if a.err != nil || len(a.result.Rows) != 1 || a.result.Rows[0]["v"].Text != "at barrier" {
		t.Fatalf("NEXT mixed later rows/catalog into fence %d: %+v %v", fence, a.result, a.err)
	}
	if source.durableIndex() != fence {
		t.Fatal("barrier certified later writes")
	}
	old, err = source.Database().ExecWithOptions("SELECT v FROM items WHERE id=1", db.ExecOptions{ReadBarrier: db.ReadBarrierPrevious})
	if err != nil || old.Rows[0]["v"].Text != "at barrier" {
		t.Fatalf("PREVIOUS did not select new certified view: %+v %v", old, err)
	}
	if _, err = source.Database().ExecWithOptions("SELECT * FROM items", db.ExecOptions{ReadBarrier: db.ReadBarrierPrevious, MinimumPosition: late}); !errors.Is(err, ErrDurableReadUnavailable) {
		t.Fatalf("historical read ignored later minimum: %v", err)
	}
	latest, err := source.Database().ExecWithOptions("SELECT v FROM items WHERE id=1", db.ExecOptions{ReadBarrier: db.ReadBarrierNext, MinimumPosition: late})
	if err != nil || latest.Rows[0]["v"].Text != "after barrier" {
		t.Fatalf("NEXT did not cover minimum: %+v %v", latest, err)
	}
}

func TestDurableReadDefaultAndCancellation(t *testing.T) {
	for _, mode := range []Durability{DurabilitySync, DurabilityAsync} {
		t.Run(string(mode), func(t *testing.T) {
			c := asyncConfig(t, "source", "tcp")
			c.Durability, c.Bootstrap = mode, true
			source := openTestNode(t, c)
			ready(t, source)
			exec(t, source, "CREATE TABLE items(id INTEGER PRIMARY KEY,v TEXT); INSERT INTO items VALUES(1,'stable'); BEGIN; COMMIT DURABLE")
			other := asyncConfig(t, "reader", "tcp")
			other.Durability, other.ReadBarrier, other.Join = mode, db.ReadBarrierPrevious, source.ControlAddress()
			replica := openTestNode(t, other)
			waitDurableReadView(t, replica, source.durableIndex())
			reader, err := replica.Database().NewSession()
			if err != nil {
				t.Fatal(err)
			}
			defer reader.Close()
			result, err := reader.Exec("SELECT v FROM items")
			if err != nil || result.Rows[0]["v"].Text != "stable" {
				t.Fatalf("default durable read: %+v %v", result, err)
			}
			before := source.durableIndex()
			if _, err = reader.ExecWithOptions("SELECT * FROM items", db.ExecOptions{ReadBarrier: db.ReadBarrierNext}); err != nil {
				t.Fatal(err)
			}
			if source.durableIndex() <= before {
				t.Fatal("next did not create barrier")
			}
			replica.fsmMu.Lock()
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
			_, err = reader.ExecContextWithOptions(ctx, "SELECT * FROM items", db.ExecOptions{ReadBarrier: db.ReadBarrierNext})
			cancel()
			replica.fsmMu.Unlock()
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("NEXT discovery ignored cancellation: %v", err)
			}
			if err = source.httpServer.Close(); err != nil {
				t.Fatal(err)
			}
			if _, err = reader.Exec("SELECT * FROM items"); err != nil {
				t.Fatalf("PREVIOUS required live leader: %v", err)
			}
			if _, err = reader.ExecWithOptions("SELECT * FROM items", db.ExecOptions{ReadBarrier: db.ReadBarrierNext}); err == nil {
				t.Fatal("NEXT silently fell back while leader unavailable")
			}
		})
	}
}
