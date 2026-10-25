package main

import (
	"bytes"
	"context"
	stdsql "database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"slopdb/db"
)

func waitBenchmarkServerReady(t *testing.T, path string, done <-chan error) benchmarkServerReady {
	t.Helper()
	timer := time.NewTimer(10 * time.Second)
	defer timer.Stop()
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	for {
		data, err := os.ReadFile(path)
		if err == nil {
			var ready benchmarkServerReady
			if err = json.Unmarshal(data, &ready); err != nil {
				t.Fatalf("non-atomic readiness JSON: %v", err)
			}
			return ready
		}
		if !errors.Is(err, os.ErrNotExist) {
			t.Fatal(err)
		}
		select {
		case err := <-done:
			t.Fatalf("server ended before readiness: %v", err)
		case <-timer.C:
			t.Fatal("server readiness deadline")
		case <-tick.C:
		}
	}
}
func benchmarkServerFixture(t *testing.T) config {
	t.Helper()
	c := config{Dir: filepath.Join(t.TempDir(), "database"), SeedBytes: rowBytes * 17, CacheBytes: 1 << 20, CacheMemory: "heap", CachePolicy: "clock", ServeMySQL: "127.0.0.1:0", ServePostgres: "127.0.0.1:0", StatsListen: "127.0.0.1:0", JSON: filepath.Join(t.TempDir(), "server.json")}
	if err := seed(c); err != nil {
		t.Fatal(err)
	}
	c.SeedBytes = 0
	return c
}
func benchmarkFixtureSequence(t *testing.T, c config) uint64 {
	t.Helper()
	database, err := db.Open(c.Dir, options(c))
	if err != nil {
		t.Fatal(err)
	}
	sequence := database.Stats().SeqNum
	if state, e := database.ChangefeedState(context.Background()); e != nil || state.Enabled {
		t.Fatalf("unexpected changefeed state %+v %v", state, e)
	}
	if err = database.Close(); err != nil {
		t.Fatal(err)
	}
	return sequence
}
func benchmarkStats(t *testing.T, url string) benchmarkServerStats {
	t.Helper()
	client := http.Client{Timeout: 5 * time.Second}
	response, err := client.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatal(response.Status)
	}
	var snapshot benchmarkServerStats
	if err = json.NewDecoder(response.Body).Decode(&snapshot); err != nil {
		t.Fatal(err)
	}
	return snapshot
}
func TestBenchmarkServerDualProtocolsStatsAndShutdown(t *testing.T) {
	c := benchmarkServerFixture(t)
	sequence := benchmarkFixtureSequence(t, c)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- serveBenchmark(ctx, c) }()
	stopped := false
	t.Cleanup(func() {
		cancel()
		if !stopped {
			select {
			case err := <-done:
				if err != nil {
					t.Error(err)
				}
			case <-time.After(10 * time.Second):
				t.Error("benchmark server cleanup blocked")
			}
		}
	})
	ready := waitBenchmarkServerReady(t, c.JSON, done)
	if ready.PID != os.Getpid() || ready.GoVersion != runtime.Version() || ready.ServerCPUStart < 0 || ready.Seed.Rows != 17 || ready.Seed.LogicalBytes != 17*rowBytes || ready.Config.CacheBytes != c.CacheBytes || len(ready.Endpoints) != 2 {
		t.Fatalf("incomplete readiness %+v", ready)
	}
	before := benchmarkStats(t, ready.StatsURL)
	if before.Stats.SeqNum != sequence || before.CPUSeconds < ready.ServerCPUStart || before.UptimeSeconds <= 0 {
		t.Fatalf("invalid initial stats %+v", before)
	}
	for i, protocol := range []string{"mysql", "postgres"} {
		t.Run(protocol, func(t *testing.T) {
			address := ready.Endpoints[protocol]
			host, _, err := net.SplitHostPort(address)
			if err != nil || !net.ParseIP(host).IsLoopback() {
				t.Fatalf("non-loopback endpoint %q %v", address, err)
			}
			driver, dsn := "mysql", fmt.Sprintf("slopdb@tcp(%s)/slopdb?timeout=3s&readTimeout=3s&writeTimeout=3s", address)
			if protocol == "postgres" {
				driver = "postgres"
				u := url.URL{Scheme: "postgres", Host: address, User: url.User("slopdb"), Path: "/slopdb", RawQuery: "sslmode=disable&connect_timeout=3"}
				dsn = u.String()
			}
			client, err := stdsql.Open(driver, dsn)
			if err != nil {
				t.Fatal(err)
			}
			defer client.Close()
			client.SetMaxOpenConns(1)
			marker := "?"
			if protocol == "postgres" {
				marker = "$1"
			}
			statement, err := client.Prepare("SELECT id,v,payload FROM small_rows WHERE id=" + marker)
			if err != nil {
				t.Fatal(err)
			}
			defer statement.Close()
			for _, id := range []int64{1, 17, 8} {
				var got, v int64
				var data []byte
				if err = statement.QueryRow(id).Scan(&got, &v, &data); err != nil {
					t.Fatal(err)
				}
				expected := make([]byte, payloadBytes)
				payload(id, expected)
				if got != id || v != id*17 || !bytes.Equal(data, expected) {
					t.Fatalf("incorrect network row %d %d %x", got, v, data)
				}
			}
			// The second frontend observes the same physical seed and contributes to
			// the same process resource counters, without creating persistent state.
			if i == 1 {
				var count int64
				if err = client.QueryRow("SELECT count(*) FROM small_rows").Scan(&count); err != nil || count != 17 {
					t.Fatalf("shared fixture count%d %v", count, err)
				}
			}
		})
	}
	after := benchmarkStats(t, ready.StatsURL)
	if after.Stats.SeqNum != sequence || after.CPUSeconds < before.CPUSeconds || after.MemStats.TotalAlloc < before.MemStats.TotalAlloc || after.MemStats.Mallocs < before.MemStats.Mallocs || after.UptimeSeconds < before.UptimeSeconds {
		t.Fatalf("invalid resource deltas before%+v after%+v", before, after)
	}
	response, err := http.Post(ready.StatsURL, "application/json", nil)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusMethodNotAllowed {
		t.Fatal(response.Status)
	}
	cancel()
	select {
	case err = <-done:
		stopped = true
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("benchmark server did not shut down")
	}
	for _, address := range ready.Endpoints {
		connection, e := net.DialTimeout("tcp", address, time.Second)
		if e == nil {
			connection.Close()
			t.Fatalf("listener remained open: %s", address)
		}
	}
	if got := benchmarkFixtureSequence(t, c); got != sequence {
		t.Fatalf("server/statistics mutated seed sequence %d => %d", sequence, got)
	}
	if err = verify(c, ready.Seed); err != nil {
		t.Fatal(err)
	}
}
func TestBenchmarkServerPartialFailureClosesDatabase(t *testing.T) {
	c := benchmarkServerFixture(t)
	sequence := benchmarkFixtureSequence(t, c)
	for _, failure := range []string{"listener", "readiness"} {
		t.Run(failure, func(t *testing.T) {
			broken := c
			var occupied net.Listener
			if failure == "listener" {
				var err error
				occupied, err = net.Listen("tcp", "127.0.0.1:0")
				if err != nil {
					t.Fatal(err)
				}
				defer occupied.Close()
				broken.ServePostgres = occupied.Addr().String()
			} else {
				broken.JSON = t.TempDir()
			}
			done := make(chan error, 1)
			go func() { done <- serveBenchmark(context.Background(), broken) }()
			select {
			case err := <-done:
				if err == nil {
					t.Fatal("failed startup succeeded")
				}
			case <-time.After(10 * time.Second):
				t.Fatal("partial startup cleanup blocked")
			}
			if got := benchmarkFixtureSequence(t, c); got != sequence {
				t.Fatalf("partial startup mutated sequence %d => %d", sequence, got)
			}
		})
	}
}
func TestBenchmarkServerRequiresExistingSeedAndLoopback(t *testing.T) {
	missing := config{Dir: filepath.Join(t.TempDir(), "missing"), CacheBytes: 1 << 20, ServeMySQL: "127.0.0.1:0", JSON: filepath.Join(t.TempDir(), "ready.json")}
	if err := serveBenchmark(context.Background(), missing); err == nil {
		t.Fatal("missing seed accepted")
	}
	if _, err := os.Stat(missing.Dir); !os.IsNotExist(err) {
		t.Fatalf("created missing seed: %v", err)
	}
	for _, address := range []string{"0.0.0.0:0", ":0", "192.0.2.1:0"} {
		if listener, err := benchmarkServerListener(address); err == nil {
			listener.Close()
			t.Fatalf("accepted non-loopback listener %q", address)
		}
	}
}
