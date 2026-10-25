package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"

	"slopdb/db"
	"slopdb/mysqlserver"
	"slopdb/postgresserver"
	"slopdb/storage"
)

type benchmarkServerReady struct {
	PID            int               `json:"pid"`
	Endpoints      map[string]string `json:"endpoints"`
	StatsURL       string            `json:"stats_url"`
	Config         config            `json:"config"`
	GoVersion      string            `json:"go_version"`
	ServerCPUStart float64           `json:"server_cpu_start"`
	Started        time.Time         `json:"started"`
	Seed           seedInfo          `json:"seed"`
}
type benchmarkServerMemory struct {
	TotalAlloc, Mallocs uint64
	NumGC               uint32
	PauseTotalNs        uint64
}
type benchmarkServerUsage struct {
	UserSeconds, SystemSeconds                           float64
	VoluntaryContextSwitches, InvoluntaryContextSwitches int64
}
type benchmarkServerStats struct {
	Stats           storage.LSMStats
	CPUSeconds      float64
	MemStats        benchmarkServerMemory
	ProcessUsageCPU benchmarkServerUsage
	UptimeSeconds   float64
}
type benchmarkServeResult struct {
	name string
	err  error
}

// serveBenchmark opens the existing fixture once. It owns only its listeners
// and does no periodic statistics work: clients fetch /stats outside each timed
// sample so server CPU/allocations can be measured separately from client costs.
func serveBenchmark(ctx context.Context, c config) (err error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if ctx.Err() != nil {
		return nil
	}
	if c.ServeMySQL == "" && c.ServePostgres == "" {
		return fmt.Errorf("benchmark server requires a MySQL or PostgreSQL listener")
	}
	if c.JSON == "" {
		return fmt.Errorf("benchmark server requires a readiness JSON destination")
	}
	if c.SeedBytes != 0 {
		return fmt.Errorf("benchmark server opens an existing seed; seed-bytes must be zero")
	}
	started := time.Now()
	cpuStart := cpuSeconds(processUsage())
	seedData, e := os.ReadFile(filepath.Join(c.Dir, "seed.json"))
	if e != nil {
		return e
	}
	var info seedInfo
	if e = json.Unmarshal(seedData, &info); e != nil {
		return e
	}
	if info.Rows <= 0 || info.RowBytes != rowBytes || info.Rows > (1<<63-1)/rowBytes || info.LogicalBytes != info.Rows*rowBytes || info.Generator != "splitmix64-v1" {
		return fmt.Errorf("unsupported seed")
	}
	database, e := db.Open(c.Dir, options(c))
	if e != nil {
		return e
	}
	var mysql *mysqlserver.Server
	var postgres *postgresserver.Server
	var statsServer *http.Server
	var listeners []net.Listener
	results := make(chan benchmarkServeResult, 3)
	var servers sync.WaitGroup
	launched, finished := 0, 0
	defer func() {
		// Stop every frontend before closing the shared physical database. Close
		// listeners explicitly too, covering cancellation before Serve registers one.
		if statsServer != nil {
			shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			closeErr := statsServer.Shutdown(shutdown)
			cancel()
			if closeErr != nil {
				err = errors.Join(err, fmt.Errorf("stats shutdown: %w", closeErr), statsServer.Close())
			}
		}
		if mysql != nil {
			err = errors.Join(err, mysql.Close())
		}
		if postgres != nil {
			err = errors.Join(err, postgres.Close())
		}
		for _, listener := range listeners {
			if closeErr := listener.Close(); closeErr != nil && !errors.Is(closeErr, net.ErrClosed) {
				err = errors.Join(err, closeErr)
			}
		}
		servers.Wait()
		for finished < launched {
			result := <-results
			finished++
			if result.err != nil && !errors.Is(result.err, net.ErrClosed) && !errors.Is(result.err, http.ErrServerClosed) {
				err = errors.Join(err, fmt.Errorf("%s server: %w", result.name, result.err))
			}
		}
		err = errors.Join(err, database.Close())
	}()
	if database.Stats().PageSize != 4096 {
		return fmt.Errorf("benchmark server requires a 4096-byte seed page size")
	}
	endpoints := make(map[string]string)
	type serveTask struct {
		name     string
		listener net.Listener
		serve    func(net.Listener) error
	}
	var tasks []serveTask
	if c.ServeMySQL != "" {
		mysql, e = mysqlserver.New(database, mysqlserver.Config{Username: "slopdb", Database: "slopdb"})
		if e != nil {
			return e
		}
		listener, listenErr := benchmarkServerListener(c.ServeMySQL)
		if listenErr != nil {
			return fmt.Errorf("MySQL listener: %w", listenErr)
		}
		listeners = append(listeners, listener)
		endpoints["mysql"] = listener.Addr().String()
		tasks = append(tasks, serveTask{"mysql", listener, mysql.Serve})
	}
	if c.ServePostgres != "" {
		postgres, e = postgresserver.New(database, postgresserver.Config{Username: "slopdb", Database: "slopdb"})
		if e != nil {
			return e
		}
		listener, listenErr := benchmarkServerListener(c.ServePostgres)
		if listenErr != nil {
			return fmt.Errorf("PostgreSQL listener: %w", listenErr)
		}
		listeners = append(listeners, listener)
		endpoints["postgres"] = listener.Addr().String()
		tasks = append(tasks, serveTask{"postgres", listener, postgres.Serve})
	}
	statsAddress := c.StatsListen
	if statsAddress == "" {
		statsAddress = "127.0.0.1:0"
	}
	statsListener, e := benchmarkServerListener(statsAddress)
	if e != nil {
		return fmt.Errorf("statistics listener: %w", e)
	}
	listeners = append(listeners, statsListener)
	mux := http.NewServeMux()
	mux.HandleFunc("/stats", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.Header().Set("Allow", http.MethodGet)
			http.Error(w, "GET required", http.StatusMethodNotAllowed)
			return
		}
		// This uses storage diagnostics directly: observing the benchmark must not
		// enable CDC, open SQL sessions, update the catalog or change sequence numbers.
		snapshot := database.Stats()
		usage := processUsage()
		var memory runtime.MemStats
		runtime.ReadMemStats(&memory)
		report := benchmarkServerStats{Stats: snapshot, CPUSeconds: cpuSeconds(usage), MemStats: benchmarkServerMemory{memory.TotalAlloc, memory.Mallocs, memory.NumGC, memory.PauseTotalNs}, ProcessUsageCPU: benchmarkServerUsage{float64(usage.Utime.Sec) + float64(usage.Utime.Usec)/1e6, float64(usage.Stime.Sec) + float64(usage.Stime.Usec)/1e6, usage.Nvcsw, usage.Nivcsw}, UptimeSeconds: time.Since(started).Seconds()}
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "no-store")
		_ = json.NewEncoder(w).Encode(report)
	})
	statsServer = &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second, WriteTimeout: 5 * time.Second, IdleTimeout: 10 * time.Second}
	tasks = append(tasks, serveTask{"stats", statsListener, statsServer.Serve})
	for _, task := range tasks {
		task := task
		launched++
		servers.Add(1)
		go func() { defer servers.Done(); results <- benchmarkServeResult{task.name, task.serve(task.listener)} }()
	}
	if ctx.Err() != nil {
		return nil
	}
	ready := benchmarkServerReady{PID: os.Getpid(), Endpoints: endpoints, StatsURL: "http://" + statsListener.Addr().String() + "/stats", Config: c, GoVersion: runtime.Version(), ServerCPUStart: cpuStart, Started: started.UTC(), Seed: info}
	if e = writeBenchmarkServerReady(c.JSON, ready); e != nil {
		return fmt.Errorf("server readiness: %w", e)
	}
	select {
	case <-ctx.Done():
		return nil
	case result := <-results:
		finished++
		if result.err == nil {
			return fmt.Errorf("%s server stopped unexpectedly", result.name)
		}
		return fmt.Errorf("%s server: %w", result.name, result.err)
	}
}
func benchmarkServerListener(address string) (net.Listener, error) {
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		return nil, err
	}
	if ip := net.ParseIP(host); !strings.EqualFold(host, "localhost") && (ip == nil || !ip.IsLoopback()) {
		return nil, fmt.Errorf("benchmark listeners require a loopback address")
	}
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return nil, err
	}
	if tcp, ok := listener.Addr().(*net.TCPAddr); !ok || !tcp.IP.IsLoopback() {
		_ = listener.Close()
		return nil, fmt.Errorf("benchmark listener did not resolve to loopback")
	}
	return listener, nil
}
func writeBenchmarkServerReady(path string, ready benchmarkServerReady) (err error) {
	data, err := json.MarshalIndent(ready, "", "  ")
	if err != nil {
		return err
	}
	if err = os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return err
	}
	file, err := os.CreateTemp(filepath.Dir(path), ".select-bench-ready-*")
	if err != nil {
		return err
	}
	defer func() { _ = file.Close(); _ = os.Remove(file.Name()) }()
	if _, err = file.Write(append(data, '\n')); err != nil {
		return err
	}
	if err = file.Close(); err != nil {
		return err
	}
	return os.Rename(file.Name(), path)
}
