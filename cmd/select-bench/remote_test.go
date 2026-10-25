package main

import (
	"context"
	"path/filepath"
	"testing"
	"time"
)

func TestExternalAccessSamplesServerCounters(t *testing.T) {
	serverConfig := benchmarkServerFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- serveBenchmark(ctx, serverConfig) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Error(err)
			}
		case <-time.After(10 * time.Second):
			t.Error("server did not stop")
		}
	})
	ready := waitBenchmarkServerReady(t, serverConfig.JSON, done)
	for _, api := range []string{"mysql", "pgx"} {
		endpoint := ready.Endpoints["mysql"]
		if api != "mysql" {
			endpoint = ready.Endpoints["postgres"]
		}
		c := serverConfig
		c.ServeMySQL, c.ServePostgres = "", ""
		c.API, c.QueryMode = api, "prepared"
		c.Endpoint = endpoint
		c.ServerStatsURL = ready.StatsURL
		c.Workers, c.Duration, c.Samples, c.Prewarm = 2, 10*time.Millisecond, 2, true
		c.JSON = filepath.Join(t.TempDir(), api+".json")
		result, err := run(c)
		if err != nil {
			t.Fatal(err)
		}
		for _, sample := range result.Samples {
			if sample.ServerBefore == nil || sample.ServerAfter == nil || sample.ServerCPUSeconds < 0 || sample.ServerAverageCPUCores < 0 || sample.ServerAllocatedBytesPerQuery < 0 || sample.Queries == 0 || sample.Before.SeqNum != sample.After.SeqNum || sample.After.PageSize != 4096 {
				t.Fatal(sample)
			}
		}
	}
}
