package main

import (
	"path/filepath"
	"slopdb/db"
	"testing"
	"time"
)

func TestRegularSQLTextBenchmarkChecksSeed(t *testing.T) {
	c := config{Dir: filepath.Join(t.TempDir(), "db"), SeedBytes: 256 * 129, CacheBytes: 1 << 20, Workers: 4, Duration: 10 * time.Millisecond, Samples: 2, Prewarm: true, API: "native", QueryMode: "text", JSON: filepath.Join(t.TempDir(), "text.json")}
	if err := seed(c); err != nil {
		t.Fatal(err)
	}
	c.SeedBytes = 0
	report, err := run(c)
	if err != nil {
		t.Fatal(err)
	}
	if report.APIMode != "Session.Exec" || len(report.Samples) != 2 || report.PrewarmQueries != 17 {
		t.Fatal(report)
	}
	for _, sample := range report.Samples {
		if sample.Queries == 0 || sample.Before.SeqNum != sample.After.SeqNum {
			t.Fatal(sample)
		}
	}
	if err := verify(c, report.Seed); err != nil {
		t.Fatal(err)
	}
}
func TestNativeBackendFailureDoesNotLoseCleanup(t *testing.T) {
	d, err := db.Open(t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if err = d.Close(); err != nil {
		t.Fatal(err)
	}
	for _, mode := range []string{"text", "prepared"} {
		if backend, err := newNativeBackend(config{Workers: 2, QueryMode: mode}, d); err == nil || backend != nil {
			t.Fatalf("closed backend: %+v, %v", backend, err)
		}
	}
}
func TestAccessConfigurationRejectsMislabelledModes(t *testing.T) {
	for _, c := range []config{{API: "missing"}, {API: "native", QueryMode: "bad"}, {API: "mysql", ReuseResults: true}, {API: "native", QueryMode: "text", ReuseResults: true}, {API: "embedded", Endpoint: "127.0.0.1:3"}} {
		if _, err := accessConfig(c); err == nil {
			t.Fatal(c)
		}
	}
}
