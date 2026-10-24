package replication

import (
	"context"
	"crypto/sha256"
	"flag"
	"fmt"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"slopdb/db"
	"slopdb/storage"
)

// benchmarkNoWALFsync is a diagnostic test-binary flag, not a supported node
// option. Replicated no-sync runs require the explicitly labelled build overlay
// recorded alongside their results; ordinary Open must continue rejecting it.
var benchmarkNoWALFsync = flag.Bool("raft-bench-no-wal-fsync", false, "diagnostic benchmark only: disable application and Raft WAL fsync; requires isolated build overlay")

// BenchmarkRaftTransactions measures SQL commits, not Raft append-only
// throughput. All nodes share one process/host and use TCP loopback. Each worker
// owns a different row. Setup, warmup, integrity and restart checks are untimed.
//
// go test ./replication -run '^$' -bench '^BenchmarkRaftTransactions$' -benchtime=5s -count=3
func BenchmarkRaftTransactions(b *testing.B) {
	for _, mode := range []struct {
		name       string
		voters     int
		durability Durability
	}{
		{"standalone", 0, DurabilitySync},
		{"raft1", 1, DurabilitySync},
		{"raft3", 3, DurabilitySync},
		{"raft3-async", 3, DurabilityAsync},
	} {
		b.Run(mode.name, func(b *testing.B) {
			for _, workers := range []int{1, 4, 16} {
				b.Run(fmt.Sprintf("workers%d", workers), func(b *testing.B) { benchmarkTransactions(b, mode.voters, workers, mode.durability) })
			}
		})
	}
}

func benchmarkTransactions(b *testing.B, voters, workers int, durability Durability) {
	benchmarkTransactionWorkload(b, voters, workers, durability, false)
}

// BenchmarkRaftHotRowTransactions uses the same fixture and checks as the
// disjoint-row benchmark, but every independent session updates row 1. Row
// locks are held through commit, so this measures contention and cannot rely on
// independent-row group commit for its throughput.
func BenchmarkRaftHotRowTransactions(b *testing.B) {
	for _, mode := range []struct {
		name       string
		durability Durability
	}{{"raft3", DurabilitySync}, {"raft3-async", DurabilityAsync}} {
		b.Run(mode.name, func(b *testing.B) {
			for _, workers := range []int{1, 4, 16} {
				b.Run(fmt.Sprintf("workers%d", workers), func(b *testing.B) {
					benchmarkTransactionWorkload(b, 3, workers, mode.durability, true)
				})
			}
		})
	}
}

func benchmarkTransactionWorkload(b *testing.B, voters, workers int, durability Durability, hotRow bool) {
	b.StopTimer()
	rowID := func(worker int) int64 {
		if hotRow {
			return 1
		}
		return int64(worker + 1)
	}
	if durability == DurabilityAsync && *benchmarkNoWALFsync {
		b.Skip("async durability requires synchronization capability")
	}
	root := b.TempDir()
	options := storage.DefaultOptions()
	options.DisableWAL = false
	options.DisableWALSync = *benchmarkNoWALFsync
	seedPath := filepath.Join(root, "seed")
	seed, e := db.Open(seedPath, options)
	if e != nil {
		b.Fatal(e)
	}
	var fixture strings.Builder
	fixture.WriteString("CREATE TABLE counters(id INTEGER PRIMARY KEY,n INTEGER NOT NULL); INSERT INTO counters VALUES ")
	for i := 1; i <= 128; i++ {
		if i > 1 {
			fixture.WriteByte(',')
		}
		fmt.Fprintf(&fixture, "(%d,0)", i)
	}
	if _, e = seed.Exec(fixture.String()); e != nil {
		_ = seed.Close()
		b.Fatal(e)
	}
	if e = seed.Close(); e != nil {
		b.Fatal(e)
	}
	b.Logf("fixture_sha256=%x voters=%d workers=%d shared_row=%t durability=%s WAL=on WAL_fsync_capability=%t TCP=loopback TLS=off one prepared point update/explicit transaction", sha256.Sum256([]byte(fixture.String())), voters, workers, hotRow, durability, !*benchmarkNoWALFsync)
	var database *db.Database
	var nodes []*Node
	var configurations []Config
	var finalToken DurabilityToken
	closed := false
	closeAll := func() {
		if closed {
			return
		}
		closed = true
		if voters == 0 {
			if e := database.Close(); e != nil {
				b.Error(e)
			}
			return
		}
		for i := len(nodes) - 1; i >= 0; i-- {
			if e := nodes[i].Close(); e != nil {
				b.Error(e)
			}
		}
	}
	b.Cleanup(closeAll)
	startup, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	if voters == 0 {
		database, e = db.Open(seedPath, options)
		if e != nil {
			b.Fatal(e)
		}
	} else {
		for i := 0; i < voters; i++ {
			c := Config{ID: fmt.Sprintf("bench-%d", i+1), Directory: filepath.Join(root, fmt.Sprintf("node-%d", i+1)), Listen: "tcp://127.0.0.1:0", ControlListen: "tcp://127.0.0.1:0", Token: "local-throughput-benchmark-token", StorageOptions: options, Durability: durability}
			if i == 0 {
				c.Bootstrap = true
				c.SeedDirectory = seedPath
			} else {
				c.Join = nodes[0].ControlAddress()
			}
			node, e := Open(startup, c)
			if e != nil {
				b.Fatal(e)
			}
			nodes = append(nodes, node)
			c.Listen = node.Address()
			c.ControlListen = node.ControlAddress()
			c.Bootstrap = false
			c.SeedDirectory = ""
			c.Join = ""
			configurations = append(configurations, c)
			if i == 0 {
				if e = node.WaitReady(startup); e != nil {
					b.Fatal(e)
				}
			} else {
				if e = nodes[0].Promote(startup, c.ID); e != nil {
					b.Fatal(e)
				}
			}
		}
		status := nodes[0].Status()
		count := 0
		for _, m := range status.Members {
			if m.Suffrage == "Voter" {
				count++
			}
		}
		if count != voters {
			b.Fatalf("voters=%d want=%d", count, voters)
		}
		database = nodes[0].Database()
	}
	if database.Stats().WALSyncDisabled != *benchmarkNoWALFsync {
		b.Fatal("application WAL fsync does not match benchmark mode")
	}
	for _, node := range nodes {
		if node.database.Stats().WALSyncDisabled != *benchmarkNoWALFsync || node.store.tree.Stats().WALSyncDisabled != *benchmarkNoWALFsync {
			b.Fatal("replica application/consensus WAL fsync does not match benchmark mode")
		}
	}
	statement, e := database.Prepare("UPDATE counters SET n=n+1 WHERE id=?")
	if e != nil {
		b.Fatal(e)
	}
	defer statement.Close()
	sessions := make([]*db.Session, workers)
	for i := range sessions {
		sessions[i], e = database.NewSession()
		if e != nil {
			b.Fatal(e)
		}
		defer sessions[i].Close()
		// Warm plans and transport connections without retaining any row changes.
		if _, e = sessions[i].Exec("BEGIN"); e != nil {
			b.Fatal(e)
		}
		if _, e = sessions[i].ExecPrepared(statement, rowID(i)); e != nil {
			b.Fatal(e)
		}
		if _, e = sessions[i].Exec("ROLLBACK"); e != nil {
			b.Fatal(e)
		}
	}
	latencies := make([]int64, b.N)
	completed := make([]int64, workers)
	failures := make(chan error, workers)
	var wg sync.WaitGroup
	start := make(chan struct{})
	var beforeIndex uint64
	if voters > 0 {
		beforeIndex = nodes[0].raft.LastIndex()
	}
	for worker := 0; worker < workers; worker++ {
		worker := worker
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			session := sessions[worker]
			for operation := worker; operation < b.N; operation += workers {
				began := time.Now()
				if _, e := session.Exec("BEGIN"); e != nil {
					failures <- e
					return
				}
				if _, e := session.ExecPrepared(statement, rowID(worker)); e != nil {
					_, _ = session.Exec("ROLLBACK")
					failures <- e
					return
				}
				if _, e := session.Exec("COMMIT"); e != nil {
					failures <- e
					return
				}
				latencies[operation] = time.Since(began).Nanoseconds()
				completed[worker]++
			}
		}()
	}
	b.ReportAllocs()
	b.ResetTimer()
	b.StartTimer()
	close(start)
	wg.Wait()
	b.StopTimer()
	elapsed := b.Elapsed()
	close(failures)
	for e := range failures {
		b.Fatal(e)
	}
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	percentile := func(p float64) float64 {
		index := int(float64(len(latencies)-1) * p)
		return float64(latencies[index]) / float64(time.Millisecond)
	}
	b.ReportMetric(float64(b.N)/elapsed.Seconds(), "tx/s")
	b.ReportMetric(percentile(.50), "p50-ms")
	b.ReportMetric(percentile(.95), "p95-ms")
	b.ReportMetric(percentile(.99), "p99-ms")
	if voters > 0 {
		b.ReportMetric(float64(nodes[0].raft.LastIndex()-beforeIndex)/float64(b.N), "raft-entries/tx")
	}
	validate := func(database *db.Database) {
		b.Helper()
		result, e := database.Exec("SELECT id,n FROM counters ORDER BY id")
		if e != nil {
			b.Fatal(e)
		}
		if len(result.Rows) != 128 {
			b.Fatalf("row count %d", len(result.Rows))
		}
		var total int64
		var expected [128]int64
		for worker, count := range completed {
			expected[rowID(worker)-1] += count
		}
		for i, row := range result.Rows {
			want := expected[i]
			if row["id"].Int != int64(i+1) || row["n"].Int != want {
				b.Fatalf("row %d=%+v expected %d", i+1, row, want)
			}
			total += row["n"].Int
		}
		if total != int64(b.N) {
			b.Fatalf("committed=%d want=%d", total, b.N)
		}
		if _, e = database.CheckIntegrity(); e != nil {
			b.Fatal(e)
		}
	}
	validate(database)
	for _, session := range sessions {
		if e = session.Close(); e != nil {
			b.Fatal(e)
		}
	}
	if voters > 0 {
		// Drain asynchronous application, then compare raw state on every replica.
		if e = nodes[0].propose(command{Version: 1, Kind: "fence"}, nil); e != nil {
			b.Fatal(e)
		}
		if durability == DurabilityAsync {
			seal, stop := context.WithTimeout(context.Background(), 30*time.Second)
			defer stop()
			if e = nodes[0].takeStableSnapshot(seal); e != nil {
				b.Fatal(e)
			}
			certificate := nodes[0].currentCertificate()
			for _, node := range nodes[1:] {
				if e = nodes[0].postJSON(seal, node.ControlAddress(), "/v1/durability/finalize", certificate, nil); e != nil {
					b.Fatal(e)
				}
			}
			finalToken = nodes[0].DurableToken()
		}
		index := nodes[0].applied.Load()
		deadline := time.Now().Add(30 * time.Second)
		for _, node := range nodes {
			for node.applied.Load() < index {
				if e = node.Err(); e != nil {
					b.Fatal(e)
				}
				if time.Now().After(deadline) {
					b.Fatal("replica catch-up timeout")
				}
				time.Sleep(time.Millisecond)
			}
		}
		digest := func(node *Node) [32]byte {
			snapshot, e := node.Snapshot()
			if e != nil {
				b.Fatal(e)
			}
			defer snapshot.Release()
			hash := sha256.New()
			if e = snapshot.(*stateSnapshot).write(hash); e != nil {
				b.Fatal(e)
			}
			var sum [32]byte
			copy(sum[:], hash.Sum(nil))
			return sum
		}
		expected := digest(nodes[0])
		for _, node := range nodes[1:] {
			if got := digest(node); got != expected {
				b.Fatal("replica state differs")
			}
		}
	}
	closeAll()
	if b.Failed() {
		return
	}
	// Reopen and verify exact committed totals; do not time close/recovery.
	if voters == 0 {
		database, e = db.Open(seedPath, options)
		if e != nil {
			b.Fatal(e)
		}
		closed = false
		validate(database)
		closeAll()
	} else if durability == DurabilityAsync {
		// Cold async members cannot vote until re-cloned. Verify the complete
		// benchmark totals through the supported offline stable recovery path.
		sources := make([]string, len(configurations))
		for i, c := range configurations {
			sources[i] = c.Directory
		}
		recovery, stop := context.WithTimeout(context.Background(), time.Minute)
		defer stop()
		destination := filepath.Join(root, "recovered-seed")
		if _, e = RecoverStable(recovery, RecoveryConfig{Sources: sources, Destination: destination, Token: configurations[0].Token, Fenced: true, Minimum: &finalToken}); e != nil {
			b.Fatal(e)
		}
		restored, e := db.Open(destination, options)
		if e != nil {
			b.Fatal(e)
		}
		defer restored.Close()
		validate(restored)
		if e = restored.Close(); e != nil {
			b.Fatal(e)
		}
	} else {
		nodes = nil
		closed = false
		recovery, stop := context.WithTimeout(context.Background(), time.Minute)
		defer stop()
		for _, c := range configurations {
			node, e := Open(recovery, c)
			if e != nil {
				b.Fatal(e)
			}
			nodes = append(nodes, node)
		}
		var leader *Node
		for leader == nil {
			for _, node := range nodes {
				if node.Status().Ready {
					leader = node
					break
				}
			}
			if leader != nil {
				break
			}
			select {
			case <-recovery.Done():
				b.Fatal("recovery leader timeout")
			case <-time.After(10 * time.Millisecond):
			}
		}
		if e = leader.WaitReady(recovery); e != nil {
			b.Fatal(e)
		}
		validate(leader.Database())
		closeAll()
	}
}
