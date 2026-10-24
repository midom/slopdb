package replication

import (
	"context"
	"encoding/json"
	"flag"
	"os"
	"runtime"
	"runtime/pprof"
	"runtime/trace"
	"testing"
	"time"

	"slopdb/db"
)

var benchmarkLatencyPath = flag.String("raft-bench-latencies", "", "diagnostic per-transaction stage timings JSON; one case/sample")
var benchmarkTracePath = flag.String("raft-bench-trace", "", "execution trace for timed transaction window; one case/sample")
var benchmarkContentionPath = flag.String("raft-bench-contention", "", "timed block/mutex profile prefix; one case/sample")

type benchmarkLatencyRecord struct {
	Worker                                        int
	StartNS, BeginNS, UpdateNS, CommitNS, TotalNS int64
}
type benchmarkLatencyDiagnostics struct {
	b                 *testing.B
	records           []benchmarkLatencyRecord
	origin            time.Time
	end               time.Time
	traceFile         *os.File
	previousMutexRate int
}

func newBenchmarkLatencyDiagnostics(b *testing.B, count int) *benchmarkLatencyDiagnostics {
	if *benchmarkLatencyPath == "" && *benchmarkTracePath == "" && *benchmarkContentionPath == "" {
		return nil
	}
	return &benchmarkLatencyDiagnostics{b: b, records: make([]benchmarkLatencyRecord, count)}
}

func (d *benchmarkLatencyDiagnostics) start() {
	if d == nil {
		return
	}
	if *benchmarkTracePath != "" {
		var err error
		d.traceFile, err = os.Create(*benchmarkTracePath)
		if err == nil {
			err = trace.Start(d.traceFile)
		}
		if err != nil {
			d.b.Fatal(err)
		}
	}
	if *benchmarkContentionPath != "" {
		runtime.SetBlockProfileRate(1)
		d.previousMutexRate = runtime.SetMutexProfileFraction(1)
	}
}

func (d *benchmarkLatencyDiagnostics) markStart() {
	if d != nil {
		d.origin = time.Now()
	}
}

func (d *benchmarkLatencyDiagnostics) worker(id int) (context.Context, func()) {
	if d == nil || *benchmarkTracePath == "" {
		return context.Background(), func() {}
	}
	ctx, task := trace.NewTask(context.Background(), "benchmark.worker")
	trace.Logf(ctx, "worker", "%d", id)
	return ctx, task.End
}

func (d *benchmarkLatencyDiagnostics) transaction(ctx context.Context, operation, worker int, session *db.Session, statement *db.Stmt, row int64) (int64, error) {
	start := time.Now()
	var transaction, phase *trace.Region
	if *benchmarkTracePath != "" {
		transaction = trace.StartRegion(ctx, "transaction")
		defer transaction.End()
		phase = trace.StartRegion(ctx, "BEGIN")
	}
	_, err := session.Exec("BEGIN")
	if phase != nil {
		phase.End()
	}
	begin := time.Now()
	if err != nil {
		return 0, err
	}
	if transaction != nil {
		phase = trace.StartRegion(ctx, "UPDATE")
	}
	_, err = session.ExecPrepared(statement, row)
	if phase != nil {
		phase.End()
	}
	update := time.Now()
	if err != nil {
		_, _ = session.Exec("ROLLBACK")
		return 0, err
	}
	if transaction != nil {
		phase = trace.StartRegion(ctx, "COMMIT")
	}
	_, err = session.Exec("COMMIT")
	if phase != nil {
		phase.End()
	}
	end := time.Now()
	if err != nil {
		return 0, err
	}
	total := end.Sub(start).Nanoseconds()
	d.records[operation] = benchmarkLatencyRecord{Worker: worker, StartNS: start.Sub(d.origin).Nanoseconds(), BeginNS: begin.Sub(start).Nanoseconds(), UpdateNS: update.Sub(begin).Nanoseconds(), CommitNS: end.Sub(update).Nanoseconds(), TotalNS: total}
	return total, nil
}

// Freeze the observed window before stopping a CPU profile, which can wait for
// its writer. Trace/profile/JSON serialization must not enter the CPU sample.
func (d *benchmarkLatencyDiagnostics) endWindow() {
	if d == nil || !d.end.IsZero() {
		return
	}
	d.end = time.Now()
	if *benchmarkContentionPath != "" {
		runtime.SetBlockProfileRate(0)
		runtime.SetMutexProfileFraction(d.previousMutexRate)
	}
}

func (d *benchmarkLatencyDiagnostics) stop() {
	if d == nil {
		return
	}
	d.endWindow()
	if d.traceFile != nil {
		trace.Stop()
		if err := d.traceFile.Close(); err != nil {
			d.b.Fatal(err)
		}
	}
	if *benchmarkContentionPath != "" {
		for _, name := range []string{"block", "mutex"} {
			file, err := os.Create(*benchmarkContentionPath + "." + name + ".pprof")
			if err != nil {
				d.b.Fatal(err)
			}
			err = pprof.Lookup(name).WriteTo(file, 0)
			closeErr := file.Close()
			if err != nil {
				d.b.Fatal(err)
			}
			if closeErr != nil {
				d.b.Fatal(closeErr)
			}
		}
	}
	if *benchmarkLatencyPath != "" {
		file, err := os.Create(*benchmarkLatencyPath)
		if err != nil {
			d.b.Fatal(err)
		}
		err = json.NewEncoder(file).Encode(struct {
			Benchmark              string
			StartUnixNS, EndUnixNS int64
			Records                []benchmarkLatencyRecord
		}{d.b.Name(), d.origin.UnixNano(), d.end.UnixNano(), d.records})
		closeErr := file.Close()
		if err != nil {
			d.b.Fatal(err)
		}
		if closeErr != nil {
			d.b.Fatal(closeErr)
		}
	}
}
