// tpch runs the 22 TPC-H query shapes on DBGEN .tbl files. It reports local
// analytical timings and correctness outcomes, not a compliant QphH score.
package main

import (
	"context"
	"embed"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"

	"slopdb/db"
	"slopdb/internal/benchload"
	"slopdb/sql"
	"slopdb/storage"
)

//go:embed schema.sql indexes.sql queries/*.sql
var assets embed.FS

type config struct {
	LoadOnly      bool          `json:"load_only"`
	LoadBatchRows int64         `json:"load_batch_rows"`
	PageSize      int64         `json:"requested_page_size_bytes"`
	Data          string        `json:"data_directory"`
	Dir           string        `json:"database_directory"`
	JSON          string        `json:"report_file,omitempty"`
	ProfileDir    string        `json:"profile_directory,omitempty"`
	Queries       string        `json:"query_selection"`
	Runs          int           `json:"runs"`
	Warmups       int           `json:"warmups"`
	Timeout       time.Duration `json:"query_timeout_ns"`
	Prepared      bool          `json:"prepared_statements"`
	Indexes       bool          `json:"secondary_indexes"`
	WALFsync      bool          `json:"wal_fsync"`
}

type sample struct {
	Status      string  `json:"status"`
	Error       string  `json:"error,omitempty"`
	WallMS      float64 `json:"wall_ms"`
	CPUMS       float64 `json:"process_cpu_ms"`
	AllocBytes  uint64  `json:"allocated_bytes"`
	Allocations uint64  `json:"allocations"`
	Rows        int64   `json:"rows"`
}

type queryReport struct {
	Number     int      `json:"number"`
	Name       string   `json:"name"`
	SQL        string   `json:"sql"`
	PrepareMS  float64  `json:"prepare_ms"`
	Plan       []string `json:"plan,omitempty"`
	PlanError  string   `json:"plan_error,omitempty"`
	Warmups    []sample `json:"warmups,omitempty"`
	Samples    []sample `json:"samples"`
	MinMS      float64  `json:"min_ms,omitempty"`
	MedianMS   float64  `json:"median_ms,omitempty"`
	Columns    []string `json:"columns,omitempty"`
	Results    [][]any  `json:"results,omitempty"`
	CPUProfile string   `json:"cpu_profile,omitempty"`
}

type report struct {
	ZstdWindowBytes  int64            `json:"zstd_window_bytes"`
	QueryBenchmarked bool             `json:"query_benchmarked"`
	LoadMode         string           `json:"load_mode"`
	LoadTransactions int64            `json:"load_transactions"`
	PageSize         int64            `json:"page_size_bytes"`
	Config           config           `json:"config"`
	Started          time.Time        `json:"started_at"`
	GoVersion        string           `json:"go_version"`
	Platform         string           `json:"platform"`
	CPUs             int              `json:"cpus"`
	DataManifest     json.RawMessage  `json:"data_manifest,omitempty"`
	TableRows        map[string]int64 `json:"table_rows"`
	LoadSeconds      float64          `json:"load_seconds"`
	ReopenSeconds    float64          `json:"reopen_seconds"`
	DatabaseBytes    int64            `json:"database_bytes_after_load"`
	CountsVerified   bool             `json:"counts_verified_after_reopen"`
	SetupSQL         string           `json:"setup_sql,omitempty"`
	RunSeconds       float64          `json:"run_seconds"`
	Successful       int              `json:"successful_executions"`
	TimedOut         int              `json:"timed_out_executions"`
	Failed           int              `json:"failed_executions"`
	PeakRSSKiB       int64            `json:"process_peak_rss_kib"`
	Queries          []queryReport    `json:"queries"`
}

var names = []string{"Pricing summary", "Minimum cost supplier", "Shipping priority", "Order priority", "Local supplier volume", "Forecasting revenue", "Volume shipping", "National market share", "Product type profit", "Returned item reporting", "Important stock identification", "Shipping modes", "Customer distribution", "Promotion effect", "Top supplier", "Parts supplier relationship", "Small quantity order revenue", "Large volume customer", "Discounted revenue", "Potential part promotion", "Suppliers who kept orders waiting", "Global sales opportunity"}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "tpch:", err)
		os.Exit(1)
	}
}

func run() (retErr error) {
	var c config
	flag.StringVar(&c.Data, "data", "", "directory containing all eight DBGEN .tbl files (required)")
	flag.StringVar(&c.Dir, "dir", "", "fresh database directory; default uses /tmp")
	flag.StringVar(&c.JSON, "json", "", "JSON report path (default: benchmark.json in the database directory)")
	flag.StringVar(&c.ProfileDir, "profile-dir", "", "write a separate CPU profile for each query (diagnostic run)")
	flag.StringVar(&c.Queries, "queries", "all", "all or comma-separated query numbers 1..22")
	flag.IntVar(&c.Runs, "runs", 1, "measured executions per query")
	flag.IntVar(&c.Warmups, "warmups", 0, "unmeasured executions per query")
	flag.DurationVar(&c.Timeout, "timeout", 10*time.Second, "deadline for each query execution")
	flag.BoolVar(&c.Prepared, "prepared", true, "prepare each query once before executing")
	flag.BoolVar(&c.Indexes, "indexes", true, "create the documented secondary indexes after loading")
	flag.BoolVar(&c.WALFsync, "wal-fsync", false, "enable WAL fsync during loading; WAL stays enabled")
	flag.Int64Var(&c.PageSize, "page-size", 0, "uncompressed SSTable data-block target in bytes (0 uses persisted/default target)")
	flag.BoolVar(&c.LoadOnly, "load-only", false, "bulk load, index and verify persisted counts without executing TPC-H queries")
	flag.Int64Var(&c.LoadBatchRows, "load-batch-rows", benchload.DefaultBatchRows, "maximum rows per atomic bulk-load transaction")
	flag.Parse()
	if flag.NArg() != 0 || c.PageSize < 0 || c.LoadBatchRows <= 0 || c.Data == "" || c.Runs < 1 || c.Warmups < 0 || c.Timeout <= 0 {
		return errors.New("-data is required; runs and timeout must be positive; warmups must be nonnegative")
	}
	selected, err := queryNumbers(c.Queries)
	if err != nil {
		return err
	}
	if c.LoadOnly {
		selected = nil
	}
	c.Data, err = filepath.Abs(c.Data)
	if err != nil {
		return err
	}
	if err = checkData(c.Data); err != nil {
		return err
	}
	if c.ProfileDir != "" {
		c.ProfileDir, err = filepath.Abs(c.ProfileDir)
		if err != nil {
			return err
		}
		if err = os.MkdirAll(c.ProfileDir, 0755); err != nil {
			return err
		}
	}
	if c.Dir == "" {
		c.Dir, err = os.MkdirTemp("/tmp", "slopdb-tpch-*")
	} else {
		c.Dir, err = filepath.Abs(c.Dir)
		if err == nil {
			err = os.Mkdir(c.Dir, 0755)
		}
	}
	if err != nil {
		return err
	} // Never erase or reuse an existing database.
	if c.JSON == "" {
		c.JSON = filepath.Join(c.Dir, "benchmark.json")
	}
	parent, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	options := storage.DefaultOptions()
	options.PageSize = c.PageSize
	options.DisableWALSync = !c.WALFsync
	database, err := db.Open(c.Dir, options)
	if err != nil {
		return err
	}
	defer func() {
		if database != nil {
			retErr = errors.Join(retErr, database.Close())
		}
	}()
	r := report{ZstdWindowBytes: storage.SSTableZstdWindowSizeForBlock(database.PageSize()), QueryBenchmarked: !c.LoadOnly, LoadMode: "bulk_insert", PageSize: database.PageSize(), Config: c, Started: time.Now().UTC(), GoVersion: runtime.Version(), Platform: runtime.GOOS + "/" + runtime.GOARCH, CPUs: runtime.NumCPU()}
	if manifest, e := os.ReadFile(filepath.Join(c.Data, "manifest.json")); e == nil {
		if !json.Valid(manifest) {
			return errors.New("invalid data manifest JSON")
		}
		r.DataManifest = manifest
	}
	fmt.Printf("TPC-H query workload: %d queries, runs=%d warmups=%d timeout=%s prepared=%t indexes=%t\n", len(selected), c.Runs, c.Warmups, c.Timeout, c.Prepared, c.Indexes)
	fmt.Printf("Data: %s\nDatabase: %s\nWAL enabled; WAL fsync=%t. Single query stream.\n", c.Data, c.Dir, c.WALFsync)
	loadStart := time.Now()
	r.TableRows, r.LoadTransactions, err = load(parent, database, c)
	if err != nil {
		return fmt.Errorf("load: %w", err)
	}
	r.LoadSeconds = time.Since(loadStart).Seconds()
	if err = database.Close(); err != nil {
		database = nil
		return err
	}
	database = nil
	reopenStart := time.Now()
	database, err = db.Open(c.Dir, options)
	if err != nil {
		return err
	}
	r.ReopenSeconds = time.Since(reopenStart).Seconds()
	if err = verifyCounts(parent, database, r.TableRows); err != nil {
		return err
	}
	r.CountsVerified = true
	r.DatabaseBytes, err = directoryBytes(c.Dir)
	if err != nil {
		return err
	}
	fmt.Printf("Loaded and indexed in %.3fs, reopened in %.3fs; persisted row counts verified.\n", r.LoadSeconds, r.ReopenSeconds)
	fmt.Printf("Rows: %v; database bytes=%d\n", r.TableRows, r.DatabaseBytes)
	needsView := c.LoadOnly
	for _, n := range selected {
		needsView = needsView || n == 15
	}
	if needsView {
		setup, _ := assets.ReadFile("queries/q15-setup.sql")
		r.SetupSQL = string(setup)
		if _, err = database.ExecContext(parent, string(setup)); err != nil {
			return fmt.Errorf("Q15 view: %w", err)
		}
	}
	start := time.Now()
	for _, n := range selected {
		if parent.Err() != nil {
			retErr = parent.Err()
			break
		}
		qr := benchmarkQuery(parent, database, c, n)
		for _, s := range qr.Samples {
			switch s.Status {
			case "ok":
				r.Successful++
			case "timeout":
				r.TimedOut++
			default:
				r.Failed++
			}
		}
		r.Queries = append(r.Queries, qr)
	}
	r.RunSeconds = time.Since(start).Seconds()
	var usage syscall.Rusage
	if syscall.Getrusage(syscall.RUSAGE_SELF, &usage) == nil {
		r.PeakRSSKiB = usage.Maxrss
	}
	fmt.Printf("Executions: %d completed, %d timed out, %d failed; query phase %.3fs\n", r.Successful, r.TimedOut, r.Failed, r.RunSeconds)
	if c.JSON != "" {
		data, e := json.MarshalIndent(r, "", "  ")
		if e != nil {
			return e
		}
		if e = os.WriteFile(c.JSON, append(data, '\n'), 0644); e != nil {
			return e
		}
		fmt.Println("Report:", c.JSON)
	}
	// Failures/timeouts are benchmark outcomes: preserve the full report and make
	// the exit status meaningful for automated runs.
	if r.Failed+r.TimedOut > 0 {
		retErr = errors.Join(retErr, fmt.Errorf("%d failed, %d timed out query executions (see report)", r.Failed, r.TimedOut))
	}
	return retErr
}

func queryNumbers(selection string) ([]int, error) {
	if selection == "all" {
		numbers := make([]int, 22)
		for i := range numbers {
			numbers[i] = i + 1
		}
		return numbers, nil
	}
	seen := map[int]bool{}
	var numbers []int
	for _, s := range strings.Split(selection, ",") {
		n, e := strconv.Atoi(strings.TrimSpace(s))
		if e != nil || n < 1 || n > 22 || seen[n] {
			return nil, fmt.Errorf("invalid or repeated query number %q", s)
		}
		seen[n] = true
		numbers = append(numbers, n)
	}
	return numbers, nil
}

func benchmarkQuery(parent context.Context, database *db.Database, c config, n int) queryReport {
	raw, _ := assets.ReadFile(fmt.Sprintf("queries/q%02d.sql", n))
	text := string(raw)
	q := queryReport{Number: n, Name: names[n-1], SQL: text}
	var stmt *db.Stmt
	var prepErr error
	if c.Prepared {
		start := time.Now()
		stmt, prepErr = database.PrepareContext(parent, text)
		q.PrepareMS = float64(time.Since(start)) / float64(time.Millisecond)
		if stmt != nil {
			defer stmt.Close()
		}
	}
	// EXPLAIN describes available operators; it is not an instrumented actual plan.
	explainCtx, cancel := context.WithTimeout(parent, c.Timeout)
	plan, e := database.ExecContext(explainCtx, "EXPLAIN "+text)
	cancel()
	if e != nil {
		q.PlanError = e.Error()
	} else {
		for _, row := range plan.Rows {
			q.Plan = append(q.Plan, row["opcode"].Text+" "+row["p4"].Text+" "+row["comment"].Text)
		}
	}
	execute := func() (sample, *db.ExecResult) {
		if prepErr != nil {
			return sample{Status: "prepare_error", Error: prepErr.Error()}, nil
		}
		ctx, cancel := context.WithTimeout(parent, c.Timeout)
		defer cancel()
		var before, after runtime.MemStats
		runtime.ReadMemStats(&before)
		cpuBefore := cpuMS()
		start := time.Now()
		var result *db.ExecResult
		var err error
		pprof.Do(ctx, pprof.Labels("query", fmt.Sprintf("Q%02d", n)), func(ctx context.Context) {
			if stmt != nil {
				result, err = stmt.ExecContext(ctx)
			} else {
				result, err = database.ExecContext(ctx, text)
			}
		})
		elapsed := time.Since(start)
		cpuAfter := cpuMS()
		runtime.ReadMemStats(&after)
		s := sample{Status: "ok", WallMS: float64(elapsed) / float64(time.Millisecond), CPUMS: cpuAfter - cpuBefore, AllocBytes: after.TotalAlloc - before.TotalAlloc, Allocations: after.Mallocs - before.Mallocs}
		if err != nil {
			s.Status = "error"
			s.Error = err.Error()
			if errors.Is(err, context.DeadlineExceeded) {
				s.Status = "timeout"
			} else if errors.Is(err, context.Canceled) {
				s.Status = "canceled"
			}
		} else {
			s.Rows = int64(len(result.Rows))
		}
		return s, result
	}
	fmt.Printf("Q%02d %-35s ", n, q.Name)
	for i := 0; i < c.Warmups; i++ {
		s, _ := execute()
		q.Warmups = append(q.Warmups, s)
		if s.Status != "ok" {
			break
		}
	}
	var profile *os.File
	if c.ProfileDir != "" {
		path := filepath.Join(c.ProfileDir, fmt.Sprintf("q%02d.cpu.pprof", n))
		var err error
		profile, err = os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0644)
		if err == nil {
			err = pprof.StartCPUProfile(profile)
		}
		if err != nil {
			q.Samples = append(q.Samples, sample{Status: "profile_error", Error: err.Error()})
			if profile != nil {
				profile.Close()
			}
			fmt.Println(err)
			return q
		}
		q.CPUProfile = path
	}
	var times []float64
	for i := 0; i < c.Runs; i++ {
		s, result := execute()
		q.Samples = append(q.Samples, s)
		if s.Status == "ok" {
			times = append(times, s.WallMS)
			q.Columns, q.Results = outputValues(result)
		}
		if parent.Err() != nil {
			break
		}
	}
	if profile != nil {
		pprof.StopCPUProfile()
		profile.Close()
	}
	if len(times) > 0 {
		sort.Float64s(times)
		q.MinMS = times[0]
		q.MedianMS = times[len(times)/2]
		if len(times)%2 == 0 {
			q.MedianMS = (times[len(times)/2-1] + times[len(times)/2]) / 2
		}
	}
	statuses := map[string]int{}
	for _, s := range q.Samples {
		statuses[s.Status]++
	}
	if len(times) > 0 {
		fmt.Printf("%v median=%.3fms rows=%d\n", statuses, q.MedianMS, len(q.Results))
	} else {
		fmt.Printf("%v no completed samples; elapsed=%.3fms\n", statuses, q.Samples[len(q.Samples)-1].WallMS)
	}
	for _, s := range q.Samples {
		if s.Status != "ok" {
			fmt.Printf("  %s\n", s.Error)
			break
		}
	}
	return q
}

func outputValues(result *db.ExecResult) ([]string, [][]any) {
	columns := make([]string, len(result.Columns))
	for i, c := range result.Columns {
		columns[i] = c.Name
	}
	rows := make([][]any, len(result.Rows))
	for i, row := range result.Rows {
		values := make([]any, len(columns))
		for j, name := range columns {
			v := row[name]
			if v.Null {
				continue
			}
			switch v.Type {
			case sql.ColInteger:
				values[j] = v.Int
			case sql.ColReal:
				values[j] = v.Real
			case sql.ColText:
				values[j] = v.Text
			case sql.ColBlob:
				values[j] = v.Blob
			}
		}
		rows[i] = values
	}
	return columns, rows
}

func cpuMS() float64 {
	var u syscall.Rusage
	if syscall.Getrusage(syscall.RUSAGE_SELF, &u) != nil {
		return 0
	}
	return float64(u.Utime.Sec+u.Stime.Sec)*1000 + float64(u.Utime.Usec+u.Stime.Usec)/1000
}
func directoryBytes(dir string) (int64, error) {
	var size int64
	err := filepath.WalkDir(dir, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		info, err := entry.Info()
		if err == nil {
			size += info.Size()
		}
		return err
	})
	return size, err
}
