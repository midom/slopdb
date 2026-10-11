// This separate module keeps the native DuckDB reference driver out of SlopDB.
package main

import (
	"bufio"
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"math"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"

	duckdb "github.com/duckdb/duckdb-go/v2"
)

type config struct {
	Data     string        `json:"data_directory"`
	Database string        `json:"database_file"`
	Root     string        `json:"query_root"`
	JSON     string        `json:"report_file"`
	Threads  int           `json:"threads"`
	Runs     int           `json:"runs"`
	Warmups  int           `json:"warmups"`
	Timeout  time.Duration `json:"query_timeout_ns"`
	Queries  string        `json:"query_selection"`
	Indexes  bool          `json:"secondary_indexes"`
	Memory   string        `json:"memory_limit"`
}

type sample struct {
	Status string  `json:"status"`
	Error  string  `json:"error,omitempty"`
	WallMS float64 `json:"wall_ms"`
	CPUMS  float64 `json:"process_cpu_ms"`
	Rows   int64   `json:"rows"`
}

type queryReport struct {
	Number    int      `json:"number"`
	SQL       string   `json:"sql"`
	PrepareMS float64  `json:"prepare_ms"`
	Plan      string   `json:"plan,omitempty"`
	Warmups   []sample `json:"warmups,omitempty"`
	Samples   []sample `json:"samples"`
	MinMS     float64  `json:"min_ms,omitempty"`
	MedianMS  float64  `json:"median_ms,omitempty"`
	Columns   []string `json:"columns,omitempty"`
	Results   [][]any  `json:"results,omitempty"`
}

type report struct {
	Engine         string           `json:"engine"`
	Version        string           `json:"version"`
	Driver         string           `json:"driver"`
	Config         config           `json:"config"`
	Started        time.Time        `json:"started_at"`
	GoVersion      string           `json:"go_version"`
	Platform       string           `json:"platform"`
	CPUs           int              `json:"cpus"`
	Manifest       json.RawMessage  `json:"data_manifest,omitempty"`
	TableRows      map[string]int64 `json:"table_rows"`
	LoadSeconds    float64          `json:"load_seconds"`
	ReopenSeconds  float64          `json:"reopen_seconds"`
	DatabaseBytes  int64            `json:"database_bytes_after_load"`
	CountsVerified bool             `json:"counts_verified_after_reopen"`
	RunSeconds     float64          `json:"run_seconds"`
	Successful     int              `json:"successful_executions"`
	TimedOut       int              `json:"timed_out_executions"`
	Failed         int              `json:"failed_executions"`
	PeakRSSKiB     int64            `json:"process_peak_rss_kib"`
	Queries        []queryReport    `json:"queries"`
}

var tables = []string{"region", "nation", "supplier", "customer", "part", "partsupp", "orders", "lineitem"}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "tpch-duckdb:", err)
		os.Exit(1)
	}
}
func run() (retErr error) {
	var c config
	flag.StringVar(&c.Data, "data", "", "same DBGEN .tbl data directory used by SlopDB (required)")
	flag.StringVar(&c.Database, "database", "", "fresh DuckDB database file (default: new directory under /tmp)")
	flag.StringVar(&c.Root, "query-root", "..", "directory containing shared schema, indexes, and query pack")
	flag.StringVar(&c.JSON, "json", "", "report path (default: database path plus .json)")
	flag.IntVar(&c.Threads, "threads", 1, "DuckDB execution threads")
	flag.IntVar(&c.Runs, "runs", 1, "measured executions per query")
	flag.IntVar(&c.Warmups, "warmups", 0, "unmeasured executions per query")
	flag.DurationVar(&c.Timeout, "timeout", 10*time.Second, "deadline per query")
	flag.StringVar(&c.Queries, "queries", "all", "all or comma-separated query numbers 1..22")
	flag.BoolVar(&c.Indexes, "indexes", true, "create the same secondary indexes as SlopDB")
	flag.StringVar(&c.Memory, "memory-limit", "1GB", "DuckDB memory limit")
	flag.Parse()
	if flag.NArg() != 0 || c.Data == "" || c.Threads < 1 || c.Runs < 1 || c.Warmups < 0 || c.Timeout <= 0 {
		return errors.New("invalid arguments: -data, positive threads/runs/timeout, nonnegative warmups required")
	}
	selected, err := queryNumbers(c.Queries)
	if err != nil {
		return err
	}
	c.Data, err = filepath.Abs(c.Data)
	if err != nil {
		return err
	}
	c.Root, err = filepath.Abs(c.Root)
	if err != nil {
		return err
	}
	for _, table := range tables {
		if _, err = os.Stat(filepath.Join(c.Data, table+".tbl")); err != nil {
			return err
		}
	}
	schema, err := os.ReadFile(filepath.Join(c.Root, "schema.sql"))
	if err != nil {
		return err
	}
	// SlopDB INTEGER and REAL hold int64 and float64. DuckDB's equivalent
	// physical types are BIGINT and DOUBLE, not its int32 INTEGER/float32 REAL.
	ddl := strings.ReplaceAll(strings.ReplaceAll(string(schema), "INTEGER", "BIGINT"), "REAL", "DOUBLE")
	if c.Database == "" {
		dir, e := os.MkdirTemp("/tmp", "slopdb-tpch-duckdb-*")
		if e != nil {
			return e
		}
		c.Database = filepath.Join(dir, "benchmark.duckdb")
	} else {
		c.Database, err = filepath.Abs(c.Database)
		if err != nil {
			return err
		}
		if _, e := os.Lstat(c.Database); e == nil {
			return errors.New("database path already exists")
		} else if !os.IsNotExist(e) {
			return e
		}
	}
	if c.JSON == "" {
		c.JSON = c.Database + ".json"
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	database, err := open(ctx, c)
	if err != nil {
		return err
	}
	defer func() {
		if database != nil {
			retErr = errors.Join(retErr, database.Close())
		}
	}()
	r := report{Engine: "DuckDB", Driver: "github.com/duckdb/duckdb-go/v2 v2.10506.0", Config: c, Started: time.Now().UTC(), GoVersion: runtime.Version(), Platform: runtime.GOOS + "/" + runtime.GOARCH, CPUs: runtime.NumCPU(), TableRows: map[string]int64{}}
	if raw, e := os.ReadFile(filepath.Join(c.Data, "manifest.json")); e == nil {
		if !json.Valid(raw) {
			return errors.New("invalid manifest JSON")
		}
		r.Manifest = raw
	}
	if err = database.QueryRowContext(ctx, "SELECT version()").Scan(&r.Version); err != nil {
		return err
	}
	fmt.Printf("DuckDB %s: threads=%d queries=%d runs=%d warmups=%d\n", r.Version, c.Threads, len(selected), c.Runs, c.Warmups)
	start := time.Now()
	if _, err = database.ExecContext(ctx, ddl); err != nil {
		return err
	}
	for _, table := range tables {
		count, e := loadTable(ctx, database, c.Data, table)
		if e != nil {
			return e
		}
		r.TableRows[table] = count
		fmt.Printf("  loaded %-8s %d rows\n", table, count)
	}
	if c.Indexes {
		indexes, e := os.ReadFile(filepath.Join(c.Root, "indexes.sql"))
		if e != nil {
			return e
		}
		if _, err = database.ExecContext(ctx, string(indexes)); err != nil {
			return err
		}
	}
	if _, err = database.ExecContext(ctx, "ANALYZE; CHECKPOINT"); err != nil {
		return err
	}
	r.LoadSeconds = time.Since(start).Seconds()
	if err = database.Close(); err != nil {
		database = nil
		return err
	}
	database = nil
	start = time.Now()
	database, err = open(ctx, c)
	if err != nil {
		return err
	}
	r.ReopenSeconds = time.Since(start).Seconds()
	for table, n := range r.TableRows {
		var count int64
		if err = database.QueryRowContext(ctx, "SELECT count(*) FROM "+table).Scan(&count); err != nil {
			return err
		}
		if count != n {
			return fmt.Errorf("reopen count mismatch for %s", table)
		}
	}
	if len(r.Manifest) > 0 {
		var manifest struct {
			Rows map[string]int64 `json:"rows"`
		}
		if err = json.Unmarshal(r.Manifest, &manifest); err != nil {
			return err
		}
		for table, n := range manifest.Rows {
			if r.TableRows[table] != n {
				return fmt.Errorf("manifest count mismatch for %s", table)
			}
		}
	}
	r.CountsVerified = true
	if info, e := os.Stat(c.Database); e == nil {
		r.DatabaseBytes = info.Size()
	}
	for _, n := range selected {
		if n == 15 {
			setup, e := os.ReadFile(filepath.Join(c.Root, "queries/q15-setup.sql"))
			if e != nil {
				return e
			}
			if _, err = database.ExecContext(ctx, string(setup)); err != nil {
				return err
			}
		}
	}
	fmt.Printf("Loaded in %.3fs; reopened in %.3fs; persisted counts verified.\n", r.LoadSeconds, r.ReopenSeconds)
	start = time.Now()
	for _, n := range selected {
		if ctx.Err() != nil {
			retErr = ctx.Err()
			break
		}
		q, e := benchmark(ctx, database, c, n)
		if e != nil {
			return e
		}
		for _, s := range q.Samples {
			switch s.Status {
			case "ok":
				r.Successful++
			case "timeout":
				r.TimedOut++
			default:
				r.Failed++
			}
		}
		r.Queries = append(r.Queries, q)
	}
	r.RunSeconds = time.Since(start).Seconds()
	var usage syscall.Rusage
	if syscall.Getrusage(syscall.RUSAGE_SELF, &usage) == nil {
		r.PeakRSSKiB = usage.Maxrss
	}
	raw, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		return err
	}
	if err = os.WriteFile(c.JSON, append(raw, '\n'), 0644); err != nil {
		return err
	}
	fmt.Printf("%d completed, %d timed out, %d failed; query phase %.3fs; report %s\n", r.Successful, r.TimedOut, r.Failed, r.RunSeconds, c.JSON)
	if r.TimedOut+r.Failed > 0 {
		return fmt.Errorf("%d timed out, %d failed query executions", r.TimedOut, r.Failed)
	}
	return retErr
}

func open(ctx context.Context, c config) (*sql.DB, error) {
	database, err := sql.Open("duckdb", c.Database)
	if err != nil {
		return nil, err
	}
	database.SetMaxOpenConns(1)
	if _, err = database.ExecContext(ctx, fmt.Sprintf("SET threads = %d; SET memory_limit = '%s'", c.Threads, strings.ReplaceAll(c.Memory, "'", "''"))); err != nil {
		database.Close()
		return nil, err
	}
	return database, nil
}

func loadTable(ctx context.Context, database *sql.DB, data, table string) (int64, error) {
	rows, err := database.QueryContext(ctx, "SELECT name, type FROM pragma_table_info(?) ORDER BY cid", table)
	if err != nil {
		return 0, err
	}
	var types []string
	for rows.Next() {
		var name, typ string
		if err = rows.Scan(&name, &typ); err != nil {
			rows.Close()
			return 0, err
		}
		types = append(types, typ)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return 0, err
	}
	file, err := os.Open(filepath.Join(data, table+".tbl"))
	if err != nil {
		return 0, err
	}
	defer file.Close()
	connection, err := database.Conn(ctx)
	if err != nil {
		return 0, err
	}
	defer connection.Close()
	var count int64
	err = connection.Raw(func(raw any) (retErr error) {
		appender, e := duckdb.NewAppenderFromConn(raw.(driver.Conn), "", table)
		if e != nil {
			return e
		}
		defer func() { retErr = errors.Join(retErr, appender.Close()) }()
		scanner := bufio.NewScanner(file)
		scanner.Buffer(make([]byte, 4096), 1024*1024)
		args := make([]driver.Value, len(types))
		for scanner.Scan() {
			if err := ctx.Err(); err != nil {
				return err
			}
			fields := strings.Split(strings.TrimSuffix(scanner.Text(), "\r"), "|")
			if len(fields) == len(types)+1 && fields[len(fields)-1] == "" {
				fields = fields[:len(fields)-1]
			}
			if len(fields) != len(types) {
				return fmt.Errorf("%s row %d: wrong field count", table, count+1)
			}
			for i, typ := range types {
				var e error
				switch typ {
				case "BIGINT":
					args[i], e = strconv.ParseInt(fields[i], 10, 64)
				case "DOUBLE":
					var f float64
					f, e = strconv.ParseFloat(fields[i], 64)
					if e == nil && (math.IsNaN(f) || math.IsInf(f, 0)) {
						e = errors.New("non-finite real")
					}
					args[i] = f
				case "VARCHAR":
					args[i] = fields[i]
				default:
					e = fmt.Errorf("unexpected loader type %s", typ)
				}
				if e != nil {
					return fmt.Errorf("%s row %d column %d: %w", table, count+1, i, e)
				}
			}
			if e = appender.AppendRow(args...); e != nil {
				return e
			}
			count++
		}
		if e = scanner.Err(); e != nil {
			return e
		}
		return appender.FlushWithCancel(ctx)
	})
	return count, err
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
			return nil, fmt.Errorf("invalid query number %q", s)
		}
		seen[n] = true
		numbers = append(numbers, n)
	}
	return numbers, nil
}

func benchmark(parent context.Context, database *sql.DB, c config, n int) (queryReport, error) {
	raw, err := os.ReadFile(filepath.Join(c.Root, fmt.Sprintf("queries/q%02d.sql", n)))
	if err != nil {
		return queryReport{}, err
	}
	q := queryReport{Number: n, SQL: string(raw)}
	start := time.Now()
	stmt, prepErr := database.PrepareContext(parent, q.SQL)
	q.PrepareMS = float64(time.Since(start)) / float64(time.Millisecond)
	if stmt != nil {
		defer stmt.Close()
	}
	planRows, e := database.QueryContext(parent, "EXPLAIN "+q.SQL)
	if e == nil {
		for planRows.Next() {
			var key, value string
			if planRows.Scan(&key, &value) == nil {
				q.Plan += value
			}
		}
		planRows.Close()
	}
	execute := func() (sample, []string, [][]any) {
		if prepErr != nil {
			return sample{Status: "prepare_error", Error: prepErr.Error()}, nil, nil
		}
		ctx, cancel := context.WithTimeout(parent, c.Timeout)
		defer cancel()
		cpu := cpuMS()
		start := time.Now()
		rows, err := stmt.QueryContext(ctx)
		var columns []string
		var result [][]any
		if err == nil {
			columns, err = rows.Columns()
			if err == nil {
				for rows.Next() {
					values := make([]any, len(columns))
					pointers := make([]any, len(columns))
					for i := range values {
						pointers[i] = &values[i]
					}
					if err = rows.Scan(pointers...); err != nil {
						break
					}
					result = append(result, values)
				}
			}
			if err == nil {
				err = rows.Err()
			}
			err = errors.Join(err, rows.Close())
		}
		elapsed := time.Since(start)
		s := sample{Status: "ok", WallMS: float64(elapsed) / float64(time.Millisecond), CPUMS: cpuMS() - cpu, Rows: int64(len(result))}
		if err != nil {
			s.Status = "error"
			s.Error = err.Error()
			if ctx.Err() != nil {
				s.Error = ctx.Err().Error()
				if errors.Is(ctx.Err(), context.DeadlineExceeded) {
					s.Status = "timeout"
				} else {
					s.Status = "canceled"
				}
			}
		}
		return s, columns, result
	}
	for i := 0; i < c.Warmups; i++ {
		s, _, _ := execute()
		q.Warmups = append(q.Warmups, s)
		if s.Status != "ok" {
			break
		}
	}
	var times []float64
	for i := 0; i < c.Runs; i++ {
		s, columns, result := execute()
		q.Samples = append(q.Samples, s)
		if s.Status == "ok" {
			times = append(times, s.WallMS)
			q.Columns, q.Results = columns, result
		}
		if parent.Err() != nil {
			break
		}
	}
	if len(times) > 0 {
		sort.Float64s(times)
		q.MinMS = times[0]
		q.MedianMS = times[len(times)/2]
		if len(times)%2 == 0 {
			q.MedianMS = (times[len(times)/2-1] + times[len(times)/2]) / 2
		}
	}
	fmt.Printf("Q%02d median=%.3fms rows=%d status=%s\n", n, q.MedianMS, len(q.Results), q.Samples[len(q.Samples)-1].Status)
	return q, nil
}

func cpuMS() float64 {
	var u syscall.Rusage
	if syscall.Getrusage(syscall.RUSAGE_SELF, &u) != nil {
		return 0
	}
	return float64(u.Utime.Sec+u.Stime.Sec)*1000 + float64(u.Utime.Usec+u.Stime.Usec)/1000
}
