// storage-snapshot loads and verifies equal-data, replicated TPC-C fixtures.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"slopdb/db"
	"slopdb/internal/benchload"
	"slopdb/storage"
	"strings"
	"time"
)

type table struct {
	Name    string   `json:"name"`
	Columns []string `json:"columns"`
	Rows    [][]any  `json:"rows"`
}
type snapshot struct {
	DDL    []string `json:"ddl"`
	Tables []table  `json:"tables"`
}

func main() {
	if e := run(); e != nil {
		fmt.Fprintln(os.Stderr, e)
		os.Exit(1)
	}
}
func run() (err error) {
	var mode, dir, file, report, verifyReport string
	var copies int
	var pageSize, loadBatchRows int64
	flag.StringVar(&mode, "mode", "load", "load, verify, maintenance (TPC-C), or compact (any database)")
	flag.StringVar(&dir, "dir", "", "database directory")
	flag.StringVar(&file, "snapshot", "", "portable TPC-C snapshot JSON")
	flag.StringVar(&report, "json", "", "output JSON report (default stdout)")
	flag.StringVar(&verifyReport, "verify-report", "", "TPC-H report whose complete answers must match after compact/reopen")
	flag.IntVar(&copies, "replicas", 128, "source replicas; item table is shared")
	flag.Int64Var(&pageSize, "page-size", 0, "SSTable data-block target in bytes")
	flag.Int64Var(&loadBatchRows, "load-batch-rows", benchload.DefaultBatchRows, "maximum rows per atomic bulk-load transaction")
	flag.Parse()
	if flag.NArg() != 0 || dir == "" || (file == "" && mode != "compact") || copies < 1 || pageSize < 0 || loadBatchRows <= 0 {
		return fmt.Errorf("valid dir, snapshot, replicas and page size required")
	}
	if mode != "load" && mode != "verify" && mode != "maintenance" && mode != "compact" {
		return fmt.Errorf("unknown mode %q", mode)
	}
	dir, err = filepath.Abs(dir)
	if err != nil {
		return err
	}
	if mode == "load" {
		if err = os.Mkdir(dir, 0755); err != nil {
			return err
		}
	}
	if mode == "compact" {
		return compact(dir, pageSize, report, verifyReport)
	}
	if verifyReport != "" {
		return fmt.Errorf("-verify-report requires -mode compact")
	}
	started := time.Now()
	var loadSeconds, maintenanceSeconds float64
	var loadTransactions int64
	source, err := os.Open(file)
	if err != nil {
		return err
	}
	defer source.Close()
	dec := json.NewDecoder(source)
	dec.UseNumber()
	var data snapshot
	if err = dec.Decode(&data); err != nil {
		return err
	}
	var maxWarehouse, maxHistory int64
	for _, t := range data.Tables {
		for _, row := range t.Rows {
			if len(row) != len(t.Columns) || len(row) == 0 {
				return fmt.Errorf("%s snapshot row has wrong width", t.Name)
			}
			for i, v := range row {
				if v != nil {
					if _, ok := v.(json.Number); !ok {
						return fmt.Errorf("%s snapshot contains non-integer data", t.Name)
					}
				}
				if n, ok := v.(json.Number); ok {
					row[i], err = n.Int64()
					if err != nil {
						return err
					}
				}
			}
			if t.Name == "warehouse" {
				maxWarehouse = max(maxWarehouse, row[0].(int64))
			}
			if t.Name == "history" {
				maxHistory = max(maxHistory, row[0].(int64))
			}
		}
	}
	if maxWarehouse <= 0 || maxHistory == math.MaxInt64 || int64(copies) > math.MaxInt64/maxWarehouse {
		return fmt.Errorf("snapshot replication overflows int64")
	}
	historyStride := maxHistory + 1
	if historyStride <= 0 || int64(copies) > math.MaxInt64/historyStride {
		return fmt.Errorf("history replication overflows int64")
	}
	shift := func(t table, row []any, rep int) []any {
		args := append([]any(nil), row...)
		for i, name := range t.Columns {
			if name == "w_id" || strings.HasSuffix(name, "_w_id") {
				args[i] = row[i].(int64) + int64(rep)*maxWarehouse
			}
			if name == "h_id" {
				args[i] = row[i].(int64) + int64(rep)*historyStride
			}
		}
		return args
	}
	opt := storage.DefaultOptions()
	opt.DisableWALSync = true
	opt.PageSize = pageSize
	database, err := db.Open(dir, opt)
	if err != nil {
		return err
	}
	defer func() {
		if database != nil {
			if e := database.Close(); err == nil {
				err = e
			}
		}
	}()
	if mode == "load" {
		for _, query := range data.DDL {
			if _, err = database.Exec(query); err != nil {
				return err
			}
		}
		for _, t := range data.Tables {
			n := copies
			if t.Name == "item" {
				n = 1
			}
			var replica, offset int
			stats, e := benchload.Load(context.Background(), database, t.Name, t.Columns, func(ctx context.Context) ([]any, error) {
				if err := ctx.Err(); err != nil {
					return nil, err
				}
				if len(t.Rows) == 0 || replica == n {
					return nil, io.EOF
				}
				row := shift(t, t.Rows[offset], replica)
				offset++
				if offset == len(t.Rows) {
					offset = 0
					replica++
					if replica%16 == 0 || replica == n {
						fmt.Fprintf(os.Stderr, "%s supplied %d/%d replicas to bulk loader\n", t.Name, replica, n)
					}
				}
				return row, nil
			}, loadBatchRows)
			if e != nil {
				return fmt.Errorf("%s bulk load after %d committed rows: %w", t.Name, stats.Rows, e)
			}
			loadTransactions += stats.Transactions
		}
		if err = database.Close(); err != nil {
			return err
		}
		database = nil
		database, err = db.Open(dir, opt)
		if err != nil {
			return err
		}
	}
	if mode == "load" {
		loadSeconds = time.Since(started).Seconds()
	}
	if mode == "maintenance" {
		maintenanceStart := time.Now()
		if _, err = database.Exec("VACUUM"); err != nil {
			return err
		}
		if err = database.Close(); err != nil {
			return err
		}
		database = nil
		database, err = db.Open(dir, opt)
		if err != nil {
			return err
		}
		maintenanceSeconds = time.Since(maintenanceStart).Seconds()
	}
	verifyStart := time.Now()
	primary := map[string]string{"warehouse": "w_id", "district": "w_id,d_id", "customer": "w_id,d_id,c_id", "item": "i_id", "stock": "w_id,i_id", "orders": "w_id,d_id,o_id", "new_order": "w_id,d_id,o_id", "order_line": "w_id,d_id,o_id,number", "history": "h_id"}
	counts := map[string]int64{}
	var checked int64
	for _, t := range data.Tables {
		n := copies
		if t.Name == "item" {
			n = 1
		}
		count, e := database.Exec("SELECT COUNT(*) AS n FROM " + t.Name)
		if e != nil {
			return e
		}
		counts[t.Name] = count.Rows[0]["n"].Int
		if counts[t.Name] != int64(n*len(t.Rows)) {
			return fmt.Errorf("%s count mismatch %d", t.Name, counts[t.Name])
		}
		for rep := 0; rep < n; rep++ {
			filter := ""
			if t.Name == "history" {
				filter = fmt.Sprintf(" WHERE h_id >= %d AND h_id <= %d", int64(rep)*historyStride, int64(rep+1)*historyStride-1)
			} else if t.Name != "item" {
				filter = fmt.Sprintf(" WHERE w_id >= %d AND w_id <= %d", int64(rep)*maxWarehouse+1, int64(rep+1)*maxWarehouse)
			}
			rows, e := database.Exec("SELECT * FROM " + t.Name + filter + " ORDER BY " + primary[t.Name] + fmt.Sprintf(" LIMIT %d", len(t.Rows)+1))
			if e != nil {
				return e
			}
			if len(rows.Rows) != len(t.Rows) {
				return fmt.Errorf("%s replica %d row count mismatch", t.Name, rep)
			}
			for i, row := range t.Rows {
				expected := shift(t, row, rep)
				for j, v := range expected {
					actual := rows.Rows[i][t.Columns[j]]
					if v == nil {
						if !actual.Null {
							return fmt.Errorf("%s NULL mismatch", t.Name)
						}
						continue
					}
					if actual.Null || actual.Int != v.(int64) {
						return fmt.Errorf("%s replica %d row %d %s got %v want %v", t.Name, rep, i, t.Columns[j], actual, v)
					}
				}
			}
			checked += int64(len(rows.Rows))
			if (rep+1)%32 == 0 || rep+1 == n {
				fmt.Fprintf(os.Stderr, "%s verified %d/%d replicas\n", t.Name, rep+1, n)
			}
		}
	}
	stats := database.Stats()
	result := map[string]any{"zstd_window_bytes": storage.SSTableZstdWindowSize, "page_size_bytes": database.PageSize(), "requested_page_size_bytes": pageSize, "load_seconds": loadSeconds, "load_mode": "bulk_insert", "load_batch_rows": loadBatchRows, "load_transactions": loadTransactions, "maintenance_seconds": maintenanceSeconds, "verification_seconds": time.Since(verifyStart).Seconds(), "rows": counts, "verified_rows": checked, "all_values_verified": true, "replicas": copies, "warehouse_stride": maxWarehouse, "history_stride": historyStride, "levels": stats.Levels, "finished_at": time.Now().UTC()}
	if err = database.Close(); err != nil {
		return err
	}
	database = nil
	if report != "" {
		raw, e := json.MarshalIndent(result, "", "  ")
		if e != nil {
			return e
		}
		return os.WriteFile(report, append(raw, '\n'), 0644)
	}
	return json.NewEncoder(os.Stdout).Encode(result)
}

func compact(dir string, pageSize int64, report, verificationReport string) (err error) {
	opt := storage.DefaultOptions()
	opt.PageSize = pageSize
	opt.DisableWALSync = true
	database, err := db.Open(dir, opt)
	if err != nil {
		return err
	}
	defer func() {
		if database != nil {
			if e := database.Close(); err == nil {
				err = e
			}
		}
	}()
	counts := func(d *db.Database) (map[string]int64, error) {
		names, e := d.Exec("SELECT name FROM sqlite_master WHERE type = 'table'")
		if e != nil {
			return nil, e
		}
		result := map[string]int64{}
		for _, row := range names.Rows {
			n := row["name"].Text
			q, e := d.Exec("SELECT COUNT(*) AS n FROM \"" + strings.ReplaceAll(n, "\"", "\"\"") + "\"")
			if e != nil {
				return nil, e
			}
			result[n] = q.Rows[0]["n"].Int
		}
		return result, nil
	}
	before, err := counts(database)
	if err != nil {
		return err
	}
	start := time.Now()
	if _, err = database.Exec("VACUUM"); err != nil {
		return err
	}
	if err = database.Close(); err != nil {
		return err
	}
	database = nil
	elapsed := time.Since(start).Seconds()
	database, err = db.Open(dir, opt)
	if err != nil {
		return err
	}
	after, err := counts(database)
	if err != nil {
		return err
	}
	if len(before) != len(after) {
		return fmt.Errorf("table count changed after compaction")
	}
	for n, want := range before {
		if after[n] != want {
			return fmt.Errorf("%s count changed after compaction", n)
		}
	}
	verificationStart := time.Now()
	answersChecked := 0
	if verificationReport != "" {
		answersChecked, err = verifyQueryReport(database, verificationReport, after, 180*time.Second)
		if err != nil {
			return err
		}
	}
	result := map[string]any{"zstd_window_bytes": storage.SSTableZstdWindowSize, "page_size_bytes": database.PageSize(), "rows": after, "counts_verified_after_reopen": true, "query_answers_verified_after_maintenance": answersChecked, "query_verification_seconds": time.Since(verificationStart).Seconds(), "maintenance_seconds": elapsed, "levels": database.Stats().Levels}
	if err = database.Close(); err != nil {
		return err
	}
	database = nil
	if report != "" {
		raw, e := json.MarshalIndent(result, "", "  ")
		if e != nil {
			return e
		}
		return os.WriteFile(report, append(raw, '\n'), 0644)
	}
	return json.NewEncoder(os.Stdout).Encode(result)
}
