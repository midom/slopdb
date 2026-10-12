// storage-sql measures a canonical SQL dump after loading and full compaction.
// The dump and independently generated typed fingerprints are identical inputs
// to SlopDB, SQLite and DuckDB; no transactional benchmark is executed here.
package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime/debug"
	"strings"
	"syscall"
	"time"

	"slopdb/db"
	"slopdb/sql"
	"slopdb/storage"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
func run(args []string) (err error) {
	flags := flag.NewFlagSet("storage-sql", flag.ContinueOnError)
	var source, manifestPath, dir, report string
	var pageSize int64
	flags.StringVar(&source, "sql", "", "canonical SQL dump")
	flags.StringVar(&manifestPath, "manifest", "", "independent logical fingerprints and SQL hash")
	flags.StringVar(&dir, "dir", "", "new database directory (must not exist)")
	flags.StringVar(&report, "json", "", "success report output file")
	flags.Int64Var(&pageSize, "page-size", 4096, "uncompressed SSTable block target")
	if err = flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 || source == "" || manifestPath == "" || dir == "" || report == "" {
		return fmt.Errorf("-sql, -manifest, -dir and -json are required")
	}
	if dir, err = filepath.Abs(dir); err != nil {
		return err
	}
	if report, err = filepath.Abs(report); err != nil {
		return err
	}
	if relative, e := filepath.Rel(dir, report); e != nil || (relative != ".." && !strings.HasPrefix(relative, ".."+string(filepath.Separator))) {
		return fmt.Errorf("report must be outside the measured database")
	}
	if _, e := os.Lstat(report); e == nil {
		return fmt.Errorf("report already exists: %s", report)
	} else if !os.IsNotExist(e) {
		return e
	}
	raw, err := os.ReadFile(manifestPath)
	if err != nil {
		return err
	}
	var m manifest
	if err = json.Unmarshal(raw, &m); err != nil {
		return err
	}
	if err = m.validate(); err != nil {
		return err
	}
	in, err := os.Open(source)
	if err != nil {
		return err
	}
	defer in.Close()
	h := sha256.New()
	size, err := io.Copy(h, in)
	if err != nil {
		return err
	}
	if size != m.SQLBytes || hex.EncodeToString(h.Sum(nil)) != m.SQLSHA256 {
		return fmt.Errorf("canonical SQL hash/length mismatch")
	}
	if _, err = in.Seek(0, io.SeekStart); err != nil {
		return err
	}
	if err = os.Mkdir(dir, 0755); err != nil {
		return fmt.Errorf("fresh database directory: %w", err)
	}
	opt := storage.DefaultOptions()
	opt.PageSize = pageSize
	opt.DisableWALSync = true
	d, err := db.Open(dir, opt)
	if err != nil {
		return err
	}
	defer func() {
		if d != nil {
			e := d.Close()
			if err == nil {
				err = e
			}
		}
	}()
	started := time.Now()
	h.Reset()
	reader := newStatementReader(io.TeeReader(in, h))
	var statements int64
	var transaction bool
	var transactions int64
	for {
		query, e := reader.next()
		if e == io.EOF {
			break
		}
		if e != nil {
			return fmt.Errorf("statement %d: %w", statements+1, e)
		}
		if _, e = d.Exec(query); e != nil {
			return fmt.Errorf("statement %d: %w", statements+1, e)
		}
		first := strings.ToUpper(sql.NewTokenizer(query).Next().Value)
		switch first {
		case "BEGIN", "START":
			transaction = true
		case "COMMIT", "END":
			transaction = false
			transactions++
		case "ROLLBACK":
			transaction = false
		}
		statements++
		if statements%4096 == 0 {
			fmt.Fprintf(os.Stderr, "executed %d canonical SQL statements\n", statements)
		}
	}
	if transaction {
		return fmt.Errorf("canonical SQL ends with an uncommitted transaction")
	}
	if hex.EncodeToString(h.Sum(nil)) != m.SQLSHA256 {
		return fmt.Errorf("canonical SQL changed during execution")
	}
	if err = d.Close(); err != nil {
		return err
	}
	d = nil
	loadSeconds := time.Since(started).Seconds()
	closed, err := verifyPhase(dir, opt, m, false)
	if err != nil {
		return fmt.Errorf("after load: %w", err)
	}
	d, err = db.Open(dir, opt)
	if err != nil {
		return err
	}
	started = time.Now()
	if _, err = d.Exec("VACUUM"); err != nil {
		return err
	}
	if err = d.Close(); err != nil {
		return err
	}
	d = nil
	maintenanceSeconds := time.Since(started).Seconds()
	maintained, err := verifyPhase(dir, opt, m, true)
	if err != nil {
		return fmt.Errorf("after full compaction: %w", err)
	}
	result := map[string]any{"engine": "slopdb", "dataset": m.Dataset, "format": m.Format, "sql_sha256": m.SQLSHA256, "sql_bytes": m.SQLBytes, "page_size_bytes": pageSize, "zstd_window_bytes": storage.SSTableZstdWindowSizeForBlock(pageSize), "load_mode": "canonical_sql", "load_seconds": loadSeconds, "maintenance_seconds": maintenanceSeconds, "statements": statements, "committed_transactions": transactions, "wal_enabled": true, "wal_fsync": false, "query_benchmarked": false, "closed": closed, "after_maintenance": maintained}
	if build, ok := debug.ReadBuildInfo(); ok {
		result["go_build_info"] = build.String()
	}
	bytes, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		return err
	}
	if err = os.MkdirAll(filepath.Dir(report), 0755); err != nil {
		return err
	}
	f, err := os.CreateTemp(filepath.Dir(report), ".storage-sql-report-*")
	if err != nil {
		return err
	}
	temporary := f.Name()
	defer os.Remove(temporary)
	if _, err = f.Write(append(bytes, '\n')); err != nil {
		_ = f.Close()
		return err
	}
	if err = f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	if err = f.Close(); err != nil {
		return err
	}
	return os.Rename(temporary, report)
}

func verifyPhase(dir string, opt *storage.Options, m manifest, bottom bool) (result map[string]any, err error) {
	d, err := db.Open(dir, opt)
	if err != nil {
		return nil, err
	}
	defer func() {
		if d != nil {
			e := d.Close()
			if err == nil {
				err = e
			}
		}
	}()
	started := time.Now()
	tables, err := verify(d, m)
	if err != nil {
		return nil, err
	}
	verification := time.Since(started).Seconds()
	stats := d.Stats()
	result = map[string]any{"tables": tables, "total_rows": m.TotalRows, "total_logical_bytes": m.TotalLogicalBytes, "verification_seconds": verification, "logical_fingerprints_verified": true, "page_size_bytes": stats.PageSize, "zstd_window_bytes": storage.SSTableZstdWindowSizeForBlock(stats.PageSize), "levels": stats.Levels}
	if bottom {
		last := len(stats.Levels) - 1
		for i, l := range stats.Levels[:last] {
			if l.FileCount != 0 || l.TotalSize != 0 || len(l.Files) != 0 {
				return nil, fmt.Errorf("full compaction left SSTables in L%d", i)
			}
		}
		result["bottom_level"] = last
		result["all_sstables_at_bottom"] = true
		result["maintenance_operation"] = "full_bottom_level_rewrite"
	}
	if err = d.Close(); err != nil {
		return nil, err
	}
	d = nil
	files, fileBytes, allocated, err := inventory(dir)
	if err != nil {
		return nil, err
	}
	if bottom {
		want := map[string]int64{}
		for _, f := range stats.Levels[len(stats.Levels)-1].Files {
			want[fmt.Sprintf("%d.sst", f.Number)] = int64(f.Size)
		}
		for _, f := range files {
			if strings.HasSuffix(f.Name, ".sst") {
				size, ok := want[f.Name]
				if !ok || size != f.Bytes {
					return nil, fmt.Errorf("physical SSTable %s does not match L6 inventory", f.Name)
				}
				delete(want, f.Name)
			}
		}
		if len(want) != 0 {
			return nil, fmt.Errorf("L6 inventory has missing physical SSTables")
		}
		result["physical_sstables_match_bottom_level"] = true
	}
	result["files"] = files
	result["file_count"] = len(files)
	result["file_bytes"] = fileBytes
	result["allocated_total_bytes"] = allocated
	result["wal_files"] = []string{}
	return result, nil
}

type fileInfo struct {
	Name           string `json:"name"`
	Bytes          int64  `json:"bytes"`
	AllocatedBytes int64  `json:"allocated_bytes"`
}

func inventory(dir string) (files []fileInfo, bytes, allocated int64, err error) {
	err = filepath.Walk(dir, func(path string, info os.FileInfo, e error) error {
		if e != nil {
			return e
		}
		blocks := int64(0)
		if stat, ok := info.Sys().(*syscall.Stat_t); ok {
			blocks = stat.Blocks * 512
		}
		allocated += blocks
		if info.IsDir() {
			return nil
		}
		if !info.Mode().IsRegular() {
			return fmt.Errorf("nonregular database file %s", path)
		}
		name, e := filepath.Rel(dir, path)
		if e != nil {
			return e
		}
		if strings.HasSuffix(name, ".log") || strings.HasSuffix(name, ".wal") || strings.HasSuffix(name, "-wal") {
			return fmt.Errorf("WAL remains after close: %s", name)
		}
		files = append(files, fileInfo{Name: name, Bytes: info.Size(), AllocatedBytes: blocks})
		bytes += info.Size()
		return nil
	})
	return
}
