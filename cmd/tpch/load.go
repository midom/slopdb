package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"slopdb/db"
	"slopdb/internal/benchload"
	"slopdb/sql"
)

var tables = []string{"region", "nation", "supplier", "customer", "part", "partsupp", "orders", "lineitem"}

func checkData(dir string) error {
	for _, table := range tables {
		path := filepath.Join(dir, table+".tbl")
		info, err := os.Stat(path)
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() {
			return fmt.Errorf("%s must be a regular file", path)
		}
	}
	return nil
}

func load(ctx context.Context, database *db.Database, c config) (map[string]int64, int64, error) {
	schema, _ := assets.ReadFile("schema.sql")
	if _, err := database.ExecContext(ctx, string(schema)); err != nil {
		return nil, 0, err
	}
	statements, err := sql.Parse(string(schema))
	if err != nil {
		return nil, 0, err
	}
	counts := map[string]int64{}
	var transactions int64
	for _, statement := range statements {
		table := statement.(*sql.CreateTableStmt)
		stats, err := loadTable(ctx, database, filepath.Join(c.Data, table.Table+".tbl"), table, c.LoadBatchRows)
		if err != nil {
			return nil, 0, err
		}
		counts[table.Table] = stats.Rows
		transactions += stats.Transactions
		fmt.Printf("  loaded %-8s %d rows\n", table.Table, stats.Rows)
	}
	if raw, err := os.ReadFile(filepath.Join(c.Data, "manifest.json")); err == nil {
		var manifest struct {
			Rows map[string]int64 `json:"rows"`
		}
		if err = json.Unmarshal(raw, &manifest); err != nil {
			return nil, 0, err
		}
		for table, n := range manifest.Rows {
			if got, ok := counts[table]; !ok || got != n {
				return nil, 0, fmt.Errorf("manifest row count %s: got %d want %d", table, got, n)
			}
		}
	}
	if c.Indexes {
		indexes, _ := assets.ReadFile("indexes.sql")
		if _, err := database.ExecContext(ctx, string(indexes)); err != nil {
			return nil, 0, err
		}
	}
	if _, err := database.ExecContext(ctx, "ANALYZE"); err != nil {
		return nil, 0, err
	}
	return counts, transactions, nil
}

func loadTable(ctx context.Context, database *db.Database, path string, table *sql.CreateTableStmt, batchRows int64) (benchload.Stats, error) {
	file, err := os.Open(path)
	if err != nil {
		return benchload.Stats{}, err
	}
	defer file.Close()
	columns := make([]string, len(table.Columns))
	for i, column := range table.Columns {
		columns[i] = column.Name
	}
	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 4096), 1024*1024)
	args := make([]any, len(table.Columns))
	var rowNumber int64
	return benchload.Load(ctx, database, table.Table, columns, func(ctx context.Context) ([]any, error) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if !scanner.Scan() {
			if err := scanner.Err(); err != nil {
				return nil, err
			}
			return nil, io.EOF
		}
		rowNumber++
		fields := strings.Split(strings.TrimSuffix(scanner.Text(), "\r"), "|")
		if len(fields) == len(args)+1 && fields[len(fields)-1] == "" {
			fields = fields[:len(fields)-1]
		}
		if len(fields) != len(args) {
			return nil, fmt.Errorf("%s row %d: expected %d fields, got %d", path, rowNumber, len(args), len(fields))
		}
		for i, col := range table.Columns {
			var e error
			switch col.ColType {
			case sql.ColInteger:
				args[i], e = strconv.ParseInt(fields[i], 10, 64)
			case sql.ColReal:
				var f float64
				f, e = strconv.ParseFloat(fields[i], 64)
				if e == nil && (math.IsNaN(f) || math.IsInf(f, 0)) {
					e = fmt.Errorf("non-finite real")
				}
				args[i] = f
			case sql.ColText:
				args[i] = fields[i]
			default:
				e = fmt.Errorf("unsupported loader column type")
			}
			if e != nil {
				return nil, fmt.Errorf("%s row %d column %s: %w", path, rowNumber, col.Name, e)
			}
		}
		return args, nil
	}, batchRows)
}

func verifyCounts(ctx context.Context, database *db.Database, counts map[string]int64) error {
	for _, table := range tables {
		result, err := database.ExecContext(ctx, "SELECT COUNT(*) AS n FROM "+table)
		if err != nil {
			return err
		}
		if len(result.Rows) != 1 || result.Rows[0]["n"].Int != counts[table] {
			return fmt.Errorf("persisted row count mismatch for %s", table)
		}
	}
	return nil
}
