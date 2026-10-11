package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"slopdb/db"
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

func load(ctx context.Context, database *db.Database, c config) (map[string]int64, error) {
	schema, _ := assets.ReadFile("schema.sql")
	if _, err := database.ExecContext(ctx, string(schema)); err != nil {
		return nil, err
	}
	statements, err := sql.Parse(string(schema))
	if err != nil {
		return nil, err
	}
	counts := map[string]int64{}
	for _, statement := range statements {
		table := statement.(*sql.CreateTableStmt)
		n, err := loadTable(ctx, database, filepath.Join(c.Data, table.Table+".tbl"), table)
		if err != nil {
			return nil, err
		}
		counts[table.Table] = n
		fmt.Printf("  loaded %-8s %d rows\n", table.Table, n)
	}
	if raw, err := os.ReadFile(filepath.Join(c.Data, "manifest.json")); err == nil {
		var manifest struct {
			Rows map[string]int64 `json:"rows"`
		}
		if err = json.Unmarshal(raw, &manifest); err != nil {
			return nil, err
		}
		for table, n := range manifest.Rows {
			if got, ok := counts[table]; !ok || got != n {
				return nil, fmt.Errorf("manifest row count %s: got %d want %d", table, got, n)
			}
		}
	}
	if c.Indexes {
		indexes, _ := assets.ReadFile("indexes.sql")
		if _, err := database.ExecContext(ctx, string(indexes)); err != nil {
			return nil, err
		}
	}
	if _, err := database.ExecContext(ctx, "ANALYZE"); err != nil {
		return nil, err
	}
	return counts, nil
}

func loadTable(ctx context.Context, database *db.Database, path string, table *sql.CreateTableStmt) (count int64, retErr error) {
	file, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer file.Close()
	marks := make([]string, len(table.Columns))
	for i := range marks {
		marks[i] = "?"
	}
	stmt, err := database.Prepare("INSERT INTO " + table.Table + " VALUES(" + strings.Join(marks, ",") + ")")
	if err != nil {
		return 0, err
	}
	defer stmt.Close()
	var tx *db.Tx
	defer func() {
		if tx != nil {
			retErr = errors.Join(retErr, tx.Rollback())
		}
	}()
	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 4096), 1024*1024)
	args := make([]any, len(table.Columns))
	for scanner.Scan() {
		if err = ctx.Err(); err != nil {
			return count, err
		}
		fields := strings.Split(strings.TrimSuffix(scanner.Text(), "\r"), "|")
		if len(fields) == len(args)+1 && fields[len(fields)-1] == "" {
			fields = fields[:len(fields)-1]
		}
		if len(fields) != len(args) {
			return count, fmt.Errorf("%s row %d: expected %d fields, got %d", path, count+1, len(args), len(fields))
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
				return count, fmt.Errorf("%s row %d column %s: %w", path, count+1, col.Name, e)
			}
		}
		if tx == nil {
			tx, err = database.BeginContext(ctx)
			if err != nil {
				return count, err
			}
		}
		if _, err = tx.ExecPreparedContext(ctx, stmt, args...); err != nil {
			return count, fmt.Errorf("%s row %d: %w", path, count+1, err)
		}
		count++
		if count%512 == 0 {
			err = tx.CommitContext(ctx)
			tx = nil
			if err != nil {
				return count, err
			}
		}
	}
	if err = scanner.Err(); err != nil {
		return count, err
	}
	if tx != nil {
		err = tx.CommitContext(ctx)
		tx = nil
		if err != nil {
			return count, err
		}
	}
	return count, nil
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
