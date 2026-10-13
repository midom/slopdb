package db

import (
	"context"
	"fmt"
	"io"
	"strings"

	"slopdb/sql"
)

// BulkSource returns the next row's typed bindings, or io.EOF. It is called
// synchronously and should honor ctx if reading from an external source.
type BulkSource func(ctx context.Context) ([]any, error)

// BulkInsert inserts all rows in one transaction using batches of 256 rows and prepared INSERT shapes.
// Defaults, triggers, type/NOT NULL/UNIQUE/CHECK/foreign-key constraints and
// secondary indexes follow ordinary INSERT semantics. Source, binding, SQL or
// cancellation errors roll back the entire import. Values and row buffers may
// be reused after the next source call. Transaction memory grows with the batch;
// callers needing bounded transactions should invoke BulkInsert in chunks.
func (db *Database) BulkInsert(ctx context.Context, table string, columns []string, next BulkSource) (int64, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if next == nil {
		return 0, fmt.Errorf("nil bulk source")
	}
	if len(columns) == 0 {
		return 0, fmt.Errorf("bulk insert requires explicit columns")
	}
	seen := make(map[string]bool, len(columns))
	names := make([]string, len(columns))
	parameters := make([]string, len(columns))
	for i, name := range columns {
		normalized := strings.ToLower(name)
		if seen[normalized] {
			return 0, fmt.Errorf("duplicate bulk column %s", name)
		}
		seen[normalized] = true
		names[i] = quoteIdentifier(name)
		parameters[i] = "?"
	}
	queryPrefix := db.renderSQL("INSERT INTO " + quoteIdentifier(table) + " (" + strings.Join(names, ",") + ") VALUES ")
	rowShape := "(" + strings.Join(parameters, ",") + ")"
	tx, err := db.BeginContext(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	// Validate the target and column names even when the source is empty.
	validation, err := tx.QueryContext(ctx, db.renderSQL("SELECT "+strings.Join(names, ",")+" FROM "+quoteIdentifier(table)+" LIMIT 0"))
	if err != nil {
		return 0, err
	}
	_ = validation.Close()

	statements := make(map[int]*Stmt)
	defer func() {
		for _, statement := range statements {
			_ = statement.Close()
		}
	}()
	var count int64
	batch := make([]any, 0, 256*len(columns))
	flush := func() error {
		size := len(batch) / len(columns)
		if size == 0 {
			return nil
		}
		statement := statements[size]
		if statement == nil {
			shapes := make([]string, size)
			for i := range shapes {
				shapes[i] = rowShape
			}
			var prepareErr error
			statement, prepareErr = tx.PrepareContext(ctx, queryPrefix+strings.Join(shapes, ","))
			if prepareErr != nil {
				return prepareErr
			}
			statements[size] = statement
		}
		if _, execErr := statement.ExecContext(ctx, batch...); execErr != nil {
			return fmt.Errorf("bulk rows %d-%d: %w", count+1, count+int64(size), execErr)
		}
		count += int64(size)
		clear(batch)
		batch = batch[:0]
		return nil
	}
	for {
		if err = ctx.Err(); err != nil {
			return 0, err
		}
		values, sourceErr := next(ctx)
		if sourceErr == io.EOF {
			break
		}
		if sourceErr != nil {
			return 0, fmt.Errorf("bulk source: %w", sourceErr)
		}
		if len(values) != len(columns) {
			return 0, fmt.Errorf("bulk row expected %d values, got %d", len(columns), len(values))
		}
		for _, value := range values {
			switch v := value.(type) {
			case []byte:
				value = append([]byte{}, v...)
			case sql.Value:
				value = ownedValue(v)
			}
			batch = append(batch, value)
		}
		if len(batch)/len(columns) == 256 {
			if err = flush(); err != nil {
				return 0, err
			}
		}
	}
	if err = flush(); err != nil {
		return 0, err
	}
	if err = tx.CommitContext(ctx); err != nil {
		return 0, err
	}
	return count, nil
}
