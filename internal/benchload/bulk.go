// Package benchload loads benchmark fixtures through the public SQL bulk API.
package benchload

import (
	"context"
	"fmt"
	"io"

	"slopdb/db"
)

const DefaultBatchRows int64 = 4096

type Inserter interface {
	BulkInsert(context.Context, string, []string, db.BulkSource) (int64, error)
}

type Stats struct {
	Rows         int64 `json:"rows"`
	Transactions int64 `json:"transactions"`
}

// Load bounds each atomic BulkInsert transaction by batchRows. BulkInsert
// prepares multi-row INSERT statements internally; all SQL constraints and
// indexes remain enabled. On error, the current chunk rolls back and Stats
// describes only earlier committed chunks. A failed fresh fixture must not be
// reported as a successful load.
func Load(ctx context.Context, target Inserter, table string, columns []string, next db.BulkSource, batchRows int64) (stats Stats, err error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if batchRows <= 0 || next == nil {
		return stats, fmt.Errorf("positive load batch size and non-nil source required")
	}
	for {
		if err = ctx.Err(); err != nil {
			return stats, err
		}
		first, sourceErr := next(ctx)
		if sourceErr == io.EOF {
			return stats, nil
		}
		if sourceErr != nil {
			return stats, sourceErr
		}
		var emitted int64
		chunk := func(ctx context.Context) ([]any, error) {
			if emitted == batchRows {
				return nil, io.EOF
			}
			if emitted == 0 {
				emitted++
				return first, nil
			}
			values, e := next(ctx)
			if e == nil {
				emitted++
			}
			return values, e
		}
		n, e := target.BulkInsert(ctx, table, columns, chunk)
		if e != nil {
			return stats, e
		}
		if n != emitted {
			return stats, fmt.Errorf("bulk committed %d rows, source supplied %d", n, emitted)
		}
		stats.Rows += n
		stats.Transactions++
	}
}
