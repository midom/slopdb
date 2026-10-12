package benchload

import (
	"context"
	"errors"
	"io"
	"path/filepath"
	"strconv"
	"testing"

	"slopdb/db"
	"slopdb/storage"
)

type observedInserter struct {
	database *db.Database
	rows     []int64
}

func (o *observedInserter) BulkInsert(ctx context.Context, table string, columns []string, next db.BulkSource) (int64, error) {
	var supplied int64
	n, err := o.database.BulkInsert(ctx, table, columns, func(ctx context.Context) ([]any, error) {
		row, e := next(ctx)
		if e == nil {
			supplied++
		}
		return row, e
	})
	o.rows = append(o.rows, supplied)
	return n, err
}

func TestBoundedBulkLoadReusesRowsAndPersistsIndexes(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "database")
	database, err := db.Open(dir, storage.DefaultOptions())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	if _, err = database.Exec("CREATE TABLE fixture(id INTEGER PRIMARY KEY, value INTEGER, note TEXT, payload BLOB); CREATE INDEX by_value ON fixture(value)"); err != nil {
		t.Fatal(err)
	}
	target := &observedInserter{database: database}
	var next int64
	row := make([]any, 4)
	payload := []byte{0, 0}
	const base int64 = 9007199254740993
	stats, err := Load(context.Background(), target, "fixture", []string{"id", "value", "note", "payload"}, func(context.Context) ([]any, error) {
		if next == 601 {
			return nil, io.EOF
		}
		payload[1] = byte(next)
		row[0], row[1], row[2], row[3] = base+next, next, nil, payload
		next++
		return row, nil
	}, 300)
	if err != nil || stats.Rows != 601 || stats.Transactions != 3 {
		t.Fatalf("stats=%+v error=%v", stats, err)
	}
	if len(target.rows) != 3 || target.rows[0] != 300 || target.rows[1] != 300 || target.rows[2] != 1 {
		t.Fatalf("transactions supplied %v", target.rows)
	}
	if err = database.Close(); err != nil {
		t.Fatal(err)
	}
	database, err = db.Open(dir, storage.DefaultOptions())
	if err != nil {
		t.Fatal(err)
	}
	for _, index := range []int64{0, 255, 256, 299, 300, 600} {
		result, e := database.Exec("SELECT id,note,payload FROM fixture WHERE value = " + strconv.FormatInt(index, 10))
		if e != nil || len(result.Rows) != 1 {
			t.Fatalf("lookup %d: %v %v", index, result, e)
		}
		got := result.Rows[0]
		if got["id"].Int != base+index || !got["note"].Null || len(got["payload"].Blob) != 2 || got["payload"].Blob[1] != byte(index) {
			t.Fatalf("lookup %d incorrect: %v", index, got)
		}
	}
	if err = database.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestBulkSourceFailureRollsBackCurrentChunk(t *testing.T) {
	database, err := db.Open(filepath.Join(t.TempDir(), "database"), storage.DefaultOptions())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	if _, err = database.Exec("CREATE TABLE fixture(id INTEGER PRIMARY KEY)"); err != nil {
		t.Fatal(err)
	}
	var next int64
	sourceFailure := errors.New("fixture source failed")
	stats, err := Load(context.Background(), database, "fixture", []string{"id"}, func(context.Context) ([]any, error) {
		if next == 570 {
			return nil, sourceFailure
		}
		next++
		return []any{next}, nil
	}, 300)
	if !errors.Is(err, sourceFailure) || stats.Rows != 300 || stats.Transactions != 1 {
		t.Fatalf("stats=%+v error=%v", stats, err)
	}
	result, err := database.Exec("SELECT COUNT(*) AS n, MAX(id) AS max_id FROM fixture")
	if err != nil || result.Rows[0]["n"].Int != 300 || result.Rows[0]["max_id"].Int != 300 {
		t.Fatalf("partial failed chunk committed: %v %v", result, err)
	}
	if _, err = Load(context.Background(), database, "fixture", []string{"id"}, func(context.Context) ([]any, error) { return nil, io.EOF }, 0); err == nil {
		t.Fatal("zero chunk size accepted")
	}
	if err = database.Close(); err != nil {
		t.Fatal(err)
	}
}
