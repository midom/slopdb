package sql

import (
	"context"
	"sync"
	"testing"
	"time"

	"slopdb/storage"
)

// Pause a maintenance count after registration but before reading its snapshot.
// Concurrent commits must still publish and be captured exactly once.
type rowCountPauseContext struct {
	context.Context
	once    sync.Once
	entered chan struct{}
	resume  chan struct{}
}

func (c *rowCountPauseContext) Err() error {
	c.once.Do(func() { close(c.entered); <-c.resume })
	return c.Context.Err()
}

func TestRowCountOnlineBackfillCapturesConcurrentChanges(t *testing.T) {
	s, l := statisticsSchema(t, t.TempDir())
	statisticsSQL(t, s, "CREATE TABLE t(id INTEGER PRIMARY KEY,n INTEGER);INSERT INTO t VALUES(1,1),(2,2)")
	info := s.tables["t"]
	removed := make([]storage.Mutation, 0, rowCountShards+1)
	for j := 0; j < rowCountShards; j++ {
		removed = append(removed, storage.Mutation{Key: rowCountKey(info.ID, byte(j)), Delete: true})
	}
	removed = append(removed, storage.Mutation{Key: rowCountKey(info.ID, 255), Delete: true})
	if err := l.WriteBatch(removed); err != nil {
		t.Fatal(err)
	}
	worker := s.NewSession()
	pause := &rowCountPauseContext{Context: context.Background(), entered: make(chan struct{}), resume: make(chan struct{})}
	worker.SetContext(pause)
	done := make(chan error, 1)
	go func() { done <- worker.ensureRowCount(info) }()
	select {
	case <-pause.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("backfill did not register")
	}
	statisticsSQL(t, s, "DELETE FROM t WHERE id=1;INSERT INTO t VALUES(3,3),(4,4);UPDATE t SET n=9 WHERE id=2")
	close(pause.resume)
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("backfill did not finish")
	}
	if err := s.StartStatement(); err != nil {
		t.Fatal(err)
	}
	defer s.EndStatement()
	n, ok, err := s.exactRowCount(info)
	if !ok || err != nil || n != 3 {
		t.Fatalf("count%d enabled%v err%v", n, ok, err)
	}
}
