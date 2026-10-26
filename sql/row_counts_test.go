package sql

import (
	"context"
	"fmt"
	"strings"
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

func TestExactCountPreservesLazyDecimalLiteralErrors(t *testing.T) {
	for _, dialect := range []Dialect{DialectMySQL, DialectPostgreSQL} {
		t.Run(fmt.Sprint(dialect), func(t *testing.T) {
			s, _ := statisticsSchema(t, t.TempDir())
			s.SetDialect(dialect)
			statisticsSQL(t, s, "CREATE TABLE t(id INTEGER PRIMARY KEY)")
			// The float lexer can represent this as zero, but exact dialect evaluation
			// must reject the excessive DECIMAL scale when an input row consumes it.
			literal := "0." + strings.Repeat("0", maxDecimalDigits+40) + "1"
			stmts, err := Parse("SELECT COUNT(" + literal + ") AS n FROM t")
			if err != nil {
				t.Fatal(err)
			}
			stmt := stmts[0].(*SelectStmt)
			if err = s.StartStatement(); err != nil {
				t.Fatal(err)
			}
			r, err := s.Select(stmt)
			s.EndStatement()
			if err != nil || r.Rows[0]["n"].Int != 0 {
				t.Fatalf("empty input must not evaluate argument: %+v %v", r, err)
			}
			statisticsSQL(t, s, "INSERT INTO t VALUES(1)")
			if err = s.StartStatement(); err != nil {
				t.Fatal(err)
			}
			_, err = s.Select(stmt)
			s.EndStatement()
			if err == nil {
				t.Fatal("COUNT skipped invalid exact numeric argument")
			}
		})
	}
}
