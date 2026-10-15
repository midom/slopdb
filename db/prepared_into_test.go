package db_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"

	"slopdb/db"
)

func TestPreparedPointSkippedFieldsAndPredicates(t *testing.T) {
	d := planDatabase(t)
	planExec(t, d, "CREATE TABLE wide(id INTEGER PRIMARY KEY,payload BLOB,label TEXT,v INTEGER);INSERT INTO wide VALUES(1,X'0102','alpha',17),(2,X'','βeta',34),(3,NULL,NULL,51)")
	s, err := d.NewSession()
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	queries := []string{
		"SELECT v FROM wide WHERE id=?",
		"SELECT id,v FROM wide WHERE id=? AND LENGTH(payload)=2",
		"SELECT v FROM wide WHERE id=? AND label='alpha'",
		"SELECT payload FROM wide WHERE id=?",
		"SELECT label FROM wide WHERE id=?",
	}
	for _, query := range queries {
		stmt := planPrepare(t, d, query)
		var dst db.ExecResult
		for _, id := range []int{1, 2, 3, 99, 1} {
			text := strings.Replace(query, "?", fmt.Sprint(id), 1)
			want := planExec(t, d, text)
			for _, into := range []bool{false, true} {
				var got *db.ExecResult
				if into {
					got, err = s.ExecPreparedInto(&dst, stmt, id)
				} else {
					got, err = s.ExecPrepared(stmt, id)
				}
				if err != nil || !reflect.DeepEqual(got.Columns, want.Columns) || len(got.Rows) != len(want.Rows) || (len(want.Rows) > 0 && !reflect.DeepEqual(got.Rows, want.Rows)) {
					t.Fatalf("%s, into=%t: got=%+v want=%+v err=%v", text, into, got, want, err)
				}
			}
		}
	}
}

func TestPreparedIntoOwnershipAndFallback(t *testing.T) {
	d := planDatabase(t)
	planExec(t, d, "ALTER TABLE t ADD COLUMN payload BLOB;UPDATE t SET payload=X'0102' WHERE id=1;UPDATE t SET payload=X'030405060708' WHERE id=2;UPDATE t SET payload=X'' WHERE id=3")
	s, err := d.NewSession()
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	point := planPrepare(t, d, "SELECT id,v,payload FROM t WHERE id=?")
	owned, err := s.ExecPrepared(point, 1)
	if err != nil {
		t.Fatal(err)
	}
	var dst db.ExecResult
	var columns *string
	var blob *byte
	for _, id := range []int{2, 1, 99, 2, 3, 1} {
		r, err := s.ExecPreparedInto(&dst, point, id)
		if err != nil || r != &dst {
			t.Fatalf("destination: %v %v", r, err)
		}
		if columns == nil {
			columns = &r.Columns[0].Name
		} else if columns != &r.Columns[0].Name {
			t.Fatal("column buffer was not reused")
		}
		if id == 99 {
			if len(r.Rows) != 0 || r.Changes != 0 {
				t.Fatal("missing key retained a visible row")
			}
			continue
		}
		if len(r.Rows) != 1 || r.Rows[0]["id"].Int != int64(id) || r.Changes != 1 {
			t.Fatal("wrong destination result")
		}
		b := r.Rows[0]["payload"].Blob
		if id == 3 {
			if r.Rows[0]["payload"].Null || b == nil || len(b) != 0 {
				t.Fatal("empty BLOB changed to NULL")
			}
		} else {
			if blob == nil {
				blob = &b[0]
			} else if blob != &b[0] {
				t.Fatal("BLOB buffer was not reused")
			}
		}
	}
	if !bytes.Equal(owned.Rows[0]["payload"].Blob, []byte{1, 2}) || owned.Columns[0].Name != "id" {
		t.Fatal("ordinary result was changed by destination calls")
	}
	// Caller mutation must not change the immutable projection recipe.
	dst.Columns[0].Name = "changed"
	dst.Rows[0]["extra"] = dst.Rows[0]["v"]
	if _, err = s.ExecPreparedInto(&dst, point, 2); err != nil || dst.Columns[0].Name != "id" || len(dst.Rows[0]) != 3 {
		t.Fatal("stale destination contents", err)
	}
	filtered := planPrepare(t, d, "SELECT id FROM t WHERE id=? AND v=?")
	if _, err = s.ExecPreparedInto(&dst, filtered, 1, 999); err != nil || len(dst.Rows) != 0 {
		t.Fatal("residual predicate was skipped", err)
	}
	general := planPrepare(t, d, "SELECT id,v FROM t WHERE id>=? ORDER BY id")
	if _, err = s.ExecPreparedInto(&dst, general, 1); err != nil || len(dst.Rows) != 3 || len(dst.Columns) != 2 {
		t.Fatal("general SELECT fallback failed", err)
	}
	star := planPrepare(t, d, "SELECT * FROM t WHERE id=?")
	planExec(t, d, "ALTER TABLE t ADD COLUMN extra TEXT DEFAULT 'new'")
	if _, err = s.ExecPreparedInto(&dst, star, 1); err != nil || dst.Rows[0]["extra"].Text != "new" || len(dst.Columns) != 5 {
		t.Fatal("catalog invalidation failed", err)
	}
	update := planPrepare(t, d, "UPDATE t SET v=v+1 WHERE id=?")
	if _, err = s.ExecPreparedInto(&dst, update, 1); err != nil || dst.Changes != 1 || len(dst.Rows) != 0 || len(dst.Columns) != 0 {
		t.Fatal("point update destination failed", err)
	}
	if _, err = s.ExecPreparedInto(nil, point, 1); err == nil {
		t.Fatal("nil destination accepted")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err = s.ExecPreparedContextInto(ctx, &dst, point, 1); !errors.Is(err, context.Canceled) {
		t.Fatal("canceled call executed", err)
	}
}

func TestPreparedIntoConcurrentReadersAndExactCounters(t *testing.T) {
	d := planDatabase(t)
	point := planPrepare(t, d, "SELECT id,v FROM t WHERE id=?")
	const workers, calls = 200, 20
	var wg sync.WaitGroup
	start := make(chan struct{})
	failures := make(chan error, workers)
	for i := 0; i < workers; i++ {
		s, err := d.NewSession()
		if err != nil {
			t.Fatal(err)
		}
		wg.Add(1)
		go func(s *db.Session, worker int) {
			defer wg.Done()
			defer s.Close()
			var dst db.ExecResult
			<-start
			for j := 0; j < calls; j++ {
				id := int64((j+worker)%3 + 1)
				r, err := s.ExecPreparedInto(&dst, point, id)
				if err != nil || r != &dst || len(r.Rows) != 1 || r.Rows[0]["v"].Int != id*100 {
					failures <- fmt.Errorf("reader %d: %v %v", worker, r, err)
					return
				}
				_ = point.PlanStats() // statistics can be read during execution
			}
		}(s, i)
	}
	close(start)
	wg.Wait()
	close(failures)
	for err := range failures {
		t.Error(err)
	}
	stats := point.PlanStats()
	if stats.Builds == 0 || stats.Hits == 0 || stats.Builds+stats.Hits != workers*calls {
		t.Fatalf("lost concurrent counter increments: %+v", stats)
	}
}
