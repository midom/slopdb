package postgresserver

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/lib/pq"
)

func TestPostgresRepeatableReadWireConflictAfterLockWait(t *testing.T) {
	s := startTestServer(t, "tcp", Config{})
	pool := s.open(t, "")
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	a, e := pool.Conn(ctx)
	if e != nil {
		t.Fatal(e)
	}
	defer a.Close()
	b, e := pool.Conn(ctx)
	if e != nil {
		t.Fatal(e)
	}
	defer b.Close()
	for _, q := range []string{"CREATE TABLE repeatable_wire(id INTEGER PRIMARY KEY,v INTEGER)", "INSERT INTO repeatable_wire VALUES(1,10)"} {
		if _, e = b.ExecContext(ctx, q); e != nil {
			t.Fatal(e)
		}
	}
	if _, e = a.ExecContext(ctx, "BEGIN ISOLATION LEVEL REPEATABLE READ"); e != nil {
		t.Fatal(e)
	}
	var value int64
	if e = a.QueryRowContext(ctx, "SELECT 1").Scan(&value); e != nil {
		t.Fatal(e)
	}
	if _, e = b.ExecContext(ctx, "BEGIN; UPDATE repeatable_wire SET v=20 WHERE id=1"); e != nil {
		t.Fatal(e)
	}
	done := make(chan error, 1)
	go func() {
		var result int64
		e := a.QueryRowContext(ctx, "SELECT v FROM repeatable_wire WHERE id=$1 FOR UPDATE", 1).Scan(&result)
		done <- e
	}()
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	for len(s.database.LockWaits()) == 0 {
		select {
		case e := <-done:
			t.Fatalf("locking query did not wait: %v", e)
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-tick.C:
		}
	}
	if _, e = b.ExecContext(ctx, "COMMIT"); e != nil {
		t.Fatal(e)
	}
	select {
	case e = <-done:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	var pg *pq.Error
	if !errors.As(e, &pg) || pg.Code != "40001" {
		t.Fatalf("expected serialization failure, got %v", e)
	}
	e = a.QueryRowContext(ctx, "SELECT 1").Scan(&value)
	if !errors.As(e, &pg) || pg.Code != "25P02" {
		t.Fatalf("transaction not failed: %v", e)
	}
	if _, e = a.ExecContext(ctx, "ROLLBACK"); e != nil {
		t.Fatal(e)
	}
	if e = a.QueryRowContext(ctx, "SELECT v FROM repeatable_wire WHERE id=1").Scan(&value); e != nil || value != 20 {
		t.Fatal(value, e)
	}
}
