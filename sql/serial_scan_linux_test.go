package sql

import (
	"context"
	"fmt"
	"runtime"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"

	"slopdb/storage"
)

// Locking the caller's OS thread lets Err observe whether query work moved to
// another goroutine, without making assertions about background engine workers.
type callerThreadContext struct {
	context.Context
	caller  int
	foreign atomic.Int64
}

func (c *callerThreadContext) Err() error {
	if tid := syscall.Gettid(); tid != c.caller {
		c.foreign.Store(int64(tid))
	}
	return c.Context.Err()
}

func TestScansExecuteOnCallingThread(t *testing.T) {
	old := runtime.GOMAXPROCS(4)
	defer runtime.GOMAXPROCS(old)
	opt := storage.DefaultOptions()
	opt.DisableWALSync = true
	opt.CompressionType = storage.NoCompression
	l, err := storage.NewLSMTree(t.TempDir(), opt)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { l.Close() })
	s, err := NewSchema(l, 4096)
	if err != nil {
		t.Fatal(err)
	}
	statisticsSQL(t, s, "CREATE TABLE t(id INTEGER PRIMARY KEY,n INTEGER,p TEXT)")
	for base := 0; base < 4096; base += 128 {
		rows := make([]string, 128)
		for i := range rows {
			id := base + i
			rows[i] = fmt.Sprintf("(%d,%d,'%s')", id, id%16, strings.Repeat("x", 240))
		}
		statisticsSQL(t, s, "INSERT INTO t VALUES"+strings.Join(rows, ","))
	}
	if err = l.CompactRange(); err != nil {
		t.Fatal(err)
	}
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	ctx := &callerThreadContext{Context: context.Background(), caller: syscall.Gettid()}
	s.SetContext(ctx)
	for _, query := range []string{
		"SELECT COUNT(*) AS n FROM t",
		"SELECT COUNT(*) AS n FROM t WHERE id%2=0",
		"SELECT COUNT(n) AS n,MIN(n) AS low,MAX(n) AS high FROM t WHERE id>=0",
		"SELECT SUM(n) AS total FROM t",
	} {
		r := statisticsSQL(t, s, query)
		if len(r.Rows) != 1 {
			t.Fatalf("%s: %+v", query, r.Rows)
		}
		if tid := ctx.foreign.Load(); tid != 0 {
			t.Fatalf("%s executed on thread%d instead of caller%d", query, tid, ctx.caller)
		}
		if strings.Contains(query, "id%2") && r.Rows[0]["n"].Int != 2048 {
			t.Fatal(r.Rows)
		}
	}
}
