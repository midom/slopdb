// Adapted from standard_batch_transactions_review.go; see TESTING.md.
package regression

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"slopdb/db"
	"slopdb/storage"
)

func batchTransactionsMust(testState *regressionEnv, e error) {
	testState.t.Helper()
	if e != nil {
		testState.t.Fatalf("%v", e)
	}
}
func batchTransactionsCheck(testState *regressionEnv, b bool, msg string) {
	testState.t.Helper()
	if !b {
		testState.t.Fatalf("%v", msg)
	}
}
func TestBatchTransactions(testT *testing.T) {
	testState := newRegressionEnv(testT)

	for _, mode := range []string{"nofsync", "nowal", "durable"} {
		path := testState.path("godb_batch_transactions_review_") + mode
		batchTransactionsMust(testState, os.RemoveAll(path))
		o := storage.DefaultOptions()
		o.DisableWALSync = mode == "nofsync"
		o.DisableWAL = mode == "nowal"
		o.WriteBufferSize = 65536
		o.BlockSize = 512
		o.MaxOpenFiles = 16
		o.DeadlockDetection = true
		d, e := testState.openDatabase(path, o)
		batchTransactionsMust(testState, e)
		run := func(q string) { _, e := d.Exec(q); batchTransactionsMust(testState, e) }
		run("CREATE TABLE t(id INTEGER PRIMARY KEY,v INTEGER NOT NULL,INDEX iv(v))")
		q := "INSERT INTO t VALUES "
		for i := 1; i <= 1000; i++ {
			if i > 1 {
				q += ","
			}
			q += fmt.Sprintf("(%d,0)", i)
		}
		run(q)
		s, e := d.NewSession()
		batchTransactionsMust(testState, e)
		exec := func(q string) { _, e := s.Exec(q); batchTransactionsMust(testState, e) }
		exec("BEGIN; UPDATE t SET v=10 WHERE id=1; SAVEPOINT a; UPDATE t SET v=20 WHERE id=1; SAVEPOINT b; DELETE FROM t WHERE id=2; INSERT INTO t VALUES(1001,3); ROLLBACK TO b; ROLLBACK TO a")
		r, e := s.Exec("SELECT v FROM t WHERE id=1")
		batchTransactionsMust(testState, e)
		batchTransactionsCheck(testState, r.Rows[0]["v"].Int == 10, "nested overlay rollback")
		for i := 0; i < 5; i++ {
			exec("UPDATE t SET v=v+1 WHERE id=1")
		}
		exec("ROLLBACK")
		exec("BEGIN; UPDATE t SET v=1 WHERE id=1")
		_, e = s.Exec("INSERT INTO t VALUES(1010,0),(1011,0),(1010,0)")
		batchTransactionsCheck(testState, e != nil, "duplicate batch PK")
		r, e = s.Exec("SELECT * FROM t WHERE id=1010")
		batchTransactionsMust(testState, e)
		batchTransactionsCheck(testState, len(r.Rows) == 0, "failed statement writes retained")
		zero := o.LockTimeout - o.LockTimeout
		_, e = d.ExecWithOptions("INSERT INTO t VALUES(1010,0)", db.ExecOptions{LockTimeout: &zero})
		batchTransactionsMust(testState, e)
		run("DELETE FROM t WHERE id=1010")
		_, e = s.Exec("UPDATE t SET v=NULL WHERE id=1")
		batchTransactionsCheck(testState, e != nil, "NOT NULL omitted")
		r, e = s.Exec("SELECT v FROM t WHERE id=1")
		batchTransactionsMust(testState, e)
		batchTransactionsCheck(testState, r.Rows[0]["v"].Int == 1, "failed point update lost previous writes")
		exec("ROLLBACK; BEGIN; UPDATE t SET v=1 WHERE id=7")
		c, e := d.NewSession()
		batchTransactionsMust(testState, e)
		_, e = c.ExecWithOptions("BEGIN; UPDATE t SET v=v+1 WHERE id=7 AND v<0; UPDATE t SET v=5 WHERE id=9999; COMMIT", db.ExecOptions{LockTimeout: &zero})
		batchTransactionsMust(testState, e)
		exec("ROLLBACK")
		batchTransactionsMust(testState, c.Close())
		batchTransactionsMust(testState, s.Close())
		expected := make([]int64, 1001)
		var mu sync.Mutex
		var wg sync.WaitGroup
		var finished atomic.Bool
		observer, e := d.NewSession()
		batchTransactionsMust(testState, e)
		wg.Add(1)
		go func() {
			defer wg.Done()
			for !finished.Load() {
				r, e := observer.Exec("SELECT SUM(v) AS n FROM t")
				batchTransactionsMust(testState, e)
				batchTransactionsCheck(testState, r.Rows[0]["n"].Int%100 == 0, "partially committed 100-row transaction")
			}
		}()
		var writers sync.WaitGroup
		for w := 0; w < 32; w++ {
			writers.Add(1)
			go func(w int) {
				defer writers.Done()
				s, e := d.NewSession()
				batchTransactionsMust(testState, e)
				defer s.Close()
				rng := rand.New(rand.NewSource(int64(w + 991)))
				for j := 0; j < 10; j++ {
					ids := rng.Perm(1000)[:100]
					sort.Ints(ids)
					var b strings.Builder
					b.WriteString("BEGIN;")
					for _, id := range ids {
						fmt.Fprintf(&b, "UPDATE t SET v=v+1 WHERE id=%d;", id+1)
					}
					b.WriteString("COMMIT;")
					_, e = s.Exec(b.String())
					batchTransactionsMust(testState, e)
					mu.Lock()
					for _, id := range ids {
						expected[id+1]++
					}
					mu.Unlock()
				}
			}(w)
		}
		writers.Wait()
		finished.Store(true)
		wg.Wait()
		batchTransactionsMust(testState, observer.Close())
		r, e = d.Exec("SELECT SUM(v) AS n FROM t")
		batchTransactionsMust(testState, e)
		batchTransactionsCheck(testState, r.Rows[0]["n"].Int == 32000, "lost batch increments")
		run("CREATE TABLE sparse(id INTEGER PRIMARY KEY,v INTEGER); INSERT INTO sparse VALUES(-20,0),(255,0),(256,0),(65536,0),(1152921504606846976,0)")
		batchTransactionsMust(testState, d.Close())
		l, e := testState.openStorage(path, o)
		batchTransactionsMust(testState, e)
		objectID := func(catalog, name string) uint64 {
			b, found, e := l.Get([]byte(catalog))
			batchTransactionsMust(testState, e)
			batchTransactionsCheck(testState, found, "missing ID catalog")
			var objects map[string]struct{ ID uint64 }
			batchTransactionsMust(testState, json.Unmarshal(b, &objects))
			return objects[name].ID
		}
		tableID, indexID := objectID("\x00__schema__", "t"), objectID("\x00__schema_indexes__", "iv")
		it := l.NewIterator()
		rows, secondary := 0, 0
		seen := make([]bool, 1001)
		for it.Valid() {
			k, v := it.Key().UserKey, it.Value()
			if len(k) >= 11 && k[0] == 1 && k[1] == 0xff && binary.BigEndian.Uint64(k[2:10]) == tableID {
				id := int64(binary.LittleEndian.Uint64(v[1:9]))
				batchTransactionsCheck(testState, id >= 1 && id <= 1000 && len(v) == 18, "persisted identity")
				batchTransactionsCheck(testState, int64(binary.LittleEndian.Uint64(v[10:18])) == expected[id], "persisted values")
				rows++
			} else if len(k) >= 11 && k[0] == 2 && k[1] == 0xff && binary.BigEndian.Uint64(k[2:10]) == indexID {
				batchTransactionsCheck(testState, len(k) == 41, "index key format")
				id := int64((binary.BigEndian.Uint64(k[33:41]) ^ (uint64(1) << 63)))
				value := int64(binary.BigEndian.Uint64(k[12:20]) ^ (uint64(1) << 63))
				batchTransactionsCheck(testState, id >= 1 && id <= 1000 && !seen[id] && expected[id] == value, "stale/duplicate secondary")
				seen[id] = true
				secondary++
			}
			it.Next()
		}
		batchTransactionsMust(testState, storage.IteratorError(it))
		it.Close()
		batchTransactionsMust(testState, l.Close())
		batchTransactionsCheck(testState, rows == 1000 && secondary == 1000, "persisted row/index counts")
		d, e = testState.openDatabase(path, o)
		batchTransactionsMust(testState, e)
		_, e = d.Exec("INSERT INTO sparse(v) VALUES(1)")
		batchTransactionsCheck(testState, e != nil, "omitted non-auto primary key accepted")
		batchTransactionsMust(testState, d.Close())
		testState.t.Log(fmt.Sprint("PASS", mode, "32 workers x 10 atomic 100-row indexed transactions, concurrent snapshot reads, nested savepoints, repeated keys, statement rollback/lock cleanup, nonmatching updates, constraints, sparse IDs and raw persistence"))
	}
}
