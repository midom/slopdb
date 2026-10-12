package db

import (
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"slopdb/storage"
)

// Same harness can be run on the name-prefixed and object-ID catalog revisions.
// WAL stays enabled; WAL fsync is disabled. Setup and validation are not timed.
func BenchmarkCatalogWorkloads(b *testing.B) {
	for _, tables := range []int{1, 128} {
		for _, kind := range []string{"point-read", "secondary-read", "range-read", "scan", "join", "point-update", "transaction-update"} {
			b.Run(fmt.Sprintf("tables=%d/%s", tables, kind), func(b *testing.B) {
				opt := storage.DefaultOptions()
				opt.DisableWALSync = true
				d, err := Open(b.TempDir(), opt)
				if err != nil {
					b.Fatal(err)
				}
				defer func() {
					if err := d.Close(); err != nil {
						b.Error(err)
					}
				}()
				for i := 0; i < tables; i++ {
					if _, err = d.Exec(fmt.Sprintf("CREATE TABLE bench_%03d(id INTEGER PRIMARY KEY,k INTEGER,v INTEGER)", i)); err != nil {
						b.Fatal(err)
					}
				}
				var rows []string
				for i := 1; i <= 2048; i++ {
					rows = append(rows, fmt.Sprintf("(%d,%d,0)", i, i))
				}
				if _, err = d.Exec("INSERT INTO bench_000 VALUES" + strings.Join(rows, ",")); err != nil {
					b.Fatal(err)
				}
				if _, err = d.Exec("CREATE UNIQUE INDEX bench_k ON bench_000(k)"); err != nil {
					b.Fatal(err)
				}
				query := map[string]string{
					"point-read":         "SELECT v FROM bench_000 WHERE id=?",
					"secondary-read":     "SELECT v FROM bench_000 WHERE k=?",
					"range-read":         "SELECT id,v FROM bench_000 WHERE k>=? ORDER BY k LIMIT 10",
					"scan":               "SELECT SUM(v) AS total FROM bench_000",
					"join":               "SELECT a.v,b.v AS other FROM bench_000 a JOIN bench_000 b ON a.k=b.k WHERE a.id=?",
					"point-update":       "UPDATE bench_000 SET v=v+1 WHERE id=?",
					"transaction-update": "UPDATE bench_000 SET v=v+1 WHERE id=?",
				}[kind]
				stmt, err := d.Prepare(query)
				if err != nil {
					b.Fatal(err)
				}
				defer stmt.Close()
				var worker atomic.Int64
				b.ReportAllocs()
				b.ResetTimer()
				b.RunParallel(func(pb *testing.PB) {
					id := worker.Add(1)
					session, e := d.NewSession()
					if e != nil {
						b.Error(e)
						return
					}
					defer session.Close()
					for pb.Next() {
						var args []any
						if kind != "scan" {
							args = []any{id}
						}
						if kind == "transaction-update" {
							if _, e = session.Exec("BEGIN"); e != nil {
								b.Error(e)
								return
							}
						}
						if _, e = session.ExecPrepared(stmt, args...); e != nil {
							b.Error(e)
							return
						}
						if kind == "transaction-update" {
							if _, e = session.Exec("COMMIT"); e != nil {
								b.Error(e)
								return
							}
						}
					}
				})
				b.StopTimer()
				if kind == "point-update" || kind == "transaction-update" {
					result, e := d.Exec("SELECT SUM(v) AS n FROM bench_000")
					if e != nil {
						b.Fatal(e)
					}
					if result.Rows[0]["n"].Int != int64(b.N) {
						b.Fatalf("lost updates: got %d, want %d", result.Rows[0]["n"].Int, b.N)
					}
				}
				if _, err = d.CheckIntegrity(); err != nil {
					b.Fatal(err)
				}
			})
		}
	}
}

// Warm, repeated primary-key hits make storage work small, exposing the worst
// relative cost of driver/catalog/snapshot synchronization. Every returned row
// is checked, and the timed region contains SELECTs only.
func BenchmarkSelectPoint(b *testing.B) {
	for _, catalog := range []int{1, 128} {
		for _, workers := range []int{1, 10, 32} {
			b.Run(fmt.Sprintf("catalog=%d/workers=%d", catalog, workers), func(b *testing.B) {
				opt := storage.DefaultOptions()
				opt.DisableWALSync = true
				d, err := Open(b.TempDir(), opt)
				if err != nil {
					b.Fatal(err)
				}
				defer func() {
					if err := d.Close(); err != nil {
						b.Error(err)
					}
				}()
				for i := 0; i < catalog; i++ {
					if _, err = d.Exec(fmt.Sprintf("CREATE TABLE point_%03d(id INTEGER PRIMARY KEY,v INTEGER)", i)); err != nil {
						b.Fatal(err)
					}
				}
				if _, err = d.Exec("INSERT INTO point_000 VALUES(1,17)"); err != nil {
					b.Fatal(err)
				}
				stmt, err := d.Prepare("SELECT v FROM point_000 WHERE id=?")
				if err != nil {
					b.Fatal(err)
				}
				defer stmt.Close()
				sessions := make([]*Session, workers)
				for i := range sessions {
					sessions[i], err = d.NewSession()
					if err != nil {
						b.Fatal(err)
					}
					defer sessions[i].Close()
					if _, err = sessions[i].ExecPrepared(stmt, int64(1)); err != nil {
						b.Fatal(err)
					}
				}
				b.ReportAllocs()
				b.ResetTimer()
				var wg sync.WaitGroup
				for worker, session := range sessions {
					wg.Add(1)
					go func(worker int, s *Session) {
						defer wg.Done()
						for n := worker; n < b.N; n += workers {
							r, e := s.ExecPrepared(stmt, int64(1))
							if e != nil {
								b.Error(e)
								return
							}
							if len(r.Rows) != 1 || r.Rows[0]["v"].Int != 17 {
								b.Error("wrong point SELECT answer")
								return
							}
						}
					}(worker, session)
				}
				wg.Wait()
				b.StopTimer()
			})
		}
	}
}
