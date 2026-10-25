package main

import (
	"errors"
	"fmt"
	"net"
	"slopdb/mysqlserver"
	"slopdb/postgresserver"
	"sync"
	"testing"

	"slopdb/db"
	"slopdb/storage"
)

func wireTestDatabase(t *testing.T, withTable bool) *db.Database {
	t.Helper()
	options := storage.DefaultOptions()
	options.DisableWALSync = true
	database, err := db.Open(t.TempDir(), options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := database.Close(); err != nil {
			t.Error(err)
		}
	})
	if withTable {
		wireTestRows(t, database)
	}
	return database
}

func wireTestRows(t *testing.T, database *db.Database) {
	t.Helper()
	if _, err := database.Exec("CREATE TABLE small_rows(id BIGINT PRIMARY KEY,v BIGINT NOT NULL,payload BLOB NOT NULL)"); err != nil {
		t.Fatal(err)
	}
	statement, err := database.Prepare("INSERT INTO small_rows VALUES(?,?,?)")
	if err != nil {
		t.Fatal(err)
	}
	defer statement.Close()
	for id := int64(1); id <= 4; id++ {
		data := make([]byte, payloadBytes)
		payload(id, data)
		if _, err := statement.Exec(id, id*17, data); err != nil {
			t.Fatal(err)
		}
	}
}

func TestWireBackendsCheckedRowsAndPool(t *testing.T) {
	for _, api := range []string{"mysql", "pq", "pgx"} {
		for _, mode := range []string{"text", "prepared"} {
			t.Run(api+"/"+mode, func(t *testing.T) {
				database := wireTestDatabase(t, true)
				backend, err := newWireBackend(config{API: api, QueryMode: mode, Workers: 3}, database)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() {
					if err := backend.close(); err != nil {
						t.Error(err)
					}
				})
				if len(backend.readers) != 3 {
					t.Fatal("incorrect reader count", len(backend.readers))
				}
				stats := backend.poolStats()
				if stats.OpenConnections != 3 || stats.InUse != 0 {
					t.Fatal("pool not established before timing", stats)
				}
				var wg sync.WaitGroup
				failures := make(chan error, 3)
				for worker, reader := range backend.readers {
					wg.Add(1)
					go func(worker int, reader benchmarkReader) {
						defer wg.Done()
						expected := make([]byte, payloadBytes)
						for n := 0; n < 12; n++ {
							id := int64((n+worker)%4 + 1)
							if err := reader.query(id); err != nil {
								failures <- err
								return
							}
							if err := reader.validate(id, expected); err != nil {
								failures <- err
								return
							}
						}
					}(worker, reader)
				}
				wg.Wait()
				close(failures)
				for err := range failures {
					t.Error(err)
				}
				reader := backend.readers[0]
				if err = reader.query(99); err == nil {
					t.Fatal("missing row accepted")
				}
				if err = reader.query(2); err != nil {
					t.Fatal("query after empty result", err)
				}
				if err = reader.validate(3, make([]byte, payloadBytes)); err == nil {
					t.Fatal("incorrect typed answer accepted")
				}
				if err = reader.validate(2, make([]byte, payloadBytes)); err != nil {
					t.Fatal(err)
				}
				if err = backend.close(); err != nil {
					t.Fatal(err)
				}
				if err = backend.close(); err != nil {
					t.Fatal("second close", err)
				}
				// Backend cleanup owns clients/listeners, leaving the engine to its caller.
				if _, err = database.Exec("SELECT id FROM small_rows WHERE id=1"); err != nil {
					t.Fatal("backend closed caller database", err)
				}
			})
		}
	}
}

func TestWireBackendFailureLeavesDatabaseOpen(t *testing.T) {
	for _, api := range []string{"mysql", "pq", "pgx"} {
		t.Run(api, func(t *testing.T) {
			database := wireTestDatabase(t, false)
			if backend, err := newWireBackend(config{API: api, QueryMode: "prepared", Workers: 2}, database); err == nil {
				backend.close()
				t.Fatal("missing fixture accepted")
			}
			wireTestRows(t, database)
			backend, err := newWireBackend(config{API: api, QueryMode: "text", Workers: 2}, database)
			if err != nil {
				t.Fatal("retry after setup failure", err)
			}
			if err = backend.close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestWireBackendRejectsExtraRowsAndMalformedValues(t *testing.T) {
	for _, api := range []string{"mysql", "pq", "pgx"} {
		for _, malformed := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/malformed=%v", api, malformed), func(t *testing.T) {
				database := wireTestDatabase(t, false)
				if _, err := database.Exec("CREATE TABLE small_rows(id BIGINT,v BIGINT,payload BLOB)"); err != nil {
					t.Fatal(err)
				}
				data := make([]byte, payloadBytes)
				payload(1, data)
				value := any(int64(17))
				if malformed {
					value = nil
				}
				statement, err := database.Prepare("INSERT INTO small_rows VALUES(?,?,?)")
				if err != nil {
					t.Fatal(err)
				}
				defer statement.Close()
				if _, err := statement.Exec(int64(1), value, data); err != nil {
					t.Fatal(err)
				}
				if !malformed {
					if _, err := statement.Exec(int64(1), value, data); err != nil {
						t.Fatal(err)
					}
				}
				backend, err := newWireBackend(config{API: api, QueryMode: "text", Workers: 2}, database)
				if err == nil {
					err = errors.Join(fmt.Errorf("invalid answer accepted"), backend.close())
					t.Fatal(err)
				}
			})
		}
	}
}

func TestWireBackendAttachExistingTCPServer(t *testing.T) {
	for _, api := range []string{"mysql", "pq", "pgx"} {
		t.Run(api, func(t *testing.T) {
			database := wireTestDatabase(t, true)
			var server benchmarkWireServer
			var err error
			if api == "mysql" {
				server, err = mysqlserver.New(database, mysqlserver.Config{})
			} else {
				server, err = postgresserver.New(database, postgresserver.Config{})
			}
			if err != nil {
				t.Fatal(err)
			}
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				server.Close()
				t.Fatal(err)
			}
			done := make(chan error, 1)
			go func() { done <- server.Serve(listener) }()
			t.Cleanup(func() {
				if err := server.Close(); err != nil {
					t.Error(err)
				}
				if err := <-done; err != nil && !errors.Is(err, net.ErrClosed) {
					t.Error(err)
				}
			})
			for _, mode := range []string{"text", "prepared"} {
				// A nil Database proves attach does not create or own another server.
				backend, err := newWireBackend(config{API: api, Endpoint: listener.Addr().String(), QueryMode: mode, Workers: 2}, nil)
				if err != nil {
					t.Fatal(err)
				}
				if err = backend.readers[0].query(3); err != nil {
					t.Fatal(err)
				}
				if err = backend.readers[0].validate(3, make([]byte, payloadBytes)); err != nil {
					t.Fatal(err)
				}
				if err = backend.close(); err != nil {
					t.Fatal(err)
				}
				// The next mode reconnects to the same endpoint after client-only cleanup.
			}
		})
	}
}
