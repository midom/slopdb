package main

import (
	"errors"
	"net"
	"sync"
	"testing"

	"slopdb/mysqlserver"
)

func TestLeanMySQLBackendCheckedRowsAndOwnership(t *testing.T) {
	for _, mode := range []string{"text", "prepared"} {
		t.Run(mode, func(t *testing.T) {
			database := wireTestDatabase(t, true)
			backend, err := newLeanMySQLBackend(config{Workers: 3, QueryMode: mode}, database)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := backend.close(); err != nil {
					t.Error(err)
				}
			})
			if len(backend.readers) != 3 {
				t.Fatal("wrong reader count", len(backend.readers))
			}
			var wg sync.WaitGroup
			failures := make(chan error, 3)
			for worker, reader := range backend.readers {
				wg.Add(1)
				go func(worker int, reader benchmarkReader) {
					defer wg.Done()
					expected := make([]byte, payloadBytes)
					for n := 0; n < 12; n++ {
						id := int64((worker+n)%4 + 1)
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
			if err := reader.query(99); err == nil {
				t.Fatal("missing answer accepted")
			}
			if err := reader.query(2); err != nil {
				t.Fatal("query after drained empty answer", err)
			}
			if err := reader.validate(3, make([]byte, payloadBytes)); err == nil {
				t.Fatal("wrong typed answer accepted")
			}
			if err := reader.validate(2, make([]byte, payloadBytes)); err != nil {
				t.Fatal(err)
			}
			if err := backend.close(); err != nil {
				t.Fatal(err)
			}
			if err := backend.close(); err != nil {
				t.Fatal("second close", err)
			}
			if _, err := database.Exec("SELECT id FROM small_rows WHERE id=1"); err != nil {
				t.Fatal("backend closed caller engine", err)
			}
		})
	}
}

func TestLeanMySQLBackendAttachExistingServer(t *testing.T) {
	database := wireTestDatabase(t, true)
	server, err := mysqlserver.New(database, mysqlserver.Config{})
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
		// A nil engine proves the attach path cannot open/own a second database.
		backend, err := newLeanMySQLBackend(config{Endpoint: listener.Addr().String(), QueryMode: mode, Workers: 2}, nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := backend.readers[0].query(4); err != nil {
			t.Fatal(err)
		}
		if err := backend.readers[0].validate(4, make([]byte, payloadBytes)); err != nil {
			t.Fatal(err)
		}
		if err := backend.close(); err != nil {
			t.Fatal(err)
		}
	}
}
