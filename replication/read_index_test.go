package replication

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestReadIndexDoesNotAppendLog(t *testing.T) {
	for _, mode := range []Durability{DurabilitySync, DurabilityAsync} {
		t.Run(string(mode), func(t *testing.T) {
			config := asyncConfig(t, "reader", "tcp")
			config.Durability = mode
			config.Bootstrap = true
			node := openTestNode(t, config)
			ready(t, node)
			exec(t, node, "CREATE TABLE items(id INTEGER PRIMARY KEY,v TEXT); INSERT INTO items VALUES(1,'visible')")
			before := node.raftNode().LastIndex()
			statement, err := node.Database().Prepare("SELECT v FROM items WHERE id=?")
			if err != nil {
				t.Fatal(err)
			}
			defer statement.Close()
			for i := 0; i < 10; i++ {
				rows, err := statement.Exec(int64(1))
				if err != nil || len(rows.Rows) != 1 || rows.Rows[0]["v"].Text != "visible" {
					t.Fatalf("prepared read: %+v %v", rows, err)
				}
				stream, err := node.Database().Query("SELECT v FROM items WHERE id=1")
				if err != nil {
					t.Fatal(err)
				}
				if !stream.Next() {
					t.Fatalf("streaming read: %v", stream.Err())
				}
				var value string
				if err = stream.Scan(&value); err != nil || value != "visible" {
					t.Fatalf("streamed value=%q: %v", value, err)
				}
				if err = stream.Close(); err != nil {
					t.Fatal(err)
				}
			}
			if after := node.raftNode().LastIndex(); after != before {
				t.Fatalf("read-only SQL appended %d Raft entries", after-before)
			}
		})
	}
}

func TestReadIndexWaitsForNativeApplication(t *testing.T) {
	config := asyncConfig(t, "backlog", "tcp")
	config.Bootstrap = true
	node := openTestNode(t, config)
	ready(t, node)
	exec(t, node, "CREATE TABLE items(id INTEGER PRIMARY KEY,v TEXT); INSERT INTO items VALUES(1,'old')")
	writer, err := node.Database().NewSession()
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close()
	reader, err := node.Database().NewSession()
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	node.fsmMu.Lock()
	held := true
	defer func() {
		if held {
			node.fsmMu.Unlock()
		}
	}()
	before := node.raftNode().CommitIndex()
	writeDone := make(chan error, 1)
	go func() {
		_, err := writer.Exec("UPDATE items SET v='committed' WHERE id=1")
		writeDone <- err
	}()
	deadline := time.Now().Add(5 * time.Second)
	for node.raftNode().CommitIndex() <= before {
		if time.Now().After(deadline) {
			t.Fatal("write was not committed to Raft")
		}
		time.Sleep(time.Millisecond)
	}
	if node.applied.Load() > before {
		t.Fatal("native application was not held")
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	readDone := make(chan error, 1)
	go func() {
		_, err := reader.ExecContext(ctx, "SELECT v FROM items WHERE id=1")
		readDone <- err
	}()
	for node.applyNotice.Load() == nil {
		select {
		case err := <-readDone:
			t.Fatalf("read completed before native application: %v", err)
		default:
		}
		if time.Now().After(deadline) {
			t.Fatal("reader did not register an application wait")
		}
		time.Sleep(time.Millisecond)
	}
	cancel()
	select {
	case err := <-readDone:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("application wait ignored cancellation: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("application wait did not cancel")
	}
	node.fsmMu.Unlock()
	held = false
	if err = <-writeDone; err != nil {
		t.Fatal(err)
	}
	rows, err := reader.Exec("SELECT v FROM items WHERE id=1")
	if err != nil || len(rows.Rows) != 1 || rows.Rows[0]["v"].Text != "committed" {
		t.Fatalf("read missed the committed value: %+v %v", rows, err)
	}
}
