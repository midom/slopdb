package mysqlserver

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"slopdb/sql"
)

func TestMySQLSessionVariablesAndGlobalSettings(t *testing.T) {
	s := startTestServer(t, "tcp", Config{})
	pool := s.open(t, "slopdb", "", "slopdb")
	c, err := pool.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if _, err = c.ExecContext(context.Background(), "SET group_concat_max_len=262144, `sql_mode`=''"); err != nil {
		t.Fatal(err)
	}
	var length int64
	var mode string
	var readOnly int
	if err = c.QueryRowContext(context.Background(), "SELECT @@SESSION.group_concat_max_len, @@sql_mode, @@GLOBAL.read_only AS Value").Scan(&length, &mode, &readOnly); err != nil || length != 262144 || mode != "" || readOnly != 0 {
		t.Fatalf("settings: %d %q %d %v", length, mode, readOnly, err)
	}
	var name, value string
	if err = c.QueryRowContext(context.Background(), `SHOW GLOBAL VARIABLES LIKE 'ft\\_min\\_word\\_len'`).Scan(&name, &value); err != nil || name != "ft_min_word_len" || value != "4" {
		t.Fatalf("SHOW: %q %q %v", name, value, err)
	}
	if _, err = c.ExecContext(context.Background(), "SET group_concat_max_len=16, made_up_setting=1"); err == nil {
		t.Fatal("unknown setting accepted")
	}
	if err = c.QueryRowContext(context.Background(), "SELECT @@group_concat_max_len").Scan(&length); err != nil || length != 262144 {
		t.Fatalf("partial settings applied: %d %v", length, err)
	}
	if _, err = c.ExecContext(context.Background(), "SELECT @@autocommit; CREATE TABLE escaped(id INTEGER)"); err == nil {
		t.Fatal("multiple statements bypassed protocol gate")
	}
	if _, err = pool.Exec("SELECT * FROM escaped"); err == nil {
		t.Fatal("statement after variable query executed")
	}
	if _, err = c.ExecContext(context.Background(), "SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED"); err != nil {
		t.Fatal(err)
	}
	var other int64
	if err = pool.QueryRow("SELECT @@group_concat_max_len").Scan(&other); err != nil || other != 1024 {
		t.Fatalf("setting leaked to another connection: %d %v", other, err)
	}
}

func TestNamedLockOwnershipAndCancellation(t *testing.T) {
	var locks namedLocks
	ctx := context.Background()
	args := []sql.Value{sql.NewTextValue("lock"), sql.NewIntValue(0)}
	for i := 0; i < 2; i++ {
		v, err := locks.call(ctx, 1, "GET_LOCK", args)
		if err != nil || v.Int != 1 {
			t.Fatal(v, err)
		}
	}
	v, err := locks.call(ctx, 2, "GET_LOCK", args)
	if err != nil || v.Int != 0 {
		t.Fatal(v, err)
	}
	v, err = locks.call(ctx, 2, "RELEASE_LOCK", args[:1])
	if err != nil || v.Int != 0 {
		t.Fatal(v, err)
	}
	v, err = locks.call(ctx, 1, "RELEASE_LOCK", args[:1])
	if err != nil || v.Int != 1 {
		t.Fatal(v, err)
	}
	v, err = locks.call(ctx, 2, "IS_FREE_LOCK", args[:1])
	if err != nil || v.Int != 0 {
		t.Fatal("reentrant lock released too early", v, err)
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	_, err = locks.call(cancelled, 2, "GET_LOCK", []sql.Value{args[0], sql.NewIntValue(30)})
	if !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	locks.releaseOwner(1)
	v, err = locks.call(ctx, 2, "GET_LOCK", args)
	if err != nil || v.Int != 1 {
		t.Fatal(v, err)
	}
	locks.releaseOwner(2)
	v, err = locks.call(ctx, 1, "RELEASE_LOCK", args[:1])
	if err != nil || !v.Null {
		t.Fatal(v, err)
	}
}

func TestNamedLockWakeOnOwnerClose(t *testing.T) {
	var locks namedLocks
	args := []sql.Value{sql.NewTextValue("wake"), sql.NewIntValue(0)}
	if _, err := locks.call(context.Background(), 1, "GET_LOCK", args); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		v, err := locks.call(context.Background(), 2, "GET_LOCK", []sql.Value{args[0], sql.NewIntValue(5)})
		if err == nil && v.Int != 1 {
			err = errors.New("waiter did not acquire released lock")
		}
		done <- err
	}()
	locks.releaseOwner(1)
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("lock waiter did not wake")
	}
}

func TestMySQLVariableStringsAreNotRewritten(t *testing.T) {
	s := startTestServer(t, "tcp", Config{})
	pool := s.open(t, "slopdb", "", "slopdb")
	var value string
	if err := pool.QueryRow(`SELECT '@@GLOBAL.read_only' AS value`).Scan(&value); err != nil || !strings.Contains(value, "@@GLOBAL") {
		t.Fatal(value, err)
	}
}

func TestNamedLocksAcceptExactNumericTimeouts(t *testing.T) {
	var locks namedLocks
	decimal, err := sql.NewDecimalValue("0.001")
	if err != nil {
		t.Fatal(err)
	}
	for _, timeout := range []sql.Value{decimal, sql.NewUnsignedValue(0)} {
		value, err := locks.call(context.Background(), 1, "GET_LOCK", []sql.Value{sql.NewTextValue("exact"), timeout})
		if err != nil || value.Int != 1 {
			t.Fatal(value, err)
		}
		locks.releaseOwner(1)
	}
}
