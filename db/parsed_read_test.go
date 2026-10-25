package db

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"testing"

	"slopdb/sql"
)

func parseReadTest(t *testing.T, query string) *sql.SelectStmt {
	t.Helper()
	statements, err := sql.Parse(query)
	if err != nil || len(statements) != 1 {
		t.Fatalf("parse %q: %v", query, err)
	}
	return statements[0].(*sql.SelectStmt)
}

// Test adapter distinguishes the result and streaming alternatives without
// retaining an open cursor while the next assertion executes on the session.
func tryParsedPoint(s *Session, ctx context.Context, dst *ExecResult, query string, stmt *sql.SelectStmt) (*ExecResult, bool, error) {
	r, rows, err := s.QueryParsedContextInto(ctx, dst, query, stmt)
	if rows != nil {
		defer rows.Close()
	}
	return r, r != nil || err != nil, err
}

func parsedPointValue(t *testing.T, s *Session, dst *ExecResult, query string, want int64) {
	t.Helper()
	r, handled, err := tryParsedPoint(s, context.Background(), dst, query, parseReadTest(t, query))
	if err != nil || !handled || r != dst || len(r.Rows) != 1 || r.Rows[0]["v"].Int != want {
		t.Fatalf("%s: result%+v handled%v error%v, want%d", query, r, handled, err, want)
	}
}

func TestParsedPointOwnershipProjectionAndFallback(t *testing.T) {
	d := namespaceTestDatabase(t)
	s := isolationSession(t, d)
	isolationExec(t, s, "CREATE TABLE parsed_points(id BIGINT PRIMARY KEY,v BIGINT,b BLOB);INSERT INTO parsed_points VALUES(1,10,X'0102'),(2,20,X''),(3,30,NULL)")
	var dst ExecResult
	parsedPointValue(t, s, &dst, "SELECT id,v,b FROM parsed_points WHERE id=1", 10)
	blob := dst.Rows[0]["b"].Blob
	columns := &dst.Columns[0]
	parsedPointValue(t, s, &dst, "SELECT id,v,b FROM parsed_points WHERE 1=id", 10)
	if &dst.Rows[0]["b"].Blob[0] != &blob[0] || &dst.Columns[0] != columns {
		t.Fatal("caller-owned buffers were not reused")
	}
	owned, err := s.Exec("SELECT id,v,b FROM parsed_points WHERE id=1")
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []int{2, 3, 99, 1} {
		query := fmt.Sprintf("SELECT id,v,b FROM parsed_points WHERE id=%d", id)
		r, handled, err := tryParsedPoint(s, context.Background(), &dst, query, parseReadTest(t, query))
		if err != nil || !handled || r != &dst {
			t.Fatal(r, handled, err)
		}
		if id == 99 {
			if len(r.Rows) != 0 {
				t.Fatal("missing row retained previous result")
			}
			continue
		}
		if len(r.Rows) != 1 || r.Rows[0]["v"].Int != int64(id*10) {
			t.Fatal(r)
		}
		if id == 2 && (r.Rows[0]["b"].Null || r.Rows[0]["b"].Blob == nil || len(r.Rows[0]["b"].Blob) != 0) {
			t.Fatal("empty BLOB changed to NULL")
		}
		if id == 3 && !r.Rows[0]["b"].Null {
			t.Fatal("NULL BLOB changed to empty")
		}
	}
	if !bytes.Equal(owned.Rows[0]["b"].Blob, []byte{1, 2}) {
		t.Fatal("ordinary result aliases reused destination")
	}
	parsedPointValue(t, s, &dst, "SELECT v FROM parsed_points WHERE id=2", 20)
	if len(dst.Columns) != 1 || len(dst.Rows[0]) != 1 {
		t.Fatal("changed projection retained fields")
	}
	for _, query := range []string{
		"SELECT id,v FROM parsed_points WHERE v=10",
		"SELECT id,v FROM parsed_points WHERE id='1'",
		"SELECT id,v FROM parsed_points WHERE id=NULL",
		"SELECT id,v FROM parsed_points WHERE id=1 AND v=20",
		"SELECT id,v FROM parsed_points WHERE id=1 OR id=2",
		"SELECT id,v FROM parsed_points WHERE id=1 ORDER BY v",
		"SELECT id,v AS x FROM parsed_points WHERE id=1",
		"SELECT v,v FROM parsed_points WHERE id=1",
	} {
		before := dst
		statement := parseReadTest(t, query)
		_, handled, err := tryParsedPoint(s, context.Background(), &dst, query, statement)
		if err != nil || handled || !reflect.DeepEqual(before, dst) {
			t.Fatalf("fallback changed destination %s: %+v %v %v", query, dst, handled, err)
		}
		rows, err := s.QueryParsedContext(context.Background(), query, statement)
		if err != nil {
			t.Fatal(query, err)
		}
		for rows.Next() {
			if _, err := rows.Values(); err != nil {
				t.Fatal(query, err)
			}
		}
		if err := rows.Err(); err != nil {
			t.Fatal(query, err)
		}
		rows.Close()
	}
	for _, suffix := range []string{"LIMIT 0", "LIMIT 1 OFFSET 1"} {
		query := "SELECT v FROM parsed_points WHERE id=1 " + suffix
		if r, handled, err := tryParsedPoint(s, context.Background(), &dst, query, parseReadTest(t, query)); err != nil || !handled || len(r.Rows) != 0 {
			t.Fatal(query, r, handled, err)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, handled, err := tryParsedPoint(s, ctx, &dst, "point", parseReadTest(t, "SELECT v FROM parsed_points WHERE id=1")); !handled || !errors.Is(err, context.Canceled) {
		t.Fatal(handled, err)
	}
	if _, _, err := tryParsedPoint(s, context.Background(), nil, "point", parseReadTest(t, "SELECT v FROM parsed_points WHERE id=1")); err == nil {
		t.Fatal("nil destination accepted")
	}
}

func TestParsedPointNamespacesTemporaryAndCatalog(t *testing.T) {
	d := namespaceTestDatabase(t)
	s := isolationSession(t, d)
	other := isolationSession(t, d)
	isolationExec(t, s, "CREATE TABLE records(id INTEGER PRIMARY KEY,v INTEGER);INSERT INTO records VALUES(1,10);CREATE SCHEMA tenant;CREATE TABLE tenant.records(id INTEGER PRIMARY KEY,v INTEGER);INSERT INTO tenant.records VALUES(1,20)")
	var dst ExecResult
	query := "SELECT v FROM records WHERE id=1"
	parsedPointValue(t, s, &dst, query, 10)
	isolationExec(t, s, "USE tenant")
	parsedPointValue(t, s, &dst, query, 20)
	r, handled, err := tryParsedPoint(s, sql.WithNamespaceSearchPath(context.Background(), []string{"main"}), &dst, query, parseReadTest(t, query))
	if err != nil || !handled || r.Rows[0]["v"].Int != 10 {
		t.Fatal(r, handled, err)
	}
	isolationExec(t, s, "USE main;CREATE TEMPORARY TABLE records(id INTEGER PRIMARY KEY,v INTEGER);INSERT INTO records VALUES(1,40)")
	parsedPointValue(t, s, &dst, query, 40)
	parsedPointValue(t, other, &dst, query, 10)
	isolationExec(t, s, "DROP TEMPORARY TABLE records;ALTER TABLE records RENAME TO old_records;CREATE TABLE records(id INTEGER PRIMARY KEY,v INTEGER);INSERT INTO records VALUES(1,30)")
	parsedPointValue(t, s, &dst, query, 30)
	parsedPointValue(t, other, &dst, query, 30)
	isolationExec(t, s, "BEGIN;ALTER TABLE records RENAME COLUMN v TO x")
	q := "SELECT x FROM records WHERE id=1"
	r, handled, err = tryParsedPoint(s, context.Background(), &dst, q, parseReadTest(t, q))
	if err != nil || !handled || r.Rows[0]["x"].Int != 30 {
		t.Fatal(r, handled, err)
	}
	isolationExec(t, s, "ROLLBACK;ANALYZE records")
	parsedPointValue(t, s, &dst, query, 30)
	for _, dialect := range []sql.Dialect{sql.DialectPostgreSQL, sql.DialectMySQL, sql.DialectNative} {
		if err := s.SetDialect(dialect); err != nil {
			t.Fatal(err)
		}
		parsedPointValue(t, s, &dst, query, 30)
		parsedPointValue(t, s, &dst, "SELECT v FROM main.records WHERE id=1", 30)
		if dialect == sql.DialectPostgreSQL {
			parsedPointValue(t, s, &dst, "SELECT v FROM public.records WHERE id=1", 30)
		}
	}
	if err := s.RegisterRootSchemaAlias(context.Background(), "slopdb"); err != nil {
		t.Fatal(err)
	}
	parsedPointValue(t, s, &dst, "SELECT v FROM slopdb.records WHERE id=1", 30)
}

func TestParsedPointRepeatableReadAndPrivateWrites(t *testing.T) {
	for _, dialect := range []sql.Dialect{sql.DialectMySQL, sql.DialectPostgreSQL} {
		t.Run(fmt.Sprint(dialect), func(t *testing.T) {
			d := namespaceTestDatabase(t)
			a, b := isolationSession(t, d), isolationSession(t, d)
			if err := a.SetDialect(dialect); err != nil {
				t.Fatal(err)
			}
			isolationExec(t, a, "CREATE TABLE records(id INTEGER PRIMARY KEY,v INTEGER);INSERT INTO records VALUES(1,10);BEGIN ISOLATION LEVEL REPEATABLE READ")
			var dst ExecResult
			query := "SELECT v FROM records WHERE id=1"
			parsedPointValue(t, a, &dst, query, 10)
			isolationExec(t, b, "UPDATE records SET v=20 WHERE id=1")
			parsedPointValue(t, a, &dst, query, 10)
			isolationExec(t, a, "ROLLBACK;BEGIN;UPDATE records SET v=30 WHERE id=1")
			parsedPointValue(t, a, &dst, query, 30)
			parsedPointValue(t, b, &dst, query, 20)
			isolationExec(t, a, "ROLLBACK")
			parsedPointValue(t, a, &dst, query, 20)
		})
	}
}

func TestParsedPointConcurrentSessions(t *testing.T) {
	d := namespaceTestDatabase(t)
	isolationExec(t, isolationSession(t, d), "CREATE TABLE concurrent_points(id INTEGER PRIMARY KEY,v INTEGER);INSERT INTO concurrent_points VALUES(1,10),(2,20)")
	var wg sync.WaitGroup
	for worker := 0; worker < 32; worker++ {
		s := isolationSession(t, d)
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			var dst ExecResult
			for i := 0; i < 30; i++ {
				id := (worker+i)%2 + 1
				query := fmt.Sprintf("SELECT v FROM concurrent_points WHERE id=%d", id)
				statements, err := sql.Parse(query)
				if err != nil {
					t.Error(err)
					return
				}
				r, handled, err := tryParsedPoint(s, context.Background(), &dst, query, statements[0].(*sql.SelectStmt))
				if err != nil || !handled || len(r.Rows) != 1 || r.Rows[0]["v"].Int != int64(id*10) {
					t.Errorf("result%+v handled%v err%v", r, handled, err)
					return
				}
			}
		}(worker)
	}
	wg.Wait()
}

func TestParsedPointCancellationAfterSnapshot(t *testing.T) {
	d := namespaceTestDatabase(t)
	isolationExec(t, isolationSession(t, d), "CREATE TABLE cancelled_points(id INTEGER PRIMARY KEY,v INTEGER);INSERT INTO cancelled_points VALUES(1,10)")
	for _, query := range []string{
		"SELECT v FROM cancelled_points WHERE id=1",
		"SELECT v FROM cancelled_points WHERE id=99",
		"SELECT v FROM cancelled_points WHERE id=1 LIMIT 0",
		"SELECT v FROM cancelled_points WHERE id=1 LIMIT 1 OFFSET 1",
	} {
		s := isolationSession(t, d)
		ctx, cancel := context.WithCancel(context.Background())
		s.schema.SetStatementSnapshotHook(func(context.Context, *sql.Schema) (bool, error) {
			cancel()
			return false, nil
		})
		var dst ExecResult
		_, handled, err := tryParsedPoint(s, ctx, &dst, query, parseReadTest(t, query))
		cancel()
		if !handled || !errors.Is(err, context.Canceled) {
			t.Fatalf("%s: handled%v err%v", query, handled, err)
		}
	}
}

func TestParsedReadStreamingUsesOneSnapshot(t *testing.T) {
	d := namespaceTestDatabase(t)
	a, b := isolationSession(t, d), isolationSession(t, d)
	isolationExec(t, a, "CREATE TABLE streamed_points(id INTEGER PRIMARY KEY,v INTEGER);INSERT INTO streamed_points VALUES(1,10),(2,10)")
	snapshots := 0
	a.schema.SetStatementSnapshotHook(func(context.Context, *sql.Schema) (bool, error) {
		snapshots++
		return false, nil
	})
	query := "SELECT id,v FROM streamed_points WHERE v=10"
	dst := ExecResult{Changes: 123}
	result, rows, err := a.QueryParsedContextInto(context.Background(), &dst, query, parseReadTest(t, query))
	if err != nil || result != nil || rows == nil || dst.Changes != 123 || snapshots != 1 {
		t.Fatalf("stream result%+v rows%v err%v dst%+v snapshots%d", result, rows, err, dst, snapshots)
	}
	defer rows.Close()
	isolationExec(t, b, "UPDATE streamed_points SET v=20")
	count := 0
	for rows.Next() {
		values, err := rows.Values()
		if err != nil || len(values) != 2 || values[1].Int != 10 {
			t.Fatal(values, err)
		}
		count++
	}
	if rows.Err() != nil || count != 2 {
		t.Fatal("streaming fallback lost its original snapshot", count, rows.Err())
	}
}
