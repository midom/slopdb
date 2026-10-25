package sql

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"slopdb/storage"
)

func accessReuseSQL(tb testing.TB, s *Schema, query string) *ResultSet {
	tb.Helper()
	statements, err := Parse(query)
	if err != nil {
		tb.Fatal(err)
	}
	var result *ResultSet
	for _, statement := range statements {
		if err := s.StartStatement(); err != nil {
			tb.Fatal(err)
		}
		run := func() error {
			switch stmt := statement.(type) {
			case *CreateTableStmt:
				return s.CreateTable(stmt)
			case *CreateIndexStmt:
				return s.CreateIndex(stmt)
			case *InsertStmt:
				_, _, err := s.Insert(stmt)
				return err
			case *UpdateStmt:
				_, _, err := s.Update(stmt)
				return err
			case *DeleteStmt:
				_, _, err := s.Delete(stmt)
				return err
			case *BeginStmt:
				return s.BeginWithOptions(stmt)
			case *CommitStmt:
				return s.Commit()
			case *RollbackStmt:
				return s.Rollback()
			case *SelectStmt:
				var err error
				result, err = s.Select(stmt)
				return err
			default:
				return fmt.Errorf("unsupported test statement %T", stmt)
			}
		}
		switch statement.(type) {
		case *BeginStmt, *CommitStmt, *RollbackStmt:
			err = run()
		default:
			err = s.ExecuteStatement(run)
		}
		s.EndStatement()
		if err != nil {
			tb.Fatalf("%s: %v", query, err)
		}
	}
	return result
}

func newAccessReuseSchema(tb testing.TB, rows int) (*Schema, *storage.LSMTree) {
	tb.Helper()
	opts := storage.DefaultOptions()
	opts.DisableWALSync = true
	opts.PageSize = 512
	opts.CompactionL0Trigger = 100
	lsm, err := storage.NewLSMTree(tb.TempDir(), opts)
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() { _ = lsm.Close() })
	s, err := NewSchema(lsm, 4096)
	if err != nil {
		tb.Fatal(err)
	}
	accessReuseSQL(tb, s, "CREATE TABLE t(id BIGINT PRIMARY KEY,g BIGINT,v BIGINT,payload BLOB);CREATE INDEX tg ON t(g)")
	var input []string
	for i := 0; i < rows; i++ {
		payload := "NULL"
		if i%17 != 0 {
			data := bytes.Repeat([]byte{byte(i % 5)}, 180+i%7)
			if i%13 == 0 {
				data = []byte{}
			}
			payload = "X'" + hex.EncodeToString(data) + "'"
		}
		v := "NULL"
		if i%11 != 0 {
			v = fmt.Sprint(i % 17)
		}
		input = append(input, fmt.Sprintf("(%d,%d,%s,%s)", i, i%3, v, payload))
	}
	if rows > 0 {
		accessReuseSQL(tb, s, "INSERT INTO t VALUES "+strings.Join(input, ","))
	}
	if err := lsm.CompactRange(); err != nil {
		tb.Fatal(err)
	}
	return s, lsm
}

func TestAggregateAccessReuseAnswersAndRetainedBlobs(t *testing.T) {
	for _, dialect := range []Dialect{DialectNative, DialectMySQL, DialectPostgreSQL} {
		t.Run(dialect.String(), func(t *testing.T) {
			s, _ := newAccessReuseSchema(t, 600)
			if err := s.SetDialect(dialect); err != nil {
				t.Fatal(err)
			}
			for _, tc := range []struct {
				where string
				keep  func(int) bool
			}{
				{"", func(int) bool { return true }},
				{" WHERE id>=100 AND id<450", func(i int) bool { return i >= 100 && i < 450 }},
				{" WHERE g=1", func(i int) bool { return i%3 == 1 }},
				{" WHERE g=1 OR id BETWEEN 150 AND 350", func(i int) bool { return i%3 == 1 || i >= 150 && i <= 350 }},
				{" WHERE id IN (1,4,4,7,9999)", func(i int) bool { return i == 1 || i == 4 || i == 7 }},
			} {
				query := "SELECT COUNT(*) AS n,COUNT(v) AS nv,MIN(v) AS lo,MAX(v) AS hi,MIN(payload) AS bmin,MAX(payload) AS bmax,COUNT(DISTINCT payload) AS d FROM t" + tc.where
				got := accessReuseSQL(t, s, query)
				if len(got.Rows) != 1 {
					t.Fatal("wrong aggregate row count")
				}
				expected := expectedAccessAggregate(600, tc.keep)
				assertAccessAggregate(t, got.Rows[0], expected)
				// A subsequent read must not overwrite emitted MIN/MAX BLOBs.
				saved := append([]byte(nil), got.Rows[0]["bmax"].Blob...)
				accessReuseSQL(t, s, "SELECT MIN(payload),MAX(payload) FROM t WHERE g=2")
				if !bytes.Equal(got.Rows[0]["bmax"].Blob, saved) {
					t.Fatal("later query changed retained aggregate BLOB")
				}
			}
			grouped := accessReuseSQL(t, s, "SELECT g,COUNT(*) AS n,COUNT(v) AS nv,MIN(v) AS lo,MAX(v) AS hi,MIN(payload) AS bmin,MAX(payload) AS bmax,COUNT(DISTINCT payload) AS d FROM t GROUP BY g ORDER BY g")
			if len(grouped.Rows) != 3 {
				t.Fatal("wrong groups", len(grouped.Rows))
			}
			for g, row := range grouped.Rows {
				assertAccessAggregate(t, row, expectedAccessAggregate(600, func(i int) bool { return i%3 == g }))
			}
			// Native/PG can emit ordered PK groups through one accumulator.
			ordered := accessReuseSQL(t, s, "SELECT id,MIN(payload) AS lo,MAX(payload) AS hi FROM t GROUP BY id ORDER BY id")
			if len(ordered.Rows) != 600 {
				t.Fatal("wrong ordered groups")
			}
			for i, row := range ordered.Rows {
				if row["id"].Int != int64(i) {
					t.Fatal("ordered group id changed")
				}
				if i%17 == 0 {
					if !row["lo"].Null || !row["hi"].Null {
						t.Fatal("NULL group changed")
					}
					continue
				}
				data := bytes.Repeat([]byte{byte(i % 5)}, 180+i%7)
				if i%13 == 0 {
					data = []byte{}
				}
				if row["lo"].Null || row["hi"].Null || !bytes.Equal(row["lo"].Blob, data) || !bytes.Equal(row["hi"].Blob, data) {
					t.Fatalf("emitted group %d BLOB changed", i)
				}
			}
		})
	}
}

func expectedAccessAggregate(n int, keep func(int) bool) map[string]Value {
	result := map[string]Value{"n": NewIntValue(0), "nv": NewIntValue(0), "d": NewIntValue(0), "lo": NewNullValue(), "hi": NewNullValue(), "bmin": NewNullValue(), "bmax": NewNullValue()}
	distinct := make(map[string]bool)
	for i := 0; i < n; i++ {
		if !keep(i) {
			continue
		}
		count := result["n"]
		count.Int++
		result["n"] = count
		if i%11 != 0 {
			count := result["nv"]
			count.Int++
			result["nv"] = count
			v := NewIntValue(int64(i % 17))
			if result["lo"].Null || v.Int < result["lo"].Int {
				result["lo"] = v
			}
			if result["hi"].Null || v.Int > result["hi"].Int {
				result["hi"] = v
			}
		}
		if i%17 != 0 {
			data := bytes.Repeat([]byte{byte(i % 5)}, 180+i%7)
			if i%13 == 0 {
				data = []byte{}
			}
			distinct[string(data)] = true
			v := NewBlobValue(data)
			if result["bmin"].Null || bytes.Compare(data, result["bmin"].Blob) < 0 {
				result["bmin"] = v
			}
			if result["bmax"].Null || bytes.Compare(data, result["bmax"].Blob) > 0 {
				result["bmax"] = v
			}
		}
	}
	result["d"] = NewIntValue(int64(len(distinct)))
	return result
}

func assertAccessAggregate(t *testing.T, got, want map[string]Value) {
	t.Helper()
	for name, expected := range want {
		actual, ok := got[name]
		if !ok || actual.Null != expected.Null || !actual.Null && (actual.Type != expected.Type || actual.Int != expected.Int || !bytes.Equal(actual.Blob, expected.Blob)) {
			t.Fatalf("%s = %#v, want %#v", name, actual, expected)
		}
	}
}

func TestAggregateAccessSnapshotsPrivateWritesAndProfiles(t *testing.T) {
	s, lsm := newAccessReuseSchema(t, 600)
	reader, writer := s.NewSession(), s.NewSession()
	if err := reader.StartStatement(); err != nil {
		t.Fatal(err)
	}
	stmt := statisticsSelect(t, "SELECT COUNT(*) AS n FROM t")
	accessReuseSQL(t, writer, "DELETE FROM t WHERE id<10;UPDATE t SET payload=X'7878' WHERE g=1")
	result, err := reader.Select(stmt)
	if err != nil || result.Rows[0]["n"].Int != 600 {
		t.Fatal("statement lost snapshot", result, err)
	}
	reader.EndStatement()
	if result := accessReuseSQL(t, reader, "SELECT COUNT(*) AS n FROM t"); result.Rows[0]["n"].Int != 590 {
		t.Fatal("new statement retained old snapshot")
	}
	accessReuseSQL(t, reader, "BEGIN;INSERT INTO t VALUES(9999,1,5,X'ffff');DELETE FROM t WHERE id=10;UPDATE t SET v=90,payload=X'7777' WHERE id=11")
	if got := accessReuseSQL(t, reader, "SELECT COUNT(*) AS n,MAX(v) AS hi FROM t"); got.Rows[0]["n"].Int != 590 || got.Rows[0]["hi"].Int != 90 {
		t.Fatal("private overlay ignored", got.Rows)
	}
	if got := accessReuseSQL(t, reader, "SELECT MIN(payload) AS lo,MAX(payload) AS hi FROM t WHERE g=1"); !bytes.Equal(got.Rows[0]["hi"].Blob, []byte{255, 255}) {
		t.Fatal("noncovering index overlay lost BLOB", got.Rows)
	}
	accessReuseSQL(t, reader, "ROLLBACK")
	accessReuseSQL(t, reader, "CREATE TEMPORARY TABLE tmp(id BIGINT PRIMARY KEY,v BLOB);INSERT INTO tmp VALUES(1,X''),(2,NULL),(3,X'ffff')")
	if got := accessReuseSQL(t, reader, "SELECT COUNT(*) AS n,COUNT(v) AS nv,MIN(v) AS lo,MAX(v) AS hi FROM tmp"); got.Rows[0]["n"].Int != 3 || got.Rows[0]["nv"].Int != 2 || got.Rows[0]["lo"].Null || len(got.Rows[0]["lo"].Blob) != 0 {
		t.Fatal("temporary/empty/NULL aggregate failed", got.Rows)
	}
	profile := reader.StartQueryProfile()
	got := accessReuseSQL(t, reader, "SELECT COUNT(*) AS n FROM t WHERE g=1")
	reader.StopQueryProfile()
	if got.Rows[0]["n"].Int != 197 || profile.IndexEntriesExamined != 197 || profile.PointLookups != 0 {
		t.Fatal("covering profile changed", got.Rows, profile)
	}
	profile = reader.StartQueryProfile()
	accessReuseSQL(t, reader, "SELECT MAX(payload) FROM t WHERE g=1")
	reader.StopQueryProfile()
	if profile.IndexEntriesExamined != 197 || profile.PointLookups != 197 {
		t.Fatal("noncovering profile changed", profile)
	}
	if err := lsm.Flush(); err != nil {
		t.Fatal(err)
	}
}

func TestBorrowedAccessCancellationErrorsAndDecodeLayout(t *testing.T) {
	s, lsm := newAccessReuseSchema(t, 600)
	info := s.tables["t"]
	if err := s.StartStatement(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	s.ctx = ctx
	n := 0
	err := s.walkAccessRaw(info, map[string]bool{}, nil, func(data []byte, cols []scanColumn) error {
		if len(cols) != 0 {
			t.Fatal("constant aggregate retained decode columns")
		}
		n++
		if n == 1 {
			cancel()
		}
		return nil
	})
	if !errors.Is(err, context.Canceled) || n < 1 {
		t.Fatal("cancellation not propagated", n, err)
	}
	s.EndStatement()
	s.ctx = nil
	marker := errors.New("callback failure")
	err = s.walkAccessRaw(info, map[string]bool{"id": true}, nil, func([]byte, []scanColumn) error { return marker })
	if !errors.Is(err, marker) {
		t.Fatal("callback error swallowed", err)
	}
	if err := lsm.Close(); err != nil {
		t.Fatal(err)
	}
	err = s.walkAccessRaw(info, map[string]bool{}, nil, func([]byte, []scanColumn) error { return nil })
	if err == nil {
		t.Fatal("closed storage read succeeded")
	}
}

func TestBorrowedTransactionIteratorDirectionsOwnershipAndBounds(t *testing.T) {
	s, _ := newAccessReuseSchema(t, 20)
	accessReuseSQL(t, s, "BEGIN;UPDATE t SET v=90,payload=X'7777' WHERE id=3;DELETE FROM t WHERE id=4;INSERT INTO t VALUES(30,0,99,X'ffff')")
	if err := s.StartStatement(); err != nil {
		t.Fatal(err)
	}
	defer s.EndStatement()
	info := s.tables["t"]
	key := func(id int64) []byte { return info.encodePK("t", map[string]Value{"id": NewIntValue(id)}) }
	for _, reverse := range []bool{false, true} {
		it := s.newBorrowedRangeIterator(key(1), key(31), reverse)
		var held [][]byte
		var ids []int64
		for it.Valid() {
			owned := storage.IteratorValueInto(it, nil)
			row := decodeRow(owned, info)
			ids = append(ids, row["id"].Int)
			held = append(held, owned)
			if row["id"].Int == 3 && (row["v"].Int != 90 || !bytes.Equal(row["payload"].Blob, []byte{0x77, 0x77})) {
				t.Fatal("private shadow lost")
			}
			if reverse {
				it.Prev()
			} else {
				it.Next()
			}
		}
		want := []int64{1, 2, 3, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 30}
		if reverse {
			for i, j := 0, len(want)-1; i < j; i, j = i+1, j-1 {
				want[i], want[j] = want[j], want[i]
			}
		}
		if !reflect.DeepEqual(ids, want) || storage.IteratorError(it) != nil {
			t.Fatal("overlay traversal changed", ids, storage.IteratorError(it))
		}
		it.Seek(key(3))
		it.Seek(it.Key().UserKey)
		if !it.Valid() || decodeRow(it.Value(), info)["id"].Int != 3 {
			t.Fatal("borrowed seek lost private row")
		}
		it.Prev()
		if !it.Valid() || decodeRow(it.Value(), info)["id"].Int != 2 {
			t.Fatal("reverse predecessor changed")
		}
		it.Next()
		if !it.Valid() || decodeRow(it.Value(), info)["id"].Int != 3 {
			t.Fatal("direction switch changed")
		}
		it.Seek(key(4))
		if !it.Valid() || decodeRow(it.Value(), info)["id"].Int != 5 {
			t.Fatal("seek exposed deleted private row")
		}
		it.Close()
		it.Close()
		it.Next()
		it.Prev()
		it.Seek(nil)
		if it.Valid() {
			t.Fatal("closed overlay revived")
		}
		for i, data := range held {
			if decodeRow(data, info)["id"].Int != ids[i] {
				t.Fatal("owned value changed after overlay movement/close")
			}
		}
	}
	// Ordinary overlays continue returning independently retained encoded rows.
	it := s.newRangeIterator(key(1), key(8), false)
	var values [][]byte
	for it.Valid() {
		values = append(values, it.Value())
		it.Next()
	}
	it.Close()
	for i, want := range []int64{1, 2, 3, 5, 6, 7} {
		if decodeRow(values[i], info)["id"].Int != want {
			t.Fatal("ordinary overlay ownership changed")
		}
	}
	if err := s.Rollback(); err != nil {
		t.Fatal(err)
	}
}

func BenchmarkAggregateAccessReuse(b *testing.B) {
	const n = 4096
	s, _ := newAccessReuseSchema(b, n)
	grouped := make([]map[string]Value, 3)
	for g := range grouped {
		all := expectedAccessAggregate(n, func(i int) bool { return i%3 == g })
		grouped[g] = map[string]Value{"g": NewIntValue(int64(g)), "n": all["n"], "bmin": all["bmin"], "bmax": all["bmax"], "d": all["d"]}
	}
	selected := expectedAccessAggregate(n, func(i int) bool { return i%3 == 1 })
	cases := []struct {
		query    string
		expected []map[string]Value
	}{
		{"SELECT COUNT(*) AS n FROM t", []map[string]Value{{"n": NewIntValue(n)}}},
		{"SELECT COUNT(*) AS n FROM t WHERE id%2=0", []map[string]Value{{"n": NewIntValue(n / 2)}}},
		{"SELECT g,COUNT(*) AS n,MIN(payload) AS bmin,MAX(payload) AS bmax,COUNT(DISTINCT payload) AS d FROM t GROUP BY g ORDER BY g", grouped},
		{"SELECT COUNT(*) AS n FROM t WHERE g=1", []map[string]Value{{"n": selected["n"]}}},
		{"SELECT MAX(payload) AS bmax FROM t WHERE g=1", []map[string]Value{{"bmax": selected["bmax"]}}},
	}
	for _, dialect := range []Dialect{DialectNative, DialectMySQL} {
		if err := s.SetDialect(dialect); err != nil {
			b.Fatal(err)
		}
		for _, tc := range cases {
			b.Run(dialect.String()+"/"+tc.query, func(b *testing.B) {
				statements, err := Parse(tc.query)
				if err != nil {
					b.Fatal(err)
				}
				stmt := statements[0].(*SelectStmt)
				run := func() *ResultSet {
					if err := s.StartStatement(); err != nil {
						b.Fatal(err)
					}
					result, err := s.Select(stmt)
					s.EndStatement()
					if err != nil {
						b.Fatal(err)
					}
					return result
				}
				check := func(result *ResultSet) {
					if len(result.Rows) != len(tc.expected) {
						b.Fatal("aggregate row count changed")
					}
					for i, want := range tc.expected {
						for name, expected := range want {
							got, ok := result.Rows[i][name]
							if !ok || got.Null != expected.Null || !got.Null && (got.Type != expected.Type || got.Int != expected.Int || !bytes.Equal(got.Blob, expected.Blob)) {
								b.Fatalf("row%d %s=%+v, want %+v", i, name, got, expected)
							}
						}
					}
				}
				check(run())
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					check(run())
				}
			})
		}
	}
}
