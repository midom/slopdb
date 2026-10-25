package db

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"slopdb/sql"
)

// Exercise the public adapters independently. Parsed execution can return either
// owned results or Rows as plans improve; the SQL answer/ownership is the contract.
var nonpointAPIs = []string{"exec", "query", "parsed"}

type nonpointOutput struct {
	result *ExecResult
	rows   *Rows
}

func nonpointOpen(t *testing.T, s *Session, api string, dst *ExecResult, query string) nonpointOutput {
	t.Helper()
	var output nonpointOutput
	var err error
	switch api {
	case "exec":
		output.result, err = s.Exec(query)
	case "query":
		output.rows, err = s.Query(query)
	case "parsed":
		output.result, output.rows, err = s.QueryParsedContextInto(context.Background(), dst, query, parseReadTest(t, query))
	default:
		t.Fatalf("unknown read adapter %q", api)
	}
	if err != nil {
		t.Fatalf("%s: %s: %v", api, query, err)
	}
	if (output.result == nil) == (output.rows == nil) {
		t.Fatalf("%s returned ambiguous result for %s", api, query)
	}
	return output
}

func (output nonpointOutput) close(t *testing.T) {
	t.Helper()
	if output.rows != nil {
		if err := output.rows.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

func (output nonpointOutput) collect(t *testing.T) ([]sql.ColumnDef, [][]sql.Value) {
	t.Helper()
	var columns []sql.ColumnDef
	var values [][]sql.Value
	if output.result != nil {
		columns = output.result.Columns
		for _, row := range output.result.Rows {
			cells := make([]sql.Value, len(columns))
			for i, column := range columns {
				var ok bool
				cells[i], ok = row[column.Name]
				if !ok {
					t.Fatalf("result column %q has no value", column.Name)
				}
			}
			values = append(values, cells)
		}
		return columns, values
	}
	columns = output.rows.Columns()
	for output.rows.Next() {
		cells, err := output.rows.Values()
		if err != nil {
			t.Fatal(err)
		}
		values = append(values, cells)
	}
	if err := output.rows.Err(); err != nil {
		t.Fatal(err)
	}
	return columns, values
}

func nonpointRead(t *testing.T, s *Session, api string, dst *ExecResult, query string) ([]sql.ColumnDef, [][]sql.Value) {
	t.Helper()
	output := nonpointOpen(t, s, api, dst, query)
	defer output.close(t)
	return output.collect(t)
}

func nonpointFixture(t *testing.T, s *Session) {
	t.Helper()
	isolationExec(t, s, "CREATE TABLE nonpoint_items(id BIGINT PRIMARY KEY,v BIGINT,b BLOB,note TEXT)")
	var insert strings.Builder
	insert.WriteString("INSERT INTO nonpoint_items VALUES")
	for id := 1; id <= 30; id++ {
		if id != 1 {
			insert.WriteByte(',')
		}
		switch id {
		case 2:
			insert.WriteString("(2,20,X'',NULL)")
		case 3:
			insert.WriteString("(3,NULL,NULL,'note-3')")
		default:
			fmt.Fprintf(&insert, "(%d,%d,X'%02x','note-%d')", id, id*10, id, id)
		}
	}
	isolationExec(t, s, insert.String())
}

func nonpointAssertFixture(t *testing.T, columns []sql.ColumnDef, values [][]sql.Value, ids []int64) {
	t.Helper()
	if len(columns) != 4 || len(values) != len(ids) {
		t.Fatalf("columns=%d rows=%d, want 4 columns and ids%v", len(columns), len(values), ids)
	}
	for i, kind := range []sql.ColumnType{sql.ColInteger, sql.ColInteger, sql.ColBlob, sql.ColText} {
		if columns[i].ColType != kind {
			t.Fatalf("column %d has type%v, want%v", i, columns[i].ColType, kind)
		}
	}
	if !columns[0].NotNull || columns[1].NotNull || columns[2].NotNull {
		t.Fatal("primary key or nullable field metadata changed", columns)
	}
	for i, id := range ids {
		row := values[i]
		if len(row) != 4 || row[0].Null || row[0].Int != id {
			t.Fatalf("row %d: %+v, want id%d", i, row, id)
		}
		if id == 3 {
			if !row[1].Null || !row[2].Null {
				t.Fatal("typed NULL integer/BLOB became ordinary values", row)
			}
		} else {
			if row[1].Null || row[1].Int != id*10 || row[2].Null || row[2].Type != sql.ColBlob {
				t.Fatal("wrong integer/BLOB value", row)
			}
			if id == 2 {
				if row[2].Blob == nil || len(row[2].Blob) != 0 {
					t.Fatal("non-NULL empty BLOB became NULL", row[2])
				}
			} else if !bytes.Equal(row[2].Blob, []byte{byte(id)}) {
				t.Fatal("BLOB does not match selected row", row)
			}
		}
		if id == 2 {
			if !row[3].Null {
				t.Fatal("NULL text became a value", row)
			}
		} else if row[3].Null || row[3].Text != fmt.Sprintf("note-%d", id) {
			t.Fatal("text does not match selected row", row)
		}
	}
}

func TestNonpointINAndRangeAnswersAcrossAPIsAndDialects(t *testing.T) {
	for _, dialect := range []sql.Dialect{sql.DialectNative, sql.DialectMySQL, sql.DialectPostgreSQL} {
		t.Run(dialect.String(), func(t *testing.T) {
			d := namespaceTestDatabase(t)
			s := isolationSession(t, d)
			if err := s.SetDialect(dialect); err != nil {
				t.Fatal(err)
			}
			nonpointFixture(t, s)
			cases := []struct {
				name, suffix string
				ids          []int64
			}{
				{"in_three", "WHERE id IN(9,2,17) ORDER BY id", []int64{2, 9, 17}},
				{"in_reverse", "WHERE id IN(9,2,17) ORDER BY id DESC", []int64{17, 9, 2}},
				{"in_duplicates_missing_null", "WHERE id IN(9,2,9,99,NULL,3,2) ORDER BY id", []int64{2, 3, 9}},
				{"in_only_missing_null", "WHERE id IN(NULL,99,NULL) ORDER BY id", nil},
				{"in_mixed_numeric", "WHERE id IN(2.0,9,2.5,NULL,99) ORDER BY id", []int64{2, 9}},
				{"in_residual", "WHERE id IN(1,2,3,4,9) AND v>=20 ORDER BY id", []int64{2, 4, 9}},
				{"in_null_residual", "WHERE id IN(1,2,3,4,9) AND (v IS NULL OR v=20) ORDER BY id", []int64{2, 3}},
				{"in_offset", "WHERE id IN(17,2,9,3,9) ORDER BY id LIMIT 2 OFFSET 1", []int64{3, 9}},
				{"in_limit_zero", "WHERE id IN(1,2,3) ORDER BY id LIMIT 0", nil},
				{"not_in_null", "WHERE id NOT IN(2,NULL) ORDER BY id LIMIT 3", nil},
				{"not_in_integer_set", "WHERE id NOT IN(2,9,17) ORDER BY id LIMIT 3", []int64{1, 3, 4}},
				{"between_keeps_residual", "WHERE id BETWEEN 2 AND 8 AND id>=4 ORDER BY id LIMIT 10", []int64{4, 5, 6, 7, 8}},
				{"between_upper_intersection", "WHERE id BETWEEN 2 AND 8 AND id<=5 ORDER BY id DESC LIMIT 10", []int64{5, 4, 3, 2}},
				{"between_reversed_bounds", "WHERE id BETWEEN 8 AND 2 ORDER BY id LIMIT 10", nil},
				{"ordered_table_prefix", "ORDER BY id LIMIT 5", []int64{1, 2, 3, 4, 5}},
				{"ordered_table_suffix", "ORDER BY id DESC LIMIT 3", []int64{30, 29, 28}},
				{"range_ten", "WHERE id>=7 ORDER BY id LIMIT 10", []int64{7, 8, 9, 10, 11, 12, 13, 14, 15, 16}},
				{"range_reverse", "WHERE id>=7 AND id<17 ORDER BY id DESC LIMIT 10", []int64{16, 15, 14, 13, 12, 11, 10, 9, 8, 7}},
				{"range_offset", "WHERE id>=7 ORDER BY id LIMIT 4 OFFSET 3", []int64{10, 11, 12, 13}},
				{"range_residual_fills_limit", "WHERE id>=1 AND id<=9 AND v>=40 ORDER BY id LIMIT 3", []int64{4, 5, 6}},
				{"range_limit_zero", "WHERE id>=7 ORDER BY id LIMIT 0", nil},
				{"range_missing", "WHERE id>=99 ORDER BY id LIMIT 10", nil},
				{"range_large_offset", "WHERE id>=1 ORDER BY id LIMIT 10 OFFSET 4294967296", nil},
				{"non_pk_order_fallback", "WHERE id IN(7,8,9,10) ORDER BY v DESC LIMIT 3", []int64{10, 9, 8}},
			}
			for _, api := range nonpointAPIs {
				t.Run(api, func(t *testing.T) {
					var dst ExecResult
					// IN without ORDER BY has no promised presentation order, but
					// still must return every requested row exactly once.
					columns, values := nonpointRead(t, s, api, &dst, "SELECT id,v,b,note FROM nonpoint_items WHERE id IN(9,2,17)")
					sort.Slice(values, func(i, j int) bool { return values[i][0].Int < values[j][0].Int })
					nonpointAssertFixture(t, columns, values, []int64{2, 9, 17})
					for _, tc := range cases {
						t.Run(tc.name, func(t *testing.T) {
							query := "SELECT id,v,b,note FROM nonpoint_items " + tc.suffix
							columns, values := nonpointRead(t, s, api, &dst, query)
							nonpointAssertFixture(t, columns, values, tc.ids)
						})
					}
				})
			}
		})
	}
}

func TestNonpointCompositePrimaryKeyRanges(t *testing.T) {
	d := namespaceTestDatabase(t)
	s := isolationSession(t, d)
	isolationExec(t, s, "CREATE TABLE nonpoint_composite(a BIGINT,b BIGINT,v TEXT,PRIMARY KEY(a,b));INSERT INTO nonpoint_composite VALUES(1,1,'11'),(1,2,'12'),(1,3,'13'),(2,1,'21'),(2,2,'22'),(2,3,'23'),(3,1,'31')")
	for _, api := range nonpointAPIs {
		t.Run(api, func(t *testing.T) {
			var dst ExecResult
			for _, tc := range []struct {
				suffix string
				want   []string
			}{
				{"WHERE a=2 AND b>=1 ORDER BY b DESC LIMIT 2", []string{"23", "22"}},
				{"WHERE a>=1 AND a<=2 ORDER BY a,b LIMIT 3 OFFSET 2", []string{"13", "21", "22"}},
				{"WHERE a=1 AND b IN(1,3,3,NULL,99) ORDER BY b", []string{"11", "13"}},
				{"WHERE a>=1 AND a<=2 ORDER BY a DESC,b DESC LIMIT 4", []string{"23", "22", "21", "13"}},
			} {
				_, values := nonpointRead(t, s, api, &dst, "SELECT a,b,v FROM nonpoint_composite "+tc.suffix)
				var got []string
				for _, row := range values {
					if row[2].Text != fmt.Sprintf("%d%d", row[0].Int, row[1].Int) {
						t.Fatal("composite key/value mismatch", row)
					}
					got = append(got, row[2].Text)
				}
				if !reflect.DeepEqual(got, tc.want) {
					t.Fatalf("%s: got%v want%v", tc.suffix, got, tc.want)
				}
			}
		})
	}
}

func nonpointIDsAndValues(t *testing.T, columns []sql.ColumnDef, values [][]sql.Value, ids, expected []int64) {
	t.Helper()
	if len(columns) != 2 || len(values) != len(ids) || len(ids) != len(expected) {
		t.Fatalf("invalid result dimensions: columns%d values%v wantids%v values%v", len(columns), values, ids, expected)
	}
	for i := range ids {
		if len(values[i]) != 2 || values[i][0].Null || values[i][0].Int != ids[i] || values[i][1].Null || values[i][1].Int != expected[i] {
			t.Fatalf("row %d got%+v wantid%d value%d", i, values[i], ids[i], expected[i])
		}
	}
}

func TestNonpointStatementSnapshotAcrossConcurrentCommit(t *testing.T) {
	for _, dialect := range []sql.Dialect{sql.DialectNative, sql.DialectMySQL, sql.DialectPostgreSQL} {
		for _, api := range nonpointAPIs {
			for _, shape := range []string{"in", "range"} {
				t.Run(dialect.String()+"/"+api+"/"+shape, func(t *testing.T) {
					d := namespaceTestDatabase(t)
					a, b := isolationSession(t, d), isolationSession(t, d)
					if err := a.SetDialect(dialect); err != nil {
						t.Fatal(err)
					}
					isolationExec(t, b, "CREATE TABLE nonpoint_snapshot(id BIGINT PRIMARY KEY,v BIGINT);INSERT INTO nonpoint_snapshot VALUES(1,10),(2,20),(3,30),(4,40),(5,50),(6,60),(7,70),(8,80),(9,90),(10,100)")
					query := "SELECT id,v FROM nonpoint_snapshot WHERE id IN(1,4,7,9,11) ORDER BY id"
					ids, values := []int64{1, 4, 7, 9}, []int64{10, 40, 70, 90}
					if shape == "range" {
						query = "SELECT id,v FROM nonpoint_snapshot WHERE id>=1 ORDER BY id LIMIT 10"
						ids, values = []int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, []int64{10, 20, 30, 40, 50, 60, 70, 80, 90, 100}
					}
					var dst ExecResult
					output := nonpointOpen(t, a, api, &dst, query)
					defer output.close(t)
					committed := make(chan error, 1)
					go func() {
						_, err := b.Exec("BEGIN;UPDATE nonpoint_snapshot SET v=v+100 WHERE id IN(1,4,9);DELETE FROM nonpoint_snapshot WHERE id=7;INSERT INTO nonpoint_snapshot VALUES(11,110);COMMIT")
						committed <- err
					}()
					select {
					case err := <-committed:
						if err != nil {
							t.Fatal(err)
						}
					case <-time.After(10 * time.Second):
						t.Fatal("snapshot reader blocked an independent committing writer")
					}
					columns, got := output.collect(t)
					nonpointIDsAndValues(t, columns, got, ids, values)
					output.close(t)
					columns, got = nonpointRead(t, a, api, &dst, "SELECT id,v FROM nonpoint_snapshot WHERE id IN(1,4,7,9,11) ORDER BY id")
					nonpointIDsAndValues(t, columns, got, []int64{1, 4, 9, 11}, []int64{110, 140, 190, 110})
				})
			}
		}
	}
}

func TestNonpointRepeatableReadOverlaysAndSavepoints(t *testing.T) {
	for _, dialect := range []sql.Dialect{sql.DialectMySQL, sql.DialectPostgreSQL} {
		for _, api := range nonpointAPIs {
			t.Run(dialect.String()+"/"+api, func(t *testing.T) {
				d := namespaceTestDatabase(t)
				a, b := isolationSession(t, d), isolationSession(t, d)
				if err := a.SetDialect(dialect); err != nil {
					t.Fatal(err)
				}
				isolationExec(t, a, "CREATE TABLE nonpoint_rr(id BIGINT PRIMARY KEY,v BIGINT);INSERT INTO nonpoint_rr VALUES(1,10),(2,20),(3,30),(4,40),(5,50);BEGIN ISOLATION LEVEL REPEATABLE READ")
				var dst ExecResult
				inQuery := "SELECT id,v FROM nonpoint_rr WHERE id IN(1,2,4,9) ORDER BY id"
				columns, got := nonpointRead(t, a, api, &dst, inQuery)
				nonpointIDsAndValues(t, columns, got, []int64{1, 2, 4}, []int64{10, 20, 40})
				isolationExec(t, b, "UPDATE nonpoint_rr SET v=111 WHERE id=1;INSERT INTO nonpoint_rr VALUES(9,90)")
				columns, got = nonpointRead(t, a, api, &dst, inQuery)
				nonpointIDsAndValues(t, columns, got, []int64{1, 2, 4}, []int64{10, 20, 40})
				isolationExec(t, a, "SAVEPOINT before_changes;UPDATE nonpoint_rr SET v=25 WHERE id=2;DELETE FROM nonpoint_rr WHERE id=4;INSERT INTO nonpoint_rr VALUES(8,80)")
				columns, got = nonpointRead(t, a, api, &dst, "SELECT id,v FROM nonpoint_rr WHERE id IN(1,2,4,8,9) ORDER BY id")
				nonpointIDsAndValues(t, columns, got, []int64{1, 2, 8}, []int64{10, 25, 80})
				columns, got = nonpointRead(t, a, api, &dst, "SELECT id,v FROM nonpoint_rr WHERE id>=1 ORDER BY id LIMIT 10")
				nonpointIDsAndValues(t, columns, got, []int64{1, 2, 3, 5, 8}, []int64{10, 25, 30, 50, 80})
				isolationExec(t, a, "ROLLBACK TO before_changes")
				columns, got = nonpointRead(t, a, api, &dst, inQuery)
				nonpointIDsAndValues(t, columns, got, []int64{1, 2, 4}, []int64{10, 20, 40})
				isolationExec(t, a, "COMMIT")
				columns, got = nonpointRead(t, a, api, &dst, inQuery)
				nonpointIDsAndValues(t, columns, got, []int64{1, 2, 4, 9}, []int64{111, 20, 40, 90})
			})
		}
	}
}

func TestNonpointLockingReadDialectPolicy(t *testing.T) {
	for _, dialect := range []sql.Dialect{sql.DialectMySQL, sql.DialectPostgreSQL} {
		t.Run(dialect.String(), func(t *testing.T) {
			d := namespaceTestDatabase(t)
			a, b := isolationSession(t, d), isolationSession(t, d)
			if err := a.SetDialect(dialect); err != nil {
				t.Fatal(err)
			}
			isolationExec(t, a, "CREATE TABLE nonpoint_locks(id BIGINT PRIMARY KEY,v BIGINT);INSERT INTO nonpoint_locks VALUES(1,10),(2,20);BEGIN ISOLATION LEVEL REPEATABLE READ")
			var dst ExecResult
			query := "SELECT id,v FROM nonpoint_locks WHERE id IN(1,2) ORDER BY id"
			columns, got := nonpointRead(t, a, "parsed", &dst, query)
			nonpointIDsAndValues(t, columns, got, []int64{1, 2}, []int64{10, 20})
			isolationExec(t, b, "UPDATE nonpoint_locks SET v=111 WHERE id=1")
			result, err := a.Exec(query + " FOR UPDATE")
			if dialect == sql.DialectPostgreSQL {
				if !errors.Is(err, ErrConflict) {
					t.Fatalf("PostgreSQL snapshot locking read missed conflict: %+v %v", result, err)
				}
			} else {
				if err != nil {
					t.Fatal(err)
				}
				columns, got = (nonpointOutput{result: result}).collect(t)
				nonpointIDsAndValues(t, columns, got, []int64{1, 2}, []int64{111, 20})
				columns, got = nonpointRead(t, a, "parsed", &dst, query)
				nonpointIDsAndValues(t, columns, got, []int64{1, 2}, []int64{10, 20})
			}
			isolationExec(t, a, "ROLLBACK")
		})
	}
}

func TestNonpointOwnedResultsSurviveBufferReuse(t *testing.T) {
	d := namespaceTestDatabase(t)
	s := isolationSession(t, d)
	nonpointFixture(t, s)
	query := "SELECT id,v,b,note FROM nonpoint_items WHERE id IN(1,2,3,9) ORDER BY id"
	owned := isolationExec(t, s, query)
	rows, err := s.Query(query)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var retained [][]sql.Value
	var reuse []sql.Value
	for rows.Next() {
		values, err := rows.Values()
		if err != nil {
			t.Fatal(err)
		}
		retained = append(retained, values)
		reuse, err = rows.ValuesInto(reuse)
		if err != nil {
			t.Fatal(err)
		}
		if len(reuse[2].Blob) != 0 {
			reuse[2].Blob[0] = 255
		}
	}
	if rows.Err() != nil {
		t.Fatal(rows.Err())
	}
	rows.Close()
	var dst ExecResult
	for _, suffix := range []string{"WHERE id>=7 ORDER BY id LIMIT 10", "WHERE id IN(9,2,17) ORDER BY id DESC", "WHERE id IN(NULL,99) LIMIT 0"} {
		_, _ = nonpointRead(t, s, "parsed", &dst, "SELECT id,v,b,note FROM nonpoint_items "+suffix)
	}
	isolationExec(t, s, "UPDATE nonpoint_items SET b=X'AA' WHERE id=1;DELETE FROM nonpoint_items WHERE id=9")
	nonpointAssertFixture(t, owned.Columns, retained, []int64{1, 2, 3, 9})
	columns, values := (nonpointOutput{result: owned}).collect(t)
	nonpointAssertFixture(t, columns, values, []int64{1, 2, 3, 9})
	owned.Rows[0]["b"].Blob[0] = 99
	delete(owned.Rows[0], "id")
	if retained[0][2].Blob[0] != 1 {
		t.Fatal("caller-owned maps and retained Values share BLOB memory")
	}
	duplicate, err := s.Query("SELECT id,b AS duplicate,b AS duplicate FROM nonpoint_items WHERE id IN(1,2,3) ORDER BY id")
	if err != nil {
		t.Fatal(err)
	}
	defer duplicate.Close()
	if names := duplicate.Columns(); len(names) != 3 || names[1].ResultName() != "duplicate" || names[2].ResultName() != "duplicate" {
		t.Fatal("duplicate projection labels changed", names)
	}
	if !duplicate.Next() {
		t.Fatal("missing nonempty-BLOB row", duplicate.Err())
	}
	first, err := duplicate.Values()
	if err != nil || !bytes.Equal(first[1].Blob, []byte{0xaa}) || !bytes.Equal(first[2].Blob, []byte{0xaa}) {
		t.Fatal("duplicate nonempty BLOBs changed", first, err)
	}
	first[1].Blob[0] = 55
	if first[2].Blob[0] != 0xaa {
		t.Fatal("duplicate projected BLOBs share mutable bytes")
	}
	if !duplicate.Next() {
		t.Fatal("missing empty-BLOB row", duplicate.Err())
	}
	valuesOne, err := duplicate.Values()
	if err != nil || valuesOne[1].Null || valuesOne[1].Blob == nil || valuesOne[2].Null || valuesOne[2].Blob == nil {
		t.Fatal("duplicate empty BLOBs changed", valuesOne, err)
	}
	if !duplicate.Next() {
		t.Fatal("missing NULL-BLOB row", duplicate.Err())
	}
	valuesTwo, err := duplicate.Values()
	if err != nil || !valuesTwo[1].Null || !valuesTwo[2].Null {
		t.Fatal("duplicate NULL BLOBs changed", valuesTwo, err)
	}
}

func TestNonpointCancellationAfterStatementSnapshot(t *testing.T) {
	d := namespaceTestDatabase(t)
	isolationExec(t, isolationSession(t, d), "CREATE TABLE nonpoint_cancel(id BIGINT PRIMARY KEY,v BIGINT);INSERT INTO nonpoint_cancel VALUES(1,10),(2,20),(3,30)")
	for _, api := range nonpointAPIs {
		for _, suffix := range []string{"WHERE id IN(1,2,3)", "WHERE id IN(99,NULL)", "WHERE id>=1 ORDER BY id LIMIT 2", "WHERE id>=1 ORDER BY id LIMIT 0", "WHERE id>=99 ORDER BY id LIMIT 10"} {
			t.Run(api+"/"+suffix, func(t *testing.T) {
				s := isolationSession(t, d)
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				s.schema.SetStatementSnapshotHook(func(context.Context, *sql.Schema) (bool, error) {
					cancel()
					return false, nil
				})
				query := "SELECT id,v FROM nonpoint_cancel " + suffix
				var rows *Rows
				var err error
				switch api {
				case "exec":
					_, err = s.ExecContext(ctx, query)
				case "query":
					rows, err = s.QueryContext(ctx, query)
				case "parsed":
					var dst ExecResult
					_, rows, err = s.QueryParsedContextInto(ctx, &dst, query, parseReadTest(t, query))
				}
				if rows != nil {
					defer rows.Close()
					if rows.Next() {
						t.Fatal("cancelled read delivered a row")
					}
					err = rows.Err()
				}
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("cancelled read lost context error: %v", err)
				}
			})
		}
	}
}

func TestNonpointNamespaceTemporaryAndCatalogChanges(t *testing.T) {
	d := namespaceTestDatabase(t)
	s, other := isolationSession(t, d), isolationSession(t, d)
	isolationExec(t, s, "CREATE TABLE nonpoint_names(id BIGINT PRIMARY KEY,v BIGINT);INSERT INTO nonpoint_names VALUES(1,10),(2,20);CREATE SCHEMA tenant;CREATE TABLE tenant.nonpoint_names(id BIGINT PRIMARY KEY,v BIGINT);INSERT INTO tenant.nonpoint_names VALUES(1,110),(2,120)")
	query := "SELECT id,v FROM nonpoint_names WHERE id IN(1,2) ORDER BY id"
	var dst ExecResult
	check := func(session *Session, want []int64) {
		t.Helper()
		columns, got := nonpointRead(t, session, "parsed", &dst, query)
		nonpointIDsAndValues(t, columns, got, []int64{1, 2}, want)
	}
	check(s, []int64{10, 20})
	isolationExec(t, s, "USE tenant")
	check(s, []int64{110, 120})
	result, rows, err := s.QueryParsedContextInto(sql.WithNamespaceSearchPath(context.Background(), []string{"main"}), &dst, query, parseReadTest(t, query))
	if err != nil {
		t.Fatal(err)
	}
	output := nonpointOutput{result: result, rows: rows}
	columns, got := output.collect(t)
	output.close(t)
	nonpointIDsAndValues(t, columns, got, []int64{1, 2}, []int64{10, 20})
	isolationExec(t, s, "USE main;CREATE TEMPORARY TABLE nonpoint_names(id BIGINT PRIMARY KEY,v BIGINT);INSERT INTO nonpoint_names VALUES(1,210),(2,220)")
	check(s, []int64{210, 220})
	check(other, []int64{10, 20})
	isolationExec(t, s, "DROP TEMPORARY TABLE nonpoint_names;ALTER TABLE nonpoint_names RENAME TO nonpoint_old_names;CREATE TABLE nonpoint_names(id BIGINT PRIMARY KEY,v BIGINT);INSERT INTO nonpoint_names VALUES(1,310),(2,320)")
	check(s, []int64{310, 320})
	check(other, []int64{310, 320})
	isolationExec(t, s, "BEGIN;ALTER TABLE nonpoint_names RENAME COLUMN v TO renamed")
	columns, got = nonpointRead(t, s, "parsed", &dst, "SELECT id,renamed FROM nonpoint_names WHERE id>=1 ORDER BY id LIMIT 10")
	nonpointIDsAndValues(t, columns, got, []int64{1, 2}, []int64{310, 320})
	if columns[1].ResultName() != "renamed" {
		t.Fatal("changed projection retained old labels", columns)
	}
	isolationExec(t, s, "ROLLBACK;ANALYZE nonpoint_names")
	check(s, []int64{310, 320})
}

func TestNonpointExactInt64KeysAndMixedNumericFallback(t *testing.T) {
	const low = int64(-9223372036854775808)
	const exactFloat = int64(9007199254740992)
	const nextInteger = int64(9007199254740993)
	const high = int64(9223372036854775807)
	for _, dialect := range []sql.Dialect{sql.DialectNative, sql.DialectMySQL, sql.DialectPostgreSQL} {
		t.Run(dialect.String(), func(t *testing.T) {
			d := namespaceTestDatabase(t)
			s := isolationSession(t, d)
			if err := s.SetDialect(dialect); err != nil {
				t.Fatal(err)
			}
			isolationExec(t, s, "CREATE TABLE nonpoint_exact(id BIGINT PRIMARY KEY,v BIGINT);INSERT INTO nonpoint_exact VALUES(-9223372036854775808,-1),(9007199254740992,2),(9007199254740993,3),(9223372036854775807,4)")
			for _, api := range nonpointAPIs {
				t.Run(api, func(t *testing.T) {
					var dst ExecResult
					for _, tc := range []struct {
						suffix string
						ids, v []int64
					}{
						{"WHERE id IN(9007199254740993,9223372036854775807,-9223372036854775808) ORDER BY id", []int64{low, nextInteger, high}, []int64{-1, 3, 4}},
						{"WHERE id IN(9007199254740992.0,9007199254740992.0,NULL) ORDER BY id", []int64{exactFloat}, []int64{2}},
						{"WHERE id>=9007199254740992 ORDER BY id LIMIT 2", []int64{exactFloat, nextInteger}, []int64{2, 3}},
						{"WHERE id<=-9223372036854775808 ORDER BY id DESC LIMIT 10", []int64{low}, []int64{-1}},
						{"WHERE id<-9223372036854775808 ORDER BY id LIMIT 10", nil, nil},
						{"WHERE id>=9223372036854775807 ORDER BY id LIMIT 10", []int64{high}, []int64{4}},
						{"WHERE id>9223372036854775807 ORDER BY id LIMIT 10", nil, nil},
						{"WHERE id<=9223372036854775807 ORDER BY id DESC LIMIT 2", []int64{high, nextInteger}, []int64{4, 3}},
					} {
						columns, got := nonpointRead(t, s, api, &dst, "SELECT id,v FROM nonpoint_exact "+tc.suffix)
						nonpointIDsAndValues(t, columns, got, tc.ids, tc.v)
					}
				})
			}
		})
	}
}

func TestNonpointDialectDivisionUsesSessionSemantics(t *testing.T) {
	// Native/PostgreSQL integer division truncates; MySQL '/' returns a decimal.
	// Pure native folding must not replace the session's IN/range expressions.
	for _, dialect := range []sql.Dialect{sql.DialectNative, sql.DialectMySQL, sql.DialectPostgreSQL} {
		t.Run(dialect.String(), func(t *testing.T) {
			d := namespaceTestDatabase(t)
			s := isolationSession(t, d)
			if err := s.SetDialect(dialect); err != nil {
				t.Fatal(err)
			}
			isolationExec(t, s, "CREATE TABLE nonpoint_division(id BIGINT PRIMARY KEY,v BIGINT);INSERT INTO nonpoint_division VALUES(1,10),(2,20),(3,30),(4,40)")
			for _, api := range nonpointAPIs {
				t.Run(api, func(t *testing.T) {
					var dst ExecResult
					for _, tc := range []struct {
						suffix       string
						ordinary, my []int64
					}{
						{"WHERE id IN(5/2) ORDER BY id", []int64{2}, nil},
						{"WHERE id IN(5/2,7/2) ORDER BY id", []int64{2, 3}, nil},
						{"WHERE id IN(6/2,5/2) ORDER BY id", []int64{2, 3}, []int64{3}},
						{"WHERE id>=5/2 ORDER BY id LIMIT 2", []int64{2, 3}, []int64{3, 4}},
					} {
						ids := tc.ordinary
						if dialect == sql.DialectMySQL {
							ids = tc.my
						}
						expected := make([]int64, len(ids))
						for i, id := range ids {
							expected[i] = id * 10
						}
						columns, got := nonpointRead(t, s, api, &dst, "SELECT id,v FROM nonpoint_division "+tc.suffix)
						nonpointIDsAndValues(t, columns, got, ids, expected)
					}
				})
			}
		})
	}
}

func nonpointCopyResult(source ExecResult) ExecResult {
	copy := source
	copy.Columns = append([]sql.ColumnDef(nil), source.Columns...)
	copy.Rows = make([]map[string]sql.Value, len(source.Rows))
	for i, row := range source.Rows {
		copy.Rows[i] = make(map[string]sql.Value, len(row))
		for name, value := range row {
			if value.Type == sql.ColBlob && !value.Null {
				value.Blob = append([]byte{}, value.Blob...)
			}
			copy.Rows[i][name] = value
		}
	}
	return copy
}

func TestNonpointOversizedAndRowCountFallbackPreservesAnswers(t *testing.T) {
	d := namespaceTestDatabase(t)
	s := isolationSession(t, d)
	isolationExec(t, s, "CREATE TABLE nonpoint_large(id BIGINT PRIMARY KEY,v BIGINT,b BLOB)")
	insert, err := s.Prepare("INSERT INTO nonpoint_large VALUES(?,?,?)")
	if err != nil {
		t.Fatal(err)
	}
	defer insert.Close()
	blobs := make(map[int64][]byte)
	for id := int64(1); id <= 75; id++ {
		var blob []byte
		switch id {
		case 1:
			blob = bytes.Repeat([]byte{0x11}, 70<<10)
		case 2:
			blob = bytes.Repeat([]byte{0x22}, 40<<10)
		case 3:
			blob = bytes.Repeat([]byte{0x33}, 40<<10)
		case 4:
			blob = []byte{}
		case 5:
			blob = nil
		default:
			blob = []byte{byte(id), 0, byte(id)}
		}
		blobs[id] = blob
		if _, err := insert.Exec(id, id*10, blob); err != nil {
			t.Fatal(err)
		}
	}
	// Exercise SSTable-backed values, including mmap/compressed block ownership
	// after an iterator or a multi-get view has been released before decoding.
	if err := d.lsm.CompactRange(); err != nil {
		t.Fatal(err)
	}
	ids := func(low, high int64) []int64 {
		var result []int64
		for id := low; id <= high; id++ {
			result = append(result, id)
		}
		return result
	}
	var list strings.Builder
	for id := 1; id <= 65; id++ {
		if id > 1 {
			list.WriteByte(',')
		}
		fmt.Fprint(&list, id)
	}
	for _, api := range nonpointAPIs {
		t.Run(api, func(t *testing.T) {
			var dst ExecResult
			// Prime caller storage with a smaller result; oversized streaming
			// fallback must not partially replace or mutate that destination.
			prime := "SELECT id,v,b FROM nonpoint_large WHERE id=6"
			output := nonpointOpen(t, s, "parsed", &dst, prime)
			_, _ = output.collect(t)
			output.close(t)
			before := nonpointCopyResult(dst)
			for _, tc := range []struct {
				suffix string
				ids    []int64
			}{
				{"WHERE id IN(1,2,3,4,5) ORDER BY id", []int64{1, 2, 3, 4, 5}},
				{"WHERE id>=1 ORDER BY id LIMIT 5", []int64{1, 2, 3, 4, 5}},
				{"WHERE id IN(2,3) ORDER BY id", []int64{2, 3}},
				{"WHERE id>=2 ORDER BY id LIMIT 2", []int64{2, 3}},
				{"WHERE id IN(1,4) ORDER BY id LIMIT 1 OFFSET 1", []int64{4}},
				{"WHERE id>=1 ORDER BY id LIMIT 1 OFFSET 3", []int64{4}},
				{"WHERE id>=6 ORDER BY id DESC LIMIT 3", []int64{75, 74, 73}},
				{"WHERE id>=6 ORDER BY id LIMIT 64 OFFSET 1", ids(7, 70)},
				{"WHERE id>=1 ORDER BY id LIMIT 65", ids(1, 65)},
				{"WHERE id IN(" + list.String() + ") ORDER BY id", ids(1, 65)},
			} {
				query := "SELECT id,v,b FROM nonpoint_large " + tc.suffix
				read := nonpointOpen(t, s, api, &dst, query)
				if api == "parsed" && read.rows != nil && !reflect.DeepEqual(before, dst) {
					read.close(t)
					t.Fatalf("streaming fallback mutated destination before iteration: %s", tc.suffix)
				}
				columns, values := read.collect(t)
				read.close(t)
				if len(columns) != 3 || len(values) != len(tc.ids) {
					t.Fatalf("%s returned %d rows, want%d", tc.suffix, len(values), len(tc.ids))
				}
				for i, id := range tc.ids {
					row := values[i]
					if row[0].Int != id || row[1].Null || row[1].Int != id*10 || row[2].Null != (id == 5) || !bytes.Equal(row[2].Blob, blobs[id]) {
						t.Fatalf("%s row%d returned wrong key/value/BLOB for id%d", tc.suffix, i, id)
					}
					if id == 4 && row[2].Blob == nil {
						t.Fatal("empty BLOB changed to NULL")
					}
				}
				// The contract allows an owned result too; it becomes the prior
				// destination against which the next streaming fallback is checked.
				before = nonpointCopyResult(dst)
			}
		})
	}
}
