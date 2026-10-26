package sql

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
)

func scalarCountFixture(t *testing.T) *Schema {
	t.Helper()
	s, _ := statisticsSchema(t, t.TempDir())
	s.queryTempDir = t.TempDir()
	statisticsSQL(t, s, "CREATE TABLE scalar_rows(id INTEGER PRIMARY KEY,n INTEGER,payload BLOB);CREATE INDEX scalar_n ON scalar_rows(n)")
	for base := 1; base <= 600; base += 100 {
		var values []string
		for i := base; i < base+100; i++ {
			n := fmt.Sprint(i)
			if i%10 == 0 {
				n = "NULL"
			}
			values = append(values, fmt.Sprintf("(%d,%s,X'010203')", i, n))
		}
		statisticsSQL(t, s, "INSERT INTO scalar_rows VALUES "+strings.Join(values, ","))
	}
	if err := s.lsm.Flush(); err != nil {
		t.Fatal(err)
	}
	return s
}

func TestScalarCountPipelinePathsAndOwnership(t *testing.T) {
	s := scalarCountFixture(t)
	owned := statisticsSQL(t, s, "SELECT payload FROM scalar_rows WHERE id=1")
	for _, dialect := range []Dialect{DialectNative, DialectMySQL, DialectPostgreSQL} {
		if err := s.SetDialect(dialect); err != nil {
			t.Fatal(err)
		}
		for _, tc := range []struct {
			where string
			want  int64
		}{
			{"n%2=0", 240},
			{"id>=100 AND id<=500 AND n%2=0", 160},
			{"n>=100 AND n<=500 AND n%2=0", 160},
			{"((id>=100 AND id<=300) OR (id>=200 AND id<=500)) AND n%2=0", 160},
			{"id IN(10,20,30,31,32,33,34) AND n%2=0", 2},
			{"n IS NULL AND n/0>1", 0},
			{"0 AND n/0>1", 0},
		} {
			result := statisticsSQL(t, s, "SELECT COUNT(*) AS n FROM scalar_rows WHERE "+tc.where)
			if len(result.Rows) != 1 || result.Rows[0]["n"].Int != tc.want {
				t.Fatalf("dialect %v where %s: %#v", dialect, tc.where, result.Rows)
			}
		}
	}
	if !bytes.Equal(owned.Rows[0]["payload"].Blob, []byte{1, 2, 3}) {
		t.Fatal("count execution mutated caller-owned BLOB")
	}
	profile := s.StartQueryProfile()
	statisticsSQL(t, s, "SELECT COUNT(*) AS n FROM scalar_rows WHERE n%2=0")
	s.StopQueryProfile()
	if profile.RowsExamined != 600 || profile.BytesRead == 0 {
		t.Fatalf("profile lost row/value visits: %+v", profile)
	}
}

func TestScalarCountPipelineSnapshotOverlayAndRebind(t *testing.T) {
	root := scalarCountFixture(t)
	reader := root.NewSession()
	t.Cleanup(reader.EndStatement)
	if err := reader.StartStatement(); err != nil {
		t.Fatal(err)
	}
	statisticsSQL(t, root, "UPDATE scalar_rows SET n=3 WHERE id=2")
	stmt := statisticsSelect(t, "SELECT COUNT(*) AS n FROM scalar_rows WHERE n%2=0")
	result, err := reader.Select(stmt)
	reader.EndStatement()
	if err != nil || result.Rows[0]["n"].Int != 240 {
		t.Fatalf("statement snapshot: %v/%v", result, err)
	}
	current := statisticsSQL(t, reader, "SELECT COUNT(*) AS n FROM scalar_rows WHERE n%2=0")
	if current.Rows[0]["n"].Int != 239 {
		t.Fatal(current.Rows)
	}
	if err := reader.Begin(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if reader.InTransaction() {
			reader.Rollback()
		}
	})
	statisticsSQL(t, reader, "DELETE FROM scalar_rows WHERE id=4;INSERT INTO scalar_rows VALUES(601,2,X'0405');UPDATE scalar_rows SET n=NULL WHERE id=6")
	overlay := statisticsSQL(t, reader, "SELECT COUNT(*) AS n FROM scalar_rows WHERE n%2=0")
	if overlay.Rows[0]["n"].Int != 238 || current.Rows[0]["n"].Int != 239 {
		t.Fatalf("overlay/owned result: %+v %+v", overlay.Rows, current.Rows)
	}
	if err := reader.Rollback(); err != nil {
		t.Fatal(err)
	}
	prepared, err := Prepare("SELECT COUNT(*) AS n FROM scalar_rows WHERE n%?=0")
	if err != nil {
		t.Fatal(err)
	}
	bound, err := prepared.Bind([]Value{NewIntValue(2)})
	if err != nil {
		t.Fatal(err)
	}
	defer bound.Release()
	for _, divisor := range []int64{2, 0, 2} {
		bound.literals[0].Value = NewIntValue(divisor)
		if err := reader.StartStatement(); err != nil {
			t.Fatal(err)
		}
		result, err := reader.Select(bound.statements[0].(*SelectStmt))
		reader.EndStatement()
		if divisor == 0 {
			if err == nil || !strings.Contains(err.Error(), "division by zero") {
				t.Fatalf("missing error: %v", err)
			}
			continue
		}
		if err != nil || result.Rows[0]["n"].Int != 239 {
			t.Fatalf("rebound count %v/%v", result, err)
		}
	}
}

type scalarCountCancelContext struct {
	context.Context
	calls  atomic.Int64
	cancel context.CancelFunc
}

func (c *scalarCountCancelContext) Err() error {
	if c.calls.Add(1) == 4 {
		c.cancel()
	}
	return c.Context.Err()
}

func TestScalarCountPipelineCancellationAndReuse(t *testing.T) {
	s := scalarCountFixture(t)
	base, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx := &scalarCountCancelContext{Context: base, cancel: cancel}
	s.SetContext(ctx)
	if err := s.StartStatement(); err != nil {
		t.Fatal(err)
	}
	_, err := s.Select(statisticsSelect(t, "SELECT COUNT(*) AS n FROM scalar_rows WHERE n%2=0"))
	s.EndStatement()
	s.SetContext(context.Background())
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation error %v", err)
	}
	if got := statisticsSQL(t, s, "SELECT COUNT(*) AS n FROM scalar_rows WHERE n%2=0").Rows[0]["n"].Int; got != 240 {
		t.Fatalf("reuse after cancellation %d", got)
	}
}

func TestTypedEvalPredicateMatchesValueTruth(t *testing.T) {
	decimal, _ := NewDecimalValue("0.0001")
	enumeration := NewIntValue(0)
	enumeration.EnumValue = true
	enumeration.EnumOrdinal = 3
	values := []Value{NewNullValue(), NewIntValue(0), NewIntValue(1), NewUnsignedValue(math.MaxUint64), NewRealValue(math.NaN()), NewRealValue(math.Copysign(0, -1)), NewRealValue(0.5), decimal, NewTextValue("12tail"), NewTextValue("nonnumeric"), NewBlobValue([]byte{1}), enumeration}
	for _, dialect := range []Dialect{DialectNative, DialectMySQL, DialectPostgreSQL} {
		for _, expression := range []string{"a", "a%2=0", "a>0 AND a<100", "a IS NULL OR a/0>1", "0 AND a/0>1", "1 OR a/0>1", "a||'x'"} {
			program, ok := compileTypedProgram(typedTestExpr(t, expression), map[string]int{"a": 0})
			if !ok {
				t.Fatal(expression)
			}
			program.setDialect(&Schema{dialect: dialect, collation: BinaryCollation})
			for _, value := range values {
				row := []Value{value}
				expected, wantErr := program.eval(row)
				got, err := program.evalPredicate(row)
				if (err == nil) != (wantErr == nil) || err != nil && err.Error() != wantErr.Error() || err == nil && got != truthValue(expected) {
					t.Fatalf("dialect %v %s input %#v: %v/%v vs %#v/%v", dialect, expression, value, got, err, expected, wantErr)
				}
			}
			program.clearRegisters()
			for _, value := range program.registers {
				if !reflect.DeepEqual(value, Value{}) {
					t.Fatal("predicate retained input")
				}
			}
		}
	}
}

func TestScalarCountPipelineSmallBudgetAndEmptyInput(t *testing.T) {
	s, _ := statisticsSchema(t, t.TempDir())
	s.queryMemoryBytes = 1
	s.queryTempDir = t.TempDir()
	statisticsSQL(t, s, "CREATE TABLE scalar_empty(n INTEGER)")
	empty := statisticsSQL(t, s, "SELECT COUNT(*) AS n FROM scalar_empty WHERE n/0>1")
	if empty.Rows[0]["n"].Int != 0 {
		t.Fatal(empty.Rows)
	}
	statisticsSQL(t, s, "INSERT INTO scalar_empty VALUES(2)")
	result := statisticsSQL(t, s, "SELECT COUNT(*) AS n FROM scalar_empty WHERE n%2=0")
	if result.Rows[0]["n"].Int != 1 {
		t.Fatal(result.Rows)
	}
	if err := s.StartStatement(); err != nil {
		t.Fatal(err)
	}
	_, err := s.Select(statisticsSelect(t, "SELECT COUNT(*) AS n FROM scalar_empty WHERE n/0>1"))
	s.EndStatement()
	if err == nil || !strings.Contains(err.Error(), "division by zero") {
		t.Fatalf("nonempty error %v", err)
	}
}
