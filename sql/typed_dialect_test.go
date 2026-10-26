package sql

import (
	"math"
	"reflect"
	"testing"
)

func TestTypedProgramDialectDifferential(t *testing.T) {
	decimal, err := NewDecimalValue("2.50")
	if err != nil {
		t.Fatal(err)
	}
	enumeration := NewTextValue("two")
	enumeration.EnumValue = true
	enumeration.EnumOrdinal = 2
	collated := NewTextValue("A")
	collated.Collation = "NFC_CI"
	values := []Value{NewNullValue(), NewIntValue(0), NewIntValue(-1), NewIntValue(math.MinInt64), NewIntValue(math.MaxInt64), NewUnsignedValue(math.MaxUint64), NewUnsignedValue(2), NewRealValue(0.5), decimal, NewTextValue("2"), NewTextValue("2tail"), NewBlobValue([]byte("2")), enumeration, collated}
	for _, dialect := range []Dialect{DialectNative, DialectMySQL, DialectPostgreSQL} {
		s := &Schema{dialect: dialect, collation: BinaryCollation}
		for _, text := range []string{"a+b", "a-b", "a*b", "a/b", "a%b", "a=b", "a<>b", "a<b", "a<=b", "a>b", "a>=b", "a AND b", "a OR b", "a IS b", "a IS NOT b", "a||b", "NOT a", "-a", "+a", "a BETWEEN b AND 2", "a NOT BETWEEN b AND 2", "a='2'", "a<X'32'", "a*0.06", "a/2", "0 AND 1/0", "1 OR 1/0", "NOT NULL", "NULL=1", "0.1+0.2"} {
			expr := typedTestExpr(t, text)
			program, ok := compileTypedProgram(expr, map[string]int{"a": 0, "b": 1})
			if !ok {
				t.Fatalf("cannot compile %s", text)
			}
			program.setDialect(s)
			for _, a := range values {
				for _, b := range values {
					want, wantErr := s.evalExpr(expr, map[string]Value{"a": a, "b": b})
					got, err := program.eval([]Value{a, b})
					if (err == nil) != (wantErr == nil) || err != nil && err.Error() != wantErr.Error() || err == nil && !reflect.DeepEqual(got, want) {
						t.Fatalf("dialect %v expression %s a=%#v b=%#v: got %#v/%v want %#v/%v", dialect, text, a, b, got, err, want, wantErr)
					}
				}
			}
			program.clearRegisters()
		}
	}
}

func TestTypedProgramLiteralRebindAndPrivateClone(t *testing.T) {
	expr := typedTestExpr(t, "a*0.06")
	p, ok := compileTypedProgram(expr, map[string]int{"a": 0})
	if !ok {
		t.Fatal("compile")
	}
	s := &Schema{dialect: DialectMySQL, collation: BinaryCollation}
	p.setDialect(s)
	clone := p.clonePrivate()
	clone.setDialect(s)
	got, err := p.eval([]Value{NewIntValue(100)})
	if err != nil || got.Type != ColDecimal || got.Text != "6.00" {
		t.Fatalf("initial %#v/%v", got, err)
	}
	other, err := clone.eval([]Value{NewIntValue(200)})
	if err != nil || other.Text != "12.00" || p.registers[p.result].Text != "6.00" {
		t.Fatal("independent execution registers alias")
	}
	literal := expr.(*BinaryExpr).Right.(*LiteralExpr)
	literal.Value = NewIntValue(3)
	p.clearRegisters()
	p.setDialect(s)
	got, err = p.eval([]Value{NewIntValue(100)})
	if err != nil || got.Int != 300 {
		t.Fatalf("rebind %#v/%v", got, err)
	}
	p.clearRegisters()
	clone.clearRegisters()
	for _, v := range p.registers {
		if !reflect.DeepEqual(v, Value{}) {
			t.Fatal("register retained input")
		}
	}
}

func TestMySQLTypedAggregateExactSemantics(t *testing.T) {
	s, _ := statisticsSchema(t, t.TempDir())
	if err := s.SetDialect(DialectMySQL); err != nil {
		t.Fatal(err)
	}
	statisticsSQL(t, s, "CREATE TABLE typed_mysql(id INTEGER PRIMARY KEY,g TEXT,n BIGINT,d DECIMAL(10,2),u BIGINT UNSIGNED,shipdate TEXT)")
	statisticsSQL(t, s, "INSERT INTO typed_mysql VALUES(1,'A',1,1.25,18446744073709551615,'1994-01-01'),(2,'A',2,2.50,2,'1994-06-01'),(3,'B',NULL,NULL,NULL,NULL)")
	result := statisticsSQL(t, s, "SELECT g,COUNT(n) AS n,SUM(d) AS total,SUM(n/2) AS ratio,MIN(u) AS lo,MAX(u) AS hi FROM typed_mysql GROUP BY g ORDER BY g")
	if len(result.Rows) != 2 {
		t.Fatalf("groups %d", len(result.Rows))
	}
	a, b := result.Rows[0], result.Rows[1]
	if a["g"].Text != "A" || a["n"].Int != 2 || a["total"].Type != ColDecimal || a["total"].Text != "3.75" || a["ratio"].Type != ColDecimal || a["ratio"].Text != "1.5000" || a["lo"].Int != 2 || a["hi"].Uint != math.MaxUint64 {
		t.Fatalf("exact MySQL aggregate %#v", a)
	}
	if b["g"].Text != "B" || b["n"].Int != 0 || !b["total"].Null || !b["ratio"].Null || !b["lo"].Null || !b["hi"].Null {
		t.Fatalf("NULL group %#v", b)
	}
	result = statisticsSQL(t, s, "SELECT SUM(d) AS total FROM typed_mysql WHERE shipdate>='1994-01-01' AND shipdate<'1995-01-01'")
	if len(result.Rows) != 1 || result.Rows[0]["total"].Text != "3.75" {
		t.Fatal(result.Rows)
	}
}

func TestTypedRetainedColumnMetadata(t *testing.T) {
	s, _ := statisticsSchema(t, t.TempDir())
	statisticsSQL(t, s, "CREATE TABLE typed_meta(id INTEGER PRIMARY KEY,g TEXT BINARY,n BIGINT)")
	statisticsSQL(t, s, "INSERT INTO typed_meta VALUES(1,'A',1),(2,'A',2),(3,'B',NULL)")
	for _, dialect := range []Dialect{DialectNative, DialectMySQL, DialectPostgreSQL} {
		if err := s.SetDialect(dialect); err != nil {
			t.Fatal(err)
		}
		result := statisticsSQL(t, s, "SELECT g,MIN(n) AS lo,MAX(n) AS hi,MIN(+n) AS plus FROM typed_meta GROUP BY g ORDER BY g")
		if len(result.Rows) != 2 {
			t.Fatal(result.Rows)
		}
		first := result.Rows[0]
		if !first["g"].Binary || first["g"].SemanticType != TypeText || first["lo"].SemanticType != TypeInt64 || first["hi"].SemanticType != TypeInt64 || first["plus"].SemanticType != TypeInt64 || first["lo"].Int != 1 || first["hi"].Int != 2 || first["plus"].Int != 1 {
			t.Fatalf("dialect %v retained metadata %#v", dialect, first)
		}
	}
}
