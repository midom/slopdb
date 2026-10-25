package sql

import (
	"context"
	"encoding/json"
	"math"
	"os"
	"testing"
)

// Fixed answers are mathematical identities and Oracle MySQL 8.0.46 results,
// including signed base conversion and locale grouping edge cases.
func TestMySQLNumericFunctionsExactAndDomains(t *testing.T) {
	s := &Schema{mysqlMode: true}
	for _, tc := range []struct{ expression, want string }{
		{"CEIL(1.23)", "2"}, {"CEILING(-1.23)", "-1"}, {"FLOOR(-1.23)", "-2"},
		{"CEIL(18446744073709551615)", "18446744073709551615"},
		{"CEILING(CAST('18446744073709551614.1' AS DECIMAL(21,1)))", "18446744073709551615"},
		{"FLOOR(CAST('-18446744073709551614.1' AS DECIMAL(21,1)))", "-18446744073709551615"},
		{"SIGN(-0.000001)", "-1"}, {"SIGN('17suffix')", "1"}, {"SIGN('junk')", "0"},
		{"TRUNCATE(1.999,1)", "1.9"}, {"TRUNCATE(-1.999,1)", "-1.9"},
		{"TRUNCATE(12345,-2)", "12300"}, {"TRUNCATE(123,-9223372036854775808)", "0"},
		{"TRUNCATE(12.345,1.9)", "12.34"}, {"TRUNCATE(12.345,'1.9')", "12.3"}, {"TRUNCATE(1.23,10)", "1.23"}, {"TRUNCATE(18446744073709551615,-1)", "18446744073709551610"},
		{"TRUNCATE(CAST('9999999999999999999999999.99999' AS DECIMAL(30,5)),2)", "9999999999999999999999999.99"},
		{"MOD(34.5,3)", "1.5"}, {"MOD(-34.5,3)", "-1.5"},
		{"MOD(18446744073709551615,17)", "0"}, {"MOD(-9223372036854775808,-1)", "0"},
		{"CONV('a',16,2)", "1010"}, {"CONV('6E',18,8)", "172"}, {"CONV(-17,10,-18)", "-H"},
		{"CONV('FFFFFFFFFFFFFFFF',16,-10)", "-1"},
		{"CONV('FFFFFFFFFFFFFFFF',-16,10)", "9223372036854775807"},
		{"CONV('-9223372036854775809',-10,10)", "9223372036854775808"},
		{"CONV('18446744073709551616',10,10)", "18446744073709551615"},
		{"CONV('-18446744073709551616',10,10)", "0"},
		{"CONV('xyz',10,16)", "0"}, {"BIN(-1)", "1111111111111111111111111111111111111111111111111111111111111111"},
		{"OCT(12)", "14"}, {"BIN(1.9)", "1"},
		{"CRC32('MySQL')", "3259397556"}, {"BIT_COUNT(18446744073709551615)", "64"}, {"BIT_COUNT(-1)", "64"},
		{"BIT_COUNT(CAST('64' AS BINARY))", "7"},
		{"FORMAT(12332.123456,4)", "12,332.1235"}, {"FORMAT(12332.123456,4,'de_DE')", "12.332,1235"},
		{"FORMAT(123456789.5,2,'en_IN')", "12,34,56,789.50"},
		{"FORMAT(12345.5,2,'fr_FR')", "12345,50"}, {"FORMAT(12345.5,2,'ru_RU')", "12 345,50"},
		{"FORMAT(12345.5,2,NULL)", "12,345.50"}, {"FORMAT(-12345.55,1)", "-12,345.6"},
		{"FORMAT(CAST(-0.5 AS REAL),0)", "-0"},
		{"FORMAT(12.345,1.9)", "12.35"}, {"FORMAT(12.345,'1.9')", "12.3"}, {"FORMAT(2.5e0,0)", "2"}, {"FORMAT(2.5,0)", "3"}, {"FORMAT(12345.5,-1)", "12,346"}, {"FORMAT(12345.5,100)", "12,345.500000000000000000000000000000"},
		{"FORMAT(CAST('99999999999999999999.995' AS DECIMAL(23,3)),2)", "100,000,000,000,000,000,000.00"},
	} {
		t.Run(tc.expression, func(t *testing.T) {
			value, err := s.evalExpr(parseTestExpression(t, tc.expression), nil)
			if err != nil || value.Null || value.String() != tc.want {
				t.Fatalf("got %v (%v), want %s", value, err, tc.want)
			}
		})
	}
	for _, expression := range []string{"SQRT(-1)", "LN(0)", "LOG(-2)", "LOG(1,100)", "ASIN(2)", "ACOS(-2)", "MOD(1,0)", "CEIL(NULL)", "TRUNCATE(1,NULL)", "POW(NULL,2)", "CONV('1',1,16)", "CONV('',10,16)", "FORMAT(NULL,2)"} {
		value, err := s.evalExpr(parseTestExpression(t, expression), nil)
		if err != nil || !value.Null {
			t.Errorf("%s: expected NULL, got %v (%v)", expression, value, err)
		}
	}
	for _, tc := range []struct {
		expression string
		want       float64
	}{
		{"SQRT(9)", 3}, {"POWER(2,-2)", 0.25}, {"EXP(0)", 1}, {"LN(1)", 0}, {"LOG(2,65536)", 16},
		{"LOG2(1024)", 10}, {"LOG10(100)", 2}, {"SIN(PI()/2)", 1}, {"COS(0)", 1}, {"TAN(0)", 0},
		{"ASIN(1)", math.Pi / 2}, {"ACOS(1)", 0}, {"ATAN(1)", math.Pi / 4}, {"ATAN(-2,2)", -math.Pi / 4},
		{"ATAN2(PI(),0)", math.Pi / 2}, {"COT(PI()/4)", 1}, {"DEGREES(PI())", 180}, {"RADIANS(90)", math.Pi / 2},
	} {
		value, err := s.evalExpr(parseTestExpression(t, tc.expression), nil)
		if err != nil || value.Null || math.Abs(value.Real-tc.want) > 1e-12 {
			t.Errorf("%s: got %v (%v), want %v", tc.expression, value, err, tc.want)
		}
	}
	for _, expression := range []string{"EXP(1000)", "COT(0)"} {
		if _, err := s.evalExpr(parseTestExpression(t, expression), nil); err == nil {
			t.Errorf("accepted overflow %s", expression)
		}
	}
}

func TestMySQLNumericWarningsAndOutputBudget(t *testing.T) {
	s := &Schema{mysqlMode: true}
	var codes []uint16
	s.ctx = WithMySQLSession(context.Background(), MySQLSessionOptions{Warning: func(code uint16, _ string) { codes = append(codes, code) }})
	for _, expression := range []string{"MOD(1,0)", "LN(-1)", "FORMAT(12,2,'unknown_LOCALE')"} {
		if _, err := s.evalExpr(parseTestExpression(t, expression), nil); err != nil {
			t.Fatal(err)
		}
	}
	if len(codes) != 3 || codes[0] != 1365 || codes[1] != 3020 || codes[2] != 1649 {
		t.Fatal(codes)
	}
	s.queryMemoryBytes = 8
	if _, err := s.evalExpr(parseTestExpression(t, "FORMAT(123456789,30)"), nil); err == nil {
		t.Fatal("FORMAT exceeded output budget")
	}
}

func TestMySQLRoundingTypesAndNativeFallback(t *testing.T) {
	s := &Schema{mysqlMode: true}
	for _, tc := range []struct {
		expression, want string
		typ              ColumnType
	}{
		{"ROUND(25e-1)", "2", ColReal}, {"ROUND(12.345,1.9)", "12.35", ColDecimal}, {"ROUND(12.345,'1.9')", "12.3", ColDecimal}, {"ROUND(2.5)", "3", ColDecimal}, {"ROUND(-2.5)", "-3", ColDecimal},
		{"ROUND(CAST(2.5 AS REAL))", "2", ColReal}, {"ROUND(CAST(3.5 AS REAL))", "4", ColReal},
		{"ROUND(CAST(-2.5 AS REAL))", "-2", ColReal},
		{"ROUND(CAST(1.25 AS REAL),1)", "1.2", ColReal},
		{"ROUND(12345,-2)", "12300", ColInteger}, {"ROUND(123,-100)", "0", ColInteger},
		{"ROUND(1,2)", "1", ColInteger}, {"ROUND(1.23,10)", "1.23", ColDecimal},
		{"ROUND(99999999999999999999.995,2)", "100000000000000000000.00", ColDecimal},
		{"ROUND(18446744073709551615,2)", "18446744073709551615", ColUnsigned},
		{"ABS(-9007199254740993)", "9007199254740993", ColInteger},
		{"ABS(18446744073709551615)", "18446744073709551615", ColUnsigned},
		{"ABS(-99999999999999999999.125)", "99999999999999999999.125", ColDecimal},
		{"ABS('12suffix')", "12", ColReal},
	} {
		v, err := s.evalExpr(parseTestExpression(t, tc.expression), nil)
		if err != nil || v.String() != tc.want || v.Type != tc.typ {
			t.Errorf("%s: %v type%v (%v), want%s type%v", tc.expression, v, v.Type, err, tc.want, tc.typ)
		}
	}
	for _, expression := range []string{"ROUND(18446744073709551615,-1)", "ROUND(9223372036854775807,-1)", "ABS(-9223372036854775808)"} {
		if _, err := s.evalExpr(parseTestExpression(t, expression), nil); err == nil {
			t.Errorf("integer overflow accepted in %s", expression)
		}
	}
	for _, expression := range []string{"ROUND(NULL)", "ROUND(1,NULL)", "ABS(NULL)"} {
		if v, err := s.evalExpr(parseTestExpression(t, expression), nil); err != nil || !v.Null {
			t.Errorf("%s: %v %v", expression, v, err)
		}
	}
	native := &Schema{}
	v, err := native.evalExpr(parseTestExpression(t, "ROUND(12,-1)"), nil)
	if err != nil || v.Real != 12 || v.Type != ColReal {
		t.Fatal("native ROUND behavior changed", v, err)
	}
	if _, err := native.evalExpr(parseTestExpression(t, "ABS('12suffix')"), nil); err == nil {
		t.Fatal("native strict numeric behavior changed")
	}
}

// These factual answers were queried from Oracle MySQL 8.0.46 using
// SELECT locale, FORMAT(123456789.5,2,locale) for the documented locale list.
func TestMySQLFormatAllDocumentedLocales(t *testing.T) {
	data, err := os.ReadFile("testdata/mysql_format_locales.json")
	if err != nil {
		t.Fatal(err)
	}
	var reference []struct{ Locale, Formatted string }
	if err = json.Unmarshal(data, &reference); err != nil {
		t.Fatal(err)
	}
	s := &Schema{mysqlMode: true}
	for _, tc := range reference {
		expr := "FORMAT(123456789.5,2,'" + tc.Locale + "')"
		value, err := s.evalExpr(parseTestExpression(t, expr), nil)
		if err != nil || value.Text != tc.Formatted {
			t.Errorf("%s: got%q (%v), want%q", expr, value.Text, err, tc.Formatted)
		}
	}
}
