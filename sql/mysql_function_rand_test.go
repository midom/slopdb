package sql

import (
	"math"
	"testing"
)

func TestMySQLRandSeedSequenceAndLifecycle(t *testing.T) {
	s := &Schema{dialect: DialectMySQL}
	expression := parseTestExpression(t, "RAND(3)")
	want := []float64{0.9057697559760601, 0.37307905813034536, 0.14808605345719125}
	for _, expected := range want {
		value, err := s.evalExpr(expression, nil)
		if err != nil || value.Real != expected {
			t.Fatalf("seeded sequence: %v %v want%v", value, err, expected)
		}
	}
	s.EndStatement()
	if len(s.mysqlRand) != 0 {
		t.Fatal("RAND expression state retained after statement")
	}
	value, err := s.evalExpr(expression, nil)
	if err != nil || value.Real != want[0] {
		t.Fatal(value, err)
	}
	for _, tc := range []struct {
		sql  string
		want float64
	}{
		{"RAND(0)", 0.15522042769493574}, {"RAND(NULL)", 0.15522042769493574},
		{"RAND(1)", 0.40540353712197724}, {"RAND(1.9)", 0.6555866465490187}, {"RAND(2.5e0)", 0.6555866465490187}, {"RAND(2.5)", 0.9057697559760601}, {"RAND('2e1')", 0.6555866465490187}, {"RAND(18446744073709551614)", 0.6548542125661431}, {"RAND('18446744073709551614')", 0.6548542125661431},
		{"RAND(-1)", 0.9050373219931845}, {"RAND(1+2)", want[0]},
	} {
		value, err := s.evalExpr(parseTestExpression(t, tc.sql), nil)
		if err != nil || value.Real != tc.want {
			t.Errorf("%s: %v %v want%v", tc.sql, value, err, tc.want)
		}
	}
	dynamic := parseTestExpression(t, "RAND(seed)")
	for i := 0; i < 3; i++ {
		value, err := s.evalExpr(dynamic, map[string]Value{"seed": NewIntValue(3)})
		if err != nil || value.Real != want[0] {
			t.Fatal("column seed did not reinitialize", value, err)
		}
	}
	random := parseTestExpression(t, "RAND()")
	previous := -1.0
	for i := 0; i < 100; i++ {
		value, err := s.evalExpr(random, nil)
		if err != nil || value.Real < 0 || value.Real >= 1 || math.IsNaN(value.Real) || previous == value.Real {
			t.Fatal("invalid/unadvanced random value", value, err)
		}
		previous = value.Real
	}
	if deterministicFunction("RAND") {
		t.Fatal("RAND incorrectly constant-foldable")
	}
}
