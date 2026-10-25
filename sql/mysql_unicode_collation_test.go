package sql

import "testing"

// Facts independently checked against MariaDB 11.8.6 unicode_520_ci. Historic
// UCA revision differences outside these cases remain documented limitations.
func TestMySQLUnicodePrimaryWeightsAndLike(t *testing.T) {
	s := &Schema{}
	for _, tc := range []struct {
		expr string
		want int64
	}{
		{"'É' COLLATE utf8mb4_unicode_520_ci = 'e'", 1},
		{"'ß' COLLATE utf8mb4_unicode_520_ci = 'ss'", 1},
		{"'😀' COLLATE utf8mb4_unicode_520_ci = '😁'", 0},
		{"'é' COLLATE utf8mb4_unicode_520_ci = 'é'", 1},
		{"'a ' COLLATE utf8mb4_unicode_520_ci = 'a'", 1},
		{"'ß' COLLATE utf8mb4_unicode_520_ci LIKE 'ss'", 0},
		{"'École' COLLATE utf8mb4_unicode_520_ci LIKE 'e%'", 1},
		{"'a ' COLLATE utf8mb4_unicode_520_ci LIKE 'a'", 0},
		{"'a ' COLLATE utf8mb4_0900_ai_ci = 'a'", 0},
	} {
		v, e := s.evalExpr(parseTestExpression(t, tc.expr), nil)
		if e != nil || v.Int != tc.want {
			t.Errorf("%s: %v %v", tc.expr, v, e)
		}
	}
}
