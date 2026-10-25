package sql

// Join columns have validated homogeneous types. Read the first non-NULL value
// on each side to choose a hash only when both use the same equality domain.
func mysqlJoinHashCompatible(left, right []map[string]Value, pairs []joinKeyPair) bool {
	class := func(rows []map[string]Value, name string) int {
		for _, row := range rows {
			value, ok := row[name]
			if !ok || value.Null {
				continue
			}
			if value.Type == ColInteger || value.Type == ColReal {
				return 1
			}
			return 2
		}
		return 0
	}
	for _, pair := range pairs {
		a, b := class(left, pair.left), class(right, pair.right)
		if a != 0 && b != 0 && a != b {
			return false
		}
	}
	return true
}
