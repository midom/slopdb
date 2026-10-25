package sql

import "fmt"

func functionAnyNull(args []Value) bool {
	for _, v := range args {
		if v.Null {
			return true
		}
	}
	return false
}

// Check before allocating repeated/padded/generated output, including overflow.
func functionResultSize(s *Schema, n uint64) error {
	if n > uint64(s.memoryBudget()) {
		return fmt.Errorf("function result exceeds query memory budget")
	}
	return nil
}
