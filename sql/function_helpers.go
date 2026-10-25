package sql

import (
	"encoding/hex"
	"fmt"
)

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

func functionUUIDText(b []byte) string {
	text := hex.EncodeToString(b)
	return text[:8] + "-" + text[8:12] + "-" + text[12:16] + "-" + text[16:20] + "-" + text[20:]
}
