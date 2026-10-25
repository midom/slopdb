package sql

import "fmt"

// SequenceFloor exposes durable allocation floors for logical database import.
// The caller owns this schema's execution gate.
func (s *Schema) SequenceFloor(table string) (int64, uint64) {
	return s.autoincMax[table], s.autoincUnsigned[s.rowCounterKey(table)]
}

// AdvanceSequenceFloor remaps imported allocation metadata to a freshly
// allocated table ID. It participates in the caller's ordinary transaction.
func (s *Schema) AdvanceSequenceFloor(table string, signed int64, unsigned uint64) error {
	if !s.inTx {
		return fmt.Errorf("sequence migration requires a transaction")
	}
	if s.tables[table] == nil {
		return fmt.Errorf("sequence migration table missing: %s", table)
	}
	s.autoincMax[table] = max(s.autoincMax[table], signed)
	key := s.rowCounterKey(table)
	s.autoincUnsigned[key] = max(s.autoincUnsigned[key], unsigned)
	return nil
}
