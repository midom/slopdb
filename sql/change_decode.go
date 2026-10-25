package sql

import "fmt"

// DecodeStoredRow exposes the database's typed binary representation to native
// change-stream consumers. Values own BLOB storage and exact numeric strings;
// SQL NULL and the JSON value null remain distinct.
func DecodeStoredRow(data []byte, columns []ColumnDef) ([]Value, error) {
	values := make([]Value, len(columns))
	for i, column := range columns {
		if len(data) == 0 {
			values[i] = NewNullValue()
			continue
		}
		value, n := decodeValue(data)
		if n <= 0 || n > len(data) {
			return nil, fmt.Errorf("invalid stored value for %s", column.Name)
		}
		values[i] = decorateColumnValue(value, column)
		data = data[n:]
	}
	if len(data) != 0 {
		return nil, fmt.Errorf("stored row has extra columns")
	}
	return values, nil
}
