package sql

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
)

func NewJSONValue(text string) (Value, error) {
	decoder := json.NewDecoder(bytes.NewBufferString(text))
	decoder.UseNumber()
	var document any
	if err := decoder.Decode(&document); err != nil {
		return NewNullValue(), fmt.Errorf("invalid JSON: %w", err)
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return NewNullValue(), fmt.Errorf("invalid JSON: trailing data")
	}
	data, err := json.Marshal(document)
	if err != nil {
		return NewNullValue(), err
	}
	return Value{Type: ColJSON, Text: string(data)}, nil
}
