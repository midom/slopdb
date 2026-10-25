package sql

import (
	"bytes"
	"testing"
)

func TestAggregateBestOwnsReusedBlobInputs(t *testing.T) {
	for _, name := range []string{"MIN", "MAX"} {
		state := aggregateState{}
		f := &FuncCallExpr{Name: name}
		buffer := []byte{2, 2, 2}
		if err := addAggregateValue(&state, f, Value{Type: ColBlob, Blob: buffer}); err != nil {
			t.Fatal(err)
		}
		buffer[0] = 9
		if !bytes.Equal(state.best.Blob, []byte{2, 2, 2}) {
			t.Fatal("first best borrowed decoder input")
		}
		replacement := byte(1)
		if name == "MAX" {
			replacement = 3
		}
		buffer[0], buffer[1], buffer[2] = replacement, replacement, replacement
		if err := addAggregateValue(&state, f, Value{Type: ColBlob, Blob: buffer}); err != nil {
			t.Fatal(err)
		}
		buffer[1] = 9
		if !bytes.Equal(state.best.Blob, bytes.Repeat([]byte{replacement}, 3)) {
			t.Fatal("replacement best borrowed input")
		}
	}
	state := aggregateState{}
	if err := addAggregateValue(&state, &FuncCallExpr{Name: "COUNT"}, Value{Type: ColBlob, Blob: []byte{1}}); err != nil || state.best.Blob != nil {
		t.Fatal("COUNT retained unused input", err)
	}
}
