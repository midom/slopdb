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

func TestAggregateBestTrimsOversizedReplacedBuffers(t *testing.T) {
	for _, name := range []string{"MIN", "MAX"} {
		for _, size := range []int{1, 96 << 10} {
			state := aggregateState{}
			function := &FuncCallExpr{Name: name}
			initial := byte(2)
			replacement := byte(1)
			if name == "MAX" {
				replacement = 3
			}
			if err := addAggregateValue(&state, function, NewBlobValue(bytes.Repeat([]byte{initial}, 1<<20))); err != nil {
				t.Fatal(err)
			}
			input := bytes.Repeat([]byte{replacement}, size)
			if err := addAggregateValue(&state, function, NewBlobValue(input)); err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(state.best.Blob, input) || cap(state.best.Blob) > max(64<<10, 2*size) {
				t.Fatalf("%s retained replaced buffer: len=%d cap=%d", name, len(state.best.Blob), cap(state.best.Blob))
			}
			input[0] ^= 255
			if state.best.Blob[0] != replacement {
				t.Fatal("replacement winner aliases decoder input")
			}
		}
	}
	group := &aggregateGroup{states: []aggregateState{{best: Value{Type: ColBlob, Blob: make([]byte, 1, 1024)}}}}
	before := aggregateGroupMemory(group)
	group.states[0].best.Blob = group.states[0].best.Blob[:0]
	if after := aggregateGroupMemory(group); after != before {
		t.Fatal("retained BLOB capacity was omitted from group memory accounting")
	}
}
