package sql

import (
	"bytes"
	"testing"
)

func TestPointMutationSkippedFieldNullAndAdvance(t *testing.T) {
	cases := []struct {
		name    string
		data    []byte
		null    bool
		advance int
	}{
		{"NULL", []byte{binTypeNull}, true, 1},
		{"unknown", []byte{255}, true, 1},
		{"short_integer", []byte{binTypeInt64, 1}, true, 1},
		{"short_real", []byte{binTypeFloat64}, true, 1},
		{"incomplete_length", []byte{binTypeText, 128}, true, 1},
		{"short_payload", []byte{binTypeBlob, 9, 1}, true, 1},
		{"integer", appendValue(nil, NewIntValue(7)), false, 9},
		{"empty_blob", appendValue(nil, NewBlobValue([]byte{})), false, 2},
		{"blob", appendValue(nil, NewBlobValue(bytes.Repeat([]byte{0, 255}, 4096))), false, 8195},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			v, n := skipPointUpdateValue(tc.data)
			if v.Null != tc.null || n != tc.advance {
				t.Fatalf("NULL=%v advance=%d want%v/%d", v.Null, n, tc.null, tc.advance)
			}
		})
	}
}
