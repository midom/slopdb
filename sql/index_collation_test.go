package sql

import (
	"bytes"
	"testing"
)

func TestCollatedPhysicalKeys(t *testing.T) {
	for _, mode := range []Collation{"NFC", "NFD", "NFKC", "NFKD", "NFC_CI", "NFKC_CI"} {
		t.Run(string(mode), func(t *testing.T) {
			info := &TableInfo{Name: "t", PKColumns: []string{"name"}, collation: mode}
			idx := &IndexInfo{Name: "name_idx", Table: "t", Column: "name", collation: mode}
			for _, pair := range [][2]string{{"é", "e\u0301"}, {"ﬀ", "ff"}, {"A", "a"}} {
				if mode.Compare(pair[0], pair[1]) != 0 {
					continue
				}
				a, b := NewTextValue(pair[0]), NewTextValue(pair[1])
				if !bytes.Equal(info.encodePK("t", map[string]Value{"name": a}), info.encodePK("t", map[string]Value{"name": b})) {
					t.Fatalf("equal primary key values %q and %q encoded differently", pair[0], pair[1])
				}
				if !bytes.Equal(appendAccessValue([]byte(info.storagePrefix("t")), b, nil, info), info.encodePK("t", map[string]Value{"name": a})) {
					t.Fatalf("primary key probe for %q missed stored value %q", pair[1], pair[0])
				}
				if indexValuePrefix(idx, a) != indexValuePrefix(idx, b) {
					t.Fatalf("equal index probes %q and %q encoded differently", pair[0], pair[1])
				}
				if !bytes.Equal(encodeRowIndexKey(idx, map[string]Value{"name": a}, []byte("r")), encodeRowIndexKey(idx, map[string]Value{"name": b}, []byte("r"))) {
					t.Fatalf("equal secondary index values %q and %q encoded differently", pair[0], pair[1])
				}
			}
		})
	}
}

func TestBinaryColumnPhysicalKeys(t *testing.T) {
	info := &TableInfo{Name: "t", PKColumns: []string{"name"}, Columns: []ColumnDef{{Name: "name", ColType: ColText, Binary: true}}, collation: "NFKC_CI"}
	idx := &IndexInfo{Name: "name_idx", Table: "t", Column: "name", collation: "NFKC_CI", binaryColumns: map[string]bool{"name": true}}
	a, b := NewTextValue("Ａ"), NewTextValue("a")
	if bytes.Equal(info.encodePK("t", map[string]Value{"name": a}), info.encodePK("t", map[string]Value{"name": b})) {
		t.Fatal("BINARY primary key folded distinct values")
	}
	if indexValuePrefix(idx, a) == indexValuePrefix(idx, b) {
		t.Fatal("BINARY secondary key folded distinct values")
	}
}
