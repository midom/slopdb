package sql

import "testing"

func TestCollationKeys(t *testing.T) {
	tests := []struct {
		mode  Collation
		a, b  string
		equal bool
	}{
		{BinaryCollation, "é", "e\u0301", false},
		{"NFC", "é", "e\u0301", true},
		{"NFD", "é", "e\u0301", true},
		{"NFC", "Ａ", "A", false},
		{"NFKC", "Ａ", "A", true},
		{"NFKD", "Ａ", "A", true},
		{"NFC_CI", "É", "e\u0301", true},
		{"NFKC_CI", "Ａ", "a", true},
		{"NFC_CI", "Straße", "STRASSE", true},
		{"NFC_CI", "\xff", "\ufffd", false},
	}
	for _, tt := range tests {
		if equal := tt.mode.Compare(tt.a, tt.b) == 0; equal != tt.equal {
			t.Errorf("%s Compare(%q, %q) equality = %v, want %v", tt.mode, tt.a, tt.b, equal, tt.equal)
		}
	}
	if _, err := ParseCollation("unknown"); err == nil {
		t.Fatal("accepted unknown collation")
	}
}
