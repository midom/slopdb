package sql

import (
	"runtime"
	"testing"
)

func TestRetainedPreparedBindingOwnershipAndErrors(t *testing.T) {
	q, err := Prepare("SELECT ? LIMIT ?")
	if err != nil {
		t.Fatal(err)
	}
	p := q.NewBinding()
	b, err := p.Bind([]Value{NewBlobValue([]byte{1, 2}), NewIntValue(1)})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = p.Bind([]Value{NewIntValue(9), NewIntValue(1)}); err == nil {
		t.Fatal("rebound an active private execution tree")
	}
	b.Release()
	for _, literal := range b.literals {
		if literal != nil && (literal.Value.Blob != nil || literal.Value.Text != "") {
			t.Fatal("released binding retained argument data")
		}
	}
	runtime.GC()
	runtime.GC()
	if _, err = p.Bind([]Value{NewIntValue(9), NewTextValue("invalid limit")}); err == nil {
		t.Fatal("accepted a noninteger limit")
	}
	next, err := p.Bind([]Value{NewIntValue(10), NewIntValue(2)})
	if err != nil || next != b || next.literals[0].Value.Int != 10 {
		t.Fatalf("private tree was lost or failed binding poisoned reuse: %p %p %v", b, next, err)
	}
	next.Release()
}
