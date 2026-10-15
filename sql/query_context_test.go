package sql

import (
	"context"
	"errors"
	"testing"
)

func TestQueryMemoryContextOverridesAndParentCancellation(t *testing.T) {
	for _, parent := range []context.Context{context.Background(), context.TODO()} {
		for _, base := range []int64{0, 1024, 2 * defaultQueryMemory} {
			s := &Schema{queryMemoryBytes: base}
			for _, budget := range []int64{0, 512, 3 * defaultQueryMemory} {
				s.ctx = WithQueryMemory(parent, budget)
				want := budget
				if want == 0 {
					want = defaultQueryMemory
				}
				if got := s.memoryBudget(); got != want {
					t.Fatalf("base=%d override=%d: got %d want %d", base, budget, got, want)
				}
				// A subsequent zero must override a nonzero inherited budget.
				s.ctx = WithQueryMemory(s.ctx, 0)
				if s.memoryBudget() != defaultQueryMemory {
					t.Fatal("zero override inherited another budget")
				}
			}
		}
	}
	type key struct{}
	parent, cancel := context.WithCancel(context.WithValue(context.Background(), key{}, "retained"))
	s := &Schema{ctx: WithQueryMemory(parent, 0)}
	if got := s.ctx.Value(key{}); got != "retained" {
		t.Fatalf("lost parent value: %v", got)
	}
	if err := s.checkInterrupt(); err != nil {
		t.Fatal(err)
	}
	cancel()
	if err := s.checkInterrupt(); !errors.Is(err, context.Canceled) {
		t.Fatalf("lost cancellation: %v", err)
	}
}
