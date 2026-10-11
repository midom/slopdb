package sql

import (
	"errors"
	"fmt"
	"testing"
)

func TestProfileCoalescesRepeatedStages(t *testing.T) {
	s := &Schema{}
	p := s.StartQueryProfile()
	defer s.StopQueryProfile()
	for i := 0; i < 10000; i++ {
		parent := s.ProfileOperator("SELECT", "t")
		child := s.ProfileOperator("Aggregate", "t")
		var err error
		if i == 0 {
			err = errors.New("first invocation failed")
		}
		child(2, 1, err)
		parent(2, 1, nil)
	}
	if len(p.Operators) != 2 {
		t.Fatalf("retained %d stages", len(p.Operators))
	}
	for _, op := range p.Operators {
		if op.Invocations != 10000 || op.RowsExamined != 20000 || op.RowsReturned != 10000 || op.ElapsedMS <= 0 {
			t.Fatalf("lost repeated work: %+v", op)
		}
	}
	if p.Operators[1].ParentID != 0 || p.Operators[1].Error != "first invocation failed" || len(p.stack) != 0 {
		t.Fatalf("profile %+v", p)
	}
}

func TestProfileLimitsDistinctTraceNodes(t *testing.T) {
	s := &Schema{}
	p := s.StartQueryProfile()
	defer s.StopQueryProfile()
	for i := 0; i < 10000; i++ {
		finish := s.ProfileOperator("Stage", fmt.Sprint(i))
		finish(1, 2, nil)
	}
	if len(p.Operators) != maxProfileOperators+1 || len(p.nodes) != maxProfileOperators {
		t.Fatalf("unbounded trace: %d nodes, %d entries", len(p.Operators), len(p.nodes))
	}
	op := p.Operators[p.overflow]
	if op.Operator != "Collapsed" || op.Invocations != 10000-maxProfileOperators || op.RowsExamined != op.Invocations || op.RowsReturned != 2*op.Invocations || p.CollapsedInvocations != op.Invocations {
		t.Fatalf("lost overflow work: %+v", op)
	}
}
