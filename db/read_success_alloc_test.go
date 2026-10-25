package db

import (
	"context"
	"slopdb/sql"
	"testing"
)

func TestSuccessfulReadChecksDoNotAllocateErrorTargets(t *testing.T) {
	ctx := context.Background()
	schema := &sql.Schema{}
	statement := &sql.SelectStmt{}
	for _, transactionHandle := range []bool{false, true} {
		session := &Session{transactionHandle: transactionHandle}
		allocations := testing.AllocsPerRun(1000, func() {
			if err := session.finishTransactionAbort(nil); err != nil {
				panic(err)
			}
			if waiting, ok := statementLockWait(nil); ok || waiting != nil {
				panic("unexpected wait")
			}
			if policy, err := indexPublicationPolicy(ctx, schema, statement); policy != nil || err != nil {
				panic("unexpected publication policy")
			}
		})
		if allocations != 0 {
			t.Fatalf("successful read checks allocate %g objects (transactionHandle=%v)", allocations, transactionHandle)
		}
	}
}
