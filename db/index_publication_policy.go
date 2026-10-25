package db

import (
	"context"
	"fmt"
	"time"

	"slopdb/sql"
)

type publicationAbortContextKey struct{}

// A SQL clause takes precedence over the API option for the individual
// statement. The policy applies only to standalone index publication.
func indexPublicationPolicy(ctx context.Context, schema *sql.Schema, statement sql.Statement) (*time.Duration, error) {
	var policy *time.Duration
	eligible := false
	switch st := statement.(type) {
	case *sql.CreateIndexStmt:
		policy, eligible = st.PublicationAbortAfter, true
	case *sql.DropIndexStmt:
		policy, eligible = st.PublicationAbortAfter, true
	case *sql.AlterTableStmt:
		policy = st.PublicationAbortAfter
		eligible = (st.ObjectKind == "TABLE" || st.ObjectKind == "") && (st.Operation == "ADD_INDEX" || st.Operation == "DROP_INDEX")
	case *sql.ExplainStmt:
		return nil, nil // Describes a policy; never enforces it.
	}
	if policy == nil {
		if duration, ok := ctx.Value(publicationAbortContextKey{}).(time.Duration); ok {
			policy = new(time.Duration)
			*policy = duration
		}
	}
	if policy == nil {
		return nil, nil
	}
	if *policy < 0 {
		return nil, fmt.Errorf("index publication abort threshold must not be negative")
	}
	if !eligible || schema.InTransaction() {
		return nil, fmt.Errorf("publication cancellation requires standalone CREATE/DROP INDEX or ALTER ADD/DROP INDEX outside a transaction")
	}
	return policy, nil
}
