package db

import (
	"context"
	"fmt"

	"slopdb/sql"
)

// SetCollation changes the database-wide TEXT collation and atomically rebuilds
// clustered primary keys and secondary indexes. A change that merges distinct
// PRIMARY KEY or UNIQUE values fails without changing the database.
func (db *Database) SetCollation(name string) error {
	return db.SetCollationContext(context.Background(), name)
}

func (db *Database) SetCollationContext(ctx context.Context, name string) error {
	mode, err := sql.ParseCollation(name)
	if err != nil {
		return err
	}
	_, err = db.ExecContext(ctx, fmt.Sprintf("PRAGMA collation='%s'", mode))
	return err
}
