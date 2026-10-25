package main

import (
	gosql "database/sql"
	"slopdb/db"
	slopdriver "slopdb/driver"
)

func newEmbeddedBackend(c config, d *db.Database) (*benchmarkBackend, error) {
	connector, err := slopdriver.NewConnector(d)
	if err != nil {
		return nil, err
	}
	return newSQLPoolBackend(c, gosql.OpenDB(connector))
}
