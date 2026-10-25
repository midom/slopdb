package db

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"slopdb/sql"
)

// Server startup serializes imports before admitting wire sessions. Separate
// physical sources remain available for recovery and manual archival afterwards.
var mysqlLegacyImportMu contextMutex

type mysqlLegacyImport struct {
	Version  int    `json:"version"`
	Name     string `json:"name"`
	Complete bool   `json:"complete"`
}

func mysqlLegacyPath(root, name string) (string, []byte) {
	hash := sha256.Sum256([]byte(name))
	suffix := hex.EncodeToString(hash[:])
	return filepath.Join(root, "mysql-databases", suffix), []byte("\x00mysql_virtual_import_v1:" + suffix)
}

// MigrateLegacyMySQLDatabases imports pre-namespace MySQL child directories into
// this database's shared LSM. Call before admitting clients. Native SQL creates
// allocate fresh global object IDs, and bounded typed INSERT batches build all
// indexes. A durable ownership marker permits interrupted imports to restart;
// an unrelated preexisting schema is never overwritten. Sources are retained.
func (d *Database) MigrateLegacyMySQLDatabases(ctx context.Context, names []string, rootName string) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := mysqlLegacyImportMu.LockContext(ctx); err != nil {
		return err
	}
	defer mysqlLegacyImportMu.Unlock()
	for _, name := range names {
		if strings.EqualFold(name, rootName) {
			continue
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := d.migrateLegacyMySQLDatabase(ctx, name); err != nil {
			return fmt.Errorf("migrate legacy MySQL database %q: %w", name, err)
		}
	}
	return nil
}

func (d *Database) mysqlImportMetadata(ctx context.Context, key, value []byte, write bool) ([]byte, bool, error) {
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	if err := d.execution.RLockContext(ctx); err != nil {
		return nil, false, err
	}
	defer d.execution.RUnlock()
	if d.closed.Load() {
		return nil, false, ErrSessionClosed
	}
	if write {
		if err := d.checkReplication(ctx); err != nil {
			return nil, false, err
		}
		if err := d.currentStorage.Load().Put(key, value); err != nil {
			return nil, false, err
		}
		// Checkpoint even when the parent was opened with WAL disabled or no-fsync.
		return nil, false, d.currentStorage.Load().Flush()
	}
	return d.currentStorage.Load().Get(key)
}

func (d *Database) migrateLegacyMySQLDatabase(ctx context.Context, name string) error {
	path, key := mysqlLegacyPath(d.dir, name)
	data, found, err := d.mysqlImportMetadata(ctx, key, nil, false)
	if err != nil {
		return err
	}
	marker := mysqlLegacyImport{Version: 1, Name: name}
	if found {
		if err = json.Unmarshal(data, &marker); err != nil {
			return err
		}
		if marker.Version != 1 || marker.Name != name {
			return fmt.Errorf("invalid migration ownership marker")
		}
		if marker.Complete {
			return nil
		}
	}
	namespaces, err := d.SchemaNames(ctx)
	if err != nil {
		return err
	}
	exists := false
	for _, n := range namespaces {
		exists = exists || strings.EqualFold(n, name)
	}
	if _, err = os.Stat(filepath.Join(path, "MANIFEST")); err != nil {
		// Existing virtual schemas have no legacy directory. An unfinished import
		// must retain its source, otherwise silently accepting would lose data.
		if os.IsNotExist(err) && exists && !found {
			return nil
		}
		return fmt.Errorf("legacy source unavailable: %w", err)
	}
	if exists && !found {
		return fmt.Errorf("schema already exists without a migration ownership marker; source preserved")
	}
	options := *d.opt
	options.CreateIfNotExist = false
	options.ErrorIfExists = false
	source, err := Open(path, &options)
	if err != nil {
		return err
	}
	defer source.Close()
	sourceCollation, err := source.Exec("PRAGMA collation")
	if err != nil {
		return err
	}
	targetCollation, err := d.ExecContext(ctx, "PRAGMA collation")
	if err != nil {
		return err
	}
	if sourceCollation.Rows[0]["collation"].Text != targetCollation.Rows[0]["collation"].Text {
		return fmt.Errorf("source and target collations differ; source preserved")
	}
	if !found {
		data, _ = json.Marshal(marker)
		if _, _, err = d.mysqlImportMetadata(ctx, key, data, true); err != nil {
			return err
		}
	}
	session, err := d.NewSession()
	if err != nil {
		return err
	}
	defer session.Close()
	if exists {
		// A crash may have restored some foreign keys already. Remove those
		// first so namespace cleanup is independent of table-name ordering.
		for _, table := range d.GetTableList() {
			schema, _ := sql.SplitSchemaName(table)
			if !strings.EqualFold(schema, name) {
				continue
			}
			stmts, e := sql.ParseWithOptions(d.GetCreateTableSQL(table), d.ParseOptions())
			if e != nil {
				return e
			}
			for _, c := range stmts[0].(*sql.CreateTableStmt).Constraints {
				if c.Kind == "FOREIGN" {
					query := "ALTER TABLE " + d.renderSQL(quoteIdentifier(table)) + " DROP CONSTRAINT " + d.renderSQL(quoteIdentifier(c.Name))
					if _, e = session.ExecContext(ctx, query); e != nil {
						return e
					}
				}
			}
		}
		if _, err = session.ExecContext(ctx, "DROP SCHEMA "+d.renderSQL(quoteIdentifier(name))+" CASCADE"); err != nil {
			return err
		}
	}
	if _, err = session.ExecContext(ctx, "CREATE SCHEMA "+d.renderSQL(quoteIdentifier(name))+";USE "+d.renderSQL(quoteIdentifier(name))); err != nil {
		return err
	}
	var foreign []*sql.AlterTableStmt
	tables := source.GetTableList()
	sort.Strings(tables)
	for _, table := range tables {
		if schema, _ := sql.SplitSchemaName(table); schema != "main" {
			return fmt.Errorf("legacy source contains nested schema %q", schema)
		}
		statements, parseErr := sql.ParseWithOptions(source.GetCreateTableSQL(table), source.ParseOptions())
		if parseErr != nil {
			return parseErr
		}
		create := statements[0].(*sql.CreateTableStmt)
		kept := create.Constraints[:0]
		for _, constraint := range create.Constraints {
			if constraint.Kind == "FOREIGN" {
				copy := constraint
				copy.Inline = false
				foreign = append(foreign, &sql.AlterTableStmt{Table: table, Operation: "ADD_CONSTRAINT", Constraint: &copy})
			} else {
				kept = append(kept, constraint)
			}
		}
		create.Constraints = kept
		create.ForeignKeys = nil
		for i := range create.Columns {
			create.Columns[i].RefTable = ""
			create.Columns[i].RefColumns = nil
		}
		if err = mysqlImportStatement(ctx, session, create); err != nil {
			return err
		}
	}
	for _, table := range tables {
		rows, readErr := source.QueryContext(ctx, source.renderSQL("SELECT * FROM "+quoteIdentifier(table)))
		if readErr != nil {
			return readErr
		}
		columns := rows.Columns()
		names := make([]string, len(columns))
		for i, c := range columns {
			names[i] = c.Name
		}
		for {
			batch := make([][]any, 0, 4096)
			for len(batch) < 4096 && rows.Next() {
				values, e := rows.Values()
				if e != nil {
					rows.Close()
					return e
				}
				args := make([]any, len(values))
				for i, v := range values {
					args[i] = ownedValue(v)
				}
				batch = append(batch, args)
			}
			if e := rows.Err(); e != nil {
				rows.Close()
				return e
			}
			if len(batch) == 0 {
				break
			}
			pos := 0
			_, e := d.BulkInsert(ctx, name+"."+table, names, func(context.Context) ([]any, error) {
				if pos == len(batch) {
					return nil, io.EOF
				}
				v := batch[pos]
				pos++
				return v, nil
			})
			if e != nil {
				rows.Close()
				return e
			}
			if d.mysqlMigrationHook != nil {
				d.mysqlMigrationHook("batch")
			}
		}
		if err = rows.Close(); err != nil {
			return err
		}
	}
	// Candidate unique keys must exist before restoring referential constraints.
	for _, index := range source.GetIndexList() {
		if !source.isAutoIndex(index) {
			if _, err = session.ExecContext(ctx, source.GetIndexSQL(index)); err != nil {
				return err
			}
		}
	}
	for _, constraint := range foreign {
		if err = mysqlImportStatement(ctx, session, constraint); err != nil {
			return err
		}
	}
	views := source.GetViewList()
	for len(views) > 0 {
		remaining := make([]string, 0, len(views))
		var last error
		for _, view := range views {
			if _, e := session.ExecContext(ctx, source.GetCreateViewSQL(view)); e != nil {
				last = e
				remaining = append(remaining, view)
			}
		}
		if len(remaining) == len(views) {
			return last
		}
		views = remaining
	}
	for _, trigger := range source.GetTriggerList() {
		if _, err = session.ExecContext(ctx, source.GetTriggerSQL(trigger)); err != nil {
			return err
		}
	}
	if err = mysqlImportSequenceFloors(ctx, source, session, name, tables); err != nil {
		return err
	}
	if d.mysqlMigrationHook != nil {
		d.mysqlMigrationHook("complete")
	}
	// All imported rows and catalog mutations precede the completion checkpoint.
	marker.Complete = true
	data, _ = json.Marshal(marker)
	if _, _, err = d.mysqlImportMetadata(ctx, key, data, true); err != nil {
		return err
	}
	if d.mysqlMigrationHook != nil {
		d.mysqlMigrationHook("committed")
	}
	return source.Close()
}

func mysqlImportStatement(ctx context.Context, s *Session, statement sql.Statement) error {
	ctx, cancel, timeout, err := s.db.queryContext(ctx, ExecOptions{})
	if err != nil {
		return err
	}
	defer cancel()
	if err = s.gate.LockContext(ctx); err != nil {
		return err
	}
	defer s.gate.Unlock()
	_, err = s.execParsed(ctx, []sql.Statement{statement}, cancel, false, timeout)
	return err
}

func mysqlImportSequenceFloors(ctx context.Context, source *Database, target *Session, namespace string, tables []string) error {
	// Counter keys contain physical object IDs. Transfer the values through the
	// destination schema so names and IDs are remapped independently.
	if _, err := target.ExecContext(ctx, "BEGIN"); err != nil {
		return err
	}
	defer target.Exec("ROLLBACK")
	if err := target.gate.LockContext(ctx); err != nil {
		return err
	}
	if err := target.db.execution.RLockContext(ctx); err != nil {
		target.gate.Unlock()
		return err
	}
	var floorErr error
	for _, table := range tables {
		signed, unsigned := source.schema.SequenceFloor(table)
		if err := target.schema.AdvanceSequenceFloor(strings.ToLower(namespace)+"."+table, signed, unsigned); err != nil {
			floorErr = err
			break
		}
	}
	target.db.execution.RUnlock()
	target.gate.Unlock()
	if floorErr != nil {
		return floorErr
	}
	_, err := target.ExecContext(ctx, "COMMIT")
	return err
}
