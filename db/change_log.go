package db

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"maps"
	"sort"
	"strings"
	"sync"

	"slopdb/sql"
	"slopdb/storage"
)

type CDCTable struct {
	ID         uint64
	Schema     string
	Name       string
	Columns    []sql.ColumnDef
	Definition *sql.TableInfo
	Indexes    []*sql.IndexInfo
	CreateSQL  string // MySQL definition, without a schema qualifier on this table
}
type CDCRow struct {
	TableID uint64
	Kind    string // insert, update, delete
	Before  []sql.Value
	After   []sql.Value
}
type CDCSchemaChange struct {
	Before *CDCTable
	After  *CDCTable
}
type CDCTransaction struct {
	Sequence          uint64
	Time              int64 // UTC Unix nanoseconds, assigned before replication
	Tables            []CDCTable
	Changes           []CDCRow
	SchemaChanges     []CDCSchemaChange
	Namespaces        []string // non-nil when the namespace registry changes
	CreatedNamespaces []string
	DroppedNamespaces []string
}
type CDCMetadata struct {
	SourceID      string
	StartSequence uint64
	Tables        []CDCTable
}

func (d *Database) CDCPosition(ctx context.Context) (uint64, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := d.execution.RLockContext(ctx); err != nil {
		return 0, err
	}
	defer d.execution.RUnlock()
	if d.closed.Load() {
		return 0, fmt.Errorf("database closed")
	}
	return d.currentStorage.Load().ChangeLogPosition()
}

func (d *Database) EnableCDC(ctx context.Context) (*CDCMetadata, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := d.execution.RLockContext(ctx); err != nil {
		return nil, err
	}
	defer d.execution.RUnlock()
	if d.closed.Load() {
		return nil, fmt.Errorf("database closed")
	}
	if err := d.checkReplication(ctx); err != nil {
		return nil, err
	}
	header, err := d.currentStorage.Load().EnableChangeLog(ctx)
	if err != nil {
		return nil, err
	}
	return cdcMetadata(header)
}

func (d *Database) CDCMetadata(ctx context.Context) (*CDCMetadata, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := d.execution.RLockContext(ctx); err != nil {
		return nil, err
	}
	defer d.execution.RUnlock()
	if d.closed.Load() {
		return nil, fmt.Errorf("database closed")
	}
	header, err := d.currentStorage.Load().ChangeLogHeader()
	if err != nil {
		return nil, err
	}
	if header == nil {
		return nil, fmt.Errorf("change log is not enabled")
	}
	return cdcMetadata(header)
}

func cdcMetadata(header *storage.ChangeLogHeader) (*CDCMetadata, error) {
	tables, err := cdcCatalog(header.Catalog)
	if err != nil {
		return nil, err
	}
	result := &CDCMetadata{SourceID: header.SourceID, StartSequence: header.StartSequence}
	for _, table := range tables {
		result.Tables = append(result.Tables, table)
	}
	sort.Slice(result.Tables, func(i, j int) bool { return result.Tables[i].ID < result.Tables[j].ID })
	return result, nil
}

func cdcCatalog(catalog map[string][]byte) (map[uint64]CDCTable, error) {
	var info map[string]*sql.TableInfo
	if data := catalog["\x00__schema__"]; len(data) > 0 {
		if err := json.Unmarshal(data, &info); err != nil {
			return nil, fmt.Errorf("CDC catalog: %w", err)
		}
	}
	var indexes map[string]*sql.IndexInfo
	if data := catalog["\x00__schema_indexes__"]; len(data) > 0 {
		if err := json.Unmarshal(data, &indexes); err != nil {
			return nil, err
		}
	}
	tables := make(map[uint64]CDCTable, len(info))
	for name, table := range info {
		schema, base, qualified := strings.Cut(name, ".")
		if !qualified {
			schema, base = "main", name
		}
		columns := append([]sql.ColumnDef(nil), table.Columns...)
		for i := range table.Constraints {
			constraint := &table.Constraints[i]
			if constraint.Kind == "FOREIGN" && len(constraint.RefColumns) == 0 {
				if parent := info[constraint.RefTable]; parent != nil {
					constraint.RefColumns = append([]string(nil), parent.PKColumns...)
				}
			}
		}
		definition := CDCTable{ID: table.ID, Schema: schema, Name: base, Columns: columns, Definition: table}
		for _, index := range indexes {
			if index.Table == name {
				definition.Indexes = append(definition.Indexes, index)
			}
		}
		sort.Slice(definition.Indexes, func(i, j int) bool { return definition.Indexes[i].SQLName() < definition.Indexes[j].SQLName() })
		definition.CreateSQL = CDCMySQLCreate(definition, "")
		tables[table.ID] = definition
	}
	return tables, nil
}

// CDCReader owns only decoding state; it does not pin an MVCC snapshot or hold
// an execution lock between calls. The durable journal is the resume source.
type CDCReader struct {
	mu       sync.Mutex
	database *Database
	sequence uint64
	catalog  map[string][]byte
	tables   map[uint64]CDCTable
	metadata CDCMetadata
}

func (d *Database) NewCDCReader(ctx context.Context, after uint64) (*CDCReader, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := d.execution.RLockContext(ctx); err != nil {
		return nil, err
	}
	defer d.execution.RUnlock()
	if d.closed.Load() {
		return nil, fmt.Errorf("database closed")
	}
	tree := d.currentStorage.Load()
	position, err := tree.ChangeLogPosition()
	if err != nil {
		return nil, err
	}
	if after > position {
		return nil, fmt.Errorf("CDC resume position is ahead of the source")
	}
	header, err := tree.ChangeLogHeader()
	if err != nil {
		return nil, err
	}
	if header == nil {
		return nil, fmt.Errorf("change log is not enabled")
	}
	catalog, err := tree.ChangeCatalogAt(ctx, after)
	if err != nil {
		return nil, err
	}
	tables, err := cdcCatalog(catalog)
	if err != nil {
		return nil, err
	}
	metadata, err := cdcMetadata(header)
	if err != nil {
		return nil, err
	}
	return &CDCReader{database: d, sequence: after, catalog: catalog, tables: tables, metadata: *metadata}, nil
}

func cloneCDCTable(table CDCTable) CDCTable {
	// Definitions originate from persisted JSON, so reparsing a copy also gives
	// callers independent constraint/index slices without sharing expression ASTs.
	if table.Definition != nil {
		data, _ := json.Marshal(table.Definition)
		var definition sql.TableInfo
		_ = json.Unmarshal(data, &definition)
		table.Definition = &definition
	}
	if table.Indexes != nil {
		data, _ := json.Marshal(table.Indexes)
		var indexes []*sql.IndexInfo
		_ = json.Unmarshal(data, &indexes)
		table.Indexes = indexes
	}
	table.Columns = append([]sql.ColumnDef(nil), table.Columns...)
	for i := range table.Columns {
		column := &table.Columns[i]
		column.EnumValues = append([]string(nil), column.EnumValues...)
		column.RefColumns = append([]string(nil), column.RefColumns...)
		column.ConstraintNames = maps.Clone(column.ConstraintNames)
	}
	return table
}
func (r *CDCReader) Metadata() CDCMetadata {
	metadata := r.metadata
	metadata.Tables = append([]CDCTable(nil), metadata.Tables...)
	for i := range metadata.Tables {
		metadata.Tables[i] = cloneCDCTable(metadata.Tables[i])
	}
	return metadata
}
func (r *CDCReader) Position() uint64 { r.mu.Lock(); defer r.mu.Unlock(); return r.sequence }

func (r *CDCReader) Read(ctx context.Context, limit int) ([]CDCTransaction, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	d := r.database
	if err := d.execution.RLockContext(ctx); err != nil {
		return nil, err
	}
	defer d.execution.RUnlock()
	if d.closed.Load() {
		return nil, fmt.Errorf("database closed")
	}
	records, err := d.currentStorage.Load().ReadChanges(ctx, r.sequence, limit)
	if err != nil {
		return nil, err
	}
	result := make([]CDCTransaction, 0, len(records))
	decoder := &CDCReader{catalog: maps.Clone(r.catalog), tables: r.tables}
	for _, record := range records {
		transaction, err := decoder.decode(record)
		if err != nil {
			return nil, err
		}
		result = append(result, transaction)
	}
	if len(records) > 0 {
		r.sequence = records[len(records)-1].Sequence
		r.catalog = decoder.catalog
		r.tables = decoder.tables
	}
	return result, nil
}

func (r *CDCReader) Changes() <-chan struct{} {
	return r.database.currentStorage.Load().ChangeNotification()
}

func (r *CDCReader) decode(record storage.ChangeRecord) (CDCTransaction, error) {
	transaction := CDCTransaction{Sequence: record.Sequence, Time: record.Time}
	before := r.tables
	catalogChanged := false
	for _, mutation := range record.Mutations {
		if len(mutation.Key) > 0 && mutation.Key[0] == 0 {
			catalogChanged = true
			if mutation.Delete {
				delete(r.catalog, string(mutation.Key))
			} else {
				r.catalog[string(mutation.Key)] = mutation.After
			}
			if string(mutation.Key) == "\x00__namespaces_v1__" {
				previous := map[string]bool{"main": true}
				if mutation.BeforeExists {
					if err := json.Unmarshal(mutation.Before, &previous); err != nil {
						return transaction, err
					}
				}
				var namespaces map[string]bool
				if !mutation.Delete {
					if err := json.Unmarshal(mutation.After, &namespaces); err != nil {
						return transaction, err
					}
				}
				transaction.Namespaces = make([]string, 0, len(namespaces))
				for name := range namespaces {
					transaction.Namespaces = append(transaction.Namespaces, name)
				}
				sort.Strings(transaction.Namespaces)
				for name := range namespaces {
					if !previous[name] {
						transaction.CreatedNamespaces = append(transaction.CreatedNamespaces, name)
					}
				}
				for name := range previous {
					if !namespaces[name] {
						transaction.DroppedNamespaces = append(transaction.DroppedNamespaces, name)
					}
				}
				sort.Strings(transaction.CreatedNamespaces)
				sort.Strings(transaction.DroppedNamespaces)
			}
		}
	}
	if catalogChanged {
		tables, err := cdcCatalog(r.catalog)
		if err != nil {
			return transaction, err
		}
		for id, table := range tables {
			old, found := before[id]
			if !found {
				copy := cloneCDCTable(table)
				transaction.SchemaChanges = append(transaction.SchemaChanges, CDCSchemaChange{After: &copy})
			} else {
				previous, _ := json.Marshal(old)
				next, _ := json.Marshal(table)
				if string(previous) != string(next) {
					a, b := cloneCDCTable(old), cloneCDCTable(table)
					transaction.SchemaChanges = append(transaction.SchemaChanges, CDCSchemaChange{Before: &a, After: &b})
				}
			}
		}
		for id, table := range before {
			if _, found := tables[id]; !found {
				copy := cloneCDCTable(table)
				transaction.SchemaChanges = append(transaction.SchemaChanges, CDCSchemaChange{Before: &copy})
			}
		}
		r.tables = tables
	}
	touched := make(map[uint64]bool)
	for _, mutation := range record.Mutations {
		if len(mutation.Key) < 10 || mutation.Key[0] != 1 || mutation.Key[1] != 255 {
			continue
		}
		id := binary.BigEndian.Uint64(mutation.Key[2:10])
		table, found := r.tables[id]
		if !found {
			table, found = before[id]
		}
		if !found {
			return transaction, fmt.Errorf("CDC row references unknown table %d", id)
		}
		row := CDCRow{TableID: id, Kind: "insert"}
		if mutation.BeforeExists {
			old := table
			if previous, found := before[id]; found {
				old = previous
			}
			values, err := sql.DecodeStoredRow(mutation.Before, old.Columns)
			if err != nil {
				return transaction, err
			}
			row.Before = values
			row.Kind = "update"
		}
		if mutation.Delete {
			row.Kind = "delete"
		} else {
			values, err := sql.DecodeStoredRow(mutation.After, table.Columns)
			if err != nil {
				return transaction, err
			}
			row.After = values
		}
		transaction.Changes = append(transaction.Changes, row)
		if !touched[id] {
			touched[id] = true
			transaction.Tables = append(transaction.Tables, cloneCDCTable(table))
		}
	}
	sort.Slice(transaction.SchemaChanges, func(i, j int) bool {
		id := func(c CDCSchemaChange) uint64 {
			if c.After != nil {
				return c.After.ID
			}
			return c.Before.ID
		}
		return id(transaction.SchemaChanges[i]) < id(transaction.SchemaChanges[j])
	})
	return transaction, nil
}
