package db

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"maps"
	"sort"
	"sync"

	"slopdb/changefeed"
	"slopdb/sql"
	"slopdb/storage"
)

type ChangeTable = changefeed.Relation
type RowChange = changefeed.Row
type SchemaChange = changefeed.SchemaChange
type ChangeTransaction = changefeed.Transaction
type ChangeMetadata = changefeed.Metadata

func (d *Database) ChangefeedPosition(ctx context.Context) (uint64, error) {
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

func (d *Database) EnableChangefeed(ctx context.Context) (*ChangeMetadata, error) {
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

func (d *Database) ChangeMetadata(ctx context.Context) (*ChangeMetadata, error) {
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

func cdcMetadata(header *storage.ChangeLogHeader) (*ChangeMetadata, error) {
	tables, err := cdcCatalog(header.Catalog)
	if err != nil {
		return nil, err
	}
	result := &ChangeMetadata{SourceID: header.SourceID, StartSequence: header.StartSequence}
	for _, table := range tables {
		result.Tables = append(result.Tables, table)
	}
	sort.Slice(result.Tables, func(i, j int) bool { return result.Tables[i].ID < result.Tables[j].ID })
	return result, nil
}

func cdcCatalog(catalog map[string][]byte) (map[uint64]ChangeTable, error) {
	snapshot, err := sql.DecodeCatalogSnapshot(catalog)
	if err != nil {
		return nil, err
	}
	relations := snapshot.Relations()
	tables := make(map[uint64]ChangeTable, len(relations))
	byName := make(map[string]*sql.TableInfo, len(relations))
	for _, r := range relations {
		if r.Definition != nil {
			name := r.Name
			if r.Schema != "main" {
				name = r.Schema + "." + name
			}
			byName[name] = r.Definition
		}
	}
	for _, r := range relations {
		if r.Kind != "table" {
			continue
		}
		for i := range r.Definition.Constraints {
			c := &r.Definition.Constraints[i]
			if c.Kind == "FOREIGN" && len(c.RefColumns) == 0 {
				if parent := byName[c.RefTable]; parent != nil {
					c.RefColumns = append([]string(nil), parent.PKColumns...)
				}
			}
		}
		tables[r.ID] = r
	}
	return tables, nil
}

// ChangeReader owns only decoding state; it does not pin an MVCC snapshot or hold
// an execution lock between calls. The durable journal is the resume source.
type ChangeReader struct {
	mu         sync.Mutex
	database   *Database
	sequence   uint64
	generation uint64
	catalog    map[string][]byte
	tables     map[uint64]ChangeTable
	metadata   ChangeMetadata
}

func (d *Database) NewChangeReader(ctx context.Context, after uint64) (*ChangeReader, error) {
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
	return &ChangeReader{database: d, sequence: after, generation: d.generation.Load(), catalog: catalog, tables: tables, metadata: *metadata}, nil
}

func cloneChangeTable(table ChangeTable) ChangeTable {
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
func (r *ChangeReader) Metadata() ChangeMetadata {
	metadata := r.metadata
	metadata.Tables = append([]ChangeTable(nil), metadata.Tables...)
	for i := range metadata.Tables {
		metadata.Tables[i] = cloneChangeTable(metadata.Tables[i])
	}
	return metadata
}
func (r *ChangeReader) Position() uint64 { r.mu.Lock(); defer r.mu.Unlock(); return r.sequence }

func (r *ChangeReader) Read(ctx context.Context, limit int) ([]ChangeTransaction, error) {
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
	tree := d.currentStorage.Load()
	generation := d.generation.Load()
	if generation != r.generation {
		header, err := tree.ChangeLogHeader()
		if err != nil {
			return nil, err
		}
		if header == nil || header.SourceID != r.metadata.SourceID {
			return nil, fmt.Errorf("CDC source changed after storage replacement")
		}
		position, err := tree.ChangeLogPosition()
		if err != nil {
			return nil, err
		}
		if position < r.sequence {
			return nil, fmt.Errorf("CDC source position moved backwards after storage replacement")
		}
		r.generation = generation
	}
	records, err := tree.ReadChanges(ctx, r.sequence, limit)
	if err != nil {
		return nil, err
	}
	result := make([]ChangeTransaction, 0, len(records))
	decoder := &ChangeReader{catalog: maps.Clone(r.catalog), tables: r.tables}
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

var cdcClosedChanges = func() <-chan struct{} { ch := make(chan struct{}); close(ch); return ch }()

func (r *ChangeReader) Changes() <-chan struct{} {
	d := r.database
	// Pair storage selection/channel acquisition with the replacement writer.
	// Otherwise a handoff between those two steps can miss its notification.
	if err := d.execution.RLockContext(context.Background()); err != nil {
		return cdcClosedChanges
	}
	defer d.execution.RUnlock()
	if d.closed.Load() {
		return cdcClosedChanges
	}
	return d.currentStorage.Load().ChangeNotification()
}

func (r *ChangeReader) decode(record storage.ChangeRecord) (ChangeTransaction, error) {
	transaction := ChangeTransaction{Sequence: record.Sequence, Time: record.Time}
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
				copy := cloneChangeTable(table)
				transaction.SchemaChanges = append(transaction.SchemaChanges, SchemaChange{After: &copy})
			} else {
				previous, _ := json.Marshal(old)
				next, _ := json.Marshal(table)
				if string(previous) != string(next) {
					a, b := cloneChangeTable(old), cloneChangeTable(table)
					transaction.SchemaChanges = append(transaction.SchemaChanges, SchemaChange{Before: &a, After: &b})
				}
			}
		}
		for id, table := range before {
			if _, found := tables[id]; !found {
				copy := cloneChangeTable(table)
				transaction.SchemaChanges = append(transaction.SchemaChanges, SchemaChange{Before: &copy})
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
		row := RowChange{TableID: id, Kind: "insert"}
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
			transaction.Tables = append(transaction.Tables, cloneChangeTable(table))
		}
	}
	sort.Slice(transaction.SchemaChanges, func(i, j int) bool {
		id := func(c SchemaChange) uint64 {
			if c.After != nil {
				return c.After.ID
			}
			return c.Before.ID
		}
		return id(transaction.SchemaChanges[i]) < id(transaction.SchemaChanges[j])
	})
	return transaction, nil
}
