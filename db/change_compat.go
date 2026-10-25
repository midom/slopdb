package db

import (
	"context"
	"slopdb/internal/mysqlschema"
	"slopdb/sql"
)

// CDCTable is the original MySQL-oriented CDC metadata surface. New consumers
// should use ChangeTable, whose schema and rows contain no rendered wire SQL.
type CDCTable struct {
	ID           uint64
	Schema, Name string
	Columns      []sql.ColumnDef
	Definition   *sql.TableInfo
	Indexes      []*sql.IndexInfo
	CreateSQL    string
}
type CDCRow = RowChange
type CDCSchemaChange struct{ Before, After *CDCTable }
type CDCTransaction struct {
	Sequence                                         uint64
	Time                                             int64
	Tables                                           []CDCTable
	Changes                                          []CDCRow
	SchemaChanges                                    []CDCSchemaChange
	Namespaces, CreatedNamespaces, DroppedNamespaces []string
}
type CDCMetadata struct {
	SourceID      string
	StartSequence uint64
	Tables        []CDCTable
}

func (t CDCTable) changeTable() ChangeTable {
	return ChangeTable{ID: t.ID, Schema: t.Schema, Name: t.Name, Kind: "table", Columns: t.Columns, Definition: t.Definition, Indexes: t.Indexes}
}
func legacyCDCTable(t ChangeTable) CDCTable {
	return CDCTable{ID: t.ID, Schema: t.Schema, Name: t.Name, Columns: t.Columns, Definition: t.Definition, Indexes: t.Indexes, CreateSQL: mysqlschema.Create(t, "")}
}
func legacyCDCMetadata(m ChangeMetadata) *CDCMetadata {
	out := &CDCMetadata{SourceID: m.SourceID, StartSequence: m.StartSequence}
	for _, t := range m.Tables {
		out.Tables = append(out.Tables, legacyCDCTable(t))
	}
	return out
}
func (d *Database) EnableCDC(ctx context.Context) (*CDCMetadata, error) {
	m, e := d.EnableChangefeed(ctx)
	if e != nil || m == nil {
		return nil, e
	}
	return legacyCDCMetadata(*m), nil
}
func (d *Database) CDCMetadata(ctx context.Context) (*CDCMetadata, error) {
	m, e := d.ChangeMetadata(ctx)
	if e != nil || m == nil {
		return nil, e
	}
	return legacyCDCMetadata(*m), nil
}
func (d *Database) CDCPosition(ctx context.Context) (uint64, error) { return d.ChangefeedPosition(ctx) }

// CDCReader adapts the neutral journal reader to the legacy metadata API.
type CDCReader struct{ reader *ChangeReader }

func (d *Database) NewCDCReader(ctx context.Context, after uint64) (*CDCReader, error) {
	r, e := d.NewChangeReader(ctx, after)
	if e != nil {
		return nil, e
	}
	return &CDCReader{reader: r}, nil
}
func (r *CDCReader) Position() uint64         { return r.reader.Position() }
func (r *CDCReader) Changes() <-chan struct{} { return r.reader.Changes() }
func (r *CDCReader) Metadata() CDCMetadata    { return *legacyCDCMetadata(r.reader.Metadata()) }
func (r *CDCReader) Read(ctx context.Context, limit int) ([]CDCTransaction, error) {
	txs, e := r.reader.Read(ctx, limit)
	if e != nil {
		return nil, e
	}
	out := make([]CDCTransaction, 0, len(txs))
	for _, tx := range txs {
		v := CDCTransaction{Sequence: tx.Sequence, Time: tx.Time, Changes: tx.Changes, Namespaces: tx.Namespaces, CreatedNamespaces: tx.CreatedNamespaces, DroppedNamespaces: tx.DroppedNamespaces}
		for _, t := range tx.Tables {
			v.Tables = append(v.Tables, legacyCDCTable(t))
		}
		for _, c := range tx.SchemaChanges {
			v.SchemaChanges = append(v.SchemaChanges, CDCSchemaChange{})
			dest := &v.SchemaChanges[len(v.SchemaChanges)-1]
			if c.Before != nil {
				t := legacyCDCTable(*c.Before)
				dest.Before = &t
			}
			if c.After != nil {
				t := legacyCDCTable(*c.After)
				dest.After = &t
			}
		}
		out = append(out, v)
	}
	return out, nil
}
