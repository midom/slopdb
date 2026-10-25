// Package changefeed defines protocol-neutral committed row images and schema metadata.
package changefeed

import "slopdb/sql"

type Relation = sql.CatalogRelation

type Row struct {
	TableID uint64
	Kind    string // insert, update, delete
	Before  []sql.Value
	After   []sql.Value
}
type SchemaChange struct {
	Before *Relation
	After  *Relation
}
type Transaction struct {
	Sequence          uint64
	Time              int64 // UTC Unix nanoseconds, assigned before replication
	Tables            []Relation
	Changes           []Row
	SchemaChanges     []SchemaChange
	Namespaces        []string // non-nil when the namespace registry changes
	CreatedNamespaces []string
	DroppedNamespaces []string
}
type Metadata struct {
	SourceID      string
	StartSequence uint64
	Tables        []Relation
}
