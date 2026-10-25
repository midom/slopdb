package sql

import (
	"encoding/json"
	"fmt"
	"maps"
	"sort"
)

// CatalogRelation is a protocol-neutral description. Declaration spelling is
// preserved in Columns, while physical IDs and storage encodings remain native.
// SQL renderers and wire encoders belong to their respective adapters.
type CatalogRelation struct {
	ID         uint64
	Schema     string
	Name       string
	Kind       string
	Columns    []ColumnDef
	Definition *TableInfo
	Indexes    []*IndexInfo
	ViewSQL    string
	Temporary  bool
}

// CatalogSnapshot captures immutable metadata map headers in the caller's normal
// execution view. It never pins data, evaluates expressions or obtains row locks.
// Exported accessors return owned metadata, including their nested slices.
type CatalogSnapshot struct {
	tables     map[string]*TableInfo
	indexes    map[string]*IndexInfo
	views      map[string]*ViewInfo
	triggers   map[string]*TriggerInfo
	namespaces map[string]bool
	stats      map[string]int64
	temporary  map[string]bool
}

func (s *Schema) CatalogSnapshot() *CatalogSnapshot {
	temp := map[string]bool{}
	for name := range s.tables {
		if s.isTemporaryTable(name) {
			temp[name] = true
		}
	}
	return &CatalogSnapshot{tables: maps.Clone(s.tables), indexes: maps.Clone(s.indexes), views: maps.Clone(s.views), triggers: maps.Clone(s.triggers), namespaces: maps.Clone(s.namespaces), stats: maps.Clone(s.stats), temporary: temp}
}

// DecodeCatalogSnapshot interprets the same native catalog metadata used by
// sessions, backups and changefeeds, without invoking a dialect renderer.
func DecodeCatalogSnapshot(metadata map[string][]byte) (*CatalogSnapshot, error) {
	c := &CatalogSnapshot{namespaces: map[string]bool{"main": true}}
	for _, entry := range []struct {
		key    string
		target any
	}{
		{"\x00__schema__", &c.tables}, {"\x00__schema_indexes__", &c.indexes},
		{"\x00__schema_views__", &c.views}, {"\x00__schema_triggers__", &c.triggers},
		{"\x00__namespaces_v1__", &c.namespaces},
	} {
		if data := metadata[entry.key]; len(data) > 0 {
			if err := json.Unmarshal(data, entry.target); err != nil {
				return nil, fmt.Errorf("catalog metadata %q: %w", entry.key, err)
			}
		}
	}
	return c, nil
}
func catalogNames[T any](objects map[string]T) []string {
	names := make([]string, 0, len(objects))
	for n := range objects {
		names = append(names, n)
	}
	sort.Strings(names)
	return names
}
func (c *CatalogSnapshot) SchemaNames() []string        { return catalogNames(c.namespaces) }
func (c *CatalogSnapshot) TableNames() []string         { return catalogNames(c.tables) }
func (c *CatalogSnapshot) ViewNames() []string          { return catalogNames(c.views) }
func (c *CatalogSnapshot) IndexNames() []string         { return catalogNames(c.indexes) }
func (c *CatalogSnapshot) TriggerNames() []string       { return catalogNames(c.triggers) }
func (c *CatalogSnapshot) IsTemporary(name string) bool { return c.temporary[name] }
func (c *CatalogSnapshot) RowEstimate(name string) (int64, bool) {
	v, ok := c.stats[name]
	return v, ok
}
func cloneCatalogMetadata[T any](value T) T {
	data, _ := json.Marshal(value)
	var clone T
	_ = json.Unmarshal(data, &clone)
	return clone
}
func (c *CatalogSnapshot) relation(name string) (CatalogRelation, bool) {
	ns, base := SplitSchemaName(name)
	r := CatalogRelation{Schema: ns, Name: base, Temporary: c.temporary[name]}
	if t := c.tables[name]; t != nil {
		r.ID = t.ID
		r.Kind = "table"
		r.Definition = t
		r.Columns = t.Columns
		for _, idx := range c.indexes {
			if idx.Table == name {
				r.Indexes = append(r.Indexes, idx)
			}
		}
		sort.Slice(r.Indexes, func(i, j int) bool { return r.Indexes[i].SQLName() < r.Indexes[j].SQLName() })
		return r, true
	}
	if v := c.views[name]; v != nil {
		r.Kind = "view"
		r.ViewSQL = v.SQL
		for _, n := range v.Columns {
			r.Columns = append(r.Columns, ColumnDef{Name: n, ColType: ColText})
		}
		return r, true
	}
	return r, false
}
func (c *CatalogSnapshot) Relation(name string) (CatalogRelation, bool) {
	r, ok := c.relation(name)
	if !ok {
		return r, false
	}
	return cloneCatalogMetadata(r), true
}
func (c *CatalogSnapshot) Relations() []CatalogRelation {
	names := append(c.TableNames(), c.ViewNames()...)
	sort.Strings(names)
	out := make([]CatalogRelation, 0, len(names))
	for _, n := range names {
		r, _ := c.relation(n)
		out = append(out, cloneCatalogMetadata(r))
	}
	return out
}
