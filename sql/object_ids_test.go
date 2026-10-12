package sql

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"

	"slopdb/storage"
)

func TestLegacyObjectIDMigrationAndResume(t *testing.T) {
	for _, phase := range []string{"fresh", "partial", "published"} {
		t.Run(phase, func(t *testing.T) {
			dir := filepath.Join(t.TempDir(), "db")
			opt := storage.DefaultOptions()
			opt.DisableWALSync = true
			l, err := storage.NewLSMTree(dir, opt)
			if err != nil {
				t.Fatal(err)
			}
			defer l.Close()
			info := &TableInfo{Name: "legacy:table", Columns: []ColumnDef{{Name: "id", ColType: ColInteger, NotNull: true, PrimaryKey: true}, {Name: "payload", ColType: ColText}}, PKColumns: []string{"id"}}
			heap := &TableInfo{Name: "heap", Columns: []ColumnDef{{Name: "v", ColType: ColText}}}
			idx := &IndexInfo{Name: "iv:legacy", Table: info.Name, Column: "payload"}
			model := &Schema{tables: map[string]*TableInfo{info.Name: info, "heap": heap}, indexes: map[string]*IndexInfo{idx.Name: idx}}
			putJSON := func(key string, value any) {
				t.Helper()
				b, e := json.Marshal(value)
				if e != nil {
					t.Fatal(e)
				}
				if e = l.Put([]byte(key), b); e != nil {
					t.Fatal(e)
				}
			}
			putJSON(schemaPrefix, model.tables)
			putJSON(schemaIdxPrefix, model.indexes)
			var records []storage.Mutation
			for i := int64(1); i <= 38; i++ {
				row := map[string]Value{"id": NewIntValue(i), "payload": NewTextValue(strings.Repeat("é;\x00value", 5000) + string(rune('A'+i)))}
				key := info.encodePK(info.Name, row)
				records = append(records, storage.Mutation{Key: key, Value: encodeRow(row, info)})
				if e := l.Put(key, encodeRow(row, info)); e != nil {
					t.Fatal(e)
				}
				if e := l.Put(encodeRowIndexKey(idx, row, key), model.encodeIndexValue(idx, row)); e != nil {
					t.Fatal(e)
				}
			}
			heapKey := encodeRowKey("heap", 7)
			if e := l.Put(heapKey, encodeRow(map[string]Value{"v": NewTextValue("legacy")}, heap)); e != nil {
				t.Fatal(e)
			}
			if phase != "fresh" {
				hash, e := model.objectCatalogHash()
				if e != nil {
					t.Fatal(e)
				}
				plan := objectMigration{Source: hash, Tables: map[string]uint64{info.Name: 1, "heap": 2}, Indexes: map[string]uint64{idx.Name: 3}, Published: phase == "published"}
				if e = l.Put([]byte(objectIDPrefix), binary.BigEndian.AppendUint64(nil, 11)); e != nil {
					t.Fatal(e)
				}
				copyInfo := *info
				copyInfo.ID = 1
				copyIdx := *idx
				copyIdx.ID = 3
				limit := 1
				if phase == "published" {
					limit = len(records)
				}
				for _, record := range records[:limit] {
					row := decodeRow(record.Value, info)
					key := copyInfo.encodePK(info.Name, row)
					if e = l.Put(key, record.Value); e != nil {
						t.Fatal(e)
					}
					if e = l.Put(encodeRowIndexKey(&copyIdx, row, key), model.encodeIndexValue(&copyIdx, row)); e != nil {
						t.Fatal(e)
					}
				}
				if phase == "partial" {
					stale := map[string]Value{"id": NewIntValue(999), "payload": NewTextValue("stale destination")}
					key := copyInfo.encodePK(info.Name, stale)
					if e = l.Put(key, encodeRow(stale, &copyInfo)); e != nil {
						t.Fatal(e)
					}
					if e = l.Put(encodeRowIndexKey(&copyIdx, stale, key), model.encodeIndexValue(&copyIdx, stale)); e != nil {
						t.Fatal(e)
					}
				}
				if phase == "published" {
					heap.ID = 2
					newHeapKey := append([]byte(heap.storagePrefix("heap")), heapKey[len(tableDataPrefix("heap")):]...)
					if e = l.Put(newHeapKey, encodeRow(map[string]Value{"v": NewTextValue("legacy")}, heap)); e != nil {
						t.Fatal(e)
					}
					info.ID = 1
					idx.ID = 3
					putJSON(schemaPrefix, model.tables)
					putJSON(schemaIdxPrefix, model.indexes)
				}
				putJSON(objectMigrationPrefix, plan)
			}
			if e := l.Close(); e != nil {
				t.Fatal(e)
			}
			l, err = storage.NewLSMTree(dir, opt)
			if err != nil {
				t.Fatal(err)
			}
			defer l.Close()
			s, err := NewSchemaWithOptions(l, 4096, opt)
			if err != nil {
				t.Fatal(err)
			}
			if s.tables[info.Name].ID == 0 || s.tables["heap"].ID == 0 || s.indexes[idx.Name].ID == 0 {
				t.Fatal("legacy IDs missing")
			}
			if _, e := s.CheckIntegrity(); e != nil {
				t.Fatal(e)
			}
			it := l.NewIterator()
			for it.Valid() {
				key := it.Key().UserKey
				if len(key) > 0 && (key[0] == 1 || key[0] == 2) {
					if len(key) < 11 || key[1] != 0xff || key[10] != ':' {
						t.Fatalf("legacy key survived: %q", key)
					}
				}
				it.Next()
			}
			it.Close()
			if _, ok, e := l.Get([]byte(objectMigrationPrefix)); e != nil || ok {
				t.Fatal("migration marker survived", e)
			}
			query, e := Parse(`SELECT id,payload FROM "legacy:table" ORDER BY id`)
			if e != nil {
				t.Fatal(e)
			}
			r, e := s.Select(query[0].(*SelectStmt))
			if e != nil || len(r.Rows) != len(records) {
				t.Fatal(e, len(r.Rows))
			}
			for i, row := range r.Rows {
				want := decodeRow(records[i].Value, info)
				if row["id"].Int != want["id"].Int || row["payload"].Text != want["payload"].Text {
					t.Fatal("migration changed values")
				}
			}
			id := s.tables[info.Name].ID
			keyBefore := append([]byte(nil), records[0].Key...)
			if e = s.AlterTable(&AlterTableStmt{Table: info.Name, Operation: "RENAME_TO", NewName: "renamed"}); e != nil {
				t.Fatal(e)
			}
			if s.tables["renamed"].ID != id {
				t.Fatal("legacy table ID changed on rename")
			}
			if bytes.HasPrefix(s.tables["renamed"].encodePK("renamed", map[string]Value{"id": NewIntValue(1)}), keyBefore[:len(tableDataPrefix(info.Name))]) {
				t.Fatal("rename kept name prefix")
			}
			if e = s.CreateTable(&CreateTableStmt{Table: "fresh_object", Columns: []ColumnDef{{Name: "v", ColType: ColText}}}); e != nil {
				t.Fatal(e)
			}
			if phase != "fresh" && s.tables["fresh_object"].ID <= 11 {
				t.Fatal("migration reused reserved IDs")
			}
			if e = l.Close(); e != nil {
				t.Fatal(e)
			}
			l, err = storage.NewLSMTree(dir, opt)
			if err != nil {
				t.Fatal(err)
			}
			defer l.Close()
			s, err = NewSchemaWithOptions(l, 4096, opt)
			if err != nil {
				t.Fatal(err)
			}
			if s.tables["renamed"].ID != id {
				t.Fatal("reopen changed IDs")
			}
			if _, e = s.CheckIntegrity(); e != nil {
				t.Fatal(e)
			}
		})
	}
}
