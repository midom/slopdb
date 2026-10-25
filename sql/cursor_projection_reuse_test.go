package sql

import (
	"bytes"
	"testing"
)

func projectionReuseCursor(t *testing.T, s *Schema, query string) *RowCursor {
	t.Helper()
	if err := s.StartStatement(); err != nil {
		t.Fatal(err)
	}
	cursor, handled, err := s.OpenRowCursor(statisticsSelect(t, query))
	if err != nil || !handled {
		s.EndStatement()
		t.Fatalf("handled=%v: %v", handled, err)
	}
	t.Cleanup(func() { cursor.Close(); s.EndStatement() })
	return cursor
}

func TestCursorProjectionOnlyDecodesDependencies(t *testing.T) {
	s := cursorReuseFixture(t)
	for _, dialect := range []Dialect{DialectNative, DialectMySQL, DialectPostgreSQL} {
		t.Run(dialect.String(), func(t *testing.T) {
			s.SetDialect(dialect)
			t.Run("qualified_alias", func(t *testing.T) {
				c := projectionReuseCursor(t, s, "SELECT r.id AS n FROM cursor_reuse AS r WHERE r.id>=1 ORDER BY r.id DESC LIMIT 17 OFFSET 3")
				var dst map[string]Value
				for i := int64(0); i < 17; i++ {
					var ok bool
					var err error
					dst, ok, err = c.NextInto(dst)
					if err != nil || !ok || len(dst) != 1 || dst["n"].Int != 997-i {
						t.Fatalf("row %d: %v %v %v", i, dst, ok, err)
					}
					// Unreferenced variable-width fields must never enter private
					// source scratch just because the projection has an alias.
					for _, name := range []string{"b", "hidden", "label", "r.b", "r.label"} {
						if _, decoded := c.source[name]; decoded {
							t.Fatalf("decoded unused column %q", name)
						}
					}
				}
				if _, ok, err := c.Next(); ok || err != nil {
					t.Fatalf("extra row: %v %v", ok, err)
				}
			})
			t.Run("where_only_blob", func(t *testing.T) {
				c := projectionReuseCursor(t, s, "SELECT id+1 AS n FROM cursor_reuse WHERE hidden IS NOT NULL ORDER BY id LIMIT 17")
				var dst map[string]Value
				var buffer *byte
				for id, count := int64(1), 0; count < 17; id++ {
					if id%5 == 0 {
						continue
					}
					var ok bool
					var err error
					dst, ok, err = c.NextInto(dst)
					if err != nil || !ok || len(dst) != 1 || dst["n"].Int != id+1 {
						t.Fatalf("row %d: %v %v %v", id, dst, ok, err)
					}
					if _, decoded := c.source["b"]; decoded {
						t.Fatal("decoded unreferenced projected BLOB")
					}
					blob := c.source["hidden"].Blob
					if !bytes.Equal(blob, []byte{1, 2}) {
						t.Fatalf("WHERE-only BLOB: %x", blob)
					}
					if buffer == nil {
						buffer = &blob[0]
					} else if buffer != &blob[0] {
						t.Fatal("WHERE-only source buffer was replaced")
					}
					count++
				}
			})
		})
	}
}

func TestCursorProjectionNestedBlobOwnership(t *testing.T) {
	s := cursorReuseFixture(t)
	for _, dialect := range []Dialect{DialectNative, DialectMySQL, DialectPostgreSQL} {
		t.Run(dialect.String(), func(t *testing.T) {
			s.SetDialect(dialect)
			c := projectionReuseCursor(t, s, "SELECT r.id AS n,r.b AS duplicate,r.b AS duplicate,CASE WHEN r.id%17=0 THEN X'7f' ELSE r.b END AS n FROM cursor_reuse AS r WHERE r.hidden IS NOT NULL ORDER BY r.id")
			columns := c.Columns()
			var retained []map[string]Value
			var ids []int64
			for id := int64(1); id <= 1000; id++ {
				if id%5 == 0 {
					continue
				}
				row, ok, err := c.Next()
				if err != nil || !ok || len(row) != 4 || row[columns[0].Name].Int != id {
					t.Fatalf("row %d: %v %v %v", id, row, ok, err)
				}
				retained, ids = append(retained, row), append(ids, id)
			}
			if _, ok, err := c.Next(); ok || err != nil {
				t.Fatalf("extra row: %v %v", ok, err)
			}
			c.Close()
			for i, row := range retained {
				id := ids[i]
				blob := cursorReuseBlob(id)
				for _, column := range columns[1:3] {
					v := row[column.Name]
					if v.Null != (id%17 == 0) || !bytes.Equal(v.Blob, blob) || !v.Null && (v.Blob == nil) != (blob == nil) {
						t.Fatalf("retained row %d column %s: %#v", id, column.Name, v)
					}
				}
				want := blob
				if id%17 == 0 {
					want = []byte{127}
				}
				if v := row[columns[3].Name]; v.Null || !bytes.Equal(v.Blob, want) {
					t.Fatalf("CASE row %d: %#v", id, v)
				}
			}
			for _, row := range retained {
				if len(row[columns[1].Name].Blob) > 0 {
					original := row[columns[2].Name].Blob[0]
					row[columns[1].Name].Blob[0] ^= 255
					if row[columns[2].Name].Blob[0] != original {
						t.Fatal("duplicate projections alias one another")
					}
					break
				}
			}
		})
	}
}
