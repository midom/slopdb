package sql

import (
	"bytes"
	"fmt"
	"strings"
	"testing"
)

// These tests retain owned results while the cursor advances across SSTable
// pages. NextInto destinations are independent; only the one passed back may
// be overwritten by a later call.
func cursorReuseBlob(id int64) []byte {
	if id%17 == 0 {
		return nil
	}
	if id%19 == 0 {
		return []byte{}
	}
	n := 180 + int(id%97)
	if id == 257 || id == 509 {
		n = 6000
	}
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(id*31 + int64(i)*43)
	}
	return b
}
func cursorReuseFixture(t *testing.T) *Schema {
	t.Helper()
	s, lsm := statisticsSchema(t, t.TempDir())
	statisticsSQL(t, s, "CREATE TABLE cursor_reuse(id BIGINT PRIMARY KEY,b BLOB,hidden BLOB,label TEXT)")
	for base := int64(1); base <= 1000; base += 100 {
		var rows []string
		for id := base; id < base+100; id++ {
			literal := fmt.Sprintf("X'%x'", cursorReuseBlob(id))
			if id%17 == 0 {
				literal = "NULL"
			}
			hidden := "X'0102'"
			if id%5 == 0 {
				hidden = "NULL"
			}
			rows = append(rows, fmt.Sprintf("(%d,%s,%s,'row-%04d')", id, literal, hidden, id))
		}
		statisticsSQL(t, s, "INSERT INTO cursor_reuse VALUES "+strings.Join(rows, ","))
	}
	if e := lsm.CompactRange(); e != nil {
		t.Fatal(e)
	}
	return s
}
func cursorReuseCheck(t *testing.T, row map[string]Value, columns []ColumnDef, id int64, shape string) {
	t.Helper()
	b := NewBlobValue(cursorReuseBlob(id))
	if id%17 == 0 {
		b = NewNullValue()
	}
	fallback := b
	if b.Null {
		fallback = NewBlobValue([]byte{127})
	}
	want := []Value{NewIntValue(id), b}
	if shape == "generic" {
		want = []Value{NewIntValue(id), b, b, fallback, NewTextValue(fmt.Sprintf("row-%04d", id))}
	}
	if len(row) != len(want) || len(columns) != len(want) {
		t.Fatalf("id%d column/map count got %d/%d want%d", id, len(columns), len(row), len(want))
	}
	for i, col := range columns {
		got, ok := row[col.Name]
		w := want[i]
		if !ok || got.Null != w.Null || (!w.Null && (got.Type != w.Type || got.Int != w.Int || got.Text != w.Text || !bytes.Equal(got.Blob, w.Blob) || (got.Type == ColBlob && (got.Blob == nil) != (w.Blob == nil)))) {
			t.Fatalf("id%d col%s got %#v want %#v", id, col.Name, got, w)
		}
	}
}
func cursorReuseOpen(t *testing.T, s *Schema, shape string) *RowCursor {
	t.Helper()
	q := "SELECT id,b FROM cursor_reuse WHERE id>=1 AND hidden IS NOT NULL ORDER BY id DESC"
	if shape == "generic" {
		q = "SELECT r.id,r.b AS duplicate,r.b AS duplicate,COALESCE(CAST(r.b AS BLOB),X'7f') AS hidden,r.label AS b FROM cursor_reuse AS r WHERE r.id>=1 AND r.hidden IS NOT NULL ORDER BY r.id DESC"
	}
	if e := s.StartStatement(); e != nil {
		t.Fatal(e)
	}
	t.Cleanup(s.EndStatement)
	c, handled, e := s.OpenRowCursor(statisticsSelect(t, q))
	if e != nil || !handled {
		t.Fatalf("cursor handled=%v err=%v", handled, e)
	}
	return c
}
func TestCursorNextOwnedAcrossPages(t *testing.T) {
	s := cursorReuseFixture(t)
	for _, dialect := range []Dialect{DialectNative, DialectMySQL, DialectPostgreSQL} {
		for _, shape := range []string{"physical", "generic"} {
			t.Run(fmt.Sprintf("%v/%s", dialect, shape), func(t *testing.T) {
				s.SetDialect(dialect)
				c := cursorReuseOpen(t, s, shape)
				cols := c.Columns()
				var saved []map[string]Value
				var ids []int64
				for id := int64(1000); id > 0; id-- {
					if id%5 == 0 {
						continue
					}
					row, ok, e := c.Next()
					if e != nil || !ok {
						t.Fatalf("id%d ok=%v err=%v", id, ok, e)
					}
					cursorReuseCheck(t, row, cols, id, shape)
					saved = append(saved, row)
					ids = append(ids, id)
				}
				if _, ok, e := c.Next(); ok || e != nil {
					t.Fatalf("end ok=%v err=%v", ok, e)
				}
				c.Close()
				s.EndStatement()
				for i, row := range saved {
					cursorReuseCheck(t, row, cols, ids[i], shape)
				}
				// Caller mutation must not reach stored rows or any later output.
				for _, v := range saved[0] {
					if len(v.Blob) > 0 {
						v.Blob[0] ^= 255
					}
				}
				saved[0][cols[0].Name] = NewIntValue(-1)
				c = cursorReuseOpen(t, s, shape)
				row, ok, e := c.Next()
				if e != nil || !ok {
					t.Fatalf("reread %v %v", ok, e)
				}
				cursorReuseCheck(t, row, c.Columns(), 999, shape)
				c.Close()
				s.EndStatement()
				cursorReuseCheck(t, saved[1], cols, ids[1], shape)
			})
		}
	}
}

type cursorOwnedNextInto interface {
	NextInto(map[string]Value) (map[string]Value, bool, error)
}

func TestCursorNextIntoAlternatingOwnedDestinations(t *testing.T) {
	s := cursorReuseFixture(t)
	for _, dialect := range []Dialect{DialectNative, DialectMySQL, DialectPostgreSQL} {
		for _, shape := range []string{"physical", "generic"} {
			t.Run(fmt.Sprintf("%v/%s", dialect, shape), func(t *testing.T) {
				s.SetDialect(dialect)
				c := cursorReuseOpen(t, s, shape)
				defer c.Close()
				next, ok := any(c).(cursorOwnedNextInto)
				if !ok {
					t.Fatal("RowCursor must support independently owned NextInto destinations")
				}
				cols := c.Columns()
				dst := [2]map[string]Value{
					map[string]Value{"obsolete": NewBlobValue([]byte{1, 2, 3})},
					map[string]Value{"obsolete": NewBlobValue([]byte{4, 5, 6})},
				}
				var dstID [2]int64
				var owned []map[string]Value
				var ownedIDs []int64
				ordinal := 0
				for id := int64(1000); id > 0; id-- {
					if id%5 == 0 {
						continue
					}
					var row map[string]Value
					var e error
					var valid bool
					switch ordinal % 3 {
					case 0:
						row, valid, e = c.Next()
						owned = append(owned, row)
						ownedIDs = append(ownedIDs, id)
					default:
						slot := ordinal%3 - 1
						row, valid, e = next.NextInto(dst[slot])
						dst[slot] = row
						dstID[slot] = id
					}
					if e != nil || !valid {
						t.Fatalf("id%d ok=%v err=%v", id, valid, e)
					}
					cursorReuseCheck(t, row, cols, id, shape)
					for j := range dst {
						if dstID[j] != 0 {
							cursorReuseCheck(t, dst[j], cols, dstID[j], shape)
						}
					}
					if len(owned) > 0 {
						cursorReuseCheck(t, owned[0], cols, ownedIDs[0], shape)
					}
					ordinal++
				}
				if _, ok, e := next.NextInto(nil); ok || e != nil {
					t.Fatalf("end %v %v", ok, e)
				}
				c.Close()
				s.EndStatement()
				for i, row := range owned {
					cursorReuseCheck(t, row, cols, ownedIDs[i], shape)
				}
				for j := range dst {
					cursorReuseCheck(t, dst[j], cols, dstID[j], shape)
				}
				// Mutate a final destination, then reread the same physical data.
				for _, v := range dst[0] {
					if len(v.Blob) > 0 {
						v.Blob[0] ^= 255
					}
				}
				dst[0][cols[0].Name] = NewIntValue(-1)
				c = cursorReuseOpen(t, s, shape)
				row, valid, e := c.Next()
				if e != nil || !valid {
					t.Fatalf("reread %v %v", valid, e)
				}
				cursorReuseCheck(t, row, c.Columns(), 999, shape)
				c.Close()
				s.EndStatement()
				cursorReuseCheck(t, dst[1], cols, dstID[1], shape)
			})
		}
	}
}
