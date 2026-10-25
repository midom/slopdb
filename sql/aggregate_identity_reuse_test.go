package sql

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"math"
	"os"
	"strings"
	"testing"
)

func distinctIdentityValue(t testing.TB, kind, text string) Value {
	t.Helper()
	var v Value
	var e error
	if kind == "decimal" {
		v, e = NewDecimalValue(text)
	} else {
		v, e = NewJSONValue(text)
	}
	if e != nil {
		t.Fatal(e)
	}
	return v
}

func TestAggregateDistinctNumericJSONAndNullIdentity(t *testing.T) {
	values := []Value{NewNullValue(), NewIntValue(1), NewUnsignedValue(1), NewRealValue(1), distinctIdentityValue(t, "decimal", "1.000"), NewRealValue(math.Copysign(0, -1)), NewIntValue(0), distinctIdentityValue(t, "decimal", "0.00"), NewUnsignedValue(math.MaxUint64), distinctIdentityValue(t, "decimal", "18446744073709551615"), NewIntValue(math.MaxInt64), distinctIdentityValue(t, "decimal", "9223372036854775807"), NewTextValue("1"), NewBlobValue([]byte("1")), NewTextValue(""), NewBlobValue([]byte{}), distinctIdentityValue(t, "json", `{"b":2,"a":1}`), distinctIdentityValue(t, "json", `{"a":1,"b":2}`), distinctIdentityValue(t, "json", `1`), NewNullValue()}
	for _, mode := range []string{"nil-owner", "memory", "spill"} {
		t.Run(mode, func(t *testing.T) {
			var s *Schema
			if mode != "nil-owner" {
				s = &Schema{queryTempDir: t.TempDir(), queryMemoryBytes: 1 << 20}
				if mode == "spill" {
					s.queryMemoryBytes = 256
				}
				defer s.clearSelectCaches()
			}
			a := aggregateState{owner: s}
			f := &FuncCallExpr{Name: "COUNT", Distinct: true}
			for repeat := 0; repeat < 3; repeat++ {
				for _, v := range values {
					if e := addAggregateValue(&a, f, v); e != nil {
						t.Fatal(e)
					}
				}
				if a.count != 10 {
					t.Fatalf("round%d distinct=%d want10", repeat, a.count)
				}
			}
			if mode == "spill" && a.distinctDisk == nil {
				t.Fatal("test did not spill")
			}
		})
	}
}

func TestAggregateDistinctCollationAndLargeOwnedKeys(t *testing.T) {
	for _, budget := range []int64{256, 4 << 20} {
		t.Run(fmt.Sprint(budget), func(t *testing.T) {
			s := &Schema{queryTempDir: t.TempDir(), queryMemoryBytes: budget, collation: "NFC_CI"}
			defer s.clearSelectCaches()
			a := aggregateState{owner: s}
			f := &FuncCallExpr{Name: "COUNT", Distinct: true}
			binary := NewTextValue("CAFÉ")
			binary.Binary = true
			for _, v := range []Value{NewTextValue("CAFÉ"), NewTextValue("cafe\u0301"), NewTextValue("cafÉ"), NewTextValue("tea"), NewTextValue("TEA"), binary, binary, NewNullValue()} {
				if e := addAggregateValue(&a, f, v); e != nil {
					t.Fatal(e)
				}
			}
			if a.count != 3 {
				t.Fatalf("collated count=%d want3", a.count)
			}
			input := bytes.Repeat([]byte{0, 127, 255}, 50000)
			for round := 0; round < 3; round++ {
				for _, first := range []byte{1, 2, 1, 2} {
					input[0] = first
					if e := addAggregateValue(&a, f, NewBlobValue(input)); e != nil {
						t.Fatal(e)
					}
				}
			}
			if a.count != 5 {
				t.Fatalf("large retained keys changed: count=%d", a.count)
			}
			// Rewriting caller bytes and many equal-length identities must not modify
			// existing map entries or diskHash's retained last-key string.
			var last string
			if a.distinctDisk != nil {
				last = strings.Clone(a.distinctDisk.lastKey)
			}
			for n := 0; n < 100; n++ {
				other := aggregateState{}
				if e := addAggregateValue(&other, f, NewIntValue(int64(n))); e != nil {
					t.Fatal(e)
				}
			}
			input[0] = 255
			if a.distinctDisk != nil && a.distinctDisk.lastKey != last {
				t.Fatal("retained disk identity aliases scratch")
			}
			input[0] = 1
			if e := addAggregateValue(&a, f, NewBlobValue(input)); e != nil {
				t.Fatal(e)
			}
			if a.count != 5 {
				t.Fatal("caller mutation changed retained key")
			}
			s.clearSelectCaches()
			entries, e := os.ReadDir(s.queryTempDir)
			if e != nil || len(entries) != 0 {
				t.Fatalf("spill cleanup files=%v err=%v", entries, e)
			}
		})
	}
}

func groupIdentityFixture(tb testing.TB, large bool) (*Schema, [][]byte) {
	tb.Helper()
	s, lsm := newAccessReuseSchema(tb, 0)
	accessReuseSQL(tb, s, "CREATE TABLE identity_facts(id INTEGER PRIMARY KEY,k BLOB,n REAL);CREATE TABLE identity_dim(id INTEGER PRIMARY KEY,flag INTEGER);INSERT INTO identity_dim VALUES(1,1)")
	keys := [][]byte{nil, {}, bytes.Repeat([]byte{1, 0, 255}, 1500), bytes.Repeat([]byte{2, 0, 255}, 2500), {0}}
	if large {
		keys[3] = bytes.Repeat([]byte{2, 0, 255}, 30000)
	}
	var rows []string
	for id := 1; id <= 100; id++ {
		k := "NULL"
		if key := keys[(id-1)%len(keys)]; key != nil {
			k = "X'" + hex.EncodeToString(key) + "'"
		}
		rows = append(rows, fmt.Sprintf("(%d,%s,%d)", id, k, id%4))
	}
	accessReuseSQL(tb, s, "INSERT INTO identity_facts VALUES "+strings.Join(rows, ","))
	if e := lsm.CompactRange(); e != nil {
		tb.Fatal(e)
	}
	return s, keys
}

func TestGenericGroupedIdentityOwnedWideKeysAndSpill(t *testing.T) {
	s, keys := groupIdentityFixture(t, true)
	query := "SELECT f.k AS k,COUNT(DISTINCT f.n) AS dn,COUNT(*) AS n,MIN(f.id) AS first_id FROM identity_facts f JOIN identity_dim d ON d.id=1 WHERE d.flag=1 GROUP BY f.k ORDER BY first_id"
	var retained []*ResultSet
	for _, budget := range []int64{8 << 20, 1024} {
		s.queryMemoryBytes = budget
		s.queryTempDir = t.TempDir()
		r := accessReuseSQL(t, s, query)
		retained = append(retained, r)
		if len(r.Rows) != len(keys) {
			t.Fatalf("budget%d groups=%d", budget, len(r.Rows))
		}
		for i, row := range r.Rows {
			if row["first_id"].Int != int64(i+1) || row["n"].Int != 20 || row["dn"].Int != 4 {
				t.Fatalf("budget%d group%d: %#v", budget, i, row)
			}
			k := row["k"]
			if k.Null != (keys[i] == nil) || !bytes.Equal(k.Blob, keys[i]) {
				t.Fatalf("budget%d group%d key ownership/type", budget, i)
			}
		}
		if len(s.queryDisks) != 0 {
			t.Fatal("aggregate retained spill engines")
		}
		entries, e := os.ReadDir(s.queryTempDir)
		if e != nil || len(entries) != 0 {
			t.Fatalf("spill artifacts=%v err=%v", entries, e)
		}
	}
	// Original results stay owned when subsequent grouping reuses key/decoder
	// buffers, including numeric identities shared by integer-valued REALs.
	r := accessReuseSQL(t, s, "SELECT f.n AS n,COUNT(*) AS c FROM identity_facts f JOIN identity_dim d ON d.id=1 GROUP BY f.n ORDER BY n")
	if len(r.Rows) != 4 {
		t.Fatalf("numeric groups=%d", len(r.Rows))
	}
	for i, row := range r.Rows {
		if row["n"].Real != float64(i) || row["c"].Int != 25 {
			t.Fatalf("numeric group%d: %#v", i, row)
		}
	}
	for _, held := range retained {
		for i, row := range held.Rows {
			if !bytes.Equal(row["k"].Blob, keys[i]) || row["first_id"].Int != int64(i+1) {
				t.Fatal("retained grouped result changed")
			}
		}
	}
}

func BenchmarkAggregateDistinctDuplicateIdentity(b *testing.B) {
	for _, owner := range []bool{false, true} {
		for _, kind := range []string{"integer", "blob", "wide_blob"} {
			b.Run(fmt.Sprintf("owner=%v/%s", owner, kind), func(b *testing.B) {
				s := &Schema{queryMemoryBytes: 8 << 20, queryTempDir: b.TempDir()}
				a := aggregateState{}
				if owner {
					a.owner = s
				}
				f := &FuncCallExpr{Name: "COUNT", Distinct: true}
				values := make([]Value, 64)
				for i := range values {
					switch kind {
					case "integer":
						values[i] = NewIntValue(int64(i))
					case "blob":
						values[i] = NewBlobValue([]byte{byte(i), 0, 255})
					default:
						values[i] = NewBlobValue(bytes.Repeat([]byte{byte(i)}, 4096))
					}
					if e := addAggregateValue(&a, f, values[i]); e != nil {
						b.Fatal(e)
					}
				}
				b.ReportAllocs()
				b.ResetTimer()
				for n := 0; n < b.N; n++ {
					if e := addAggregateValue(&a, f, values[n%len(values)]); e != nil {
						b.Fatal(e)
					}
				}
				b.StopTimer()
				if a.count != 64 {
					b.Fatalf("distinct=%d", a.count)
				}
			})
		}
	}
}

func BenchmarkGenericJoinedWideGroupIdentity(b *testing.B) {
	s, _ := groupIdentityFixture(b, false)
	query := "SELECT f.k,COUNT(*) FROM identity_facts f JOIN identity_dim d ON d.id=1 GROUP BY f.k"
	statements, e := Parse(query)
	if e != nil {
		b.Fatal(e)
	}
	stmt := statements[0].(*SelectStmt)
	b.ReportAllocs()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		if e = s.StartStatement(); e != nil {
			b.Fatal(e)
		}
		r, e := s.Select(stmt)
		s.EndStatement()
		if e != nil || len(r.Rows) != 5 {
			b.Fatalf("groups=%v err=%v", r, e)
		}
	}
}

func TestGenericOrderedWideGroupKeysRemainOwned(t *testing.T) {
	s, lsm := newAccessReuseSchema(t, 0)
	accessReuseSQL(t, s, "CREATE TABLE identity_ordered(k BLOB,id INTEGER,n INTEGER,PRIMARY KEY(k,id))")
	keys := [][]byte{{}, bytes.Repeat([]byte{1, 0, 255}, 1500), bytes.Repeat([]byte{2, 0, 255}, 2500)}
	var input []string
	for _, key := range keys {
		for id := 1; id <= 12; id++ {
			input = append(input, fmt.Sprintf("(X'%x',%d,1)", key, id))
		}
	}
	accessReuseSQL(t, s, "INSERT INTO identity_ordered VALUES "+strings.Join(input, ","))
	if e := lsm.CompactRange(); e != nil {
		t.Fatal(e)
	}
	query := "SELECT k,SUM(CAST(n AS DECIMAL(10,2))) AS total,COUNT(DISTINCT id%4) AS dn FROM identity_ordered GROUP BY k ORDER BY k"
	for _, budget := range []int64{8 << 20, 1024} {
		s.queryMemoryBytes = budget
		s.queryTempDir = t.TempDir()
		r := accessReuseSQL(t, s, query)
		if len(r.Rows) != 3 {
			t.Fatalf("groups=%d", len(r.Rows))
		}
		for i, row := range r.Rows {
			if row["k"].Null || !bytes.Equal(row["k"].Blob, keys[i]) || s.valueCompare(row["total"], NewIntValue(12)) != 0 || row["dn"].Int != 4 {
				t.Fatalf("budget%d ordered group%d changed", budget, i)
			}
		}
		if len(s.queryDisks) != 0 {
			t.Fatal("ordered group retained spill engines")
		}
	}
}
