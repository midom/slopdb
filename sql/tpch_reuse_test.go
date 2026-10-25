package sql

import (
	"bytes"
	"fmt"
	"strings"
	"testing"

	"slopdb/storage"
)

// These small, deterministic TPC-H-shaped fixtures are not DBGEN data or TPC
// scores. Expected answers are accumulated from the fixture's defining formula,
// independently of SQL execution.
func tpchReuseSchema(tb testing.TB, rows int) *Schema {
	tb.Helper()
	options := storage.DefaultOptions()
	options.DisableWAL = true
	tree, err := storage.NewLSMTree(tb.TempDir(), options)
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() {
		if err := tree.Close(); err != nil {
			tb.Error(err)
		}
	})
	s, err := NewSchema(tree, 4096)
	if err != nil {
		tb.Fatal(err)
	}
	s.queryTempDir = tb.TempDir()
	tpchReuseSQL(tb, s, "CREATE TABLE lineitem(id INTEGER PRIMARY KEY,flag TEXT,status TEXT,quantity INTEGER,price INTEGER,discount INTEGER,shipdate TEXT,payload BLOB)")
	for start := 0; start < rows; start += 256 {
		var values []string
		for i := start; i < min(start+256, rows); i++ {
			values = append(values, fmt.Sprintf("(%d,'%c','%c',%d,%d,%d,'%s',X'%04x')", i, 'A'+rune(i%3), 'F'+rune(i%2), i%50+1, i*7+11, i%3, []string{"1994-06-01", "1996-06-01"}[i%2], i))
		}
		tpchReuseSQL(tb, s, "INSERT INTO lineitem VALUES "+strings.Join(values, ","))
	}
	return s
}

func tpchReuseSQL(tb testing.TB, s *Schema, text string) *ResultSet {
	tb.Helper()
	statements, err := Parse(text)
	if err != nil {
		tb.Fatal(err)
	}
	var result *ResultSet
	for _, statement := range statements {
		if err = s.StartStatement(); err != nil {
			tb.Fatal(err)
		}
		err = s.ExecuteStatement(func() error {
			switch stmt := statement.(type) {
			case *CreateTableStmt:
				return s.CreateTable(stmt)
			case *InsertStmt:
				_, _, e := s.Insert(stmt)
				return e
			case *SelectStmt:
				var e error
				result, e = s.Select(stmt)
				return e
			default:
				return fmt.Errorf("unsupported fixture statement %T", statement)
			}
		})
		s.EndStatement()
		if err != nil {
			tb.Fatalf("%s: %v", text, err)
		}
	}
	return result
}

func TestTypedAggregateBatchOwnership(t *testing.T) {
	const count = 1031 // multiple complete batches plus a partial batch
	s := tpchReuseSchema(t, count)
	for _, budget := range []int64{64 << 20, 128} {
		t.Run(fmt.Sprint(budget), func(t *testing.T) {
			s.queryMemoryBytes = budget
			result := tpchReuseSQL(t, s, "SELECT flag,status,COUNT(*) AS n,SUM(quantity) AS q,MIN(payload) AS lo,MAX(payload) AS hi FROM lineitem GROUP BY flag,status ORDER BY flag,status")
			if len(result.Rows) != 6 {
				t.Fatalf("group count %d", len(result.Rows))
			}
			for _, row := range result.Rows {
				flag, status := row["flag"].Text, row["status"].Text
				var n, q int64
				var lo, hi []byte
				for i := 0; i < count; i++ {
					if flag != string(rune('A'+i%3)) || status != string(rune('F'+i%2)) {
						continue
					}
					v := []byte{byte(i >> 8), byte(i)}
					n++
					q += int64(i%50 + 1)
					if lo == nil {
						lo = append([]byte(nil), v...)
					}
					hi = append(hi[:0], v...)
				}
				if row["n"].Int != n || row["q"].Int != q || !bytes.Equal(row["lo"].Blob, lo) || !bytes.Equal(row["hi"].Blob, hi) {
					t.Fatalf("group %s/%s: got %#v; want count %d sum %d min %x max %x", flag, status, row, n, q, lo, hi)
				}
			}
			// Keep the old results alive while the next statement reuses its own scratch.
			before := append([]byte(nil), result.Rows[0]["lo"].Blob...)
			tpchReuseSQL(t, s, "SELECT MAX(payload) AS hi FROM lineitem WHERE id>500")
			if !bytes.Equal(before, result.Rows[0]["lo"].Blob) {
				t.Fatal("later statement mutated a returned BLOB")
			}
		})
	}
	s.queryMemoryBytes = 64 << 20
	result := tpchReuseSQL(t, s, "SELECT payload,COUNT(*) AS n FROM lineitem GROUP BY payload ORDER BY payload")
	if len(result.Rows) != count {
		t.Fatalf("BLOB group count %d", len(result.Rows))
	}
	for i, row := range result.Rows {
		if !bytes.Equal(row["payload"].Blob, []byte{byte(i >> 8), byte(i)}) || row["n"].Int != 1 {
			t.Fatalf("BLOB representative %d: %#v", i, row)
		}
	}
}

func TestTypedBatchScratchLifetime(t *testing.T) {
	row := make([]Value, 1)
	decodeTypedRow(encodeValue(NewBlobValue([]byte{1, 2, 3})), []int{0}, row)
	buffer := &row[0].Blob[0]
	resetTypedBatch(row)
	decodeTypedRow(encodeValue(NewBlobValue([]byte{4, 5, 6})), []int{0}, row)
	if buffer != &row[0].Blob[0] || !bytes.Equal(row[0].Blob, []byte{4, 5, 6}) {
		t.Fatal("private batch did not reuse its BLOB buffer")
	}
	decodeTypedRow(encodeValue(NewBlobValue([]byte{})), []int{0}, row)
	if row[0].Null || row[0].Type != ColBlob || row[0].Blob == nil || len(row[0].Blob) != 0 {
		t.Fatal("empty BLOB changed to NULL")
	}
	decodeTypedRow(nil, []int{0}, row)
	if !row[0].Null || row[0].Blob != nil {
		t.Fatal("missing column retained old BLOB")
	}
	row[0] = NewBlobValue(make([]byte, typedScratchLimit+1))
	resetTypedBatch(row)
	if row[0].Blob != nil {
		t.Fatal("oversized input pinned in batch scratch")
	}
}

func TestTypedOrderedBlobRepresentativeOwnership(t *testing.T) {
	s := tpchReuseSchema(t, 0)
	tpchReuseSQL(t, s, "CREATE TABLE blobgroups(g BLOB,id INTEGER,v BLOB,PRIMARY KEY(g,id))")
	for group := 0; group < 4; group++ {
		var values []string
		for id := 0; id < 301; id++ {
			values = append(values, fmt.Sprintf("(X'%04x',%d,X'%04x')", group, id, id))
		}
		tpchReuseSQL(t, s, "INSERT INTO blobgroups VALUES "+strings.Join(values, ","))
	}
	result := tpchReuseSQL(t, s, "SELECT g,MIN(v) AS lo,MAX(v) AS hi,COUNT(*) AS n FROM blobgroups GROUP BY g ORDER BY g")
	if len(result.Rows) != 4 {
		t.Fatalf("groups %d", len(result.Rows))
	}
	for group, row := range result.Rows {
		if !bytes.Equal(row["g"].Blob, []byte{0, byte(group)}) || !bytes.Equal(row["lo"].Blob, []byte{0, 0}) || !bytes.Equal(row["hi"].Blob, []byte{1, 44}) || row["n"].Int != 301 {
			t.Fatalf("ordered group %d: %#v", group, row)
		}
	}
}

func TestJoinCandidateScratchOwnership(t *testing.T) {
	left := map[string]Value{"id": NewIntValue(1), "left.id": NewIntValue(1)}
	right := map[string]Value{"id": NewIntValue(2), "payload": NewBlobValue([]byte{1, 2})}
	names := make(map[string]string)
	rejected := combineJoinRowInto(nil, left, right, "right", names)
	delete(right, "payload")
	right["id"] = NewIntValue(3)
	accepted := combineJoinRowInto(rejected, left, right, "right", names)
	if _, exists := accepted["payload"]; exists {
		t.Fatal("rejected candidate columns survived reuse")
	}
	if accepted["id"].Int != 1 || accepted["right.id"].Int != 3 {
		t.Fatal("join name precedence changed")
	}
	// Once accepted, the map must be detached from the next scratch candidate.
	right["id"] = NewIntValue(4)
	next := combineJoinRowInto(nil, left, right, "right", names)
	next["left.id"] = NewIntValue(9)
	if accepted["right.id"].Int != 3 || accepted["left.id"].Int != 1 {
		t.Fatal("later candidate changed retained result")
	}
	for _, v := range []Value{NewIntValue(17), NewRealValue(17), NewNullValue(), NewTextValue("a\x00b"), NewBlobValue([]byte{0, 1, 2})} {
		s := &Schema{}
		row := map[string]Value{"key": v}
		pairs := []joinKeyPair{{"key", "key"}}
		owned, valid := s.joinRowKey(row, pairs, false)
		key, ok := s.appendJoinRowKey(make([]byte, 0, 128), row, pairs, false)
		if valid != ok || owned != string(key) {
			t.Fatalf("probe identity changed for %#v", v)
		}
	}
}

func TestTPCHJoinReuseAnswers(t *testing.T) {
	const rows = 1031
	s := tpchReuseSchema(t, rows)
	tpchReuseSQL(t, s, "CREATE TABLE dimension(id INTEGER PRIMARY KEY,band TEXT)")
	var values []string
	for i := 1; i <= 50; i++ {
		values = append(values, fmt.Sprintf("(%d,'%c')", i, 'A'+rune(i%3)))
	}
	tpchReuseSQL(t, s, "INSERT INTO dimension VALUES "+strings.Join(values, ","))
	var counts, revenues [3]int64
	for i := 0; i < rows; i++ {
		j := (i%50 + 1) % 3
		counts[j]++
		revenues[j] += int64(i*7 + 11)
	}
	for _, dialect := range []Dialect{DialectNative, DialectMySQL, DialectPostgreSQL} {
		if err := s.SetDialect(dialect); err != nil {
			t.Fatal(err)
		}
		result := tpchReuseSQL(t, s, "SELECT d.band AS band,COUNT(*) AS n,SUM(l.price) AS revenue FROM lineitem l JOIN dimension d ON l.quantity=d.id GROUP BY d.band ORDER BY d.band")
		if len(result.Rows) != 3 {
			t.Fatalf("dialect %v: group count %d", dialect, len(result.Rows))
		}
		for j, row := range result.Rows {
			if row["band"].Text != string(rune('A'+j)) || row["n"].Int != counts[j] || row["revenue"].Int != revenues[j] {
				t.Fatalf("dialect %v: unexpected joined aggregate %#v", dialect, row)
			}
		}
	}
}

func BenchmarkTPCHAggregateReuse(b *testing.B) {
	const rows = 8192
	s := tpchReuseSchema(b, rows)
	type expectedGroup struct {
		n, quantity, revenue int64
		lo, hi               []byte
	}
	expected := make(map[[2]string]expectedGroup)
	var q6Revenue int64
	for i := 0; i < rows; i++ {
		key := [2]string{string(rune('A' + i%3)), string(rune('F' + i%2))}
		want := expected[key]
		want.n++
		want.quantity += int64(i%50 + 1)
		want.revenue += int64((i*7 + 11) * (3 - i%3))
		v := []byte{byte(i >> 8), byte(i)}
		if want.lo == nil {
			want.lo = append([]byte(nil), v...)
		}
		want.hi = append(want.hi[:0], v...)
		expected[key] = want
		if i%2 == 0 && i%3 >= 1 && i%50+1 < 24 {
			q6Revenue += int64((i*7 + 11) * (i % 3))
		}
	}
	for _, tc := range []struct {
		name, query string
		groups      int
	}{
		{"Q1", "SELECT flag,status,COUNT(*) AS n,SUM(quantity) AS q,SUM(price*(3-discount)) AS revenue FROM lineitem GROUP BY flag,status", 6},
		{"Q6", "SELECT SUM(price*discount) AS revenue FROM lineitem WHERE shipdate>='1994-01-01' AND shipdate<'1995-01-01' AND discount BETWEEN 1 AND 2 AND quantity<24", 1},
		{"BlobExtrema", "SELECT flag,status,MIN(payload) AS lo,MAX(payload) AS hi FROM lineitem GROUP BY flag,status", 6},
	} {
		b.Run(tc.name, func(b *testing.B) {
			stmt := func() *SelectStmt {
				ss, e := Parse(tc.query)
				if e != nil {
					b.Fatal(e)
				}
				return ss[0].(*SelectStmt)
			}()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := s.StartStatement(); err != nil {
					b.Fatal(err)
				}
				var result *ResultSet
				err := s.ExecuteStatement(func() error { var e error; result, e = s.Select(stmt); return e })
				s.EndStatement()
				if err != nil || len(result.Rows) != tc.groups {
					b.Fatalf("result %v, error %v", result, err)
				}
				for _, row := range result.Rows {
					key := [2]string{row["flag"].Text, row["status"].Text}
					want := expected[key]
					if tc.name == "Q6" {
						want.revenue = q6Revenue
					}
					if tc.name == "Q1" && (row["n"].Int != want.n || row["q"].Int != want.quantity || row["revenue"].Int != want.revenue) || tc.name == "Q6" && row["revenue"].Int != want.revenue || tc.name == "BlobExtrema" && (!bytes.Equal(row["lo"].Blob, want.lo) || !bytes.Equal(row["hi"].Blob, want.hi)) {
						b.Fatalf("incorrect %s result %#v", tc.name, row)
					}
				}
			}
		})
	}
}

func BenchmarkJoinCandidateReuse(b *testing.B) {
	s := tpchReuseSchema(b, 0)
	tpchReuseSQL(b, s, "CREATE TABLE dimension(id INTEGER PRIMARY KEY,k INTEGER,cap INTEGER)")
	var values []string
	for i := 0; i < 128; i++ {
		values = append(values, fmt.Sprintf("(%d,%d,%d)", i, i%8, i%7))
	}
	tpchReuseSQL(b, s, "INSERT INTO dimension VALUES "+strings.Join(values, ","))
	for _, reject := range []bool{false, true} {
		b.Run(fmt.Sprint("reject=", reject), func(b *testing.B) {
			stmt := statisticsSelectForBenchmark(b, "SELECT l.id,d.id FROM lineitem l JOIN dimension d ON l.k=d.k AND l.quantity<d.cap")
			rows := make([]map[string]Value, 128)
			var want int
			for i := range rows {
				quantity := i % 7
				if reject {
					quantity = 100
				}
				rows[i] = map[string]Value{"id": NewIntValue(int64(i)), "l.id": NewIntValue(int64(i)), "l.k": NewIntValue(int64(i % 8)), "l.quantity": NewIntValue(int64(quantity))}
				for j := 0; j < 128; j++ {
					if i%8 == j%8 && quantity < j%7 {
						want++
					}
				}
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := s.StartStatement(); err != nil {
					b.Fatal(err)
				}
				var out []map[string]Value
				err := s.ExecuteStatement(func() error { var e error; out, _, e = s.executeJoins(stmt, rows, nil); return e })
				s.EndStatement()
				if err != nil || len(out) != want {
					b.Fatalf("rows %d, want %d, error %v", len(out), want, err)
				}
			}
		})
	}
}

func BenchmarkTPCHJoinReuse(b *testing.B) {
	s := tpchReuseSchema(b, 8192)
	tpchReuseSQL(b, s, "CREATE TABLE dimension(id INTEGER PRIMARY KEY,band TEXT)")
	var values []string
	for i := 1; i <= 50; i++ {
		values = append(values, fmt.Sprintf("(%d,'%c')", i, 'A'+rune(i%3)))
	}
	tpchReuseSQL(b, s, "INSERT INTO dimension VALUES "+strings.Join(values, ","))
	stmt := statisticsSelectForBenchmark(b, "SELECT d.band AS band,COUNT(*) AS n,SUM(l.price) AS revenue FROM lineitem l JOIN dimension d ON l.quantity=d.id GROUP BY d.band")
	var counts, revenues [3]int64
	for i := 0; i < 8192; i++ {
		band := (i%50 + 1) % 3
		counts[band]++
		revenues[band] += int64(i*7 + 11)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := s.StartStatement(); err != nil {
			b.Fatal(err)
		}
		var result *ResultSet
		err := s.ExecuteStatement(func() error { var e error; result, e = s.Select(stmt); return e })
		s.EndStatement()
		if err != nil || len(result.Rows) != 3 {
			b.Fatalf("rows %v, error %v", result, err)
		}
		for _, row := range result.Rows {
			band := row["band"].Text
			if len(band) != 1 || band[0] < 'A' || band[0] > 'C' {
				b.Fatalf("unexpected group %#v", row)
			}
			j := band[0] - 'A'
			if row["n"].Int != counts[j] || row["revenue"].Int != revenues[j] {
				b.Fatalf("wrong join answer %#v", row)
			}
		}
	}
}

func statisticsSelectForBenchmark(b *testing.B, text string) *SelectStmt {
	b.Helper()
	statements, err := Parse(text)
	if err != nil {
		b.Fatal(err)
	}
	return statements[0].(*SelectStmt)
}
