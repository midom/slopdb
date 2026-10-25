package db

import (
	"bytes"
	"context"
	"reflect"
	"strings"
	"testing"
)

func TestBoundedReadReusesIndependentRowBuffers(t *testing.T) {
	d := namespaceTestDatabase(t)
	s := isolationSession(t, d)
	isolationExec(t, s, "CREATE TABLE reusable(id BIGINT PRIMARY KEY,v BIGINT,b BLOB);INSERT INTO reusable VALUES(1,10,X'0102'),(2,20,X'0304'),(3,30,X'0506'),(4,40,X'0708')")
	var dst ExecResult
	queries := []struct {
		query string
		ids   []int64
	}{
		{"SELECT id,v,b FROM reusable WHERE id IN(3,1,2) ORDER BY id", []int64{1, 2, 3}},
		{"SELECT id,v,b FROM reusable WHERE id>=2 ORDER BY id LIMIT 3", []int64{2, 3, 4}},
		{"SELECT id,v,b FROM reusable WHERE id IN(4,4,NULL,2,1,99) ORDER BY id DESC", []int64{4, 2, 1}},
		{"SELECT id,v,b FROM reusable WHERE id>=1 ORDER BY id LIMIT 0", nil},
		{"SELECT id,v,b FROM reusable WHERE id IN(3,1,2) ORDER BY id", []int64{1, 2, 3}},
	}
	var buffers []*byte
	for _, tc := range queries {
		r, rows, err := s.QueryParsedContextInto(context.Background(), &dst, tc.query, parseReadTest(t, tc.query))
		if err != nil || rows != nil || r != &dst || len(r.Rows) != len(tc.ids) {
			if rows != nil {
				rows.Close()
			}
			t.Fatalf("bounded result not available for %q: %v %v %v", tc.query, r, rows, err)
		}
		for i, id := range tc.ids {
			if r.Rows[i]["id"].Int != id || r.Rows[i]["v"].Int != id*10 || !bytes.Equal(r.Rows[i]["b"].Blob, []byte{byte(id*2 - 1), byte(id * 2)}) {
				t.Fatal(tc.query, r.Rows)
			}
			blob := &r.Rows[i]["b"].Blob[0]
			if len(buffers) <= i {
				buffers = append(buffers, blob)
			} else if buffers[i] != blob {
				t.Fatal("row position did not reuse its own BLOB capacity")
			}
			for j := 0; j < i; j++ {
				if buffers[j] == blob {
					t.Fatal("separate rows share BLOB storage")
				}
			}
		}
	}
	for _, query := range []string{"SELECT missing FROM reusable WHERE id IN(1,2,3) LIMIT 0", "SELECT missing FROM reusable WHERE id>=1 ORDER BY id LIMIT 0"} {
		if _, rows, err := s.QueryParsedContextInto(context.Background(), &dst, query, parseReadTest(t, query)); err == nil {
			if rows != nil {
				rows.Close()
			}
			t.Fatal("empty result skipped column validation", query)
		}
	}
}

func TestBoundedReadLargeValueFallsBackInOriginalSnapshot(t *testing.T) {
	d := namespaceTestDatabase(t)
	s := isolationSession(t, d)
	isolationExec(t, s, "CREATE TABLE oversized_read(id BIGINT PRIMARY KEY,b BLOB);INSERT INTO oversized_read VALUES(1,X'01'),(2,X'"+strings.Repeat("5a", 128<<10)+"'),(3,X'03')")
	for _, query := range []string{"SELECT id,b FROM oversized_read WHERE id IN(1,2,3) ORDER BY id", "SELECT id,b FROM oversized_read WHERE id>=1 ORDER BY id LIMIT 3"} {
		dst := ExecResult{Changes: 123}
		before := dst
		r, rows, err := s.QueryParsedContextInto(context.Background(), &dst, query, parseReadTest(t, query))
		if err != nil || r != nil || rows == nil || !reflect.DeepEqual(dst, before) {
			t.Fatalf("oversized fallback changed destination: %v %v %v %+v", r, rows, err, dst)
		}
		count := 0
		for rows.Next() {
			v, err := rows.Values()
			if err != nil || len(v) != 2 || v[0].Int != int64(count+1) {
				t.Fatal(v, err)
			}
			if count == 1 && !bytes.Equal(v[1].Blob, bytes.Repeat([]byte{0x5a}, 128<<10)) {
				t.Fatal("large value lost in fallback")
			}
			count++
		}
		if rows.Err() != nil || count != 3 {
			t.Fatal(count, rows.Err())
		}
		rows.Close()
	}
}
