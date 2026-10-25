package db

import (
	"bytes"
	"context"
	"reflect"
	"testing"

	"slopdb/sql"
)

func TestRowsValuesIntoReusesOwnedStorageAndPreservesNulls(t *testing.T) {
	d, _ := bulkDatabase(t)
	if _, err := d.Exec("CREATE TABLE value_rows(id BIGINT PRIMARY KEY,a BLOB,b BLOB,n TEXT);INSERT INTO value_rows VALUES(1,X'01020304',X'99',NULL),(2,X'0506',X'','text'),(3,NULL,X'00',NULL),(4,X'',X'',NULL)"); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		query        string
		materialized bool
	}{
		{"SELECT id,a,b,a,n FROM value_rows ORDER BY id", false},
		{"SELECT id,a,b,a,n FROM value_rows ORDER BY id+0", true},
	} {
		t.Run(tc.query, func(t *testing.T) {
			rows, err := d.Query(tc.query)
			if err != nil {
				t.Fatal(err)
			}
			defer rows.Close()
			if rows.Materialized() != tc.materialized {
				t.Fatalf("materialized=%v", rows.Materialized())
			}
			if _, err = rows.ValuesInto(nil); err == nil {
				t.Fatal("ValuesInto before Next succeeded")
			}
			dst := make([]sql.Value, 8)
			for i := range dst {
				dst[i] = sql.NewBlobValue(make([]byte, 1, 16))
			}
			firstSlot, firstBlob := &dst[0], &dst[1].Blob[0]
			var retained []sql.Value
			for id := int64(1); id <= 4; id++ {
				if !rows.Next() {
					t.Fatalf("missing row %d: %v", id, rows.Err())
				}
				if id == 1 {
					retained, err = rows.Values()
					if err != nil {
						t.Fatal(err)
					}
				}
				dst, err = rows.ValuesInto(dst)
				if err != nil || len(dst) != 5 || &dst[0] != firstSlot || dst[0].Int != id {
					t.Fatalf("row %d failed/reallocated: %+v %v", id, dst, err)
				}
				switch id {
				case 1:
					if !bytes.Equal(dst[1].Blob, []byte{1, 2, 3, 4}) || !dst[4].Null {
						t.Fatal(dst)
					}
					if &dst[1].Blob[0] != firstBlob {
						t.Fatal("preallocated BLOB not reused")
					}
					dst[1].Blob[0] = 77
					if dst[3].Blob[0] != 1 || retained[1].Blob[0] != 1 {
						t.Fatal("duplicate or independent Values result aliases reused BLOB")
					}
					independent, e := rows.Values()
					if e != nil || independent[1].Blob[0] != 1 {
						t.Fatal("caller changed cursor source row", independent, e)
					}
				case 2:
					if !bytes.Equal(dst[1].Blob, []byte{5, 6}) || &dst[1].Blob[0] != firstBlob || dst[2].Null || dst[2].Blob == nil || len(dst[2].Blob) != 0 || dst[4].Null || dst[4].Text != "text" {
						t.Fatal("smaller BLOB/empty BLOB/text changed", dst)
					}
				case 3:
					if !dst[1].Null || dst[1].Blob != nil || !dst[3].Null || !dst[4].Null {
						t.Fatal("NULL retained old BLOB/text", dst)
					}
				case 4:
					if dst[1].Null || dst[1].Type != sql.ColBlob || dst[1].Blob == nil || len(dst[1].Blob) != 0 || dst[3].Null || dst[3].Blob == nil {
						t.Fatal("non-NULL empty BLOB became NULL", dst)
					}
				}
			}
			if rows.Next() || rows.Err() != nil {
				t.Fatal("extra row or failure", rows.Err())
			}
			if _, err := rows.ValuesInto(dst); err == nil {
				t.Fatal("ValuesInto after exhaustion succeeded")
			}
			if !bytes.Equal(retained[1].Blob, []byte{1, 2, 3, 4}) {
				t.Fatal("later reuse changed retained Values result")
			}
			if dst[:cap(dst)][7].Blob != nil {
				t.Fatal("shrinking destination retained unused BLOB")
			}
		})
	}
	result, err := d.Exec("SELECT a FROM value_rows WHERE id=1")
	if err != nil || !bytes.Equal(result.Rows[0]["a"].Blob, []byte{1, 2, 3, 4}) {
		t.Fatal("caller changed stored BLOB", result, err)
	}
}

func TestRowsCatalogProjectionMetadataAcrossDialects(t *testing.T) {
	for _, dialect := range []sql.Dialect{sql.DialectMySQL, sql.DialectPostgreSQL} {
		t.Run(dialect.String(), func(t *testing.T) {
			d, _ := bulkDatabase(t)
			s, err := d.NewSession()
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			if err = s.SetDialect(dialect); err != nil {
				t.Fatal(err)
			}
			bulkExec(t, s, "CREATE TABLE projected(ID BIGINT PRIMARY KEY,Label VARCHAR(12) NOT NULL,Amount DECIMAL(20,4),Data VARBINARY(8),Flag BOOLEAN,Body JSON)")
			for _, query := range []string{
				"SELECT * FROM projected",
				"SELECT id,label,amount,data,flag,body FROM projected WHERE id=99",
				"SELECT id,label AS label_again,amount,data,label AS duplicate,label AS duplicate FROM projected WHERE id IN(1,2)",
				"SELECT p.id,p.label,p.amount,p.data FROM projected AS p",
			} {
				stmt, err := s.Prepare(query)
				if err != nil {
					t.Fatal(query, err)
				}
				expected, err := s.DescribePrepared(context.Background(), stmt)
				stmt.Close()
				if err != nil {
					t.Fatal(query, err)
				}
				rows, err := s.Query(query)
				if err != nil {
					t.Fatal(query, err)
				}
				if rows.Materialized() || rows.cursor == nil || !rows.cursor.ColumnsComplete() {
					t.Fatalf("expected complete catalog projection: %s", query)
				}
				actual := rows.Columns()
				rows.Close()
				for i := range expected {
					expected[i].SemanticType = expected[i].LogicalType().Kind
				}
				if !reflect.DeepEqual(actual, expected) {
					t.Fatalf("%s: declaration metadata changed\nactual: %+v\nexpected: %+v", query, actual, expected)
				}
			}
			// Complex metadata must still take Describe's validation path, and
			// rejected queries must release their gate for following calls.
			for _, query := range []string{"SELECT absent FROM projected", "SELECT label FROM projected WHERE missing=1", "SELECT id FROM projected WHERE CAST(id AS nonexistent_type)=1"} {
				rows, err := s.Query(query)
				if err == nil {
					rows.Close()
					t.Fatalf("invalid metadata accepted: %s", query)
				}
			}
			rows, err := s.Query("SELECT label||'' AS result FROM projected WHERE ABS(id)=1")
			if err != nil {
				t.Fatal(err)
			}
			rows.Close()
		})
	}
}

func TestRowsCatalogProjectionMetadataOwnsMutableDeclarations(t *testing.T) {
	d, _ := bulkDatabase(t)
	s, err := d.NewSession()
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err = s.SetDialect(sql.DialectMySQL); err != nil {
		t.Fatal(err)
	}
	bulkExec(t, s, "CREATE TABLE parents(id BIGINT PRIMARY KEY);CREATE TABLE owned_columns(ID BIGINT CONSTRAINT id_pk PRIMARY KEY,StatusValue ENUM('first','second'),RefID BIGINT REFERENCES parents(id))")
	for pass := 0; pass < 2; pass++ {
		rows, err := s.Query("SELECT * FROM owned_columns")
		if err != nil {
			t.Fatal(err)
		}
		columns := rows.Columns()
		if len(columns) != 3 || columns[0].ConstraintNames["PRIMARY"] != "id_pk" || !reflect.DeepEqual(columns[1].EnumValues, []string{"first", "second"}) || !reflect.DeepEqual(columns[2].RefColumns, []string{"id"}) {
			t.Fatal("catalog metadata was changed", columns)
		}
		columns[0].ConstraintNames["PRIMARY"] = "caller_changed"
		columns[1].EnumValues[0] = "caller_changed"
		columns[2].RefColumns[0] = "caller_changed"
		again := rows.Columns()
		rows.Close()
		if again[0].ConstraintNames["PRIMARY"] != "id_pk" || again[1].EnumValues[0] != "first" || again[2].RefColumns[0] != "id" {
			t.Fatal("column metadata aliases another Columns call", again)
		}
	}
}
