package sql

import (
	"reflect"
	"testing"
)

func TestCursorCompleteCatalogMetadataCapability(t *testing.T) {
	s, _ := statisticsSchema(t, t.TempDir())
	statisticsSQL(t, s, "CREATE TABLE cursor_metadata(id BIGINT PRIMARY KEY,Label VARCHAR(12) NOT NULL,Amount DECIMAL(20,4),b VARBINARY(8));INSERT INTO cursor_metadata VALUES(1,'label',123.4500,X'0001')")
	for _, tc := range []struct {
		query    string
		complete bool
	}{
		{"SELECT * FROM cursor_metadata", true},
		{"SELECT id,Label,id AS again,Label FROM cursor_metadata WHERE id=1", true},
		{"SELECT m.id,m.Label FROM cursor_metadata m WHERE id IN(1,2)", true},
		{"SELECT Label FROM cursor_metadata WHERE id BETWEEN 1 AND 2", true},
		{"SELECT Label FROM cursor_metadata WHERE id IS NOT NULL", true},
		{"SELECT Label FROM cursor_metadata WHERE ABS(id)=1", false},
		{"SELECT Label||'' FROM cursor_metadata", false},
		{"SELECT Label FROM cursor_metadata WHERE cursor_metadata.id=1", false},
		{"SELECT id FROM cursor_metadata WHERE id=CAST('1' AS INTEGER)", false},
	} {
		t.Run(tc.query, func(t *testing.T) {
			statements, err := Parse(tc.query)
			if err != nil {
				t.Fatal(err)
			}
			stmt := statements[0].(*SelectStmt)
			if err := s.StartStatement(); err != nil {
				t.Fatal(err)
			}
			defer s.EndStatement()
			cursor, handled, err := s.OpenRowCursor(stmt)
			if err != nil || !handled {
				t.Fatalf("cursor unavailable: %v", err)
			}
			defer cursor.Close()
			if got := cursor.ColumnsComplete(); got != tc.complete {
				t.Fatalf("complete metadata=%v, want %v", got, tc.complete)
			}
			if !tc.complete {
				return
			}
			want, err := s.Describe(stmt)
			if err != nil {
				t.Fatal(err)
			}
			got := cursor.Columns()
			for i := range want {
				// The cursor eagerly resolves the logical type; Describe may
				// carry the same type through its physical declaration instead.
				want[i].SemanticType = want[i].LogicalType().Kind
			}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("catalog metadata differs from full Describe:\ngot: %+v\nwant: %+v", got, want)
			}
		})
	}
}
