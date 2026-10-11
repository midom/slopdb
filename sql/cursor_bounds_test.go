package sql

import (
	"fmt"
	"reflect"
	"strings"
	"testing"
)

func TestCursorClusteredIntervals(t *testing.T) {
	s, _ := statisticsSchema(t, t.TempDir())
	statisticsSQL(t, s, "CREATE TABLE t(id INTEGER PRIMARY KEY)")
	var input strings.Builder
	input.WriteString("INSERT INTO t VALUES")
	for i := 0; i < 1000; i++ {
		if i > 0 {
			input.WriteByte(',')
		}
		fmt.Fprintf(&input, "(%d)", i)
	}
	statisticsSQL(t, s, input.String())
	for _, tc := range []struct {
		query    string
		want     []int64
		examined int64
	}{
		{"SELECT id FROM t WHERE id IN(901,903,905) ORDER BY id DESC", []int64{905, 903, 901}, 3},
		{"SELECT id FROM t WHERE id BETWEEN 901 AND 903 OR id BETWEEN 903 AND 905 ORDER BY id", []int64{901, 902, 903, 904, 905}, 5},
		{"SELECT id FROM t WHERE id BETWEEN 905 AND 901", nil, 0},
		{"SELECT id FROM t WHERE id IN(901,903) LIMIT 4294967296 OFFSET 4294967296", nil, 2},
		{"SELECT id FROM t AS x WHERE x.id BETWEEN 901+0 AND 903 ORDER BY x.id DESC", []int64{903, 902, 901}, 3},
	} {
		t.Run(tc.query, func(t *testing.T) {
			statements, err := Parse(tc.query)
			if err != nil {
				t.Fatal(err)
			}
			if err := s.StartStatement(); err != nil {
				t.Fatal(err)
			}
			defer s.EndStatement()
			p := s.StartQueryProfile()
			cursor, handled, err := s.OpenRowCursor(statements[0].(*SelectStmt))
			if err != nil || !handled {
				t.Fatalf("streaming cursor not available: %v", err)
			}
			defer cursor.Close()
			var got []int64
			for {
				row, found, err := cursor.Next()
				if err != nil {
					t.Fatal(err)
				}
				if !found {
					break
				}
				got = append(got, row["id"].Int)
			}
			s.StopQueryProfile()
			if !reflect.DeepEqual(got, tc.want) || p.RowsExamined != tc.examined {
				t.Fatalf("rows=%v, reads=%d; expected %v with %d reads", got, p.RowsExamined, tc.want, tc.examined)
			}
		})
	}
}
