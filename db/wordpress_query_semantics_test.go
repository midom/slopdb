package db

import (
	"context"
	"slopdb/sql"
	"testing"
	"time"
)

func TestWordPressPrimaryKeyGroupedJoinOrdering(t *testing.T) {
	d := projectionDatabase(t)
	for _, q := range []string{
		"CREATE TABLE wp_posts(ID INTEGER PRIMARY KEY,post_date TEXT,post_status TEXT)",
		"CREATE TABLE wp_term_relationships(object_id INTEGER,term_taxonomy_id INTEGER)",
		"INSERT INTO wp_posts VALUES(1,'2026-01-01','publish'),(2,'2026-02-01','publish'),(3,'2026-03-01','draft')",
		"INSERT INTO wp_term_relationships VALUES(1,10),(1,11),(2,10),(3,10)",
		"CREATE TABLE multi(a INTEGER,b INTEGER,stamp TEXT,PRIMARY KEY(a,b));INSERT INTO multi VALUES(1,1,'old'),(1,2,'new')",
	} {
		objectExec(t, d, q)
	}
	for _, dialect := range []sql.Dialect{sql.DialectNative, sql.DialectMySQL, sql.DialectPostgreSQL} {
		s, e := d.NewSession()
		if e != nil {
			t.Fatal(e)
		}
		s.SetDialect(dialect)
		for _, prepared := range []bool{false, true} {
			stock := "SELECT p.ID FROM main.wp_posts p LEFT JOIN main.wp_term_relationships t ON p.ID=t.object_id WHERE p.post_status='publish' GROUP BY p.ID ORDER BY p.post_date DESC LIMIT 1"
			if dialect == sql.DialectMySQL {
				stock = "SELECT SQL_CALC_FOUND_ROWS" + stock[len("SELECT"):]
			}
			var stockResult *ExecResult
			if prepared {
				p, err := s.Prepare(stock)
				if err != nil {
					t.Fatal(err)
				}
				stockResult, e = p.Exec()
				p.Close()
			} else {
				stockResult, e = s.Exec(stock)
			}
			if e != nil || len(stockResult.Rows) != 1 || stockResult.Rows[0][stockResult.Columns[0].Name].Int != 2 {
				t.Fatalf("stock WordPress shape %s prepared=%v: %v %v", dialect, prepared, stockResult, e)
			}
			if dialect == sql.DialectMySQL {
				r, err := s.Exec("SELECT FOUND_ROWS() AS n")
				if err != nil || r.Rows[0]["n"].Uint != 2 {
					t.Fatal("stock WordPress group count", r, err)
				}
			}
			query := "SELECT p.ID AS id,p.post_date AS dt,COUNT(t.term_taxonomy_id) AS n FROM main.wp_posts p LEFT JOIN main.wp_term_relationships t ON p.ID=t.object_id WHERE p.post_status='publish' GROUP BY p.ID ORDER BY p.post_date DESC"
			var r *ExecResult
			if prepared {
				p, e := s.Prepare(query)
				if e != nil {
					t.Fatal(e)
				}
				r, e = p.Exec()
				p.Close()
			} else {
				r, e = s.Exec(query)
			}
			if e != nil || len(r.Rows) != 2 || r.Rows[0]["id"].Int != 2 || r.Rows[0]["n"].Int != 1 || r.Rows[1]["n"].Int != 2 {
				t.Fatalf("%s prepared=%v: %v %v", dialect, prepared, r, e)
			}
		}
		if _, e = s.Exec("SELECT p.ID,post_date FROM main.wp_posts p JOIN main.wp_term_relationships t ON p.ID=t.object_id GROUP BY p.ID ORDER BY post_date"); e != nil {
			t.Fatal("unambiguous base dependency rejected", dialect, e)
		}
		for _, q := range []string{
			"SELECT p.ID,p.post_date FROM main.wp_posts p JOIN main.wp_term_relationships t ON p.ID=t.object_id GROUP BY p.post_date",
			"SELECT p.ID,t.term_taxonomy_id FROM main.wp_posts p JOIN main.wp_term_relationships t ON p.ID=t.object_id GROUP BY p.ID",
			"SELECT p.ID FROM main.wp_posts p JOIN main.wp_term_relationships t ON p.ID=t.object_id GROUP BY p.ID ORDER BY t.term_taxonomy_id",
			"SELECT a,stamp FROM main.multi GROUP BY a",
			"SELECT a,stamp FROM main.multi m JOIN main.wp_term_relationships t ON m.a=t.object_id GROUP BY m.a",
		} {
			if _, e = s.Exec(q); e == nil {
				t.Fatal("invalid grouping accepted", dialect, q)
			}
		}
		if _, e = s.Exec("SELECT m.a,m.b,m.stamp FROM main.multi m JOIN main.wp_term_relationships t ON m.a=t.object_id GROUP BY m.a,m.b ORDER BY m.stamp"); e != nil {
			t.Fatal("full composite primary key did not imply dependency", dialect, e)
		}
		s.Close()
	}
}

func TestMySQLCalculatedFoundRowsDirectPreparedAndCursor(t *testing.T) {
	d := projectionDatabase(t)
	s, e := d.NewSession()
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	s.SetDialect(sql.DialectMySQL)
	run := func(q string) *ExecResult {
		t.Helper()
		r, e := s.Exec(q)
		if e != nil {
			t.Fatal(q, e)
		}
		return r
	}
	found := func(want uint64) {
		t.Helper()
		r := run("SELECT FOUND_ROWS() AS n")
		if r.Rows[0]["n"].Uint != want {
			t.Fatalf("found rows: %v want %d", r.Rows, want)
		}
	}
	run("CREATE TABLE found(id INTEGER PRIMARY KEY,g INTEGER);INSERT INTO found VALUES(1,1),(2,1),(3,2),(4,2),(5,3);CREATE INDEX found_g ON found(g)")
	for _, tc := range []struct {
		q        string
		want     uint64
		returned int
	}{
		{"SELECT SQL_CALC_FOUND_ROWS found.id FROM found LIMIT 2", 5, 2},
		{"SELECT SQL_CALC_FOUND_ROWS id FROM found ORDER BY id DESC LIMIT 1 OFFSET 3", 5, 1},
		{"SELECT SQL_CALC_FOUND_ROWS id FROM found LIMIT 0", 5, 0},
		{"SELECT SQL_CALC_FOUND_ROWS id FROM found LIMIT 2 OFFSET 99", 5, 0},
		{"SELECT SQL_CALC_FOUND_ROWS id FROM found WHERE g>=2 ORDER BY g LIMIT 1", 3, 1},
		{"SELECT SQL_CALC_FOUND_ROWS id FROM found WHERE id=1 LIMIT 0", 1, 0},
		{"SELECT SQL_CALC_FOUND_ROWS DISTINCT g FROM found LIMIT 1", 3, 1},
		{"SELECT SQL_CALC_FOUND_ROWS g,COUNT(*) FROM found GROUP BY g HAVING COUNT(*)>1 LIMIT 1", 2, 1},
		{"SELECT SQL_CALC_FOUND_ROWS id FROM found UNION ALL SELECT id FROM found LIMIT 1", 10, 1},
		{"SELECT SQL_CALC_FOUND_ROWS id FROM found WHERE id<0 LIMIT 1", 0, 0},
	} {
		for _, prepared := range []bool{false, true} {
			var r *ExecResult
			var e error
			if prepared {
				p, err := s.Prepare(tc.q)
				if err != nil {
					t.Fatal(tc.q, err)
				}
				r, e = p.Exec()
				p.Close()
			} else {
				r, e = s.Exec(tc.q)
			}
			if e != nil || len(r.Rows) != tc.returned {
				t.Fatalf("%s prepared=%v => %v %v", tc.q, prepared, r, e)
			}
			found(tc.want)
		}
	}
	p, e := s.Prepare("SELECT SQL_CALC_FOUND_ROWS id FROM found WHERE g>=? ORDER BY id LIMIT ? OFFSET ?")
	if e != nil {
		t.Fatal(e)
	}
	defer p.Close()
	for _, args := range [][]any{{2, 1, 0}, {1, 0, 0}, {3, 1, 10}} {
		if _, e = p.Exec(args...); e != nil {
			t.Fatal(e)
		}
		want := uint64(3)
		if args[0] == 1 {
			want = 5
		}
		if args[0] == 3 {
			want = 1
		}
		found(want)
	}
	rows, e := s.Query("SELECT SQL_CALC_FOUND_ROWS id FROM found LIMIT 1")
	if e != nil {
		t.Fatal(e)
	}
	if !rows.Materialized() {
		t.Fatal("counting cursor stopped at LIMIT")
	}
	for rows.Next() {
	}
	if e = rows.Close(); e != nil {
		t.Fatal(e)
	}
	found(5)
	// A count is obtained from this execution, without evaluating the predicate
	// again or opening another statement snapshot.
	run("SET @visits=0")
	run("SELECT SQL_CALC_FOUND_ROWS id FROM found WHERE (@visits:=@visits+1)>0 LIMIT 1")
	found(5)
	if r := run("SELECT @visits AS n"); r.Rows[0]["n"].Int != 5 {
		t.Fatal("predicate evaluated twice", r.Rows)
	}
	run("SELECT id FROM found LIMIT 2")
	found(2)
	other, e := d.NewSession()
	if e != nil {
		t.Fatal(e)
	}
	defer other.Close()
	other.SetDialect(sql.DialectMySQL)
	r, e := other.Exec("SELECT FOUND_ROWS() AS n")
	if e != nil || r.Rows[0]["n"].Uint != 0 {
		t.Fatal("session count leaked", r, e)
	}
}

func TestCalculatedFoundRowsRetainsOneStatementSnapshot(t *testing.T) {
	d := projectionDatabase(t)
	objectExec(t, d, "CREATE TABLE counted(id INTEGER PRIMARY KEY);INSERT INTO counted VALUES(1),(2),(3)")
	reader, e := d.NewSession()
	if e != nil {
		t.Fatal(e)
	}
	defer reader.Close()
	reader.SetDialect(sql.DialectMySQL)
	writer, e := d.NewSession()
	if e != nil {
		t.Fatal(e)
	}
	defer writer.Close()
	parent, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	calls := 0
	ctx := sql.WithMySQLSession(parent, sql.MySQLSessionOptions{Function: func(ctx context.Context, name string, args []sql.Value) (sql.Value, bool, error) {
		if name != "GET_LOCK" {
			return sql.Value{}, false, nil
		}
		calls++
		if calls == 1 {
			if _, e := writer.ExecContext(ctx, "INSERT INTO counted VALUES(4)"); e != nil {
				return sql.NewNullValue(), true, e
			}
		}
		return sql.NewIntValue(1), true, nil
	}})
	r, e := reader.ExecContext(ctx, "SELECT SQL_CALC_FOUND_ROWS id FROM counted WHERE GET_LOCK('snapshot-check',0) LIMIT 1")
	if e != nil || len(r.Rows) != 1 || calls != 3 {
		t.Fatal("query re-evaluated or changed snapshot", r, calls, e)
	}
	r, e = reader.Exec("SELECT FOUND_ROWS() AS n")
	if e != nil || r.Rows[0]["n"].Uint != 3 {
		t.Fatal("count used a second snapshot", r, e)
	}
	if r = objectExec(t, d, "SELECT COUNT(*) AS n FROM counted"); r.Rows[0]["n"].Int != 4 {
		t.Fatal("concurrent writer did not commit", r.Rows)
	}
}
