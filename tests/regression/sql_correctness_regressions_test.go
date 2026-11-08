package regression

import (
	"context"
	"reflect"
	"testing"

	"slopdb/db"
	"slopdb/sql"
)

type correctnessSession struct {
	*db.Session
	t        *testing.T
	env      *regressionEnv
	database *db.Database
	mysql    bool
}

func (s *correctnessSession) open() {
	s.t.Helper()
	var err error
	s.database, err = s.env.openDatabase(s.env.path("db"), nil)
	if err != nil {
		s.t.Fatal(err)
	}
	s.Session, err = s.database.NewSession()
	if err != nil {
		s.t.Fatal(err)
	}
	current := s.Session
	s.t.Cleanup(func() { _ = current.Close() })
	if s.mysql {
		if err := s.SetMySQLMode(true); err != nil {
			s.t.Fatal(err)
		}
	}
}

func (s *correctnessSession) reopen() {
	s.t.Helper()
	if err := s.Session.Close(); err != nil {
		s.t.Fatal(err)
	}
	if err := s.database.Close(); err != nil {
		s.t.Fatal(err)
	}
	s.open()
}

func newCorrectnessSession(t *testing.T, mysql bool) (*correctnessSession, func(string) *db.ExecResult) {
	t.Helper()
	s := &correctnessSession{t: t, env: newRegressionEnv(t), mysql: mysql}
	s.open()
	return s, func(q string) *db.ExecResult {
		t.Helper()
		r, err := s.Exec(q)
		if err != nil {
			t.Fatalf("%s: %v", q, err)
		}
		return r
	}
}

func TestViewJoinStarColumnsAndReopen(t *testing.T) {
	s, exec := newCorrectnessSession(t, false)
	for _, q := range []string{
		`CREATE TABLE a(id INTEGER PRIMARY KEY,v INT)`,
		`CREATE TABLE b(id INTEGER PRIMARY KEY,w INT)`,
		`INSERT INTO a VALUES(1,10)`, `INSERT INTO b VALUES(1,100)`,
		`CREATE VIEW v AS SELECT * FROM a JOIN b ON a.id=b.id`,
		`CREATE VIEW vn AS SELECT * FROM a NATURAL JOIN b`,
		`CREATE VIEW vu AS SELECT * FROM a JOIN b USING(id)`,
		`CREATE VIEW nested AS SELECT * FROM v`,
		`CREATE VIEW mixed AS SELECT *,b.w+1 AS next FROM a JOIN b USING(id)`,
		`CREATE VIEW qualified AS SELECT a.*,b.w AS w FROM a JOIN b ON a.id=b.id`,
	} {
		exec(q)
	}
	check := func() {
		t.Helper()
		for _, tc := range []struct {
			name    string
			columns int
		}{
			{"v", 4}, {"vn", 3}, {"vu", 3}, {"nested", 4}, {"mixed", 4}, {"qualified", 3},
		} {
			r := exec(`SELECT * FROM ` + tc.name)
			if len(r.Rows) != 1 || len(r.Columns) != tc.columns {
				t.Fatalf("%s: columns=%v rows=%v", tc.name, r.Columns, r.Rows)
			}
			if w, ok := r.Rows[0]["w"]; !ok || w.Int != 100 {
				t.Errorf("%s omitted right column: %v", tc.name, r.Rows)
			}
			if tc.name == "mixed" && r.Rows[0]["next"].Int != 101 {
				t.Fatal(r.Rows)
			}
			prepared, err := s.Prepare(`SELECT * FROM ` + tc.name)
			if err != nil {
				t.Fatal(err)
			}
			got, err := prepared.Exec()
			_ = prepared.Close()
			if err != nil || !reflect.DeepEqual(got.Rows, r.Rows) || len(got.Columns) != tc.columns {
				t.Fatalf("prepared %s: %v %v", tc.name, got, err)
			}
		}
	}
	check()
	s.reopen()
	check()
}

func TestNaturalUsingStarProjection(t *testing.T) {
	_, exec := newCorrectnessSession(t, false)
	for _, q := range []string{
		`CREATE TABLE a(id INTEGER PRIMARY KEY,v INT,ax INT)`,
		`CREATE TABLE b(id INTEGER PRIMARY KEY,v INT,bx INT)`,
		`INSERT INTO a VALUES(1,10,100)`, `INSERT INTO b VALUES(1,10,200)`,
		`CREATE TABLE c(id INT,v INT,ax INT,bx INT)`,
		`CREATE TABLE d(id INTEGER PRIMARY KEY,dx INT)`,
		`INSERT INTO d VALUES(1,300)`,
	} {
		exec(q)
	}
	for _, tc := range []struct {
		q       string
		columns int
	}{
		{`SELECT * FROM a NATURAL JOIN b`, 4},
		{`SELECT * FROM a JOIN b USING(id)`, 5},
		{`SELECT * FROM a NATURAL JOIN b NATURAL JOIN d`, 5},
		{`SELECT * FROM a JOIN b USING(id) JOIN d USING(id)`, 6},
		{`SELECT * FROM (SELECT * FROM a NATURAL JOIN b) AS j`, 4},
		{`WITH j AS (SELECT * FROM a JOIN b USING(id)) SELECT * FROM j`, 5},
		{`SELECT a.*,b.* FROM a NATURAL JOIN b`, 6},
		{`SELECT 99 AS extra,* FROM a NATURAL JOIN b`, 5},
		{`SELECT * FROM a NATURAL JOIN b WHERE a.id=99`, 4},
	} {
		r := exec(tc.q)
		if len(r.Columns) != tc.columns {
			t.Errorf("%s: got %d columns, want %d: %v", tc.q, len(r.Columns), tc.columns, r.Columns)
		}
	}
	exec(`INSERT INTO c SELECT * FROM a NATURAL JOIN b`)
	r := exec(`SELECT * FROM c`)
	if len(r.Rows) != 1 || r.Rows[0]["id"].Int != 1 || r.Rows[0]["v"].Int != 10 || r.Rows[0]["ax"].Int != 100 || r.Rows[0]["bx"].Int != 200 {
		t.Fatal(r.Rows)
	}
}

func TestMySQLMixedNumericJoin(t *testing.T) {
	for _, indexed := range []bool{false, true} {
		t.Run(map[bool]string{false: "hash", true: "indexed"}[indexed], func(t *testing.T) {
			s, exec := newCorrectnessSession(t, true)
			for _, q := range []string{
				`CREATE TABLE a(id INTEGER PRIMARY KEY,k TEXT)`,
				`CREATE TABLE b(id INTEGER PRIMARY KEY,k INTEGER)`,
				`INSERT INTO a VALUES(1,'5'),(2,'x'),(3,'7')`,
				`INSERT INTO b VALUES(10,5),(20,7),(30,9)`,
			} {
				exec(q)
			}
			if indexed {
				exec(`CREATE INDEX bk ON b(k)`)
			}
			for _, q := range []string{
				`SELECT a.id,b.id AS bid FROM a JOIN b ON a.k=b.k`,
				`SELECT a.id,b.id AS bid FROM a JOIN (SELECT id,k FROM b) b ON a.k=b.k`,
			} {
				r := exec(q)
				if len(r.Rows) != 2 {
					t.Errorf("%s: rows=%v, want 2", q, r.Rows)
				} else {
					t.Logf("control: %s: %v", q, r.Rows)
				}
			}
			r := exec(`SELECT a.id,b.id AS bid FROM a LEFT JOIN b ON a.k=b.k`)
			matches := 0
			for _, row := range r.Rows {
				if !row["bid"].Null {
					matches++
				}
			}
			if len(r.Rows) != 3 || matches != 2 {
				t.Errorf("LEFT: rows=%v matches=%d, want 2", r.Rows, matches)
			}
			// Coercion must retain duplicates, match nonnumeric strings to zero,
			// and exclude NULL, even after statistics change the join order.
			exec(`INSERT INTO a VALUES(4,'05'),(5,'5.0'),(6,NULL)`)
			exec(`INSERT INTO b VALUES(40,0),(50,NULL)`)
			check := func(q string) {
				t.Helper()
				r := exec(q)
				var pairs [][2]int64
				for _, row := range r.Rows {
					pairs = append(pairs, [2]int64{row["aid"].Int, row["bid"].Int})
				}
				want := [][2]int64{{1, 10}, {2, 40}, {3, 20}, {4, 10}, {5, 10}}
				if !reflect.DeepEqual(pairs, want) {
					t.Errorf("%s: %v, want %v", q, pairs, want)
				}
			}
			query := `SELECT a.id AS aid,b.id AS bid FROM a JOIN b ON a.k=b.k ORDER BY a.id,b.id`
			check(query)
			exec(`ANALYZE`)
			check(query)
			prepared, err := s.Prepare(query)
			if err != nil {
				t.Fatal(err)
			}
			defer prepared.Close()
			for i := 0; i < 2; i++ {
				if r, err := prepared.Exec(); err != nil || len(r.Rows) != 5 {
					t.Fatalf("prepared mixed join: %v %v", r, err)
				}
			}
			exec(`CREATE TABLE blobs(id INTEGER PRIMARY KEY,k BLOB)`)
			exec(`INSERT INTO blobs VALUES(1,X'35'),(2,X'78'),(3,NULL)`)
			if r := exec(`SELECT COUNT(*) AS n FROM blobs JOIN b ON blobs.k=b.k`); len(r.Rows) != 1 || r.Rows[0]["n"].Int != 2 {
				t.Fatal(r.Rows)
			}
		})
	}
}

func TestAliasedInnerTableCorrelation(t *testing.T) {
	_, exec := newCorrectnessSession(t, false)
	exec(`CREATE TABLE t(id INTEGER PRIMARY KEY)`)
	exec(`INSERT INTO t VALUES(1),(2)`)
	for _, q := range []string{
		`SELECT id,(SELECT COUNT(*) FROM t AS x WHERE x.id<=t.id+0) AS n FROM t ORDER BY id`,
		`SELECT id,(WITH c(n) AS (SELECT COUNT(*) FROM t AS x WHERE x.id<=t.id) SELECT n FROM c) AS n FROM t ORDER BY id`,
		`SELECT id,(WITH c(n) AS (SELECT COUNT(*) FROM t AS x WHERE x.id<=o.id) SELECT n FROM c) AS n FROM t AS o ORDER BY id`,
		`SELECT id,(SELECT COUNT(*) FROM t AS x WHERE x.id IN (1,2) AND x.id<=t.id+0) AS n FROM t ORDER BY id`,
		`SELECT id,(SELECT COUNT(*) FROM t AS x WHERE x.id<=t.id+0 GROUP BY 1+0) AS n FROM t ORDER BY id`,
	} {
		r := exec(q)
		if len(r.Rows) != 2 || r.Rows[0]["n"].Int != 1 || r.Rows[1]["n"].Int != 2 {
			t.Errorf("%s: rows=%v, want n=[1,2]", q, r.Rows)
		} else {
			t.Logf("control: %s: %v", q, r.Rows)
		}
	}
	for _, q := range []string{
		`SELECT id,EXISTS(SELECT 1 FROM t AS x WHERE x.id>t.id+0) AS n FROM t ORDER BY id`,
		`SELECT id,2 IN (SELECT x.id FROM t AS x WHERE x.id>t.id+0) AS n FROM t ORDER BY id`,
	} {
		r := exec(q)
		if len(r.Rows) != 2 || r.Rows[0]["n"].Int != 1 || r.Rows[1]["n"].Int != 0 {
			t.Errorf("%s: %v, want n=[1,0]", q, r.Rows)
		}
	}
}

func TestMySQLMixedNumericPredicates(t *testing.T) {
	for _, indexed := range []bool{false, true} {
		t.Run(map[bool]string{false: "scan", true: "index"}[indexed], func(t *testing.T) {
			s, exec := newCorrectnessSession(t, true)
			exec(`CREATE TABLE a(id INTEGER PRIMARY KEY,k TEXT,n INT)`)
			exec(`INSERT INTO a VALUES(1,'05',5),(2,'5.0',5),(3,'5',5),(4,'x',0)`)
			if indexed {
				exec(`CREATE INDEX ak ON a(k)`)
			}
			for _, tc := range []struct {
				q    string
				want int64
			}{
				{`SELECT ('05'=5) AS n`, 1},
				{`SELECT COUNT(*) AS n FROM a WHERE k=n`, 4},
				{`SELECT COUNT(*) AS n FROM a WHERE k=5`, 3},
				{`SELECT COUNT(*) AS n FROM a WHERE 5=k`, 3},
				{`SELECT COUNT(*) AS n FROM a WHERE k=0`, 1},
				{`SELECT COUNT(*) AS n FROM a WHERE k>4`, 3},
				{`SELECT COUNT(*) AS n FROM a WHERE k<5`, 1},
				{`SELECT COUNT(*) AS n FROM a WHERE n='5x'`, 3},
				{`SELECT COUNT(*) AS n FROM a WHERE k=5 OR id=4`, 4},
			} {
				r := exec(tc.q)
				if len(r.Rows) != 1 || r.Rows[0]["n"].Int != tc.want {
					t.Errorf("%s: %v, want %d", tc.q, r.Rows, tc.want)
				}
			}
			stmt, err := s.Prepare(`SELECT COUNT(*) AS n FROM a WHERE k=?`)
			if err != nil {
				t.Fatal(err)
			}
			defer stmt.Close()
			r, err := stmt.Exec(int64(5))
			if err != nil {
				t.Fatal(err)
			}
			if len(r.Rows) != 1 || r.Rows[0]["n"].Int != 3 {
				t.Errorf("prepared k=?: %v, want 3", r.Rows)
			}
			for _, tc := range []struct {
				binding any
				want    int64
			}{{"5", 1}, {float64(5), 3}, {int64(0), 1}, {int64(5), 3}} {
				r, err := stmt.Exec(tc.binding)
				if err != nil {
					t.Fatal(err)
				}
				if len(r.Rows) != 1 || r.Rows[0]["n"].Int != tc.want {
					t.Errorf("prepared k=%v (%T): %v, want %d", tc.binding, tc.binding, r.Rows, tc.want)
				}
			}
			r = exec(`SELECT id,(k=5) AS n FROM a ORDER BY id`)
			if len(r.Rows) != 4 {
				t.Fatal(r.Rows)
			}
			for i, want := range []int64{1, 1, 1, 0} {
				if r.Rows[i]["n"].Int != want {
					t.Errorf("projection: %v", r.Rows)
				}
			}
		})
	}
	_, exec := newCorrectnessSession(t, false)
	exec(`CREATE TABLE native(k TEXT)`)
	exec(`INSERT INTO native VALUES('05'),('5.0'),('5')`)
	if r := exec(`SELECT COUNT(*) AS n FROM native WHERE k=5`); r.Rows[0]["n"].Int != 1 {
		t.Fatal(r.Rows)
	}
}

func TestForeignCascadeNullableUniqueKey(t *testing.T) {
	s, exec := newCorrectnessSession(t, false)
	for _, q := range []string{
		`CREATE TABLE p(id INTEGER PRIMARY KEY,k INTEGER UNIQUE)`,
		`CREATE TABLE c(id INTEGER PRIMARY KEY,k INTEGER REFERENCES p(k) ON UPDATE CASCADE)`,
		`INSERT INTO p VALUES(1,10)`, `INSERT INTO c VALUES(1,10)`,
	} {
		exec(q)
	}
	if _, err := s.Exec(`UPDATE p SET k=NULL WHERE id=1`); err != nil {
		t.Errorf("cascade NULL: %v", err)
	}
	for _, table := range []string{"p", "c"} {
		r := exec(`SELECT k FROM ` + table)
		if len(r.Rows) != 1 || !r.Rows[0]["k"].Null {
			t.Errorf("%s: %v, want NULL", table, r.Rows)
		}
	}
	s.reopen()
	for _, table := range []string{"p", "c"} {
		if r := exec(`SELECT k FROM ` + table); len(r.Rows) != 1 || !r.Rows[0]["k"].Null {
			t.Fatal(r.Rows)
		}
	}
	for _, q := range []string{
		`CREATE TABLE parent(id INTEGER PRIMARY KEY,a INT,b INT,UNIQUE(a,b))`,
		`CREATE TABLE child(id INTEGER PRIMARY KEY,a INT,b INT,FOREIGN KEY(a,b) REFERENCES parent(a,b) ON UPDATE CASCADE)`,
		`INSERT INTO parent VALUES(1,10,20)`, `INSERT INTO child VALUES(1,10,20)`,
		`UPDATE parent SET a=NULL,b=30 WHERE id=1`,
	} {
		exec(q)
	}
	r := exec(`SELECT a,b FROM child`)
	if len(r.Rows) != 1 || !r.Rows[0]["a"].Null || r.Rows[0]["b"].Int != 30 {
		t.Fatal(r.Rows)
	}
	for _, q := range []string{
		`CREATE TABLE strict_parent(id INTEGER PRIMARY KEY,k INT UNIQUE)`,
		`CREATE TABLE strict_child(id INTEGER PRIMARY KEY,k INT NOT NULL REFERENCES strict_parent(k) ON UPDATE CASCADE)`,
		`INSERT INTO strict_parent VALUES(1,10)`, `INSERT INTO strict_child VALUES(1,10)`,
	} {
		exec(q)
	}
	if _, err := s.Exec(`UPDATE strict_parent SET k=NULL WHERE id=1`); err == nil {
		t.Error("accepted NULL cascade into NOT NULL child")
	}
	for _, table := range []string{"strict_parent", "strict_child"} {
		if r := exec(`SELECT k FROM ` + table); len(r.Rows) != 1 || r.Rows[0]["k"].Int != 10 || r.Rows[0]["k"].Null {
			t.Fatal(r.Rows)
		}
	}
	s.reopen()
	r = exec(`SELECT a,b FROM child`)
	if len(r.Rows) != 1 || !r.Rows[0]["a"].Null || r.Rows[0]["b"].Int != 30 {
		t.Fatal(r.Rows)
	}
	if _, err := s.database.CheckIntegrity(); err != nil {
		t.Fatal(err)
	}
}

func TestMySQLAllowExistingUnassignedUpdate(t *testing.T) {
	for _, mode := range []string{"point", "trigger", "upsert"} {
		t.Run(mode, func(t *testing.T) {
			s, _ := newCorrectnessSession(t, true)
			ctx := sql.WithMySQLSession(context.Background(), sql.MySQLSessionOptions{SQLMode: ""})
			exec := func(q string) *db.ExecResult {
				t.Helper()
				r, err := s.ExecContext(ctx, q)
				if err != nil {
					t.Fatalf("%s: %v", q, err)
				}
				return r
			}
			for _, q := range []string{
				`CREATE TABLE t(id INTEGER PRIMARY KEY,name VARCHAR(4),note TEXT)`,
				`INSERT INTO t VALUES(1,'abcd','old')`,
				`ALTER TABLE t ALTER COLUMN name TYPE VARCHAR(2) ALLOW EXISTING`,
			} {
				exec(q)
			}
			if mode == "trigger" {
				exec(`CREATE TRIGGER tr AFTER UPDATE ON t BEGIN SELECT 1; END`)
			}
			if mode == "upsert" {
				exec(`INSERT INTO t(id,name,note) VALUES(1,'ok','x') ON CONFLICT(id) DO UPDATE SET note=excluded.note`)
			} else {
				exec(`UPDATE t SET note='x' WHERE id=1`)
			}
			r := exec(`SELECT name,note FROM t WHERE id=1`)
			if len(r.Rows) != 1 || r.Rows[0]["name"].Text != "abcd" || r.Rows[0]["note"].Text != "x" {
				t.Errorf("unassigned name changed: %v", r.Rows)
			} else {
				t.Logf("preserved: %v", r.Rows)
			}
			s.reopen()
			r = exec(`SELECT name,note FROM t WHERE id=1`)
			if len(r.Rows) != 1 || r.Rows[0]["name"].Text != "abcd" || r.Rows[0]["note"].Text != "x" {
				t.Fatal(r.Rows)
			}
			if _, err := s.database.CheckIntegrity(); err != nil {
				t.Fatal(err)
			}
			exec(`UPDATE t SET name='wxyz' WHERE id=1`)
			if r := exec(`SELECT name FROM t`); r.Rows[0]["name"].Text != "wx" {
				t.Errorf("new assignment not normalized: %v", r.Rows)
			}
		})
	}
}

func TestMySQLTemporaryNamespaceControl(t *testing.T) {
	s, exec := newCorrectnessSession(t, true)
	exec(`CREATE SCHEMA app`)
	exec(`CREATE TEMPORARY TABLE sticky(id INTEGER PRIMARY KEY)`)
	exec(`INSERT INTO sticky VALUES(1)`)
	exec(`USE app`)
	if _, err := s.Exec(`SELECT id FROM sticky`); err == nil {
		t.Error("unqualified name should resolve in app")
	} else {
		t.Logf("expected namespace error: %v", err)
	}
	r := exec(`SELECT id FROM main.sticky`)
	if len(r.Rows) != 1 || r.Rows[0]["id"].Int != 1 {
		t.Fatal(r.Rows)
	}
	exec(`USE main`)
	r = exec(`SELECT id FROM sticky`)
	if len(r.Rows) != 1 || r.Rows[0]["id"].Int != 1 {
		t.Fatal(r.Rows)
	}
}
