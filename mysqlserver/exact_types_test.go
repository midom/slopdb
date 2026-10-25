package mysqlserver

import (
	"encoding/json"
	"math"
	"testing"
)

func TestMySQLWireUnsignedDecimalAndJSON(t *testing.T) {
	s := startTestServer(t, "tcp", Config{})
	pool := s.open(t, "slopdb", "", "slopdb")
	mustExec(t, pool, "CREATE TABLE exact_wire(u BIGINT UNSIGNED PRIMARY KEY,d DECIMAL(65,20),j JSON)")
	decimal := "123456789012345678901234567890.12345678901234567890"
	document := `{"n":18446744073709551615,"label":"unicode 🪐"}`
	mustExec(t, pool, "INSERT INTO exact_wire VALUES(?,?,?)", uint64(math.MaxUint64), decimal, document)
	stmt, err := pool.Prepare("SELECT u,d,j FROM exact_wire WHERE u=?")
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Close()
	var unsigned uint64
	var exact, stored string
	if err = stmt.QueryRow(uint64(math.MaxUint64)).Scan(&unsigned, &exact, &stored); err != nil || unsigned != math.MaxUint64 || exact != decimal {
		t.Fatal(unsigned, exact, err)
	}
	if !json.Valid([]byte(stored)) {
		t.Fatal(stored)
	}
	var number, label string
	if err = pool.QueryRow("SELECT JSON_EXTRACT(j,'$.n'),j->>'$.label' FROM exact_wire").Scan(&number, &label); err != nil || number != "18446744073709551615" || label != "unicode 🪐" {
		t.Fatal(number, label, err)
	}
	rows, err := stmt.Query(uint64(math.MaxUint64))
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	cols, err := rows.ColumnTypes()
	if err != nil {
		t.Fatal(err)
	}
	for i, want := range []string{"UNSIGNED BIGINT", "DECIMAL", "JSON"} {
		if got := cols[i].DatabaseTypeName(); got != want {
			t.Fatalf("column%d type %q want%q", i, got, want)
		}
	}
}
func TestMySQLWireUnsignedAutoIncrementID(t *testing.T) {
	s := startTestServer(t, "tcp", Config{})
	c := connectRaw(t, s)
	expectOK(t, c.command(t, append([]byte{3}, "CREATE TABLE auto_unsigned(id BIGINT UNSIGNED PRIMARY KEY AUTO_INCREMENT,v INT)"...)), 2)
	expectOK(t, c.command(t, append([]byte{3}, "INSERT INTO auto_unsigned VALUES(18446744073709551614,1)"...)), 2)
	p := c.command(t, append([]byte{3}, "INSERT INTO auto_unsigned(v) VALUES(2)"...))
	if len(p) == 0 || p[0] != 0 {
		t.Fatal(p)
	}
	d := decoder{data: p[1:]}
	if changes := d.length(); changes != 1 {
		t.Fatal(changes)
	}
	if id := d.length(); id != math.MaxUint64 {
		t.Fatalf("lastid %d", id)
	}
}
