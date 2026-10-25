package postgresserver

import (
	"bytes"
	"context"
	"github.com/jackc/pgx/v5"
	"slopdb/sql"
	"testing"
)

func TestPostgreSQLExactNumericWire(t *testing.T) {
	for _, text := range []string{"0", "0.00", "1", "-1", "0.00001", "-0.0100", "10000", "12345.678900", "18446744073709551615", "123456789012345678901234567890.12345678901234567890"} {
		encoded, err := encodeNumeric(text)
		if err != nil {
			t.Fatal(text, err)
		}
		decoded, err := decodeNumeric(encoded)
		if err != nil || decoded.Text != text {
			t.Fatal(text, decoded, err)
		}
	}
	// A known PostgreSQL numeric_send representation for 12345.6789:
	// 3 base10000 digits, weight1, positive, scale4: [1,2345,6789].
	fixture := []byte{0, 3, 0, 1, 0, 0, 0, 4, 0, 1, 9, 41, 26, 133}
	got, err := encodeNumeric("12345.6789")
	if err != nil || !bytes.Equal(got, fixture) {
		t.Fatal(got, err)
	}
	for _, malformed := range [][]byte{{0}, {0, 1, 0, 0, 0, 0, 0, 0}, {0, 1, 0, 0, 0, 0, 0, 0, 255, 255}, {0, 0, 0, 0, 0xc0, 0, 0, 0}} {
		if _, err = decodeNumeric(malformed); err == nil {
			t.Fatal("accepted malformed numeric", malformed)
		}
	}
	value, err := decodeValue(append([]byte{1}, []byte(`{"n":18446744073709551615}`)...), oidJSONB, true)
	if err != nil || value.(sql.Value).Type != sql.ColJSON {
		t.Fatal(value, err)
	}
	packet, err := wireValue(value.(sql.Value), oidJSONB, true)
	if err != nil || packet[0] != 1 {
		t.Fatal(packet, err)
	}
}
func TestPostgreSQLExactTypesPreparedAndSimple(t *testing.T) {
	s := startTestServer(t, "tcp", Config{})
	ctx := context.Background()
	c, err := pgx.Connect(ctx, s.dsn("slopdb", "", "slopdb"))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close(ctx)
	if _, err = c.Exec(ctx, "CREATE TABLE exact_pg(id INTEGER PRIMARY KEY,u BIGINT UNSIGNED,d DECIMAL(65,20),j JSON)"); err != nil {
		t.Fatal(err)
	}
	decimal := "123456789012345678901234567890.12345678901234567890"
	if _, err = c.Exec(ctx, "INSERT INTO exact_pg VALUES(1,18446744073709551615,$1::numeric,$2::json)", decimal, `{"n":18446744073709551615}`); err != nil {
		t.Fatal(err)
	}
	var u, d, j string
	if err = c.QueryRow(ctx, "SELECT u,d,j FROM exact_pg WHERE id=$1", 1).Scan(&u, &d, &j); err != nil || u != "18446744073709551615" || d != decimal {
		t.Fatal(u, d, j, err)
	}
	var extracted string
	if err = c.QueryRow(ctx, "SELECT j->>'$.n' FROM exact_pg WHERE id=1").Scan(&extracted); err != nil || extracted != "18446744073709551615" {
		t.Fatal(extracted, err)
	}
}
