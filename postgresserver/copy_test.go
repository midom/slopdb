package postgresserver

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/lib/pq"
)

func copyConnection(t *testing.T, s *testServer) *pgx.Conn {
	t.Helper()
	c, err := pgx.Connect(context.Background(), s.dsn("slopdb", "", "slopdb"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close(context.Background()) })
	return c
}
func TestCopyTextCSVAndQuery(t *testing.T) {
	s := startTestServer(t, "tcp", Config{})
	c := copyConnection(t, s)
	ctx := context.Background()
	if _, err := c.Exec(ctx, "CREATE TABLE c(id BIGINT PRIMARY KEY, v TEXT, missing TEXT, def TEXT DEFAULT 'default')"); err != nil {
		t.Fatal(err)
	}
	input := "1\thello\\t世界\t\\N\n2\t\\\\N\t\n3\tline\\nnext\tx\n"
	tag, err := c.PgConn().CopyFrom(ctx, strings.NewReader(input), "COPY c(id,v,missing) FROM STDIN")
	if err != nil || tag.RowsAffected() != 3 {
		t.Fatalf("%s %v", tag, err)
	}
	var out bytes.Buffer
	tag, err = c.PgConn().CopyTo(ctx, &out, "COPY (SELECT id,v,missing FROM c ORDER BY id) TO STDOUT")
	if err != nil || out.String() != input {
		t.Fatalf("%s %v output %q", tag, err, out.String())
	}
	csv := "id,v,missing\r\n4,\"a,b\nnext\"\"quote\",\r\n5,\"\",\"\"\r\n6,\\N,value\r\n"
	tag, err = c.PgConn().CopyFrom(ctx, strings.NewReader(csv), "COPY c(id,v,missing) FROM STDIN WITH (FORMAT csv, HEADER MATCH)")
	if err != nil || tag.RowsAffected() != 3 {
		t.Fatalf("%s %v", tag, err)
	}
	out.Reset()
	_, err = c.PgConn().CopyTo(ctx, &out, "COPY (SELECT id,v,missing FROM c WHERE id>=4 ORDER BY id) TO STDOUT WITH (FORMAT csv, HEADER true)")
	want := "id,v,missing\n4,\"a,b\nnext\"\"quote\",\n5,\"\",\"\"\n6,\\N,value\n"
	if err != nil || out.String() != want {
		t.Fatalf("csv %v output %q want %q", err, out.String(), want)
	}
	var n int
	if err = c.QueryRow(ctx, "SELECT count(*) FROM c WHERE def='default'").Scan(&n); err != nil || n != 6 {
		t.Fatalf("default count %d %v", n, err)
	}
}
func TestCopyBinaryPGXAndIndependentFixture(t *testing.T) {
	s := startTestServer(t, "tcp", Config{})
	c := copyConnection(t, s)
	ctx := context.Background()
	if _, err := c.Exec(ctx, "CREATE TABLE binary_copy(id BIGINT PRIMARY KEY, txt TEXT, b BYTEA, amount DOUBLE PRECISION)"); err != nil {
		t.Fatal(err)
	}
	rows := [][]any{{int64(1), "hello 世界", []byte{0, 1, 255}, 1.25}, {int64(2), nil, []byte{}, -2.5}}
	n, err := c.CopyFrom(ctx, pgx.Identifier{"binary_copy"}, []string{"id", "txt", "b", "amount"}, pgx.CopyFromRows(rows))
	if err != nil || n != 2 {
		t.Fatalf("pgx binary copy %d %v", n, err)
	}
	var out bytes.Buffer
	_, err = c.PgConn().CopyTo(ctx, &out, "COPY (SELECT id,txt,b,amount FROM binary_copy ORDER BY id) TO STDOUT (FORMAT binary)")
	if err != nil {
		t.Fatal(err)
	}
	// Independent format fixture: signature, flags, extension, typed field lengths,
	// network-endian integer/float payload, null -1, empty bytea length 0, trailer.
	fixture := []byte{'P', 'G', 'C', 'O', 'P', 'Y', '\n', 255, '\r', '\n', 0, 0, 0, 0, 0, 0, 0, 0, 0}
	put := func(v []byte) {
		if v == nil {
			fixture = binary.BigEndian.AppendUint32(fixture, 0xffffffff)
		} else {
			fixture = binary.BigEndian.AppendUint32(fixture, uint32(len(v)))
			fixture = append(fixture, v...)
		}
	}
	for i := 0; i < 2; i++ {
		fixture = binary.BigEndian.AppendUint16(fixture, 4)
		put(binary.BigEndian.AppendUint64(nil, uint64(i+1)))
		if i == 0 {
			put([]byte("hello 世界"))
			put([]byte{0, 1, 255})
			put([]byte{0x3f, 0xf4, 0, 0, 0, 0, 0, 0})
		} else {
			put(nil)
			put([]byte{})
			put([]byte{0xc0, 0x04, 0, 0, 0, 0, 0, 0})
		}
	}
	fixture = append(fixture, 255, 255)
	if !bytes.Equal(out.Bytes(), fixture) {
		t.Fatalf("binary output\n%x\nwant\n%x", out.Bytes(), fixture)
	}
}
func TestCopyLibPQAndAtomicFailure(t *testing.T) {
	s := startTestServer(t, "tcp", Config{})
	c := s.open(t, "")
	mustExec(t, c, "CREATE TABLE copy_atomic(id BIGINT PRIMARY KEY,v TEXT)")
	tx, err := c.Begin()
	if err != nil {
		t.Fatal(err)
	}
	stmt, err := tx.Prepare(pq.CopyIn("copy_atomic", "id", "v"))
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range [][]any{{int64(1), "one"}, {int64(2), "tab\tline\nslash\\"}} {
		if _, err = stmt.Exec(row...); err != nil {
			t.Fatal(err)
		}
	}
	if _, err = stmt.Exec(); err != nil {
		t.Fatal(err)
	}
	if err = stmt.Close(); err != nil {
		t.Fatal(err)
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	pg := copyConnection(t, s)
	ctx := context.Background()
	if _, err = pg.PgConn().CopyFrom(ctx, strings.NewReader("3\tgood\n1\tduplicate\n4\tlate\n"), "COPY copy_atomic FROM STDIN"); err == nil {
		t.Fatal("duplicate COPY succeeded")
	}
	var n int
	if err = pg.QueryRow(ctx, "SELECT count(*) FROM copy_atomic").Scan(&n); err != nil || n != 2 {
		t.Fatalf("atomic count %d %v", n, err)
	}
	if _, err = pg.Exec(ctx, "BEGIN; INSERT INTO copy_atomic VALUES(10,'before'); SAVEPOINT caller"); err != nil {
		t.Fatal(err)
	}
	_, err = pg.PgConn().CopyFrom(ctx, strings.NewReader("11\tgood\ninvalid\tbad\n"), "COPY copy_atomic FROM STDIN")
	if err == nil {
		t.Fatal("invalid COPY succeeded")
	}
	_, err = pg.Exec(ctx, "SELECT 1")
	var pe *pgconn.PgError
	if !errors.As(err, &pe) || pe.Code != "25P02" {
		t.Fatalf("failed tx %v", err)
	}
	if _, err = pg.Exec(ctx, "ROLLBACK TO caller; COMMIT"); err != nil {
		t.Fatal(err)
	}
	if err = pg.QueryRow(ctx, "SELECT count(*) FROM copy_atomic WHERE id>=10").Scan(&n); err != nil || n != 1 {
		t.Fatalf("savepoint recovery %d %v", n, err)
	}
}
func TestCopyFragmentedExtendedAndLateMessages(t *testing.T) {
	s := startTestServer(t, "tcp", Config{})
	c := connectRaw(t, s)
	c.query(t, "CREATE TABLE fragmented(id BIGINT PRIMARY KEY,v TEXT)")
	c.send(t, &pgproto3.Parse{Name: "copy", Query: "COPY fragmented FROM STDIN (FORMAT csv)"}, &pgproto3.Describe{ObjectType: 'S', Name: "copy"}, &pgproto3.Bind{PreparedStatement: "copy"}, &pgproto3.Execute{}, &pgproto3.Flush{}, &pgproto3.Sync{})
	receive[*pgproto3.ParseComplete](t, c)
	receive[*pgproto3.ParameterDescription](t, c)
	receive[*pgproto3.NoData](t, c)
	receive[*pgproto3.BindComplete](t, c)
	receive[*pgproto3.CopyInResponse](t, c)
	for _, b := range []byte("1,\"héllo\n世界\"\n2,\"\"\n") {
		c.send(t, &pgproto3.CopyData{Data: []byte{b}})
	}
	c.send(t, &pgproto3.CopyDone{}, &pgproto3.Sync{})
	if tag := receive[*pgproto3.CommandComplete](t, c); string(tag.CommandTag) != "COPY 2" {
		t.Fatal(tag)
	}
	for {
		m, err := c.frontend.Receive()
		if err != nil {
			t.Fatal(err)
		}
		if _, ok := m.(*pgproto3.ReadyForQuery); ok {
			break
		}
		if _, ok := m.(*pgproto3.ParameterStatus); !ok {
			t.Fatalf("%T", m)
		}
	}
	c.send(t, &pgproto3.Query{String: "COPY fragmented FROM STDIN"})
	receive[*pgproto3.CopyInResponse](t, c)
	c.send(t, &pgproto3.CopyData{Data: []byte("3\tgood\n")}, &pgproto3.CopyFail{Message: "source failed"}, &pgproto3.CopyData{Data: []byte("4\tlate\n")}, &pgproto3.CopyDone{})
	receive[*pgproto3.ErrorResponse](t, c)
	receive[*pgproto3.ReadyForQuery](t, c)
	c.send(t, &pgproto3.Query{String: "SELECT count(*) FROM fragmented"})
	receive[*pgproto3.RowDescription](t, c)
	if row := receive[*pgproto3.DataRow](t, c); string(row.Values[0]) != "2" {
		t.Fatal(row)
	}
	receive[*pgproto3.CommandComplete](t, c)
	receive[*pgproto3.ReadyForQuery](t, c)
}
func TestCopyRecordFormatsAndBounds(t *testing.T) {
	cases := []struct {
		format, input string
		want          []string
	}{
		{"text", `a\tb` + "\tc\\141\\x62\\v\n", []string{"a\tb", "cab\v"}},
		{"csv", "\"a\r\nb\",\"double\"\"quote\"\r\n", []string{"a\r\nb", "double\"quote"}},
	}
	for _, tc := range cases {
		command, _ := parseCopy("COPY t FROM STDIN (FORMAT " + tc.format + ")")
		r := copyRecords{reader: bufio.NewReader(strings.NewReader(tc.input)), command: command, limit: 1024}
		record, err := r.next(2)
		if err != nil {
			t.Fatal(err)
		}
		var got []string
		for _, f := range record {
			data := f.data
			if tc.format == "text" {
				data = copyUnescape(data)
			}
			got = append(got, string(data))
		}
		if !reflect.DeepEqual(got, tc.want) {
			t.Fatalf("%q != %q", got, tc.want)
		}
		if _, err = r.next(2); err != io.EOF {
			t.Fatal(err)
		}
	}
	command, _ := parseCopy("COPY t FROM STDIN")
	r := copyRecords{reader: bufio.NewReader(strings.NewReader(strings.Repeat("x", 100))), command: command, limit: 10}
	if _, err := r.next(1); err == nil {
		t.Fatal("unbounded record")
	}
}

func TestCopyCancellationDisconnectAndBinaryErrors(t *testing.T) {
	s := startTestServer(t, "tcp", Config{})
	c := connectRaw(t, s)
	c.query(t, "CREATE TABLE cancelled_copy(id BIGINT PRIMARY KEY,v TEXT)")
	c.send(t, &pgproto3.Query{String: "COPY cancelled_copy FROM STDIN"})
	receive[*pgproto3.CopyInResponse](t, c)
	c.send(t, &pgproto3.CopyData{Data: []byte("1\tfirst\n")})
	s.server.mu.Lock()
	var id, secret uint32
	for _, backend := range s.server.backends {
		id = backend.id
		secret = backend.secret
	}
	s.server.mu.Unlock()
	cancelConn, err := net.Dial(s.network, s.address)
	if err != nil {
		t.Fatal(err)
	}
	request, err := (&pgproto3.CancelRequest{ProcessID: id, SecretKey: secret}).Encode(nil)
	if err != nil {
		t.Fatal(err)
	}
	if err = writeAll(cancelConn, request); err != nil {
		t.Fatal(err)
	}
	_ = cancelConn.Close()
	if response := receive[*pgproto3.ErrorResponse](t, c); response.Code != "57014" {
		t.Fatal(response)
	}
	if ready := receive[*pgproto3.ReadyForQuery](t, c); ready.TxStatus != 'I' {
		t.Fatal(ready)
	}
	c.send(t, &pgproto3.CopyDone{})
	c.query(t, "SELECT 1")
	connection := copyConnection(t, s)
	var n int
	if err = connection.QueryRow(context.Background(), "SELECT count(*) FROM cancelled_copy").Scan(&n); err != nil || n != 0 {
		t.Fatalf("cancel count %d %v", n, err)
	}
	c.send(t, &pgproto3.Query{String: "COPY cancelled_copy FROM STDIN"})
	receive[*pgproto3.CopyInResponse](t, c)
	c.send(t, &pgproto3.CopyData{Data: []byte("2\tsecond\n")})
	_ = c.conn.Close()
	// A conflicting writer waits for the disconnected COPY to release its lock.
	if _, err = connection.Exec(context.Background(), "INSERT INTO cancelled_copy VALUES(2,'after disconnect')"); err != nil {
		t.Fatal(err)
	}
	bad := [][]byte{nil, []byte("bad"), append(append([]byte{}, copySignature...), 0, 1, 0, 0, 0, 0, 0, 0), append(append([]byte{}, copySignature...), 0, 0, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 8, 0)}
	for _, data := range bad {
		if _, err = connection.PgConn().CopyFrom(context.Background(), bytes.NewReader(data), "COPY cancelled_copy FROM STDIN (FORMAT binary)"); err == nil {
			t.Fatalf("invalid binary accepted %x", data)
		}
	}
	if err = connection.QueryRow(context.Background(), "SELECT count(*) FROM cancelled_copy").Scan(&n); err != nil || n != 1 {
		t.Fatalf("disconnect/malformed atomic count %d %v", n, err)
	}
}
func TestCopyOptionsNullForceAndEndMarker(t *testing.T) {
	s := startTestServer(t, "tcp", Config{})
	c := copyConnection(t, s)
	ctx := context.Background()
	if _, err := c.Exec(ctx, "CREATE TABLE opts(id BIGINT PRIMARY KEY,a TEXT,b TEXT)"); err != nil {
		t.Fatal(err)
	}
	data := "1;NULL;NULL\n2;'NULL';'NULL'\n3;'escape!'quote';'bang!!'\n"
	command := "COPY opts FROM STDIN (FORMAT csv, DELIMITER ';', NULL 'NULL', QUOTE '''', ESCAPE '!', FORCE_NOT_NULL(a), FORCE_NULL(b))"
	if _, err := c.PgConn().CopyFrom(ctx, strings.NewReader(data), command); err != nil {
		t.Fatal(err)
	}
	var a, b *string
	if err := c.QueryRow(ctx, "SELECT a,b FROM opts WHERE id=1").Scan(&a, &b); err != nil || a == nil || *a != "NULL" || b != nil {
		t.Fatalf("force not null %v %v %v", a, b, err)
	}
	if err := c.QueryRow(ctx, "SELECT a,b FROM opts WHERE id=2").Scan(&a, &b); err != nil || a == nil || *a != "NULL" || b != nil {
		t.Fatalf("force null %v %v %v", a, b, err)
	}
	if _, err := c.PgConn().CopyFrom(ctx, strings.NewReader("4\tx\ty\n\\.\nignored\n"), "COPY opts FROM STDIN"); err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	if _, err := c.PgConn().CopyTo(ctx, &out, "COPY (SELECT a,b FROM opts WHERE id=3) TO STDOUT WITH CSV FORCE_QUOTE *"); err != nil || out.String() != "\"escape'quote\",\"bang!\"\n" {
		t.Fatalf("custom csv %q %v", out.String(), err)
	}
	for _, q := range []string{"COPY opts FROM '/tmp/secret'", "COPY opts TO PROGRAM 'cat'", "COPY opts FROM STDIN (FORMAT unknown)", "COPY opts FROM STDIN (FORMAT binary, DELIMITER ',')", "COPY opts FROM STDIN (FORMAT text, HEADER false)", "COPY opts TO STDOUT (FORMAT csv, FORCE_NULL(a))", "COPY opts FROM STDIN (FORMAT csv, NULL 'a,b')"} {
		if _, err := parseCopy(q); err == nil {
			t.Fatalf("invalid options accepted: %s", q)
		}
	}
}

func TestCopyPostgresPrimitiveTypesPGX(t *testing.T) {
	s := startTestServer(t, "tcp", Config{})
	c := copyConnection(t, s)
	ctx := context.Background()
	_, err := c.Exec(ctx, `CREATE TABLE copy_pg_types(id INTEGER PRIMARY KEY,flag BOOLEAN,u UUID,d DATE,tm TIME,ts TIMESTAMP,tz TIMESTAMP WITH TIME ZONE,iv INTERVAL,small SMALLINT,number NUMERIC(10,2),doc JSONB)`)
	if err != nil {
		t.Fatal(err)
	}
	u := pgtype.UUID{Bytes: [16]byte{0x55, 0x0e, 0x84, 0, 0xe2, 0x9b, 0x41, 0xd4, 0xa7, 0x16, 0x44, 0x66, 0x55, 0x44, 0, 0}, Valid: true}
	date := time.Date(2000, 1, 2, 0, 0, 0, 0, time.UTC)
	stamp := date.Add(3*time.Hour + 4*time.Minute + 5*time.Second + 123456*time.Microsecond)
	interval := pgtype.Interval{Months: 1, Days: 2, Microseconds: 11045123456, Valid: true}
	var amount pgtype.Numeric
	if err = amount.Scan("1234.50"); err != nil {
		t.Fatal(err)
	}
	columns := []string{"id", "flag", "u", "d", "tm", "ts", "tz", "iv", "small", "number", "doc"}
	n, err := c.CopyFrom(ctx, pgx.Identifier{"copy_pg_types"}, columns, pgx.CopyFromRows([][]any{{int32(1), true, u, date, pgtype.Time{Microseconds: 45296123456, Valid: true}, stamp, stamp, interval, int16(-123), amount, map[string]any{"x": true}}}))
	if err != nil || n != 1 {
		t.Fatalf("typed copy %d %v", n, err)
	}
	var id int32
	var flag bool
	var gotU pgtype.UUID
	var d, ts, tz time.Time
	var tm pgtype.Time
	var iv pgtype.Interval
	var small int16
	var gotAmount pgtype.Numeric
	var doc map[string]any
	err = c.QueryRow(ctx, "SELECT id,flag,u,d,tm,ts,tz,iv,small,number,doc FROM copy_pg_types").Scan(&id, &flag, &gotU, &d, &tm, &ts, &tz, &iv, &small, &gotAmount, &doc)
	if err != nil || id != 1 || !flag || gotU != u || !d.Equal(date) || tm.Microseconds != 45296123456 || !ts.Equal(stamp) || !tz.Equal(stamp) || iv != interval || small != -123 || doc["x"] != true {
		t.Fatalf("typed COPY results id=%d flag=%v uuid=%v date=%v time=%v ts=%v tz=%v iv=%v small=%d numeric=%v json=%v error=%v", id, flag, gotU, d, tm, ts, tz, iv, small, gotAmount, doc, err)
	}
	for _, format := range []string{"binary", "text", "csv"} {
		var out bytes.Buffer
		if _, err = c.PgConn().CopyTo(ctx, &out, "COPY copy_pg_types TO STDOUT (FORMAT "+format+")"); err != nil {
			t.Fatal(err)
		}
		if _, err = c.Exec(ctx, "DELETE FROM copy_pg_types"); err != nil {
			t.Fatal(err)
		}
		if _, err = c.PgConn().CopyFrom(ctx, bytes.NewReader(out.Bytes()), "COPY copy_pg_types FROM STDIN (FORMAT "+format+")"); err != nil {
			t.Fatalf("%s roundtrip: %v data=%x", format, err, out.Bytes())
		}
		var text string
		if err = c.QueryRow(ctx, "SELECT number::text FROM copy_pg_types").Scan(&text); err != nil || text != "1234.50" {
			t.Fatalf("%s decimal %q %v", format, text, err)
		}
	}
}

func TestCopyFormatErrorsAreAtomic(t *testing.T) {
	s := startTestServer(t, "tcp", Config{MaxPacketBytes: 1024})
	c := copyConnection(t, s)
	ctx := context.Background()
	if _, err := c.Exec(ctx, "CREATE TABLE invalid_copy(a TEXT,b TEXT)"); err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		format string
		data   []byte
	}{
		{"text", []byte("a\tb\r\nx\ty\n")},
		{"csv", []byte("a,b\r\nx,y\n")},
		{"csv", []byte("ok,row\n\"unfinished,row\n")},
		{"text", []byte("ok\trow\nx\n")},
		{"text", []byte("ok\trow\nx\ty\textra\n")},
		{"text", []byte("ok\trow\ninvalid\t\\000\n")},
		{"text", []byte("ok\trow\ninvalid\t\\xff\n")},
		{"csv", []byte("ok,row\n" + strings.Repeat("x", 1025) + ",y\n")},
	}
	// Independent PostgreSQL binary shape; invalid text payloads remain invalid
	// even though the outer stream is binary rather than escaped text.
	for _, payload := range [][]byte{{0}, {255}} {
		data := append([]byte{}, copySignature...)
		data = append(data, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 1, 'x', 0, 0, 0, 1)
		data = append(data, payload...)
		data = append(data, 255, 255)
		cases = append(cases, struct {
			format string
			data   []byte
		}{"binary", data})
	}
	for _, tc := range cases {
		// Split the input into small CopyData packets so the record limit, rather
		// than the independently enforced packet limit, rejects oversized records.
		reader := &shortCopyReader{Reader: bytes.NewReader(tc.data), limit: 64}
		if _, err := c.PgConn().CopyFrom(ctx, reader, "COPY invalid_copy FROM STDIN (FORMAT "+tc.format+")"); err == nil {
			t.Fatalf("accepted %s %q", tc.format, tc.data)
		}
		var n int
		if err := c.QueryRow(ctx, "SELECT count(*) FROM invalid_copy").Scan(&n); err != nil || n != 0 {
			t.Fatalf("%s rollback count %d error %v", tc.format, n, err)
		}
	}
	for _, q := range []string{"COPY t FROM STDIN (FORMAT csv HEADER true)", "COPY t FROM STDIN (FORMAT csv,)", "COPY t FROM STDIN (FORMAT csv, FORMAT text)", "COPY t FROM STDIN (FORMAT csv, FORCE_NULL(a), FORCE_NULL(b))"} {
		if _, err := parseCopy(q); err == nil {
			t.Fatalf("malformed COPY options accepted: %s", q)
		}
	}
}

type shortCopyReader struct {
	io.Reader
	limit int
}

func (r *shortCopyReader) Read(out []byte) (int, error) {
	if len(out) > r.limit {
		out = out[:r.limit]
	}
	return r.Reader.Read(out)
}

func TestCopyOutputSnapshotAndBlockedWriteCancellation(t *testing.T) {
	s := startTestServer(t, "tcp", Config{})
	if _, err := s.database.Exec("CREATE TABLE copy_snapshot(id BIGINT PRIMARY KEY,v TEXT);INSERT INTO copy_snapshot VALUES(1,'before'),(2,'before'),(3,'before')"); err != nil {
		t.Fatal(err)
	}
	for _, cancelled := range []bool{false, true} {
		t.Run(fmt.Sprint(cancelled), func(t *testing.T) {
			session, err := s.database.NewSession()
			if err != nil {
				t.Fatal(err)
			}
			defer session.Close()
			if err = session.SetPostgresMode(true); err != nil {
				t.Fatal(err)
			}
			serverConn, readerConn := net.Pipe()
			defer serverConn.Close()
			defer readerConn.Close()
			_ = readerConn.SetDeadline(time.Now().Add(5 * time.Second))
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			client := &client{server: s.server, session: session, conn: serverConn, id: 99}
			command, err := parseCopy("COPY copy_snapshot TO STDOUT")
			if err != nil {
				t.Fatal(err)
			}
			type outcome struct {
				count int64
				err   error
			}
			done := make(chan outcome, 1)
			go func() { count, e := client.runCopy(ctx, command); done <- outcome{count, e} }()
			frontend := pgproto3.NewFrontend(readerConn, readerConn)
			if m, e := frontend.Receive(); e != nil {
				t.Fatal(e)
			} else if _, ok := m.(*pgproto3.CopyOutResponse); !ok {
				t.Fatalf("%T", m)
			}
			if cancelled {
				// net.Pipe gives a deterministic blocked writer; CancelRequest uses the
				// same context cancellation path in a real network connection.
				cancel()
				select {
				case result := <-done:
					if !errors.Is(result.err, context.Canceled) {
						t.Fatalf("cancel result %v", result)
					}
				case <-time.After(5 * time.Second):
					t.Fatal("COPY output remained blocked after cancellation")
				}
				if session.InTransaction() {
					t.Fatal("cancelled COPY transaction remains active")
				}
				if _, err = session.Exec("SELECT 1"); err != nil {
					t.Fatal(err)
				}
				return
			}
			first, err := frontend.Receive()
			if err != nil {
				t.Fatal(err)
			}
			if data, ok := first.(*pgproto3.CopyData); !ok || !bytes.Contains(data.Data, []byte("before")) {
				t.Fatalf("first data %T %v", first, first)
			}
			if _, err = s.database.Exec("UPDATE copy_snapshot SET v='after'"); err != nil {
				t.Fatal(err)
			}
			n := 1
			for {
				message, e := frontend.Receive()
				if e != nil {
					t.Fatal(e)
				}
				if _, ok := message.(*pgproto3.CopyDone); ok {
					break
				}
				data, ok := message.(*pgproto3.CopyData)
				if !ok || !bytes.Contains(data.Data, []byte("before")) {
					t.Fatalf("snapshot output %T %v", message, message)
				}
				n++
			}
			if result := <-done; result.err != nil || result.count != 3 || n != 3 {
				t.Fatalf("COPY output %v rows=%d", result, n)
			}
		})
	}
}

// Captured independently through pgconn.CopyFrom/CopyTo against PostgreSQL
// 16.15, including error SQLSTATEs. Ordinary tests need no reference server.
func TestCopyPostgres16ReferenceFixtures(t *testing.T) {
	data, err := os.ReadFile("testdata/copy-postgres16.json")
	if err != nil {
		t.Fatal(err)
	}
	var cases []struct{ Name, Options, Input, Output, Code, Error string }
	if err = json.Unmarshal(data, &cases); err != nil {
		t.Fatal(err)
	}
	s := startTestServer(t, "tcp", Config{})
	c := copyConnection(t, s)
	ctx := context.Background()
	if _, err = c.Exec(ctx, "CREATE TABLE copy_oracle(id BIGINT,a TEXT,b TEXT)"); err != nil {
		t.Fatal(err)
	}
	for _, tc := range cases {
		t.Run(tc.Name, func(t *testing.T) {
			if _, err = c.Exec(ctx, "DELETE FROM copy_oracle"); err != nil {
				t.Fatal(err)
			}
			_, copyErr := c.PgConn().CopyFrom(ctx, strings.NewReader(tc.Input), "COPY copy_oracle FROM STDIN "+tc.Options)
			if tc.Code != "" {
				var pe *pgconn.PgError
				if !errors.As(copyErr, &pe) || pe.Code != tc.Code {
					t.Fatalf("COPY error %v, want SQLSTATE %s", copyErr, tc.Code)
				}
				var n int
				if err = c.QueryRow(ctx, "SELECT count(*) FROM copy_oracle").Scan(&n); err != nil || n != 0 {
					t.Fatalf("failed COPY count %d %v", n, err)
				}
				return
			}
			if copyErr != nil {
				t.Fatal(copyErr)
			}
			var out bytes.Buffer
			if _, err = c.PgConn().CopyTo(ctx, &out, "COPY copy_oracle TO STDOUT (FORMAT csv)"); err != nil {
				t.Fatal(err)
			}
			if out.String() != tc.Output {
				t.Fatalf("COPY output %q, PostgreSQL16 %q", out.String(), tc.Output)
			}
		})
	}
}
