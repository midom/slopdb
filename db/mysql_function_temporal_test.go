package db_test

import (
	"slopdb/db"

	"strings"
	"testing"
)

// Answers were checked independently against Oracle MySQL 8.0.46 in UTC.
func TestMySQLTemporalFunctionAnswers(t *testing.T) {
	d, e := db.Open(t.TempDir(), nil)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { d.Close() })
	s := mysqlTypeSession(t, d)
	cases := []struct{ expression, want string }{
		{"YEAR('2024-02-29 12:34:56.123456')", "2024"}, {"MONTH('2024-02-29')", "2"}, {"DAY('2024-02-29')", "29"}, {"DAYOFMONTH('2024-02-29')", "29"},
		{"DAYOFWEEK('2024-02-29')", "5"}, {"DAYOFYEAR('2024-02-29')", "60"}, {"WEEKDAY('2024-02-29')", "3"}, {"QUARTER('2024-11-01')", "4"}, {"MONTHNAME('2024-02-29')", "February"}, {"DAYNAME('2024-02-29')", "Thursday"}, {"LAST_DAY('2024-02-01')", "2024-02-29"}, {"LAST_DAY('2001-02-00')", "2001-02-28"},
		{"WEEK('2016-01-01',0)", "0"}, {"WEEK('2016-01-01',1)", "0"}, {"WEEK('2016-01-01',2)", "52"}, {"WEEK('2016-01-01',3)", "53"}, {"WEEK('2016-01-01',4)", "0"}, {"WEEK('2016-01-01',5)", "0"}, {"WEEK('2016-01-01',6)", "52"}, {"WEEK('2016-01-01',7)", "52"}, {"WEEKOFYEAR('2016-01-01')", "53"}, {"YEARWEEK('2016-01-01',3)", "201553"},
		{"HOUR('-32:03:04.123456')", "32"}, {"MINUTE('-32:03:04.123456')", "3"}, {"SECOND('-32:03:04.123456')", "4"}, {"MICROSECOND('-32:03:04.123456')", "123456"}, {"TIME_TO_SEC('01:02:03.12')", "3723"}, {"SEC_TO_TIME(3661)", "01:01:01"}, {"SEC_TO_TIME(3661.25)", "01:01:01.25"}, {"SEC_TO_TIME('1.23')", "00:00:01.230000"}, {"SEC_TO_TIME(10000000.123)", "838:59:59.000"}, {"SEC_TO_TIME(1.9999999)", "00:00:02.000000"},
		{"MAKETIME(-12,34,56.123)", "-12:34:56.123"}, {"MAKEDATE(2024,60)", "2024-02-29"}, {"DATEDIFF('2024-03-01 01:00:00','2024-02-28 23:00:00')", "2"}, {"TIMEDIFF('2001-01-02 00:00:00','2001-01-01 00:00:00')", "24:00:00"}, {"TIMEDIFF('00:00:01.123','00:00:00.1')", "00:00:01.023"}, {"ADDTIME('2001-01-01 23:59:59.999999','00:00:00.000001')", "2001-01-02 00:00:00.000000"}, {"SUBTIME('02:00:00','03:00:00')", "-01:00:00"},
		{"TO_DAYS('2007-10-07')", "733321"}, {"FROM_DAYS(733321)", "2007-10-07"}, {"FROM_DAYS(3652424)", "9999-12-31"}, {"TO_SECONDS('2009-11-29 13:43:32')", "63426721412"}, {"PERIOD_ADD(200801,2)", "200803"}, {"PERIOD_DIFF(200802,200703)", "11"},
		{"DATE_ADD('2024-01-31',INTERVAL 1 MONTH)", "2024-02-29"}, {"DATE_SUB('2024-02-29',INTERVAL 1 YEAR)", "2023-02-28"}, {"ADDDATE('2024-02-28',2)", "2024-03-01"}, {"SUBDATE('2024-03-01',2)", "2024-02-28"}, {"DATE_ADD('2020-01-01',INTERVAL 1.5 DAY)", "2020-01-03"}, {"DATE_ADD('2020-01-01',INTERVAL '1.5' DAY)", "2020-01-02"}, {"DATE_ADD('2020-01-01',INTERVAL '1 2:3:4.5' DAY_MICROSECOND)", "2020-01-02 02:03:04.500000"}, {"DATE_ADD('2020-01-01',INTERVAL '1' SECOND_MICROSECOND)", "2020-01-01 00:00:00.100000"}, {"DATE_ADD('2020-01-01',INTERVAL '1:2' MINUTE_MICROSECOND)", "2020-01-01 00:00:01.200000"},
		{"TIMESTAMPADD(QUARTER,1,'2024-01-31')", "2024-04-30"}, {"TIMESTAMPDIFF(YEAR,'2002-05-01','2001-01-01')", "-1"}, {"TIMESTAMPDIFF(MONTH,'2024-01-31','2024-02-29')", "0"}, {"TIMESTAMPDIFF(MICROSECOND,'2024-01-01 23:59:59.999999','2024-01-02 00:00:00')", "1"}, {"EXTRACT(YEAR_MONTH FROM '2024-02-29')", "202402"}, {"EXTRACT(DAY_MICROSECOND FROM '2024-02-29 12:34:56.123456')", "29123456123456"}, {"EXTRACT(HOUR FROM '-32:03:04')", "-32"},
		{"DATE_FORMAT('2024-02-29 13:04:05.123456','%W %M %D %Y %j %r %f')", "Thursday February 29th 2024 060 01:04:05 PM 123456"}, {"TIME_FORMAT('-01:02:03','%i %H %s %p')", "-02 01 03 AM"}, {"DATE_FORMAT('2016-01-01','%X-%V %x-%v')", "2015-52 2015-53"}, {"STR_TO_DATE('February 29, 2024 01:04:05 PM','%M %d, %Y %r')", "2024-02-29 13:04:05"}, {"STR_TO_DATE('12:01:02','%h:%i:%s')", "00:01:02"}, {"STR_TO_DATE('2024-060','%Y-%j')", "2024-02-29"}, {"STR_TO_DATE('200442 Monday','%X%V %W')", "2004-10-18"}, {"FROM_UNIXTIME(1)", "1970-01-01 00:00:01"}, {"FROM_UNIXTIME(1.25,'%Y-%m-%d %T.%f')", "1970-01-01 00:00:01.250000"},
	}
	for _, c := range cases {
		t.Run(c.expression, func(t *testing.T) {
			r, e := s.Exec("SELECT " + c.expression + " AS v")
			if e != nil {
				t.Fatal(e)
			}
			v := r.Rows[0]["v"]
			if v.Null || v.String() != c.want {
				t.Fatalf("got %s (%v) want %q", v.String(), v, c.want)
			}
		})
	}
	for _, expression := range []string{"YEAR(NULL)", "YEAR('bad')", "YEAR('2026-02-31')", "YEAR('0000-00-00')", "DAYNAME('2001-00-00')", "DATE_ADD('2024-02-31',INTERVAL 1 DAY)", "DATE_ADD('9999-12-31',INTERVAL 1 DAY)", "DATE_ADD('2020-01-01',INTERVAL 18446744073709551615 YEAR)", "MAKETIME(1,60,0)", "STR_TO_DATE('2024-02-30','%Y-%m-%d')", "STR_TO_DATE('2024-01-00','%Y-%m-%d')", "TIME_FORMAT('12:30:00','%Y')"} {
		r, e := s.Exec("SELECT " + expression + " AS v")
		if e != nil {
			t.Errorf("%s: %v", expression, e)
		} else if !r.Rows[0]["v"].Null {
			t.Errorf("%s: got %v wantNULL", expression, r.Rows)
		}
	}
	for _, expression := range []string{"YEAR()", "EXTRACT(BOGUS FROM '2024-01-01')", "DATE_ADD('2024-01-01',INTERVAL 1 BOGUS)", "CURTIME(7)"} {
		if _, e := s.Exec("SELECT " + expression); e == nil {
			t.Errorf("accepted %s", expression)
		}
	}
}

func TestMySQLTemporalPreparedPersistenceAndClock(t *testing.T) {
	path := t.TempDir()
	d, e := db.Open(path, nil)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { d.Close() })
	s := mysqlTypeSession(t, d)
	mysqlTypeExec(t, s, "CREATE TABLE dates(id INTEGER PRIMARY KEY,d TEXT);INSERT INTO dates VALUES(1,'2024-01-31');CREATE VIEW calendar_view AS SELECT id,EXTRACT(YEAR FROM d) AS y,DATE_ADD(d,INTERVAL 1 MONTH) AS shifted FROM dates")
	prepared, e := s.Prepare("SELECT DATE_ADD(?,INTERVAL ? MONTH) AS v")
	if e != nil {
		t.Fatal(e)
	}
	defer prepared.Close()
	for _, day := range []string{"2024-01-31", "2023-01-31"} {
		r, e := prepared.Exec(day, 1)
		if e != nil {
			t.Fatal(e)
		}
		want := "2024-02-29"
		if strings.HasPrefix(day, "2023") {
			want = "2023-02-28"
		}
		if r.Rows[0]["v"].Text != want {
			t.Fatal(r.Rows)
		}
	}
	r := mysqlTypeExec(t, s, "SELECT CURRENT_DATE AS d,UTC_DATE() AS u,CURRENT_TIME(6) AS t,UTC_TIMESTAMP(6) AS stamp,LOCALTIME(6) AS local,LOCALTIMESTAMP(6) AS localstamp")
	row := r.Rows[0]
	if row["d"].Text != row["u"].Text || row["stamp"].Text != row["local"].Text || row["stamp"].Text != row["localstamp"].Text || row["t"].Text != row["stamp"].Text[11:] {
		t.Fatal(row)
	}
	if e = s.Close(); e != nil {
		t.Fatal(e)
	}
	if e = d.Close(); e != nil {
		t.Fatal(e)
	}
	d, e = db.Open(path, nil)
	if e != nil {
		t.Fatal(e)
	}
	r, e = d.Exec("SELECT y,shifted FROM calendar_view")
	if e != nil || r.Rows[0]["y"].Int != 2024 || r.Rows[0]["shifted"].Text != "2024-02-29" {
		t.Fatal(r, e)
	}
}
