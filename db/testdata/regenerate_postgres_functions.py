#!/usr/bin/env python3
"""Regenerate independent scalar answers from PostgreSQL 16 (never SlopDB).
PGHOST, PGPORT, PGUSER, PGDATABASE select the reference instance.
"""
import json, pathlib, subprocess
cases = [
"concat(NULL)", "concat('a',NULL,42,'b')", "concat_ws(',',NULL,'a','','b')", "concat_ws(NULL,'a')",
"char_length('é🪐')", "length('é🪐')", "octet_length('é🪐')", "bit_length('é🪐')", "ascii('é')", "ascii('')", "chr(129680)",
"strpos('aé🪐','🪐')", "strpos('abc','')", "strpos('abc','x')", "left('café🪐',-2)", "right('café🪐',-2)", "left('abc',-99)", "right('abc',99)",
"repeat('é',3)", "repeat('é',-3)", "reverse('café🪐')", "initcap('FOO_bar 123ABC café')", "translate('éabcé','éac','XY')", "split_part('a.b.c','.',-2)", "split_part('abc','',-1)", "split_part('a.b','.',5)",
"lpad('é',5,'xy')", "rpad('é',5,'xy')", "lpad('abc',5,'')", "rpad('abc',-1,'x')", "lpad('abc',2)",
"btrim('xyabxy','xy')", "ltrim('xyabxy','xy')", "rtrim('xyabxy','xy')", "btrim(' a ')",
"quote_ident('hello')", "quote_ident('Hello')", "quote_ident('select')", "quote_ident('a\"b')", "quote_literal('a''b')", "quote_nullable(NULL)", "quote_literal(NULL)",
"format('%s %I %L','a','b c','d''e')", "format('%2$s %1$s %s','a','b','c')", "format('|%*s|',-5,'é')", "format('%s:%L',NULL,NULL)", "format('%% %1$*2$s','x',5)",
"encode(decode('00ff615c','hex'),'escape')", "encode(decode('YWJj','base64'),'hex')", "get_byte(decode('ff00','hex'),0)", "get_bit(decode('01','hex'),0)", "encode(set_byte(decode('0000','hex'),1,256),'hex')", "encode(set_bit(decode('0000','hex'),9,1),'hex')", "convert_from(decode('c3a9','hex'),'UTF8')", "encode(convert_to('é','UTF8'),'hex')",
"length(NULL)", "translate(NULL,'a','b')", "split_part(NULL,'.',1)", "format(NULL,'x')",
]
json_cases=[
"json_build_array()", "jsonb_build_array(1,'a',NULL,CAST('{\"x\":true}' AS JSON))", "json_build_object('n',42,'text','é','null',NULL)", "jsonb_build_object('a',1,'a',2)", "to_json(CAST('é' AS TEXT))", "to_jsonb(1234567890123456789::bigint)", "json_extract_path(CAST('{\"a\":[1,2,3]}' AS JSON),'a','-1')", "jsonb_extract_path(CAST('{\"a\":[1,2,3]}' AS JSONB),'a')",
"jsonb_set('{\"a\":[1,2,3]}','{a,-1}','9')", "jsonb_set('{\"a\":1}','{missing,x}','9')", "jsonb_set('[1,2]','{-9}','0')", "jsonb_set('[1,2]','{9}','0',false)", "jsonb_insert('[1,2]','{0}','3')", "jsonb_insert('[1,2]','{0}','3',true)", "jsonb_insert('{\"a\":1}','{b}','2')", "jsonb_strip_nulls('{\"a\":null,\"b\":[null,{\"c\":null,\"d\":2}]}')",
]
# Cast syntax below also exercises PostgreSQL declared type names.
json_cases=[x.replace('::text','').replace('::bigint','') for x in json_cases]
float_cases=["sqrt(4)","cbrt(-27)","ln(1)","log(100)","log(2,8)","sin(0)","sind(180)","cosd(90)","asind(1)","atan2d(1,0)","degrees(pi())","power(2,10)"]
cases += ["json_typeof('null')", "jsonb_typeof('{\"a\":true}')", "json_array_length('[1,null,3]')", "json_extract_path_text('{\"a\":[1,null,3]}','a','1')", "json_extract_path_text('{\"a\":[1,null,3]}','a','-1')", "jsonb_extract_path_text('{\"a\":1}','missing')", "to_json(CAST(NULL AS TEXT))", "jsonb_typeof(NULL)",
"abs(CAST('-12345678901234567890.1234' AS DECIMAL(30,4)))", "ceil(CAST('-1.2' AS DECIMAL(4,1)))", "floor(CAST('-1.2' AS DECIMAL(4,1)))", "round(CAST('2.5' AS DECIMAL(3,1)))", "round(CAST('-123.456' AS DECIMAL(9,3)),-1)", "trunc(CAST('-123.456' AS DECIMAL(9,3)),2)", "scale(CAST('8.4100' AS DECIMAL(9,4)))", "min_scale(CAST('8.4100' AS DECIMAL(9,4)))", "trim_scale(CAST('8.4100' AS DECIMAL(9,4)))", "gcd(1071,462)", "lcm(1071,462)", "gcd(CAST('1.2' AS DECIMAL(3,1)),CAST('0.9' AS DECIMAL(3,1)))", "mod(-7,3)", "div(7,3)", "factorial(20)", "width_bucket(5,0,10,5)", "width_bucket(5,10,0,5)", "width_bucket(10,0,10,5)", "width_bucket(-1,0,10,5)", "sqrt(NULL)", "gcd(NULL,1)"]
cases += json_cases + float_cases
cases += ["to_timestamp(0)", "to_timestamp(-0.5)", "to_timestamp(946684800.123456)", "to_date('05 Dec 2000','DD Mon YYYY')", "to_timestamp('2024-02-29 13:02:03.123456','YYYY-MM-DD HH24:MI:SS.US')", "to_char(CAST('2024-02-29 13:02:03.123456' AS TIMESTAMP),'YYYY-MM-DD HH24:MI:SS.US')", "to_char(CAST('2024-02-29' AS DATE),'Day, DD Month YYYY')", "to_char(CAST('2024-02-29' AS DATE),'FMDay DDth FMMonth YYYY Q IW IYYY D ID DDD')", "make_date(2024,2,29)", "make_time(13,2,3.123456)", "make_timestamp(2024,2,29,13,2,3.123456)", "date_trunc('month',CAST('2024-02-29 13:02:03' AS TIMESTAMP))", "date_trunc('week',CAST('2024-02-29 13:02:03' AS TIMESTAMP))", "date_trunc('century',CAST('2000-02-29 13:02:03' AS TIMESTAMP))", "date_trunc('hour',CAST('2024-02-29 13:02:03+00' AS TIMESTAMPTZ))", "date_trunc('day',CAST('2024-02-29 13:02:03+00' AS TIMESTAMPTZ),'America/New_York')", "date_part('year',CAST('2024-02-29 13:02:03' AS TIMESTAMP))", "date_part('isodow',CAST('2024-02-29' AS DATE))", "date_part('isoyear',CAST('2023-01-01' AS DATE))", "date_part('microseconds',CAST('2024-02-29 13:02:03.123456' AS TIMESTAMP))", "to_char(CAST(NULL AS TIMESTAMP),'YYYY')", "date_trunc(NULL,CAST('2024-02-29' AS DATE))"]
cases += ["substring('abcdef',0,3)", "substring('abcdef',-1,3)", "substring('abcdef',1,99)", "substr('abcdef',-1)", "substring('foobar','o(.)b')", "substring('foobar','x')", "position('é' in 'aéx')", "overlay('café' placing 'XX' from 2 for 2)", "overlay('abc' placing 'x' from 9)", "overlay('abc' placing 'x' from 2 for 0)", "quote_ident('user')", "quote_ident('value')", "quote_ident('boolean')", "regexp_count('a','a*')", "regexp_count('aa','(a*)')", "regexp_count('abc','')", "regexp_count('abc','a',99)", "regexp_substr('aa','(a*)',1,2,'',1)", "regexp_substr('ab','(a)|(b)',1,1,'',2)", "regexp_substr('aéé','é+',2)", "regexp_substr('aBc','(b)(c)',1,1,'i',2)", "regexp_instr('aéé','é+',1,1,1)", "regexp_instr('abcdef','(c.)(ef)',1,1,0,'',2)", "regexp_instr('abc','',4)", "regexp_instr('abc','x')", "regexp_instr('abc','(a)|(b)',1,1,0,'',2)", "regexp_replace('a','a*','x','g')", "regexp_replace('ab12cd34','[0-9]+','x')", "regexp_replace('ab12cd34','[0-9]+','x','g')", "regexp_replace('ab12cd34','[0-9]+','x',1,2)", "regexp_replace('aaa','a+','x',2)", "regexp_replace('éabc','a','x',2,0)", "regexp_replace('abc','(a)(b)',E'\\\\2\\\\1-\\\\&-$1')", "regexp_replace('abc','a',E'\\\\q')", "regexp_replace('', '', 'x', 'g')", "regexp_replace('a.b','.', 'x','qg')", "regexp_replace('Aab','a','x','ig')", "regexp_like('abc','B','i')", "regexp_like('abc','B')", "regexp_like(E'a\\nb','a.b')", "regexp_like(E'a\\nb','a.b','n')", "regexp_count('abc','a #comment',1,'x')", "regexp_replace(NULL,'a','b')", "substring(NULL,1)"]
errors=["concat()", "concat_ws(',')", "chr(0)", "chr(55296)", "split_part('a','.',0)", "format('%I',NULL)", "format('%s')", "format('%q','x')", "decode('x','hex')", "get_byte(decode('00','hex'),1)", "set_bit(decode('00','hex'),0,2)", "convert_from(decode('ff','hex'),'UTF8')"]
output=[]
errors += ["json_build_object('a')", "json_build_object(NULL,1)", "json_array_length('{}')", "jsonb_insert('{\"a\":1}','{a}','2')", "jsonb_set('1','{a}','2')", "jsonb_set('[1]','{x}','2')", "sqrt(-1)", "ln(0)", "acos(2)", "log(1,2)", "mod(1,0)", "div(1,0)", "factorial(-1)", "width_bucket(1,0,0,5)", "width_bucket(1,0,10,0)"]
errors += ["make_date(2023,2,29)", "make_time(25,0,0)", "make_timestamp(2024,2,30,0,0,0)", "date_trunc('invalid',CAST('2024-02-29' AS DATE))", "date_part('invalid',CAST('2024-02-29' AS DATE))", "to_date('2024-02-30','YYYY-MM-DD')"]
errors += ["substring('abc',1,-1)", "overlay('abc' placing 'x' from 0)", "regexp_count('a','a',0)", "regexp_substr('a','a',1,0)", "regexp_instr('a','a',1,1,2)", "regexp_like('a','a','g')", "regexp_replace('a','[','x')"]
for expr in cases+errors:
    p=subprocess.run(['psql','-X','-At','-v','ON_ERROR_STOP=1','-c',"SELECT json_build_object('value',("+expr+")::text)"],text=True,capture_output=True)
    if expr in errors:
        if not p.returncode: raise RuntimeError('Expected PostgreSQL error: '+expr)
        output.append({'sql':expr,'error':True})
    else:
        if p.returncode: raise RuntimeError(expr+'\n'+p.stderr)
        answer=json.loads(p.stdout)
        if expr.startswith('regexp_like('): answer['value']={'true':'1','false':'0'}.get(answer['value'],answer['value'])
        output.append({'sql':expr,**answer,**({'json':True} if expr in json_cases else {}),**({'float':True} if expr in float_cases or expr.startswith('date_part(') else {})})
pathlib.Path(__file__).with_name('postgres_functions.json').write_text(json.dumps({'source':'PostgreSQL 16.15','cases':output},ensure_ascii=False,indent=2)+'\n')
