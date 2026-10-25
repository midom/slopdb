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
errors=["concat()", "concat_ws(',')", "chr(0)", "chr(55296)", "split_part('a','.',0)", "format('%I',NULL)", "format('%s')", "format('%q','x')", "decode('x','hex')", "get_byte(decode('00','hex'),1)", "set_bit(decode('00','hex'),0,2)", "convert_from(decode('ff','hex'),'UTF8')"]
output=[]
for expr in cases+errors:
    p=subprocess.run(['psql','-X','-At','-v','ON_ERROR_STOP=1','-c',"SELECT json_build_object('value',("+expr+")::text)"],text=True,capture_output=True)
    if expr in errors:
        if not p.returncode: raise RuntimeError('Expected PostgreSQL error: '+expr)
        output.append({'sql':expr,'error':True})
    else:
        if p.returncode: raise RuntimeError(expr+'\n'+p.stderr)
        output.append({'sql':expr,**json.loads(p.stdout)})
pathlib.Path(__file__).with_name('postgres_functions.json').write_text(json.dumps({'source':'PostgreSQL 16.15','cases':output},ensure_ascii=False,indent=2)+'\n')
