"""Regenerate the deterministic SQLite join/LIKE fixtures; no third-party packages."""
import sqlite3, random, json, pathlib
output = pathlib.Path(__file__).resolve().parent
rng = random.Random(91)
c = sqlite3.connect(':memory:')

def lit(v):
    if v is None:
        return 'NULL'
    if isinstance(v, str):
        return "'" + v.replace("'", "''") + "'"
    if isinstance(v, bytes):
        return "X'" + v.hex() + "'"
    return str(v)
schema = 'CREATE TABLE a(id INTEGER PRIMARY KEY,k INTEGER,v INTEGER,s TEXT);CREATE TABLE b(id INTEGER PRIMARY KEY,k INTEGER,v INTEGER,s TEXT);CREATE TABLE c(id INTEGER PRIMARY KEY,k INTEGER,v INTEGER,s TEXT);CREATE TABLE z(id INTEGER PRIMARY KEY,k INTEGER);CREATE TABLE r(id INTEGER PRIMARY KEY,k REAL,v INTEGER);CREATE TABLE u(id INTEGER PRIMARY KEY,t TEXT,x INTEGER);CREATE TABLE blobs(id INTEGER PRIMARY KEY,k BLOB,x INTEGER);CREATE TABLE cp(x TEXT,y INTEGER,v INTEGER,PRIMARY KEY(x,y));CREATE TABLE uq(id INTEGER PRIMARY KEY,t TEXT UNIQUE);'
strings = ['', 'a', 'a!', 'a:', 'aa', 'a0', 'a9', 'A', 'b', 'b:', 'Ä', 'ä', '一', 'x\ny', '%', '_', '\\', 'a\\', "b'c"]
setup = [schema]
c.executescript(schema)
for t in ['a', 'b', 'c']:
    for i in range(1, 91):
        vals = [i, rng.choice([None, -2, -1, 0, 1, 2, 3, 4, 58, 14848]), rng.choice([None, 0, 1, 2, 5, 8]), rng.choice(strings)]
        q = 'INSERT INTO ' + t + ' VALUES(' + ','.join(map(lit, vals)) + ');'
        setup.append(q)
        c.execute(q)
for i, x in enumerate([None, -2.0, -1.0, 0.0, 1.0, 2.0, 3.0, 58.0, 9007199254740992.0], 1):
    q = f'INSERT INTO r VALUES({i},{lit(x)},{i})'
    setup.append(q + ';')
    c.execute(q)
for i, s in enumerate(strings, 1):
    for t, q in [('u', f'INSERT INTO u VALUES({i},{lit(s)},{i})'), ('blobs', f'INSERT INTO blobs VALUES({i},{lit(s.encode())},{i})'), ('cp', f'INSERT INTO cp VALUES({lit(s)},{i},{i})'), ('uq', f'INSERT INTO uq VALUES({i},{lit(s)})')]:
        setup.append(q + ';')
        c.execute(q)
idx = 'CREATE INDEX ak ON a(k);CREATE INDEX bk ON b(k);CREATE INDEX bkv ON b(k,v);CREATE INDEX ck ON c(k);CREATE INDEX rk ON r(k);CREATE INDEX ut ON u(t);CREATE INDEX blobk ON blobs(k);CREATE INDEX cpx ON cp(x,y);ANALYZE;'
setup.append(idx)
c.executescript(idx)
cases = []

def add(q):
    try:
        rows = [list(row) for row in c.execute(q)]
    except Exception as e:
        print('GEN FAIL', q, e)
        raise
    cases.append({'query': q, 'rows': rows})
for mode in ['JOIN', 'LEFT JOIN', 'CROSS JOIN']:
    for n in range(45):
        v = rng.randrange(0, 6)
        v2 = rng.randrange(0, 6)
        on = 'a.k=b.k' if mode != 'CROSS JOIN' else '1=1'
        if n % 4 == 0:
            on += ' AND a.v=1'
        if n % 4 == 1:
            on += ' AND b.v=2'
        wh = [f'(a.v={v} OR b.v={v2})', f'(a.k IN(1,2,3) AND b.v={v}) OR (a.k IN(-1,0,2) AND b.v={v2})', 'b.id IS NULL', 'a.k IS NULL OR b.k IS NULL', f'a.v={v} AND b.v={v2}'][n % 5]
        add(f'SELECT a.id,b.id,a.k,b.v FROM a {mode} b ON {on} WHERE {wh} ORDER BY a.id,b.id LIMIT 50 OFFSET {n % 3}')
for m1 in ['JOIN', 'LEFT JOIN']:
    for m2 in ['JOIN', 'LEFT JOIN']:
        for pred in ['b.v=1', 'a.v=2', 'b.v IS NULL', 'c.v=2', '1=0', '1=1']:
            add(f'SELECT a.id,b.id,c.id FROM a {m1} b ON a.k=b.k {m2} c ON b.k=c.k AND {pred} WHERE a.id<12 ORDER BY a.id,b.id,c.id')
for expr in ['a.v', 'a.k+1', 'CASE WHEN a.v=1 THEN 0 ELSE 1 END']:
    for join in ['JOIN', 'LEFT JOIN']:
        add(f'SELECT {expr} AS g,COUNT(b.id) AS n,SUM(b.v) AS total,MIN(b.id) AS lo,COUNT(DISTINCT b.k) AS d FROM a {join} b ON a.k=b.k GROUP BY g HAVING COUNT(b.id)>=0 ORDER BY g')
for op in ['=', '<', '<=', '>', '>=']:
    for value in strings:
        for table, col in [('u', 't'), ('cp', 'x'), ('uq', 't')]:
            add(f'SELECT COUNT(*) AS n,SUM(id) AS total FROM {table} WHERE {col}{op}{lit(value)}' if table != 'cp' else f'SELECT COUNT(*) AS n,SUM(y) AS total FROM cp WHERE x{op}{lit(value)}')
        add(f'SELECT SUM(x) AS total FROM blobs WHERE k{op}{lit(value.encode())}')
for q in ["SELECT SUM(x) FROM u WHERE t IN('a','a:','a','b') OR t>='a' AND t<='b'", "SELECT SUM(x) FROM u WHERE t BETWEEN 'a' AND 'b'", "SELECT SUM(x) FROM blobs WHERE k>=X'61' AND k<=X'623a'", 'SELECT a.id,b.id FROM a JOIN b ON a.k=b.k WHERE (a.v=1 AND b.v=2) OR (a.v=2 AND b.v=1) ORDER BY a.id,b.id', 'SELECT a.id,r.id FROM a JOIN r ON a.k=r.k ORDER BY a.id,r.id', 'SELECT COUNT(*) FROM a LEFT JOIN z ON a.k=z.k', 'SELECT COUNT(*) FROM z JOIN a ON z.k=a.k']:
    add(q)
for off in [0, 1, 2, 8, 4294967296]:
    for lim in [0, 1, 2, -1]:
        for agg in ['id', 'COUNT(*)']:
            add(f'SELECT a.id,EXISTS(SELECT {agg} FROM b WHERE b.k=a.k LIMIT {lim} OFFSET {off}) AS found FROM a ORDER BY a.id')
for q in ['SELECT a.id,(SELECT MIN(b.v) FROM b JOIN c ON b.k=c.k WHERE b.k=a.k) AS m FROM a ORDER BY a.id', 'SELECT a.id FROM a WHERE a.k IN(SELECT b.k FROM b WHERE b.v=1) ORDER BY a.id', 'SELECT a.id FROM a WHERE a.k NOT IN(SELECT b.k FROM b WHERE b.v=1) ORDER BY a.id', 'SELECT a.id FROM a WHERE EXISTS(SELECT 1 FROM b JOIN c ON b.k=c.k WHERE b.k=a.k AND c.v=2) ORDER BY a.id', 'SELECT a.id FROM a WHERE NOT EXISTS(SELECT 1 FROM b WHERE b.k=a.k AND b.v=2) ORDER BY a.id']:
    add(q)
for i in range(1200):
    text = ''.join((rng.choice(['a', 'b', 'A', '%', '_', '\\', 'Ä', 'ä', '一', '\n']) for _ in range(rng.randrange(0, 15))))
    pat = ''.join((rng.choice(['a', 'b', 'A', '%', '_', '\\', 'Ä', 'ä', '一', '\n']) for _ in range(rng.randrange(0, 10))))
    esc = '\\' if i % 2 else '!'
    add(f'SELECT {lit(text)} LIKE {lit(pat)} ESCAPE {lit(esc)} AS m')
(output / 'join_optimization_setup.sql').write_text('\n'.join(setup), encoding='utf-8')
(output / 'join_optimization_cases.json').write_text(json.dumps(cases, ensure_ascii=False, indent=2) + '\n', encoding='utf-8')
print('Generated', len(cases), 'SQLite differential cases')
