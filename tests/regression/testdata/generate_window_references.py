"""Regenerate window fixtures independently with Python's SQLite driver."""
import json
import pathlib
import sqlite3

setup = "CREATE TABLE t(id INTEGER PRIMARY KEY,g TEXT,v INTEGER); INSERT INTO t VALUES(1,'a',10),(2,'a',10),(3,'a',20),(4,'b',NULL),(5,'b',7),(6,'b',7),(7,'b',9),(8,'c',NULL),(9,'c',NULL);"
db = sqlite3.connect(":memory:")
db.executescript(setup)
queries = []
for unit in ("ROWS", "RANGE", "GROUPS"):
    for start, end in (("UNBOUNDED PRECEDING", "CURRENT ROW"), ("CURRENT ROW", "UNBOUNDED FOLLOWING"), ("2 PRECEDING", "1 FOLLOWING"), ("2 PRECEDING", "1 PRECEDING"), ("1 FOLLOWING", "2 FOLLOWING"), ("CURRENT ROW", "CURRENT ROW"), ("4294967296 PRECEDING", "4294967296 FOLLOWING")):
        for ordering in ("v", "v DESC NULLS LAST"):
            spec = f"PARTITION BY g ORDER BY {ordering} {unit} BETWEEN {start} AND {end}"
            queries.append(f"SELECT id,COUNT(*) OVER({spec}) AS n,SUM(v) OVER({spec}) AS s,AVG(v) OVER({spec}) AS a,MIN(v) OVER({spec}) AS lo,MAX(v) OVER({spec}) AS hi,FIRST_VALUE(v) OVER({spec}) AS f,LAST_VALUE(v) OVER({spec}) AS l,NTH_VALUE(v,2) OVER({spec}) AS nth FROM t ORDER BY id")
queries.extend([
    "SELECT id,RANK() OVER(ORDER BY v) AS r,DENSE_RANK() OVER(ORDER BY v) AS d,ROW_NUMBER() OVER(ORDER BY v,id) AS n FROM t ORDER BY id",
    "SELECT id,LAG(v,0,-1) OVER(ORDER BY id) AS prev,LEAD(v,9223372036854775807,-2) OVER(ORDER BY id) AS next FROM t ORDER BY id",
    "SELECT id,SUM(v) OVER() AS s,AVG(v) OVER() AS a,COUNT(v) OVER() AS n FROM t ORDER BY id",
    "SELECT g,COUNT(*) AS n,SUM(COUNT(*)) OVER(ORDER BY g) AS running,RANK() OVER(ORDER BY SUM(v) DESC) AS r FROM t GROUP BY g HAVING COUNT(*)>1 ORDER BY g",
    "SELECT x.id,ROW_NUMBER() OVER(PARTITION BY x.g ORDER BY x.id,y.id) AS n FROM t x LEFT JOIN t y ON x.id=y.id WHERE x.id<5 ORDER BY x.id",
    "SELECT id,SUM(v) OVER w AS s,COUNT(*) OVER(w ROWS CURRENT ROW) AS n FROM t WINDOW base AS(PARTITION BY g),w AS(base ORDER BY v) ORDER BY id",
    "SELECT v AS id,ROW_NUMBER() OVER(ORDER BY id) AS n FROM t ORDER BY n",
    "SELECT g AS v,COUNT(*) OVER(ORDER BY v RANGE CURRENT ROW) AS n FROM t ORDER BY id",
    "SELECT id,COUNT(*) OVER(ORDER BY id RANGE BETWEEN 0.5 PRECEDING AND 1.5 FOLLOWING) AS n FROM t ORDER BY id",
])
result = {"reference_engine": "SQLite", "version": sqlite3.sqlite_version, "setup": setup, "cases": [{"query": q, "rows": db.execute(q).fetchall()} for q in queries]}
pathlib.Path(__file__).with_name("window_reference.json").write_text(json.dumps(result, indent=2)+"\n")
