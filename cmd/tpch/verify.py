#!/usr/bin/env python3
"""Validate the query pack and SlopDB report against SQLite on the same .tbl data.

No Python dependencies beyond the standard library are required. Native DBGEN
qualification answers, if present, also validate the dialect translations.
Query order is checked using each query's declared ORDER BY keys; ties can
be reordered. Floating point results use rel_tol=1e-8, abs_tol=1e-6.
"""
import argparse
import hashlib
import json
import math
from pathlib import Path
import sqlite3
import time

ROOT = Path(__file__).resolve().parent
TABLES = ("region", "nation", "supplier", "customer", "part", "partsupp", "orders", "lineitem")
# (output column position, descending), including every declared sort key.
ORDER = {1: [(0, False), (1, False)], 2: [(0, True), (2, False), (1, False), (3, False)],
         3: [(1, True), (2, False)], 4: [(0, False)], 5: [(1, True)],
         7: [(0, False), (1, False), (2, False)], 8: [(0, False)], 9: [(0, False), (1, True)],
         10: [(2, True)], 11: [(1, True)], 12: [(0, False)], 13: [(1, True), (0, True)],
         15: [(0, False)], 16: [(3, True), (0, False), (1, False), (2, False)],
         18: [(4, True), (3, False)], 20: [(0, False)], 21: [(1, True), (0, False)], 22: [(0, False)]}


def order_error(n, rows):
    for i, (a, b) in enumerate(zip(rows, rows[1:])):
        for col, desc in ORDER.get(n, []):
            if a[col] == b[col]:
                continue
            if a[col] is None or b[col] is None:
                return f"unexpected NULL sort key at row {i}"
            if (a[col] < b[col]) == desc:
                return f"ORDER BY violation at rows {i} and {i+1}"
            break
    return None


def number(value):
    if isinstance(value, (float, int)):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value)
        except ValueError:
            pass
    return None


def cell_equal(a, b):
    if a == b:
        return True
    if a is None or b is None or isinstance(a, int) and isinstance(b, int):
        return False
    x, y = number(a), number(b)
    return x is not None and y is not None and math.isclose(x, y, rel_tol=1e-8, abs_tol=1e-6)


def compare(actual, expected):
    if len(actual) != len(expected):
        return f"row count: got {len(actual)}, expected {len(expected)}"
    # Full row matching permits only permutations of identical result sets.
    # TPC-H has nonunique ordering keys in several queries; use multiset matching
    # for comparison, then independently inspect sorted keys during diagnostics.
    unmatched = list(expected)
    for i, row in enumerate(actual):
        for j, reference in enumerate(unmatched):
            if len(row) == len(reference) and all(cell_equal(a, b) for a, b in zip(row, reference)):
                unmatched.pop(j)
                break
        else:
            return f"row {i}: {row!r} has no reference match"
    return None


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data", type=Path, required=True)
    parser.add_argument("--report", type=Path)
    parser.add_argument("--duckdb-report", type=Path, action="append", default=[], help="DuckDB Go result report; may be repeated")
    parser.add_argument("--out", type=Path)
    args = parser.parse_args()
    con = sqlite3.connect(":memory:")
    con.executescript((ROOT / "schema.sql").read_text())
    manifest_path = args.data / "manifest.json"
    manifest = json.loads(manifest_path.read_text()) if manifest_path.exists() else {}
    for table in TABLES:
        cols = con.execute(f"PRAGMA table_info({table})").fetchall()
        marks = ",".join("?" for _ in cols)
        path = args.data / f"{table}.tbl"
        if table in manifest.get("sha256", {}):
            digest = hashlib.sha256()
            with path.open("rb") as source:
                for block in iter(lambda: source.read(1024 * 1024), b""):
                    digest.update(block)
            if digest.hexdigest() != manifest["sha256"][table]:
                raise ValueError(f"hash mismatch: {path}")
        with path.open() as source:
            for i, line in enumerate(source):
                fields = line.rstrip("\n").rstrip("\r").split("|")
                if len(fields) == len(cols) + 1 and fields[-1] == "":
                    fields.pop()
                if len(fields) != len(cols):
                    raise ValueError(f"{path} row {i+1}: wrong field count")
                values = [int(v) if c[2] == "INTEGER" else float(v) if c[2] == "REAL" else v
                          for c, v in zip(cols, fields)]
                con.execute(f"INSERT INTO {table} VALUES ({marks})", values)
        if table in manifest.get("rows", {}):
            assert con.execute(f"SELECT count(*) FROM {table}").fetchone()[0] == manifest["rows"][table]
    report = json.loads(args.report.read_text()) if args.report else {}
    if report.get("config", {}).get("secondary_indexes", True):
        con.executescript((ROOT / "indexes.sql").read_text())
    con.executescript((ROOT / "queries/q15-setup.sql").read_text())
    con.execute("ANALYZE")
    native_path = args.data / "native-results.json"
    native = json.loads(native_path.read_text()) if native_path.exists() else {}
    if report and Path(report["config"]["data_directory"]).resolve() != args.data.resolve():
        raise ValueError("report and reference must use the same data directory")
    if report.get("data_manifest", {}).get("sha256", {}) != manifest.get("sha256", {}) and report:
        raise ValueError("report manifest and current data hashes differ")
    queries = {q["number"]: q for q in report.get("queries", [])}
    duckdb_reports = {}
    for path in args.duckdb_report:
        result = json.loads(path.read_text())
        if Path(result["config"]["data_directory"]).resolve() != args.data.resolve():
            raise ValueError("DuckDB report and reference must use the same data directory")
        if result.get("data_manifest", {}).get("sha256", {}) != manifest.get("sha256", {}):
            raise ValueError("DuckDB report manifest and current data hashes differ")
        duckdb_reports[str(path)] = {q["number"]: q for q in result["queries"]}
    output = {"sqlite_version": sqlite3.sqlite_version, "data_directory": str(args.data), "queries": []}
    failures = 0
    for n in range(1, 23):
        text = (ROOT / f"queries/q{n:02}.sql").read_text()
        start = time.perf_counter()
        cur = con.execute(text)
        expected = [list(row) for row in cur.fetchall()]
        ms = (time.perf_counter() - start) * 1000
        item = {"number": n, "sqlite_ms": ms, "rows": len(expected)}
        if str(n) in native:
            error = compare(expected, native[str(n)]["rows"]) or order_error(n, expected)
            item["translation"] = error or "pass"
            failures += bool(error)
        if n in queries:
            q = queries[n]
            ok = any(s["status"] == "ok" for s in q["samples"])
            if q["sql"] != text:
                raise ValueError(f"Q{n:02} report used a different query definition")
            error = (compare(q.get("results", []), expected) or order_error(n, q.get("results", []))) if ok else "not completed"
            item["slopdb"] = error or "pass"
            failures += bool(error)
        if duckdb_reports:
            item["duckdb"] = {}
            for path, duckdb_queries in duckdb_reports.items():
                if n not in duckdb_queries:
                    continue
                q = duckdb_queries[n]
                if q["sql"] != text:
                    raise ValueError(f"Q{n:02} DuckDB report used a different query definition")
                ok = any(sample["status"] == "ok" for sample in q["samples"])
                error = (compare(q.get("results", []), expected) or order_error(n, q.get("results", []))) if ok else "not completed"
                item["duckdb"][path] = error or "pass"
                failures += bool(error)
        output["queries"].append(item)
        print(f"Q{n:02}: SQLite {ms:.3f}ms, {len(expected)} rows, translation={item.get('translation', 'unavailable')}, slopdb={item.get('slopdb', 'not run')}, duckdb={item.get('duckdb', {})}")
    if args.out:
        args.out.write_text(json.dumps(output, indent=2) + "\n")
    if failures:
        raise SystemExit(f"{failures} incomplete or mismatched query checks")


if __name__ == "__main__":
    main()
