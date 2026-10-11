#!/usr/bin/env python3
"""Generate genuine DBGEN data using DuckDB's optional TPC-H extension.

This development helper is separate from SlopDB and adds no Go dependencies.
Install the duckdb Python package before running it. The output directory must
not exist. An explicit extension path supports offline extension installation.
"""
import argparse
import hashlib
import json
import math
from pathlib import Path
import time


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sf", type=float, default=0.001)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--extension", help="path to a DuckDB tpch extension binary")
    args = parser.parse_args()
    if not math.isfinite(args.sf) or args.sf <= 0:
        parser.error("--sf must be finite and positive")
    import duckdb
    con = duckdb.connect()
    if args.extension:
        path = str(Path(args.extension).resolve()).replace("'", "''")
        con.execute(f"LOAD '{path}'")
    else:
        con.execute("INSTALL tpch; LOAD tpch")
    args.out.mkdir(parents=True, exist_ok=False)
    start = time.monotonic()
    con.execute("CALL dbgen(sf = ?)", [args.sf])
    manifest = {"generator": "DuckDB TPC-H extension DBGEN", "duckdb_version": duckdb.__version__,
                "scale_factor": args.sf, "rows": {}, "sha256": {}, "columns": {}}
    tables = ("region", "nation", "supplier", "customer", "part", "partsupp", "orders", "lineitem")
    for table in tables:
        target = args.out / f"{table}.tbl"
        path = str(target.resolve()).replace("'", "''")
        # DBGEN fields do not contain delimiters or newlines. Omitting the
        # optional final pipe is accepted by the Go loader and standard tools.
        con.execute(f"COPY {table} TO '{path}' (FORMAT CSV, DELIMITER '|', HEADER false, QUOTE '', ESCAPE '')")
        manifest["rows"][table] = con.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
        manifest["columns"][table] = [row[0] for row in con.execute(f"DESCRIBE {table}").fetchall()]
        digest = hashlib.sha256()
        with target.open("rb") as source:
            for block in iter(lambda: source.read(1024 * 1024), b""):
                digest.update(block)
        manifest["sha256"][table] = digest.hexdigest()
        print(f"{table}: {manifest['rows'][table]} rows")
    manifest["generate_seconds"] = time.monotonic() - start
    # Save native query SQL and results so the adapted query pack can be checked
    # independently against the original query definitions on exactly this data.
    native = {}
    for n, text in con.execute("SELECT query_nr, query FROM tpch_queries() ORDER BY query_nr").fetchall():
        result = con.execute(text)
        native[str(n)] = {"sql": text, "columns": [c[0] for c in result.description],
                          "rows": [list(row) for row in result.fetchall()]}
    (args.out / "native-results.json").write_text(json.dumps(native, default=str, indent=2) + "\n")
    (args.out / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    print(f"Generated SF={args.sf} in {manifest['generate_seconds']:.3f}s: {args.out}")


if __name__ == "__main__":
    main()
