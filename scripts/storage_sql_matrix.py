#!/usr/bin/env python3
"""Replay identical canonical SQL and verify every persisted value before comparing storage.

No benchmark workload, query timing, engine-specific SQL rewriting or COPY path is
used. Each supported cell gets a fresh database and the exact same SQL statements.
"""
import argparse
import ctypes
import datetime
import hashlib
import json
import math
import os
from pathlib import Path
import platform
import shutil
import sqlite3
import struct
import subprocess
import sys
import time

from storage_page_matrix import (ROOT, digest, measure, native_info, native_open,
                                 run, supported, validate_slop_inventory,
                                 validate_slop_metadata)

FORMAT = "slopdb-canonical-sql-v1"
MAX_STATEMENT_BYTES = 8 << 20
FINGERPRINT_FIELDS = ("name", "rows", "logical_bytes", "encoded_bytes", "sha256")


def statements(source, max_bytes=MAX_STATEMENT_BYTES):
    """Split SQL incrementally, preserving quoted/commented bytes and bounding memory.

Canonical dumps do not contain stored-program delimiters. Quote escapes use SQL's
standard doubled delimiter; backslash escapes are intentionally not interpreted.
"""
    pieces, size, state, significant = [], 0, "normal", False
    for line in iter(lambda: source.readline(max_bytes + 1), ""):
        start, index = 0, 0
        while index < len(line):
            char = line[index]
            following = line[index + 1] if index + 1 < len(line) else ""
            if state == "line":
                if char in "\r\n":
                    state = "normal"
            elif state == "block":
                if char == "*" and following == "/":
                    state, index = "normal", index + 1
            elif state in ("'", '"', "`"):
                if char == state:
                    if following == state:
                        index += 1
                    else:
                        state = "normal"
            elif state == "]":
                if char == "]":
                    if following == "]":
                        index += 1
                    else:
                        state = "normal"
            elif char == "-" and following == "-":
                state, index = "line", index + 1
            elif char == "/" and following == "*":
                state, index = "block", index + 1
            elif char in ("'", '"', "`", "["):
                state, significant = ("]" if char == "[" else char), True
            elif char == ";":
                piece = line[start:index + 1]
                size += len(piece.encode("utf-8"))
                if size > max_bytes:
                    raise ValueError("canonical SQL statement exceeds bounded limit")
                pieces.append(piece)
                if significant:
                    yield "".join(pieces)
                pieces, size, significant = [], 0, False
                start = index + 1
            elif not char.isspace():
                significant = True
            index += 1
        piece = line[start:]
        size += len(piece.encode("utf-8"))
        if size > max_bytes:
            raise ValueError("canonical SQL statement exceeds bounded limit")
        pieces.append(piece)
    if state not in ("normal", "line"):
        raise ValueError("unterminated SQL quote or block comment")
    if significant:
        raise ValueError("canonical SQL statement lacks terminating semicolon")


def quoted(identifier):
    return '"' + identifier.replace('"', '""') + '"'


def encode_row(values, types):
    if len(values) != len(types):
        raise ValueError("canonical column count differs")
    output = bytearray(b"R" + struct.pack(">I", len(values)))
    logical = 0
    for value, kind in zip(values, types):
        if value is None:
            output.append(0)
        elif kind == "int64":
            if type(value) is not int or not -(1 << 63) <= value < 1 << 63:
                raise ValueError("canonical integer is not exact int64")
            output.extend(b"\x01" + struct.pack(">q", value))
            logical += 8
        elif kind == "float64":
            if type(value) is not float or not math.isfinite(value):
                raise ValueError("canonical real is not finite float64")
            output.extend(b"\x02" + struct.pack(">d", 0.0 if value == 0 else value))
            logical += 8
        elif kind == "text":
            if type(value) is not str:
                raise ValueError("canonical text value changed type")
            payload = value.encode("utf-8")
            output.extend(b"\x03" + struct.pack(">Q", len(payload)) + payload)
            logical += len(payload)
        else:
            raise ValueError("unsupported canonical logical type")
    return output, logical


def load_manifest(path, sql):
    manifest = json.loads(path.read_text())
    if manifest.get("format") != FORMAT:
        raise ValueError("unsupported canonical manifest format")
    if manifest.get("sql_sha256") != digest(sql) or manifest.get("sql_bytes") != sql.stat().st_size:
        raise ValueError("canonical SQL does not match manifest bytes/hash")
    names = set()
    for table in manifest["tables"]:
        if not table["name"] or table["name"] in names:
            raise ValueError("duplicate/empty canonical table name")
        names.add(table["name"])
        columns, types, order = table["columns"], table["logical_types"], table["order_by"]
        if not columns or len(set(columns)) != len(columns) or len(columns) != len(types):
            raise ValueError("invalid canonical column list")
        if any(kind not in ("int64", "float64", "text") for kind in types):
            raise ValueError("unsupported canonical logical type")
        if not order or any(col not in columns for col in order):
            raise ValueError("invalid canonical ordering")
        if any(type(table[field]) is not int or table[field] < 0 for field in ("rows", "logical_bytes", "encoded_bytes")):
            raise ValueError("invalid canonical row/byte counts")
    if sum(table["rows"] for table in manifest["tables"]) != manifest["total_rows"]:
        raise ValueError("canonical row total inconsistent")
    if sum(table["logical_bytes"] for table in manifest["tables"]) != manifest["total_logical_bytes"]:
        raise ValueError("canonical logical byte total inconsistent")
    return manifest


def validate_phase(phase, manifest):
    expected = [{field: table[field] for field in FINGERPRINT_FIELDS} for table in manifest["tables"]]
    actual = [{field: table[field] for field in FINGERPRINT_FIELDS} for table in phase["tables"]]
    if actual != expected:
        raise ValueError("persisted logical values do not match canonical fingerprints")
    for field in ("total_rows", "total_logical_bytes"):
        if phase.get(field) != manifest[field]:
            raise ValueError("persisted canonical totals differ")


def native_verify(con, manifest):
    start = time.perf_counter()
    result = []
    for table in manifest["tables"]:
        sha, count, logical, encoded = hashlib.sha256(), 0, 0, 0
        query = "SELECT " + ",".join(map(quoted, table["columns"])) + " FROM " + quoted(table["name"]) + " ORDER BY " + ",".join(map(quoted, table["order_by"]))
        cursor = con.execute(query)
        while True:
            batch = cursor.fetchmany(4096)
            if not batch:
                break
            for row in batch:
                payload, size = encode_row(row, table["logical_types"])
                sha.update(payload)
                count, logical, encoded = count + 1, logical + size, encoded + len(payload)
        result.append({"name": table["name"], "rows": count, "logical_bytes": logical,
                       "encoded_bytes": encoded, "sha256": sha.hexdigest()})
        print("Verified", table["name"], count, "rows", flush=True)
    phase = {"tables": result, "total_rows": sum(table["rows"] for table in result),
             "total_logical_bytes": sum(table["logical_bytes"] for table in result),
             "verification_seconds": time.perf_counter() - start}
    validate_phase(phase, manifest)
    return phase


def native_cell(args, manifest, engine, size, cell, directory):
    file = directory / ("benchmark.sqlite" if engine == "sqlite" else "benchmark.duckdb")
    con = native_open(engine, file, size)
    start, count = time.perf_counter(), 0
    try:
        with args.sql.open("r", encoding="utf-8", newline="") as source:
            for statement in statements(source):
                con.execute(statement)
                count += 1
        if engine == "sqlite" and con.in_transaction:
            raise ValueError("canonical dump left an open transaction")
        if digest(args.sql) != manifest["sql_sha256"]:
            raise ValueError("canonical SQL changed during replay")
        if engine == "duckdb":
            con.execute("CHECKPOINT")
    finally:
        con.close()
    load_seconds = time.perf_counter() - start

    def verify():
        con = native_open(engine, file, size)
        try:
            phase = native_verify(con, manifest)
            phase["storage_info"] = native_info(con, engine)
        finally:
            con.close()
        phase.update(measure(directory))
        return phase

    before = verify()
    con = native_open(engine, file, size)
    start = time.perf_counter()
    try:
        con.execute("VACUUM")
        if engine == "duckdb":
            con.execute("CHECKPOINT")
    finally:
        con.close()
    elapsed = time.perf_counter() - start
    after = verify()
    report = {"engine_version": sqlite3.sqlite_version if engine == "sqlite" else __import__("duckdb").__version__,
              "sql_sha256": manifest["sql_sha256"], "sql_bytes": manifest["sql_bytes"],
              "load_mode": "canonical_sql_replay", "load_seconds": load_seconds, "statements": count,
              "maintenance_seconds": elapsed, "closed": before, "after_maintenance": after}
    if engine == "duckdb":
        counter = ctypes.CDLL(None).slopdb_tpcc_wal_sync_skipped
        counter.restype = ctypes.c_ulonglong
        report["wal_fsync_calls_skipped"] = counter()
        if report["wal_fsync_calls_skipped"] == 0:
            raise ValueError("DuckDB WAL fsync shim skipped no calls")
    return report


def cell_run(args):
    engine, size = args.cell[0], int(args.cell[1])
    manifest = load_manifest(args.manifest, args.sql)
    cell, directory = args.output / f"{manifest['dataset']}-{engine}-{size}", args.output / f"{manifest['dataset']}-{engine}-{size}/database"
    if engine == "slopdb":
        raw = cell / "loader.json"
        run([args.slop_loader, "-sql", args.sql, "-manifest", args.manifest, "-dir", directory,
             "-page-size", size, "-json", raw], cell / "loader.log")
        report = json.loads(raw.read_text())
        validate_slop_metadata(report, size)
        validate_slop_metadata(report["after_maintenance"], size, maintained=True)
        validate_slop_inventory(report["after_maintenance"])
    else:
        directory.mkdir()
        report = native_cell(args, manifest, engine, size, cell, directory)
    if report.get("sql_sha256") != manifest["sql_sha256"] or report.get("sql_bytes") != manifest["sql_bytes"]:
        raise ValueError("loader did not replay identical canonical SQL")
    for phase in ("closed", "after_maintenance"):
        validate_phase(report[phase], manifest)
    report.update(status="ok", dataset=manifest["dataset"], engine=engine,
                  requested_page_size_bytes=size, page_size_bytes=size,
                  query_benchmarked=False, workload_transactions=0,
                  logical_data_verified_twice=True)
    (cell / "storage.json").write_text(json.dumps(report, indent=2) + "\n")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--sql", type=Path, required=True)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--slop-loader", type=Path)
    parser.add_argument("--duckdb-wal-shim", type=Path)
    parser.add_argument("--engines", nargs="+", choices=["slopdb", "sqlite", "duckdb"], default=["slopdb", "sqlite", "duckdb"])
    parser.add_argument("--page-sizes", nargs="+", type=int, default=[16384, 65536, 1048576])
    parser.add_argument("--controls", action="store_true")
    parser.add_argument("--discard-databases", action="store_true")
    parser.add_argument("--cell", nargs=2, help=argparse.SUPPRESS)
    args = parser.parse_args()
    for key, value in vars(args).items():
        if isinstance(value, Path):
            setattr(args, key, value.resolve())
    manifest = load_manifest(args.manifest, args.sql)
    if any(size < 1 for size in args.page_sizes):
        parser.error("page sizes must be positive")
    if args.cell:
        cell_run(args)
        return
    if args.output.is_relative_to(ROOT):
        parser.error("benchmark artifacts must be outside the repository")
    if "slopdb" in args.engines and not args.slop_loader:
        parser.error("--slop-loader required")
    if "duckdb" in args.engines and not args.duckdb_wal_shim:
        parser.error("--duckdb-wal-shim required for WAL fsync disabled")
    args.output.mkdir(parents=True, exist_ok=False)
    result = {"started_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
              "command": [sys.executable, str(Path(__file__).resolve()), *sys.argv[1:]],
              "slopdb_revision": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip(),
              "platform": platform.platform(), "python_version": platform.python_version(), "cpus": os.cpu_count(),
              "units": "bytes; MiB=1048576 bytes", "canonical_manifest": manifest,
              "sql_path": str(args.sql), "manifest_path": str(args.manifest),
              "method": "serial canonical SQL replay; all values independently fingerprinted after reopen and maintenance; SlopDB L6-only full rewrite",
              "durability": "WAL enabled; fsync disabled for WAL; SQLite synchronous=OFF also disables data/checkpoint sync; SlopDB/DuckDB data/checkpoint sync enabled",
              "settings": {"query_benchmarked": False, "workload_transactions": 0,
                           "zstd_window_policy": "match page target; minimum 1 KiB", "slopdb_block_cache_bytes": 128 << 20,
                           "slopdb_memtable_bytes": 4 << 20, "duckdb_threads": 1, "duckdb_memory_limit": "1GB",
                           "sqlite_cache_size_kib": 131072}, "cells": []}
    if args.slop_loader:
        result["slop_loader"] = {"path": str(args.slop_loader), "sha256": digest(args.slop_loader),
                                 "build_metadata": subprocess.check_output(["go", "version", "-m", str(args.slop_loader)], text=True)}
    failed = False
    for engine in args.engines:
        sizes = list(dict.fromkeys(args.page_sizes + ([262144 if engine == "duckdb" else 4096] if args.controls else [])))
        for size in sizes:
            cell = args.output / f"{manifest['dataset']}-{engine}-{size}"
            cell.mkdir()
            item = {"dataset": manifest["dataset"], "engine": engine, "requested_page_size_bytes": size}
            if not supported(engine, size):
                item.update(status="unsupported", reason=f"{engine} does not support this page/block size")
            else:
                env = os.environ.copy()
                if engine == "duckdb":
                    env["LD_PRELOAD"] = str(args.duckdb_wal_shim)
                    env["SLOPDB_TPCC_WAL_FILE"] = str(cell / "database/benchmark.duckdb.wal")
                command = [sys.executable, str(Path(__file__).resolve()), *sys.argv[1:], "--cell", engine, str(size)]
                with (cell / "cell.log").open("w") as log:
                    outcome = subprocess.run(command, env=env, stdout=log, stderr=subprocess.STDOUT)
                if outcome.returncode:
                    failed = True
                    item.update(status="failed", exit_code=outcome.returncode, log=str(cell / "cell.log"))
                else:
                    item = json.loads((cell / "storage.json").read_text())
                    if args.discard_databases:
                        shutil.rmtree(cell / "database")
                        item["database_removed_after_validation"] = True
            result["cells"].append(item)
            (args.output / "comparison.json").write_text(json.dumps(result, indent=2) + "\n")
            print(manifest["dataset"], engine, size, item["status"], flush=True)
    result["finished_at"] = datetime.datetime.now(datetime.timezone.utc).isoformat()
    (args.output / "comparison.json").write_text(json.dumps(result, indent=2) + "\n")
    if failed:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
