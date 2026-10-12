#!/usr/bin/env python3
"""Build equal-data storage fixtures sequentially for requested page/block sizes.

The TPC-C fixture uses a portable committed-state JSON snapshot, replicated with
warehouse/history keys shifted. It measures storage, not transaction throughput.
TPC-H uses DBGEN inputs and optionally executes all 22 queries.
--storage-only skips query execution at every SlopDB page size. Native DuckDB needs its Python
package plus the separate Go runner; SQLite uses Python's standard library.
"""
import argparse
import csv
import ctypes
import datetime
import hashlib
import importlib.util
import json
import os
import platform
import shutil
from pathlib import Path
import sqlite3
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
TABLES = ("region", "nation", "supplier", "customer", "part", "partsupp", "orders", "lineitem")


def supported(engine, size):
    bounds = {"slopdb": (512, 64 << 20), "sqlite": (512, 65536), "duckdb": (16384, 262144)}
    low, high = bounds[engine]
    return low <= size <= high and size & (size - 1) == 0


def performance_enabled(size, storage_only=False):
    # Large pages are deliberately load/storage-only in this comparison.
    return not storage_only and size < 1 << 20


def validate_slop_metadata(report, size, maintained=False):
    if report.get("page_size_bytes") != size:
        raise ValueError("SlopDB report page size does not match requested target")
    if report.get("zstd_window_bytes") != max(1024, size):
        raise ValueError("SlopDB compression window does not match page target")
    if not maintained:
        return
    if report.get("bottom_level") != 6 or report.get("all_sstables_at_bottom") is not True:
        raise ValueError("SlopDB maintenance lacks verified L6-only compaction evidence")
    if report.get("maintenance_operation") != "full_bottom_level_rewrite":
        raise ValueError("SlopDB maintenance must rewrite all files at the bottom level")
    levels = report.get("levels")
    if not isinstance(levels, list) or len(levels) != 7:
        raise ValueError("SlopDB maintenance must include all seven level inventories")
    for level in levels[:-1]:
        if level.get("FileCount") != 0 or level.get("TotalSize") != 0 or level.get("Files"):
            raise ValueError("SlopDB SSTables remain above the bottom level")
    for level in levels:
        if not isinstance(level.get("FileCount"), int) or not isinstance(level.get("TotalSize"), int):
            raise ValueError("SlopDB level inventory lacks exact counts and sizes")
        if level["FileCount"] < 0 or level["TotalSize"] < 0 or level["FileCount"] != len(level.get("Files") or []):
            raise ValueError("SlopDB level inventory is inconsistent")


def validate_slop_inventory(report):
    expected = {}
    for file in report["levels"][report["bottom_level"]].get("Files") or []:
        number, size = file.get("Number"), file.get("Size")
        if not isinstance(number, int) or number < 1 or not isinstance(size, int) or size < 1:
            raise ValueError("SlopDB L6 file inventory lacks valid file IDs and sizes")
        name = f"{number}.sst"
        if name in expected:
            raise ValueError("SlopDB L6 file inventory contains duplicate file IDs")
        expected[name] = size
    physical = {file["name"]: file["bytes"] for file in report["files"] if file["name"].endswith(".sst")}
    if physical != expected:
        raise ValueError("physical SSTable files do not match verified bottom-level inventory")


def measure(directory):
    files = []
    for path in sorted(directory.rglob("*")):
        if path.is_file():
            with path.open("rb") as source:
                os.fsync(source.fileno())
            stat = path.stat()
            files.append({"name": str(path.relative_to(directory)), "bytes": stat.st_size,
                          "allocated_bytes": getattr(stat, "st_blocks", 0) * 512})
    dirs = [directory] + [p for p in directory.rglob("*") if p.is_dir()]
    wal = [f["name"] for f in files if f["name"].endswith((".log", ".wal", "-wal"))]
    if wal:
        raise ValueError(f"WAL remains after close: {wal}")
    return {"files": files, "file_count": len(files), "file_bytes": sum(f["bytes"] for f in files),
            "allocated_total_bytes": sum(f["allocated_bytes"] for f in files) +
            sum(getattr(p.stat(), "st_blocks", 0) * 512 for p in dirs), "wal_files": wal}


def digest(path):
    with path.open("rb") as source:
        sha = hashlib.sha256()
        for block in iter(lambda: source.read(1 << 20), b""):
            sha.update(block)
        return sha.hexdigest()


def run(command, log):
    print("Running", " ".join(map(str, command)), flush=True)
    with log.open("w") as output:
        subprocess.run(list(map(str, command)), stdout=output, stderr=subprocess.STDOUT, check=True)


def verifier():
    spec = importlib.util.spec_from_file_location("tpch_verify", ROOT / "cmd/tpch/verify.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def native_open(engine, file, size):
    if engine == "sqlite":
        con = sqlite3.connect(file)
        # The setting must precede WAL and the first table, and SQLite silently
        # ignores unsupported values; explicitly check its actual stored value.
        con.execute(f"PRAGMA page_size={size}")
        assert con.execute("PRAGMA page_size").fetchone()[0] == size
        assert con.execute("PRAGMA journal_mode=WAL").fetchone()[0] == "wal"
        con.execute("PRAGMA synchronous=OFF")
        con.execute("PRAGMA cache_size=-131072")
        con.execute(f"PRAGMA wal_autocheckpoint={max(1, (64 << 20) // size)}")
        return con
    import duckdb
    con = duckdb.connect(str(file), config={"default_block_size": str(size), "threads": "1", "memory_limit": "1GB"})
    actual = con.execute("SELECT block_size FROM pragma_database_size()").fetchone()[0]
    assert actual == size, (actual, size)
    return con


def snapshot_native(args, engine, size, cell, directory):
    data = json.loads(args.tpcc_snapshot.read_text())
    tables = data["tables"]
    warehouses = max(row[0] for table in tables if table["name"] == "warehouse" for row in table["rows"])
    history = max(row[0] for table in tables if table["name"] == "history" for row in table["rows"]) + 1
    file = directory / ("benchmark.sqlite" if engine == "sqlite" else "benchmark.duckdb")
    con = native_open(engine, file, size)
    start = time.perf_counter()
    for ddl in data["ddl"]:
        con.execute(ddl.replace("INTEGER", "BIGINT") if engine == "duckdb" else ddl)
    counts = {}
    for table in tables:
        name, cols, rows = table["name"], table["columns"], table["rows"]
        replicas = 1 if name == "item" else args.tpcc_replicas
        counts[name] = len(rows) * replicas
        if engine == "sqlite":
            query = f"INSERT INTO {name} VALUES ({','.join('?' for _ in cols)})"
            for replica in range(replicas):
                for offset in range(0, len(rows), args.load_batch_rows):
                    shifted = ([value + replica * warehouses if col == "w_id" or col.endswith("_w_id") else
                                value + replica * history if col == "h_id" else value
                                for col, value in zip(cols, row)] for row in rows[offset:offset + args.load_batch_rows])
                    con.executemany(query, shifted)
                    con.commit()
        else:
            # Bulk COPY avoids millions of native prepared-call crossings. CSV is
            # an intermediate outside the database directory and size accounting.
            source = cell / f"{name}.csv"
            with source.open("w", newline="") as output:
                csv.writer(output).writerows(rows)
            con.execute(f"CREATE TEMP TABLE seed_{name} AS SELECT * FROM {name} WHERE FALSE")
            quoted = str(source).replace("'", "''")
            con.execute(f"COPY seed_{name} FROM '{quoted}' (FORMAT CSV, HEADER FALSE, NULL '')")
            expressions = [f"s.{col}+r.i*{warehouses}" if col == "w_id" or col.endswith("_w_id") else
                           f"s.{col}+r.i*{history}" if col == "h_id" else f"s.{col}" for col in cols]
            con.execute(f"INSERT INTO {name} SELECT {','.join(expressions)} FROM seed_{name} s CROSS JOIN range({replicas}) r(i)")
            con.execute(f"DROP TABLE seed_{name}")
            source.unlink()
        print(engine, name, counts[name], flush=True)
    if engine == "duckdb":
        con.execute("CHECKPOINT")
    con.close()
    load_seconds = time.perf_counter() - start

    def verify():
        con = native_open(engine, file, size)
        checked = 0
        for table in tables:
            name, cols, rows = table["name"], table["columns"], table["rows"]
            actual = con.execute(f"SELECT COUNT(*) FROM {name}").fetchone()[0]
            assert actual == counts[name], (name, actual, counts[name])
            replicas = 1 if name == "item" else args.tpcc_replicas
            for replica in range(replicas):
                where = (f" WHERE h_id BETWEEN {replica * history} AND {(replica + 1) * history - 1}" if name == "history" else
                         "" if name == "item" else
                         f" WHERE w_id BETWEEN {replica * warehouses + 1} AND {(replica + 1) * warehouses}")
                actual = con.execute(f"SELECT * FROM {name}{where} ORDER BY {','.join(cols)}").fetchall()
                expected = [[value + replica * warehouses if col == "w_id" or col.endswith("_w_id") else
                             value + replica * history if col == "h_id" else value
                             for col, value in zip(cols, row)] for row in rows]
                assert [list(row) for row in actual] == expected, (name, replica)
                checked += len(actual)
        info = native_info(con, engine)
        con.close()
        return {"rows": counts, "verified_rows": checked, "all_values_verified_after_reopen": True, "storage_info": info}
    before = verify()
    before.update(measure(directory))
    con = native_open(engine, file, size)
    start = time.perf_counter()
    con.execute("VACUUM")
    if engine == "duckdb":
        con.execute("CHECKPOINT")
    con.close()
    maintenance_seconds = time.perf_counter() - start
    after = verify()
    after.update(measure(directory))
    return {"engine_version": sqlite3.sqlite_version if engine == "sqlite" else __import__("duckdb").__version__,
            "load_mode": "executemany" if engine == "sqlite" else "copy_and_insert_select",
            "load_batch_rows": args.load_batch_rows if engine == "sqlite" else None,
            "load_seconds": load_seconds, "maintenance_seconds": maintenance_seconds, "closed": before,
            "after_maintenance": after, "warehouse_stride": warehouses, "history_stride": history}


def native_info(con, engine):
    if engine == "sqlite":
        return {key: con.execute("PRAGMA " + key).fetchone()[0] for key in ("page_size", "page_count", "freelist_count")}
    cur = con.execute("PRAGMA database_size")
    return dict(zip([column[0] for column in cur.description], cur.fetchone()))


def sqlite_tpch(args, size, cell, directory):
    file = directory / "benchmark.sqlite"
    con = native_open("sqlite", file, size)
    start = time.perf_counter()
    con.executescript((ROOT / "cmd/tpch/schema.sql").read_text())
    manifest = json.loads((args.tpch_data / "manifest.json").read_text())
    for table in TABLES:
        cols = con.execute(f"PRAGMA table_info({table})").fetchall()
        source = args.tpch_data / f"{table}.tbl"
        assert digest(source) == manifest["sha256"][table]
        marks = ",".join("?" for _ in cols)
        batch, count = [], 0
        with source.open() as input_file:
            for line in input_file:
                fields = line.rstrip("\r\n").split("|")
                if fields[-1] == "":
                    fields.pop()
                assert len(fields) == len(cols)
                batch.append([int(value) if col[2] == "INTEGER" else float(value) if col[2] == "REAL" else value
                              for col, value in zip(cols, fields)])
                count += 1
                if len(batch) == args.load_batch_rows:
                    con.executemany(f"INSERT INTO {table} VALUES ({marks})", batch)
                    con.commit()
                    batch.clear()
        if batch:
            con.executemany(f"INSERT INTO {table} VALUES ({marks})", batch)
        con.commit()
        assert count == manifest["rows"][table]
    con.executescript((ROOT / "cmd/tpch/indexes.sql").read_text())
    con.executescript((ROOT / "cmd/tpch/queries/q15-setup.sql").read_text())
    con.execute("ANALYZE")
    con.commit()
    con.close()
    load_seconds = time.perf_counter() - start
    con = native_open("sqlite", file, size)
    assert con.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
    queries = []
    for number in range(1, 23):
        query = (ROOT / f"cmd/tpch/queries/q{number:02}.sql").read_text()
        samples, results, columns = [], [], []
        for repeat in range(args.warmups + args.runs):
            start = time.perf_counter()
            con.set_progress_handler(lambda: int(time.perf_counter() - start > args.timeout_seconds), 10000)
            try:
                cur = con.execute(query)
                columns = [col[0] for col in cur.description]
                results = [list(row) for row in cur.fetchall()]
                sample = {"status": "ok", "wall_ms": (time.perf_counter() - start) * 1000, "rows": len(results)}
            except sqlite3.OperationalError as error:
                sample = {"status": "timeout" if "interrupted" in str(error) else "error", "error": str(error),
                          "wall_ms": (time.perf_counter() - start) * 1000}
            finally:
                con.set_progress_handler(None, 0)
            if repeat >= args.warmups:
                samples.append(sample)
        queries.append({"number": number, "sql": query, "samples": samples, "results": results, "columns": columns})
    rows = {table: con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0] for table in TABLES}
    assert rows == manifest["rows"]
    con.close()
    report = {"engine": "SQLite", "version": sqlite3.sqlite_version, "page_size_bytes": size,
              "config": {"data_directory": str(args.tpch_data), "secondary_indexes": True},
              "data_manifest": manifest, "table_rows": rows, "load_seconds": load_seconds, "queries": queries,
              "load_mode": "executemany", "load_batch_rows": args.load_batch_rows, "query_benchmarked": True,
              "counts_verified_after_reopen": True}
    (cell / "queries.json").write_text(json.dumps(report, indent=2) + "\n")
    return report


def tpch(args, engine, size, cell, directory):
    raw = cell / "queries.json"
    if engine == "sqlite":
        report = sqlite_tpch(args, size, cell, directory)
    else:
        command = [args.slop_tpch if engine == "slopdb" else args.duck_tpch,
                   "-data", args.tpch_data, "-page-size", size, "-json", raw,
                   "-runs", args.runs, "-warmups", args.warmups, "-timeout", f"{args.timeout_seconds}s"]
        if engine == "slopdb":
            # The runner creates its own fresh directory.
            directory.rmdir()
            command += ["-dir", directory]
            command += ["-load-batch-rows", args.load_batch_rows]
            if not performance_enabled(size, args.storage_only):
                command += ["-load-only"]
        else:
            command += ["-database", directory / "benchmark.duckdb", "-query-root", ROOT / "cmd/tpch", "-threads", "1"]
        run(command, cell / "load-queries.log")
        report = json.loads(raw.read_text())
    timed = performance_enabled(size, args.storage_only)
    if not timed and (report.get("queries") or report.get("successful_executions") or report.get("query_benchmarked") is not False):
        raise ValueError("load-only page size unexpectedly executed benchmark queries")
    queries = report.get("queries") or []
    if timed and [query["number"] for query in queries] != list(range(1, 23)):
        raise ValueError("timed TPC-H report must contain all 22 queries")
    for query in queries:
        if len(query["samples"]) != args.runs:
            raise ValueError(f"query {query['number']} has incomplete measured samples")
        if any(sample["status"] != "ok" for sample in query["samples"]):
            raise ValueError(f"query {query['number']} failed or timed out; inspect {raw}")
    if args.reference_report and timed:
        reference = {q["number"]: q for q in json.loads(args.reference_report.read_text())["queries"]}
        check = verifier()
        for query in queries:
            error = check.compare(query.get("results", []), reference[query["number"]].get("results", [])) or check.order_error(query["number"], query.get("results", []))
            if error:
                raise ValueError(f"Q{query['number']} answer mismatch: {error}")
    if engine == "slopdb":
        validate_slop_metadata(report, size)
    before = measure(directory)
    if engine == "slopdb":
        before.update({"page_size_bytes": report["page_size_bytes"], "zstd_window_bytes": report["zstd_window_bytes"],
                       "rows": report["table_rows"], "counts_verified_after_reopen": report["counts_verified_after_reopen"]})
        maintenance = cell / "maintenance.json"
        command = [args.slop_snapshot, "-mode", "compact", "-dir", directory, "-page-size", size, "-json", maintenance]
        if timed:
            command += ["-verify-report", raw]
        run(command, cell / "maintenance.log")
        maintenance = json.loads(maintenance.read_text())
        validate_slop_metadata(maintenance, size, maintained=True)
    else:
        file = directory / ("benchmark.sqlite" if engine == "sqlite" else "benchmark.duckdb")
        con = native_open(engine, file, size)
        before["storage_info"] = native_info(con, engine)
        start = time.perf_counter()
        con.execute("VACUUM")
        if engine == "duckdb":
            con.execute("CHECKPOINT")
        con.close()
        elapsed = time.perf_counter() - start
        con = native_open(engine, file, size)
        actual = {table: con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0] for table in TABLES}
        assert actual == report["table_rows"]
        check = verifier()
        for query in report["queries"]:
            answers = [list(row) for row in con.execute(query["sql"]).fetchall()]
            error = check.compare(answers, query.get("results", [])) or check.order_error(query["number"], answers)
            if error:
                raise ValueError(f"Q{query['number']} answer changed after maintenance: {error}")
        maintenance = {"maintenance_seconds": elapsed, "rows": actual, "counts_verified_after_reopen": True,
                       "query_answers_verified_after_maintenance": len(report["queries"]), "storage_info": native_info(con, engine)}
        con.close()
    maintenance.update(measure(directory))
    if engine == "slopdb":
        validate_slop_inventory(maintenance)
    return {"load_seconds": report["load_seconds"], "query_report": str(raw), "closed": before,
            "after_maintenance": maintenance, "query_benchmarked": timed,
            "query_benchmark_skip_reason": None if timed else ("Storage-only measurement requested" if args.storage_only else "User requested loading/storage only for 1 MiB blocks"),
            "load_mode": report.get("load_mode", "appender"), "load_batch_rows": report.get("config", {}).get("load_batch_rows", report.get("load_batch_rows")),
            "query_answers_verified": bool(args.reference_report and timed)}


def cell_run(args, workload, engine, size):
    cell = args.output / f"{workload}-{engine}-{size}"
    directory = cell / "database"
    directory.mkdir()
    if workload == "tpcc" and engine == "slopdb":
        directory.rmdir()
        before_json, after_json = cell / "load.json", cell / "maintenance.json"
        common = [args.slop_snapshot, "-dir", directory, "-snapshot", args.tpcc_snapshot,
                  "-replicas", args.tpcc_replicas, "-page-size", size, "-load-batch-rows", args.load_batch_rows]
        run(common + ["-mode", "load", "-json", before_json], cell / "load.log")
        before = json.loads(before_json.read_text())
        validate_slop_metadata(before, size)
        before.update(measure(directory))
        run(common + ["-mode", "maintenance", "-json", after_json], cell / "maintenance.log")
        after = json.loads(after_json.read_text())
        validate_slop_metadata(after, size, maintained=True)
        after.update(measure(directory))
        validate_slop_inventory(after)
        result = {"load_seconds": before["load_seconds"], "closed": before, "after_maintenance": after}
    elif workload == "tpcc":
        result = snapshot_native(args, engine, size, cell, directory)
    else:
        result = tpch(args, engine, size, cell, directory)
    result.update({"status": "ok", "workload": workload, "engine": engine, "requested_page_size_bytes": size, "page_size_bytes": size})
    if engine == "duckdb" and args.duckdb_wal_shim:
        counter = ctypes.CDLL(None).slopdb_tpcc_wal_sync_skipped
        counter.restype = ctypes.c_ulonglong
        result["wal_fsync_calls_skipped"] = counter()
        if workload == "tpch":
            # TPC-H loads run in a Go child, whose counter is process local.
            result["wal_fsync_calls_skipped"] += json.loads((cell / "queries.json").read_text()).get("wal_fsync_calls_skipped", 0)
        # TPC-H loads run in a Go child, so its shim counter is in that process.
        if workload == "tpcc" and result["wal_fsync_calls_skipped"] == 0:
            raise ValueError("DuckDB WAL fsync shim skipped no calls")
    (cell / "storage.json").write_text(json.dumps(result, indent=2) + "\n")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True, help="fresh output directory outside the repository")
    parser.add_argument("--tpcc-snapshot", type=Path)
    parser.add_argument("--tpcc-replicas", type=int, default=128)
    parser.add_argument("--load-batch-rows", type=int, default=4096, help="rows per bounded SlopDB bulk/SQLite executemany transaction")
    parser.add_argument("--tpch-data", type=Path)
    parser.add_argument("--reference-report", type=Path, help="independent DuckDB TPC-H answers on these inputs")
    parser.add_argument("--engines", nargs="+", choices=["slopdb", "sqlite", "duckdb"], default=["slopdb", "sqlite", "duckdb"])
    parser.add_argument("--page-sizes", nargs="+", type=int, default=[16384, 65536, 1048576])
    parser.add_argument("--slop-snapshot", type=Path)
    parser.add_argument("--slop-tpch", type=Path)
    parser.add_argument("--duck-tpch", type=Path)
    parser.add_argument("--duckdb-wal-shim", type=Path, help="Linux WAL-only preload shim; required for DuckDB no-fsync loading")
    parser.add_argument("--controls", action="store_true", help="also run SlopDB/SQLite4KiB and DuckDB256KiB controls")
    parser.add_argument("--storage-only", action="store_true", help="load and measure storage without TPC-H queries at any size (TPC-H currently requires --engines slopdb)")
    parser.add_argument("--discard-databases", action="store_true", help="remove only each successful cell's newly created database after validation and size inventory")
    parser.add_argument("--runs", type=int, default=3)
    parser.add_argument("--warmups", type=int, default=1)
    parser.add_argument("--timeout-seconds", type=float, default=120)
    parser.add_argument("--cell", nargs=3, help=argparse.SUPPRESS)
    args = parser.parse_args()
    if args.load_batch_rows < 1 or args.tpcc_replicas < 1 or args.runs < 1 or args.warmups < 0 or args.timeout_seconds <= 0 or any(n <= 0 for n in args.page_sizes):
        parser.error("replicas/runs/timeout/page sizes must be positive; warmups nonnegative")
    if not args.tpcc_snapshot and not args.tpch_data:
        parser.error("provide --tpcc-snapshot and/or --tpch-data")
    if args.storage_only and args.tpch_data and any(engine != "slopdb" for engine in args.engines):
        parser.error("--storage-only TPC-H currently supports only --engines slopdb")
    for key, value in vars(args).items():
        if isinstance(value, Path):
            setattr(args, key, value.resolve())
    if args.cell:
        workload, engine, size = args.cell
        cell_run(args, workload, engine, int(size))
        return
    if args.output.is_relative_to(ROOT):
        parser.error("benchmark artifacts must be outside the repository")
    if args.tpch_data:
        manifest = json.loads((args.tpch_data / "manifest.json").read_text())
        for table in TABLES:
            if digest(args.tpch_data / f"{table}.tbl") != manifest["sha256"][table]:
                parser.error(f"TPC-H input hash mismatch: {table}")
    args.output.mkdir(parents=True, exist_ok=False)
    workloads = (["tpcc"] if args.tpcc_snapshot else []) + (["tpch"] if args.tpch_data else [])
    result = {"started_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
              "command": [sys.executable, str(Path(__file__).resolve()), *sys.argv[1:]],
              "binaries": {name: {"path": str(path), "sha256": digest(path),
                           "build_metadata": subprocess.check_output(["go", "version", "-m", str(path)], text=True)}
                           for name, path in (("slop_snapshot", args.slop_snapshot), ("slop_tpch", args.slop_tpch), ("duck_tpch", args.duck_tpch)) if path},
              "slopdb_revision": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip(),
              "platform": platform.platform(), "python_version": platform.python_version(), "cpus": os.cpu_count(),
              "units": "bytes; MiB=1048576 bytes", "method": "serial fresh equal-data fixtures; final SlopDB size measured after atomic full bottom-level rewrite and verified L6-only inventory; pre-maintenance sizes are diagnostics",
              "durability": "WAL enabled; WAL fsync disabled; SlopDB/DuckDB checkpoint and data-file synchronization enabled; SQLite synchronous=OFF also disables its checkpoint synchronization",
              "page_size_meaning": {"slopdb": "uncompressed SSTable data-block target", "sqlite": "fixed B-tree page", "duckdb": "storage block"},
              "run_settings": {"slopdb_block_cache_bytes": 128 << 20, "slopdb_memtable_bytes": 4 << 20, "slopdb_compression": "auto", "zstd_window_policy": "configured page target; minimum 1024 bytes", "storage_only": args.storage_only, "zstd_encoder_concurrency": 1, "zstd_level": "default", "zstd_match_memory": "normal", "load_batch_rows": args.load_batch_rows, "large_page_performance": "disabled; 1 MiB is loading/storage only", "duckdb_threads": 1, "duckdb_memory_limit": "1GB", "sqlite_cache_size_kib": 131072, "sqlite_wal_checkpoint_target_bytes": 64 << 20, "warmups": args.warmups, "runs": args.runs},
              "tpcc_replicas": args.tpcc_replicas, "tpcc_snapshot_sha256": digest(args.tpcc_snapshot) if args.tpcc_snapshot else None,
              "tpch_manifest": json.loads((args.tpch_data / "manifest.json").read_text()) if args.tpch_data else None,
              "cells": []}
    failed = False
    for workload in workloads:
        for engine in args.engines:
            for size in list(dict.fromkeys(args.page_sizes + ([262144 if engine == "duckdb" else 4096] if args.controls else []))):
                cell = args.output / f"{workload}-{engine}-{size}"
                cell.mkdir()
                item = {"workload": workload, "engine": engine, "requested_page_size_bytes": size}
                if not supported(engine, size):
                    item.update(status="unsupported", reason=f"{engine} does not support this page/block size")
                else:
                    env = os.environ.copy()
                    if engine == "duckdb":
                        if not args.duckdb_wal_shim:
                            parser.error("--duckdb-wal-shim is required for DuckDB loading with WAL fsync disabled")
                        env["LD_PRELOAD"] = str(args.duckdb_wal_shim)
                        env["SLOPDB_TPCC_WAL_FILE"] = str(cell / "database/benchmark.duckdb.wal")
                    command = [sys.executable, str(Path(__file__).resolve()), *sys.argv[1:], "--cell", workload, engine, str(size)]
                    with (cell / "cell.log").open("w") as log:
                        outcome = subprocess.run(command, env=env, stdout=log, stderr=subprocess.STDOUT)
                    if outcome.returncode:
                        item.update(status="failed", exit_code=outcome.returncode, log=str(cell / "cell.log"))
                        failed = True
                    else:
                        item = json.loads((cell / "storage.json").read_text())
                        if args.discard_databases:
                            shutil.rmtree(cell / "database")
                            item["database_removed_after_validation"] = True
                result["cells"].append(item)
                (args.output / "comparison.json").write_text(json.dumps(result, indent=2) + "\n")
                print(workload, engine, size, item["status"], flush=True)
    result["finished_at"] = datetime.datetime.now(datetime.timezone.utc).isoformat()
    (args.output / "comparison.json").write_text(json.dumps(result, indent=2) + "\n")
    if failed:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
