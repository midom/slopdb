#!/usr/bin/env python3
"""Run fresh TPC-C-inspired page-size comparisons serially, retaining every artifact.

Binaries must be built before running. Defaults: 10 workers, 60 seconds,
2 warehouses, seed 42, prepared execution, WAL enabled and WAL fsync disabled.
Unsupported native sizes produce explicit cells without invoking an engine.
"""
import argparse
import datetime
import hashlib
import json
import os
from pathlib import Path
import platform
import subprocess
import time

from run_benchmarks import validate_report
from storage_page_matrix import performance_enabled, supported

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_SIZES = [16384, 65536, 1048576]
DEFAULT_CONTROLS = {"slopdb": 4096, "sqlite": 4096, "duckdb": 262144}


def cases(engines, sizes, controls=True):
    for engine in engines:
        values = sizes + ([DEFAULT_CONTROLS[engine]] if controls else [])
        for size in dict.fromkeys(values):
            yield engine, size


def validate(report, engine, size, seconds, workers, warehouses):
    validate_report("tpcc", report)
    config, info = report["config"], report["engine_info"]
    expected = {"engine": engine, "requested_page_size_bytes": size, "workers": workers,
                "duration_ns": seconds * 1_000_000_000, "warehouses": warehouses,
                "districts_per_warehouse": 10, "customers_per_district": 100,
                "items": 1000, "seed": 42, "prepared_statements": True, "wal_fsync": False}
    for key, value in expected.items():
        if config[key] != value:
            raise ValueError(f"configuration {key}: got {config[key]!r}, expected {value!r}")
    if info["page_size_bytes"] != size or info["wal_fsync"] is not False:
        raise ValueError("engine page size or WAL fsync setting differs from the request")
    if engine == "slopdb":
        if info["wal"] is not True:
            raise ValueError("SlopDB WAL must remain enabled")
        if info.get("zstd_window_bytes") != max(1024, size):
            raise ValueError("SlopDB compression window must match its page target")
    if engine == "sqlite":
        if info["journal_mode"] != "wal" or info["synchronous"] != "OFF":
            raise ValueError("SQLite must use WAL with synchronous=OFF")
        if info["cache_size_kib"] != 131072 or info["wal_checkpoint_target_bytes"] != 64 << 20:
            raise ValueError("SQLite byte cache/checkpoint budgets differ")
    if engine == "duckdb":
        if info["threads"] != 1 or info["memory_limit"] != "1GB":
            raise ValueError("DuckDB thread or memory settings differ")
        if info["wal_fsync_calls_skipped"] <= 0:
            raise ValueError("DuckDB WAL-only fsync shim skipped no calls")


def digest(path):
    sha = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(1 << 20), b""):
            sha.update(block)
    return sha.hexdigest()


def command(args, engine, size, directory, report):
    binary = args.slop_tpcc if engine == "slopdb" else args.native_tpcc
    return [str(binary), "-engine", engine, "-page-size", str(size),
            "-workers", str(args.workers), "-duration", f"{args.seconds}s",
            "-warehouses", str(args.warehouses), "-districts", "10", "-customers", "100", "-items", "1000",
            "-seed", "42", "-prepared=true", "-wal-fsync=false", "-dir", str(directory), "-json", str(report)]


def environment(args, engine, directory):
    env = os.environ.copy()
    # A caller's DuckDB WAL override must never bleed into another cell.
    env.pop("SLOPDB_TPCC_WAL_FILE", None)
    if engine == "duckdb":
        existing = env.get("LD_PRELOAD", "")
        env["LD_PRELOAD"] = str(args.duckdb_wal_shim) + (" " + existing if existing else "")
        env["SLOPDB_TPCC_WAL_FILE"] = str(directory / "benchmark.duckdb.wal")
    return env


def run(args):
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    binaries = {"slopdb": args.slop_tpcc, "native": args.native_tpcc}
    manifest = {
        "started_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "slopdb_revision": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip(),
        "working_tree": subprocess.check_output(["git", "status", "--porcelain"], cwd=ROOT, text=True).splitlines(),
        "platform": platform.platform(), "cpus": os.cpu_count(),
        "binaries": {key: {"path": str(path), "sha256": digest(path)} for key, path in binaries.items() if path},
        "duckdb_wal_shim": {"path": str(args.duckdb_wal_shim), "sha256": digest(args.duckdb_wal_shim)} if args.duckdb_wal_shim else None,
        "method": "Strictly serial subprocesses; fresh database for each cell; no cache resets. Load/preparation/validation/close/reopen excluded from transaction timing. All 15 consistency assertions checked after execution and reopening. Single runs are descriptive, not statistical proof.",
        "durability": "WAL enabled, WAL fsync disabled; DuckDB exact-WAL-path LD_PRELOAD shim; SlopDB/DuckDB checkpoints and data-file synchronization remain enabled; SQLite synchronous=OFF also disables its checkpoint synchronization.",
        "configuration": {"seconds": args.seconds, "workers": args.workers, "warehouses": args.warehouses,
                          "districts": 10, "customers": 100, "items": 1000, "seed": 42,
                          "prepared": True, "page_sizes": args.page_sizes, "controls": args.controls,
                          "slopdb_block_cache_bytes": 128 << 20, "slopdb_memtable_bytes": 4 << 20,
                          "zstd_window_policy": "configured page target; minimum 1024 bytes", "large_page_performance": "disabled; 1 MiB is loading/storage only",
                          "sqlite_cache_size_kib": 131072, "sqlite_wal_checkpoint_target_bytes": 64 << 20,
                          "duckdb_threads": 1, "duckdb_memory_limit": "1GB"},
        "cells": [],
    }
    failed = False
    for engine, size in cases(args.engines, args.page_sizes, args.controls):
        label = f"tpcc-{engine}-{size}"
        cell = output / label
        cell.mkdir()
        directory, report_path = cell / "database", cell / "report.json"
        entry = {"engine": engine, "requested_page_size_bytes": size, "report": str(report_path), "log": str(cell / "run.log")}
        if not supported(engine, size):
            entry.update(status="unsupported", reason=f"{engine} does not support the requested page/block size")
            (cell / "run.log").write_text(entry["reason"] + "\n")
        elif not performance_enabled(size):
            entry.update(status="skipped", reason="User requested loading/storage only for 1 MiB blocks")
            (cell / "run.log").write_text(entry["reason"] + "\n")
        else:
            invocation = command(args, engine, size, directory, report_path)
            entry["command"] = invocation
            start = time.perf_counter()
            print(f"Running {label}: {args.workers} workers for {args.seconds}s", flush=True)
            with (cell / "run.log").open("w") as log:
                outcome = subprocess.run(invocation, cwd=ROOT, env=environment(args, engine, directory), stdout=log, stderr=subprocess.STDOUT)
            entry.update(exit_code=outcome.returncode, whole_process_seconds=time.perf_counter() - start)
            try:
                report = json.loads(report_path.read_text())
                entry["total"] = report["total"]
                entry["engine_info"] = report["engine_info"]
                validate(report, engine, size, args.seconds, args.workers, args.warehouses)
                if outcome.returncode:
                    raise ValueError(f"benchmark exited {outcome.returncode}")
                entry.update(status="ok", consistency_checks_passed=30)
            except (OSError, KeyError, ValueError, TypeError) as error:
                entry.update(status="failed", error=str(error))
                failed = True
        (cell / "status.json").write_text(json.dumps(entry, indent=2) + "\n")
        manifest["cells"].append(entry)
        (output / "comparison.json").write_text(json.dumps(manifest, indent=2) + "\n")
        print(label, entry["status"], flush=True)
    manifest["finished_at"] = datetime.datetime.now(datetime.timezone.utc).isoformat()
    (output / "comparison.json").write_text(json.dumps(manifest, indent=2) + "\n")
    return failed


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True, help="new artifact directory outside the repository")
    parser.add_argument("--slop-tpcc", type=Path, help="prebuilt root cmd/tpcc binary")
    parser.add_argument("--native-tpcc", type=Path, help="prebuilt cmd/tpcc/compare binary")
    parser.add_argument("--duckdb-wal-shim", type=Path, help="prebuilt cmd/tpcc/compare/nofsync/wal_sync.c shared library")
    parser.add_argument("--engines", nargs="+", choices=["slopdb", "sqlite", "duckdb"], default=["slopdb", "sqlite", "duckdb"])
    parser.add_argument("--page-sizes", nargs="+", type=int, default=DEFAULT_SIZES)
    parser.add_argument("--no-controls", action="store_false", dest="controls", help="omit backend default-size controls")
    parser.add_argument("--seconds", type=int, default=60)
    parser.add_argument("--workers", type=int, default=10)
    parser.add_argument("--warehouses", type=int, default=2)
    args = parser.parse_args()
    if min(args.seconds, args.workers, args.warehouses, *args.page_sizes) <= 0:
        parser.error("seconds, workers, warehouses and page sizes must be positive")
    for name in ("output", "slop_tpcc", "native_tpcc", "duckdb_wal_shim"):
        value = getattr(args, name)
        if value:
            setattr(args, name, value.resolve())
    required = (["slop_tpcc"] if "slopdb" in args.engines else []) + (["native_tpcc"] if any(engine != "slopdb" for engine in args.engines) else []) + (["duckdb_wal_shim"] if "duckdb" in args.engines else [])
    if args.output.is_relative_to(ROOT):
        parser.error("benchmark artifacts must be outside the repository")
    for name in required:
        if not getattr(args, name) or not getattr(args, name).is_file():
            parser.error(f"--{name.replace('_', '-')} must name an existing file")
    raise SystemExit(run(args))


if __name__ == "__main__":
    main()
