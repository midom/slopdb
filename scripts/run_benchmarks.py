#!/usr/bin/env python3
"""Run sequential, reproducible SlopDB benchmarks and retain failed reports."""
import argparse
import datetime
import json
import os
from pathlib import Path
import platform
import subprocess
import tempfile


ROOT = Path(__file__).resolve().parents[1]


def positive_list(text):
    values = [int(value) for value in text.split(",")]
    if not values or any(value < 1 for value in values):
        raise argparse.ArgumentTypeError("expected comma-separated positive integers")
    return list(dict.fromkeys(values))


def validate_report(kind, report):
    if kind == "tpcc":
        if report["total"]["failed"]:
            raise ValueError("TPC-C recorded unexpected transaction failures")
        for key in ("consistency_checks", "reopen_consistency_checks"):
            if len(report[key]) != 15 or any(value != "pass" for value in report[key].values()):
                raise ValueError(f"TPC-C {key} failed")
        if report["total"]["committed"] <= 0:
            raise ValueError("TPC-C completed no transactions")
    elif kind == "tpch":
        if report["failed_executions"] or report["timed_out_executions"]:
            raise ValueError("TPC-H failed or timed out")
        if not report["counts_verified_after_reopen"]:
            raise ValueError("TPC-H reopened row counts differ")
        expected = report["config"]["runs"] * 22
        if report["successful_executions"] != expected:
            raise ValueError("TPC-H measured query executions are incomplete")


def execute(command, output, label):
    print(label, flush=True)
    with (output / f"{label}.log").open("w") as log:
        completed = subprocess.run(command, cwd=ROOT, stdout=log, stderr=subprocess.STDOUT)
    return completed.returncode


def run(args):
    if args.output:
        output = args.output.resolve()
        output.mkdir(parents=True, exist_ok=False)
    else:
        output = Path(tempfile.mkdtemp(prefix="slopdb-benchmarks-"))
    manifest = {
        "started_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "revision": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip(),
        "working_tree": subprocess.check_output(["git", "status", "--porcelain"], cwd=ROOT, text=True).splitlines(),
        "go_version": subprocess.check_output(["go", "version"], text=True).strip(),
        "platform": platform.platform(), "cpus": os.cpu_count(),
        "method": "Sequential subprocesses, fresh databases, fixed seed 42, prepared execution, WAL enabled and WAL fsync disabled. No OS cache resets. Resource counters include shutdown drain; RSS is whole-process high water. Timings are descriptive, not CI pass/fail thresholds.",
        "configuration": {"seconds": args.seconds, "repetitions": args.repetitions,
                          "workers": args.workers, "warehouses": args.warehouses,
                          "tpch_data": [str(path.resolve()) for path in args.tpch_data]},
        "runs": [],
    }
    failures = []
    manifest_path = output / "manifest.json"
    def save():
        manifest["failures"] = failures
        manifest_path.write_text(json.dumps(manifest, indent=2) + "\n")
    save()
    tpcc = output / "tpcc"
    code = execute(["go", "build", "-o", str(tpcc), "./cmd/tpcc"], output, "build-tpcc")
    if code:
        failures.append({"run": "build-tpcc", "exit_code": code})
        save()
        return True
    if not args.skip_micro:
        command = ["go", "test", "./sql", "./tests/regression", "-run", "^$", "-bench",
                   "Benchmark(TypedQ[16]Kernel|ReadPlans)$", "-benchmem", "-count", str(args.repetitions),
                   "-benchtime", "200ms", "-timeout", "10m"]
        code = execute(command, output, "microbenchmarks")
        if code:
            failures.append({"run": "microbenchmarks", "exit_code": code})
        save()
    for warehouses in args.warehouses:
        for workers in args.workers:
            for repetition in range(args.repetitions):
                label = f"tpcc-w{workers}-warehouses{warehouses}-r{repetition + 1}"
                report_path = output / f"{label}.json"
                command = [str(tpcc), "-workers", str(workers), "-warehouses", str(warehouses),
                           "-duration", f"{args.seconds}s", "-seed", "42", "-prepared=true",
                           "-wal-fsync=false", "-dir", str(output / f"{label}-database"), "-json", str(report_path)]
                code = execute(command, output, label)
                entry = {"label": label, "kind": "tpcc", "command": command, "exit_code": code, "report": report_path.name}
                try:
                    report = json.loads(report_path.read_text())
                    validate_report("tpcc", report)
                    entry["tps"] = report["total"]["transactions_per_second"]
                    entry["p99_ms"] = report["total"]["p99_ms"]
                    entry["validation"] = "pass"
                except (OSError, KeyError, ValueError) as error:
                    entry["validation"] = str(error)
                    code = code or 1
                if code:
                    failures.append({"run": label, "exit_code": code, "error": entry.get("validation")})
                manifest["runs"].append(entry)
                save()
    if args.tpch_data:
        tpch = output / "tpch"
        if execute(["go", "build", "-o", str(tpch), "./cmd/tpch"], output, "build-tpch"):
            failures.append({"run": "build-tpch", "exit_code": 1})
        else:
            for index, data in enumerate(args.tpch_data):
                label = f"tpch-scale{index + 1}"
                report_path = output / f"{label}.json"
                command = [str(tpch), "-data", str(data.resolve()), "-dir", str(output / f"{label}-database"),
                           "-warmups", "1", "-runs", str(args.repetitions), "-timeout", "60s", "-json", str(report_path)]
                code = execute(command, output, label)
                entry = {"label": label, "kind": "tpch", "command": command, "exit_code": code, "report": report_path.name}
                try:
                    validate_report("tpch", json.loads(report_path.read_text()))
                    entry["validation"] = "pass"
                except (OSError, KeyError, ValueError) as error:
                    entry["validation"] = str(error)
                    code = code or 1
                if code:
                    failures.append({"run": label, "exit_code": code, "error": entry.get("validation")})
                manifest["runs"].append(entry)
                save()
    save()
    print(f"Reports: {output}", flush=True)
    return bool(failures)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, help="new report directory; existing paths are rejected")
    parser.add_argument("--workers", type=positive_list, default=[1, 4, 10])
    parser.add_argument("--warehouses", type=positive_list, default=[2, 8])
    parser.add_argument("--seconds", type=int, default=15)
    parser.add_argument("--repetitions", type=int, default=3)
    parser.add_argument("--tpch-data", type=Path, action="append", default=[], help="repeat for multiple DBGEN scales")
    parser.add_argument("--skip-micro", action="store_true")
    args = parser.parse_args()
    if args.seconds < 1 or args.repetitions < 1:
        parser.error("seconds and repetitions must be positive")
    raise SystemExit(run(args))
