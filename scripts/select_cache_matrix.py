#!/usr/bin/env python3
"""Run checked small-row SELECT samples serially against one reusable seed.

Run on the benchmark host, after cmd/select-bench has created the seed. Sizes
are decimal bytes; the OS filesystem cache stays enabled and is not reset.
"""
import argparse
import hashlib
import json
import os
import platform
from pathlib import Path
import statistics
import subprocess
import time


def inventory(directory):
    files = []
    for path in sorted(directory.glob("*.sst")):
        digest = hashlib.sha256()
        with path.open("rb") as source:
            for chunk in iter(lambda: source.read(1024 * 1024), b""):
                digest.update(chunk)
        files.append({"name": path.name, "bytes": path.stat().st_size, "sha256": digest.hexdigest()})
    return files


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--dir", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--seconds", type=int, default=30)
    parser.add_argument("--samples", type=int, default=3)
    parser.add_argument("--workers", type=int, default=100)
    parser.add_argument("--policy", choices=["lru", "clock"], default="lru")
    parser.add_argument("--memory", choices=["heap", "mmap"], default="mmap")
    parser.add_argument("--reuse-results", action="store_true")
    parser.add_argument("--caches", type=int, nargs="+", default=[100_000_000, 1_000_000_000, 10_000_000_000, 20_000_000_000])
    args = parser.parse_args()
    api_mode = "ExecPreparedInto" if args.reuse_results else "ExecPrepared"
    args.output.mkdir(parents=True, exist_ok=False)
    manifest = {
        "started": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "host": platform.node(), "platform": platform.platform(),
        "cpus": os.cpu_count(),
        "environment": {key: os.environ.get(key, "runtime default") for key in ["GOGC", "GOMEMLIMIT", "GOMAXPROCS"]},
        "policy": args.policy,
        "memory": args.memory,
        "api_mode": api_mode, "reuse_results": args.reuse_results,
        "binary_sha256": hashlib.sha256(args.binary.read_bytes()).hexdigest(),
        "seed": json.loads((args.dir / "seed.json").read_text()),
        "method": "Serial fresh processes; same seeded SSTables; independent sessions; uniformly random prepared full-row primary-key SELECTs; every answer checked. Page-spaced warmup (stride 8 rows) before three timed samples. Linux filesystem cache enabled, never reset. Decimal byte budgets; decoded engine cache, not a process-memory limit. WAL enabled, WAL fsync disabled; no measured writes. Profiles use separate diagnostic runs.",
        "inventory_before": inventory(args.dir), "runs": [],
    }

    def save():
        (args.output / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")

    save()
    for cache in args.caches:
        label = f"cache-{cache}"
        report_path = args.output / f"{label}.json"
        command = [str(args.binary.resolve()), "-dir", str(args.dir.resolve()), "-cache-bytes", str(cache), "-cache-policy", args.policy, "-cache-memory", args.memory, "-workers", str(args.workers), "-duration", f"{args.seconds}s", "-samples", str(args.samples), "-json", str(report_path.resolve())]
        if args.reuse_results:
            command.append("-reuse-results")
        print(" ".join(command), flush=True)
        with (args.output / f"{label}.log").open("w") as log:
            completed = subprocess.run(["/usr/bin/time", "-v", "-o", str(args.output / f"{label}.resources.txt"), *command], stdout=log, stderr=subprocess.STDOUT)
        report = json.loads(report_path.read_text())
        if completed.returncode or report.get("Error") or len(report["Samples"]) != args.samples:
            raise RuntimeError(f"{label} failed: {report.get('Error')}")
        if report["Config"].get("ReuseResults", False) != args.reuse_results or report.get("APIMode", api_mode) != api_mode:
            raise RuntimeError(f"{label}: incorrect result API mode")
        for sample in report["Samples"]:
            if sample["Queries"] <= 0 or sample["Before"]["SeqNum"] != sample["After"]["SeqNum"] or sample["After"]["BlockCacheBytes"] > cache or sample["After"].get("BlockCachePolicy") != args.policy or sample["After"].get("BlockCacheMemory") != args.memory:
                raise RuntimeError(f"{label}: invalid results or mutation/cache-budget violation")
        row = {"cache_bytes": cache, "api_mode": api_mode, "command": command, "report": report_path.name,
               "median_selects_per_second": statistics.median(s["QueriesPerSecond"] for s in report["Samples"]),
               "median_cpu_us_per_query": statistics.median(s["CPUSeconds"] * 1e6 / s["Queries"] for s in report["Samples"]),
               "median_p99_us_upper_bound": statistics.median(s["P99US"] for s in report["Samples"]),
               "samples": []}
        for s in report["Samples"]:
            hits = s["After"]["BlockCacheHits"] - s["Before"]["BlockCacheHits"]
            misses = s["After"]["BlockCacheMisses"] - s["Before"]["BlockCacheMisses"]
            row["samples"].append({"selects_per_second": s["QueriesPerSecond"], "queries": s["Queries"], "hit_fraction": hits / max(1, hits + misses), "cache_bytes_before": s["Before"]["BlockCacheBytes"], "cache_bytes_after": s["After"]["BlockCacheBytes"], "mean_us": s["MeanUS"], "p99_us_upper_bound": s["P99US"], "bytes_per_query": s["AllocatedBytesPerQuery"], "allocs_per_query": s["AllocationsPerQuery"], "cpu_seconds": s.get("CPUSeconds"), "average_cpu_cores": s.get("AverageCPUCores"), "cpu_percent_of_host": s.get("CPUPercent"), "gc_cycles": s["GCs"], "gc_pause_seconds": s["GCPauseSeconds"], "voluntary_context_switches": s["VoluntaryContextSwitches"], "involuntary_context_switches": s["InvoluntaryContextSwitches"]})
        manifest["runs"].append(row)
        save()
        print(f"{cache}: {row['median_selects_per_second']:.0f} SELECT/s", flush=True)
    manifest["inventory_after"] = inventory(args.dir)
    manifest["sstables_unchanged"] = manifest["inventory_before"] == manifest["inventory_after"]
    save()
    if not manifest["sstables_unchanged"]:
        raise RuntimeError("seed SSTables changed")


if __name__ == "__main__":
    main()
