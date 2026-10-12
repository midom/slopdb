import json
from pathlib import Path
import subprocess
import tempfile
import types
import unittest
from unittest.mock import patch

from tpcc_page_matrix import cases, environment, run, validate


def report(engine="slopdb", size=16384):
    info = {"page_size_bytes": size, "zstd_window_bytes": max(1024, size), "wal_fsync": False, "wal": True,
            "journal_mode": "wal", "synchronous": "OFF", "cache_size_kib": 131072,
            "wal_checkpoint_target_bytes": 64 << 20, "threads": 1, "memory_limit": "1GB",
            "wal_fsync_calls_skipped": 8}
    return {"total": {"failed": 0, "committed": 10, "expected_rollbacks": 2, "transactions_per_second": 10},
            "config": {"engine": engine, "requested_page_size_bytes": size, "workers": 10,
                       "duration_ns": 60_000_000_000, "warehouses": 2, "districts_per_warehouse": 10,
                       "customers_per_district": 100, "items": 1000, "seed": 42,
                       "prepared_statements": True, "wal_fsync": False},
            "engine_info": info, "consistency_checks": {str(i): "pass" for i in range(15)},
            "reopen_consistency_checks": {str(i): "pass" for i in range(15)}}


class TimedPageMatrixTests(unittest.TestCase):
    def test_controls_are_unique_and_backend_specific(self):
        self.assertEqual(list(cases(["slopdb", "sqlite", "duckdb"], [16384, 65536, 1048576])), [
            ("slopdb", 16384), ("slopdb", 65536), ("slopdb", 1048576), ("slopdb", 4096),
            ("sqlite", 16384), ("sqlite", 65536), ("sqlite", 1048576), ("sqlite", 4096),
            ("duckdb", 16384), ("duckdb", 65536), ("duckdb", 1048576), ("duckdb", 262144)])
        self.assertEqual(list(cases(["slopdb"], [4096, 4096])), [("slopdb", 4096)])

    def test_validation_rejects_failed_checks_and_setting_changes(self):
        for engine in ("slopdb", "sqlite", "duckdb"):
            validate(report(engine), engine, 16384, 60, 10, 2)
        changes = [("total", "failed", 1), ("total", "committed", 0),
                   ("config", "wal_fsync", True), ("config", "prepared_statements", False),
                   ("config", "duration_ns", 1), ("engine_info", "page_size_bytes", 4096),
                   ("engine_info", "wal_fsync", True), ("engine_info", "zstd_window_bytes", 8 << 20), ("reopen_consistency_checks", "1", "failed")]
        for section, key, value in changes:
            broken = report()
            broken[section][key] = value
            with self.subTest(section=section, key=key), self.assertRaises(ValueError):
                validate(broken, "slopdb", 16384, 60, 10, 2)
        duck = report("duckdb")
        duck["engine_info"]["wal_fsync_calls_skipped"] = 0
        with self.assertRaisesRegex(ValueError, "skipped no calls"):
            validate(duck, "duckdb", 16384, 60, 10, 2)
        missing = report()
        missing["consistency_checks"].pop("1")
        with self.assertRaises(ValueError):
            validate(missing, "slopdb", 16384, 60, 10, 2)

    def test_exact_wal_path_and_no_cross_cell_override(self):
        args = types.SimpleNamespace(duckdb_wal_shim=Path("/task/wal.so"))
        with patch.dict("os.environ", {"SLOPDB_TPCC_WAL_FILE": "/old/path", "LD_PRELOAD": "/other.so"}):
            native = environment(args, "duckdb", Path("/task/current/database"))
            self.assertEqual(native["SLOPDB_TPCC_WAL_FILE"], "/task/current/database/benchmark.duckdb.wal")
            self.assertEqual(native["LD_PRELOAD"], "/task/wal.so /other.so")
            self.assertNotIn("SLOPDB_TPCC_WAL_FILE", environment(args, "sqlite", Path("/task/sqlite")))

    def test_serial_cells_retain_failures_and_unsupported_results(self):
        with tempfile.TemporaryDirectory() as temporary:
            base = Path(temporary)
            binary, shim = base / "binary", base / "shim.so"
            binary.write_bytes(b"binary")
            shim.write_bytes(b"shim")
            args = types.SimpleNamespace(output=base / "results", slop_tpcc=binary, native_tpcc=binary,
                                         duckdb_wal_shim=shim, engines=["slopdb", "sqlite", "duckdb"],
                                         page_sizes=[16384, 1048576], controls=False, seconds=60, workers=10, warehouses=2)
            calls = []
            def execute(command, **options):
                def option(name):
                    return command[command.index(name) + 1]
                engine, size = option("-engine"), int(option("-page-size"))
                calls.append((engine, size))
                value = report(engine, size)
                if engine == "sqlite":
                    value["total"]["failed"] = 1
                Path(option("-json")).write_text(json.dumps(value))
                options["stdout"].write("retained diagnostic log\n")
                return subprocess.CompletedProcess(command, 0)
            with patch("tpcc_page_matrix.subprocess.run", side_effect=execute), patch("tpcc_page_matrix.subprocess.check_output", return_value="revision\n"):
                self.assertTrue(run(args))
            self.assertEqual(calls, [("slopdb", 16384), ("sqlite", 16384), ("duckdb", 16384)])
            results = json.loads((args.output / "comparison.json").read_text())
            self.assertEqual([cell["status"] for cell in results["cells"]], ["ok", "skipped", "failed", "unsupported", "ok", "unsupported"])
            for cell in results["cells"]:
                folder = args.output / f"tpcc-{cell['engine']}-{cell['requested_page_size_bytes']}"
                self.assertTrue((folder / "run.log").is_file())
                self.assertTrue((folder / "status.json").is_file())
            self.assertTrue((args.output / "tpcc-sqlite-16384/report.json").is_file())
            with self.assertRaises(FileExistsError):
                run(args)


if __name__ == "__main__":
    unittest.main()
