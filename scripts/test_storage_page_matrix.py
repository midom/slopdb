import importlib.util
import json
from pathlib import Path
import tempfile
import types
import unittest
from unittest.mock import patch

spec = importlib.util.spec_from_file_location("matrix", Path(__file__).with_name("storage_page_matrix.py"))
matrix = importlib.util.module_from_spec(spec)
spec.loader.exec_module(matrix)


class PageMatrixTests(unittest.TestCase):
    def test_native_limits(self):
        for engine in ("slopdb", "sqlite", "duckdb"):
            self.assertTrue(matrix.supported(engine, 16384))
            self.assertTrue(matrix.supported(engine, 65536))
            self.assertFalse(matrix.supported(engine, 12345))
        self.assertTrue(matrix.supported("slopdb", 1048576))
        self.assertFalse(matrix.supported("sqlite", 1048576))
        self.assertFalse(matrix.supported("duckdb", 1048576))

    def test_sqlite_snapshot_replication_and_reopen_page_size(self):
        for size in (16384, 65536):
            with self.subTest(size=size), tempfile.TemporaryDirectory() as temporary:
                cell = Path(temporary)
                database = cell / "database"
                database.mkdir()
                snapshot = cell / "snapshot.json"
                snapshot.write_text(json.dumps({"ddl": [
                    "CREATE TABLE warehouse(w_id INTEGER PRIMARY KEY, value INTEGER)",
                    "CREATE TABLE history(h_id INTEGER PRIMARY KEY, h_w_id INTEGER, value INTEGER)",
                    "CREATE TABLE item(i_id INTEGER PRIMARY KEY, value INTEGER)"], "tables": [
                    {"name": "warehouse", "columns": ["w_id", "value"], "rows": [[1, 11], [2, 12]]},
                    {"name": "history", "columns": ["h_id", "h_w_id", "value"], "rows": [[2, 1, 40], [7, 2, 41]]},
                    {"name": "item", "columns": ["i_id", "value"], "rows": [[1, 99]]}]}))
                result = matrix.snapshot_native(types.SimpleNamespace(tpcc_snapshot=snapshot, tpcc_replicas=3, load_batch_rows=2),
                                                "sqlite", size, cell, database)
                for state in ("closed", "after_maintenance"):
                    self.assertEqual(result[state]["verified_rows"], 13)
                    self.assertEqual(result[state]["rows"], {"warehouse": 6, "history": 6, "item": 1})
                    self.assertEqual(result[state]["storage_info"]["page_size"], size)
                    self.assertEqual(result[state]["wal_files"], [])
                    self.assertGreaterEqual(result[state]["file_bytes"], size)

    def test_measure_rejects_unclosed_wal(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            (directory / "test.wal").write_bytes(b"pending")
            with self.assertRaisesRegex(ValueError, "WAL remains"):
                matrix.measure(directory)

    def test_storage_only_loads_and_compacts_without_query_verification(self):
        for size, storage_only in ((1048576, False), (4096, True), (16384, True), (65536, True), (1048576, True)):
            with self.subTest(size=size, storage_only=storage_only), tempfile.TemporaryDirectory() as temporary:
                cell = Path(temporary)
                directory = cell / "database"
                directory.mkdir()
                args = types.SimpleNamespace(slop_tpch=Path("tpch"), slop_snapshot=Path("snapshot"),
                                             tpch_data=cell / "inputs", runs=3, warmups=1,
                                             timeout_seconds=180, load_batch_rows=4096, storage_only=storage_only,
                                             reference_report=cell / "must-not-read.json")
                calls = []
                def execute(command, log):
                    calls.append(command)
                    report = Path(command[command.index("-json") + 1])
                    if "-mode" not in command:
                        self.assertIn("-load-only", command)
                        directory.mkdir()
                        (directory / "1.sst").write_bytes(b"data")
                        value = {"queries": None, "query_benchmarked": False, "load_seconds": 1,
                                 "load_mode": "bulk_insert", "config": {"load_batch_rows": 4096},
                                 "page_size_bytes": size, "zstd_window_bytes": size,
                                 "table_rows": {"lineitem": 1}, "counts_verified_after_reopen": True}
                    else:
                        self.assertNotIn("-verify-report", command)
                        value = self.maintained_report(size)
                    report.write_text(json.dumps(value))
                with patch.object(matrix, "run", side_effect=execute):
                    result = matrix.tpch(args, "slopdb", size, cell, directory)
                self.assertEqual(len(calls), 2)
                self.assertFalse(result["query_benchmarked"])
                self.assertFalse(result["query_answers_verified"])
                self.assertEqual(result["load_mode"], "bulk_insert")

    @staticmethod
    def maintained_report(size):
        return {"page_size_bytes": size, "zstd_window_bytes": max(1024, size),
                "bottom_level": 6, "all_sstables_at_bottom": True,
                "maintenance_operation": "full_bottom_level_rewrite",
                "levels": [{"FileCount": 0, "TotalSize": 0, "Files": []} for _ in range(6)] +
                          [{"FileCount": 1, "TotalSize": 4, "Files": [{"Number": 1, "Size": 4}]}]}

    def test_maintenance_rejects_upper_level_files_and_old_windows(self):
        for size in (4096, 16384, 65536, 1048576):
            matrix.validate_slop_metadata(self.maintained_report(size), size, maintained=True)
            for level in range(6):
                value = self.maintained_report(size)
                value["levels"][level] = {"FileCount": 1, "TotalSize": 4, "Files": [{"Number": 2, "Size": 4}]}
                with self.assertRaisesRegex(ValueError, "above the bottom"):
                    matrix.validate_slop_metadata(value, size, maintained=True)
            value = self.maintained_report(size)
            value["zstd_window_bytes"] = 8 << 20
            with self.assertRaisesRegex(ValueError, "compression window"):
                matrix.validate_slop_metadata(value, size, maintained=True)
            value = self.maintained_report(size)
            value["maintenance_operation"] = "promote_only"
            with self.assertRaisesRegex(ValueError, "rewrite all files"):
                matrix.validate_slop_metadata(value, size, maintained=True)

    def test_physical_sstables_must_match_bottom_inventory(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            (directory / "1.sst").write_bytes(b"data")
            report = self.maintained_report(65536)
            report.update(matrix.measure(directory))
            matrix.validate_slop_inventory(report)
            (directory / "2.sst").write_bytes(b"orphan")
            report.update(matrix.measure(directory))
            with self.assertRaisesRegex(ValueError, "physical SSTable"):
                matrix.validate_slop_inventory(report)
            (directory / "2.sst").unlink()
            (directory / "1.sst").write_bytes(b"changed size")
            report.update(matrix.measure(directory))
            with self.assertRaisesRegex(ValueError, "physical SSTable"):
                matrix.validate_slop_inventory(report)
            (directory / "1.sst").unlink()
            report.update(matrix.measure(directory))
            with self.assertRaisesRegex(ValueError, "physical SSTable"):
                matrix.validate_slop_inventory(report)

    def test_storage_only_skips_queries_at_every_size(self):
        for size in (4096, 16384, 65536, 1048576):
            self.assertFalse(matrix.performance_enabled(size, storage_only=True))
        self.assertTrue(matrix.performance_enabled(4096))
        self.assertFalse(matrix.performance_enabled(1048576))


if __name__ == "__main__":
    unittest.main()
