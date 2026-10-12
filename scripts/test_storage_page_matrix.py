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

    def test_one_mib_only_loads_and_compacts_without_query_verification(self):
        with tempfile.TemporaryDirectory() as temporary:
            cell = Path(temporary)
            directory = cell / "database"
            directory.mkdir()
            args = types.SimpleNamespace(slop_tpch=Path("tpch"), slop_snapshot=Path("snapshot"),
                                         tpch_data=cell / "inputs", runs=3, warmups=1,
                                         timeout_seconds=180, load_batch_rows=4096,
                                         reference_report=cell / "must-not-read.json")
            calls = []
            def execute(command, log):
                calls.append(command)
                report = Path(command[command.index("-json") + 1])
                if "-mode" not in command:
                    self.assertIn("-load-only", command)
                    directory.mkdir()
                    (directory / "data.sst").write_bytes(b"data")
                    value = {"queries": [], "query_benchmarked": False, "load_seconds": 1,
                             "load_mode": "bulk_insert", "config": {"load_batch_rows": 4096}}
                else:
                    self.assertNotIn("-verify-report", command)
                    value = {"counts_verified_after_reopen": True}
                report.write_text(json.dumps(value))
            with patch.object(matrix, "run", side_effect=execute):
                result = matrix.tpch(args, "slopdb", 1048576, cell, directory)
            self.assertEqual(len(calls), 2)
            self.assertFalse(result["query_benchmarked"])
            self.assertFalse(result["query_answers_verified"])
            self.assertEqual(result["load_mode"], "bulk_insert")


if __name__ == "__main__":
    unittest.main()
