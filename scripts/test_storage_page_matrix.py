import importlib.util
import json
from pathlib import Path
import tempfile
import types
import unittest

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
                result = matrix.snapshot_native(types.SimpleNamespace(tpcc_snapshot=snapshot, tpcc_replicas=3),
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


if __name__ == "__main__":
    unittest.main()
