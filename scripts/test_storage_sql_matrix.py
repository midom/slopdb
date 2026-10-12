import hashlib
import io
import json
from pathlib import Path
import sqlite3
import struct
import tempfile
import unittest
from types import SimpleNamespace

import storage_sql_matrix as matrix


class CanonicalStorageTests(unittest.TestCase):
    def test_streaming_lexer_preserves_sql(self):
        sql = "-- head ;\nBEGIN;\nCREATE TABLE \"t;\"(x TEXT);\n/* ; ' */ INSERT INTO \"t;\" VALUES ('a;\n''b'),('/*c*/'); COMMIT; -- tail ;\n"
        pieces = list(matrix.statements(io.StringIO(sql)))
        self.assertEqual(len(pieces), 4)
        self.assertIn("'a;\n''b'", pieces[2])
        con = sqlite3.connect(":memory:")
        for piece in pieces:
            con.execute(piece)
        self.assertEqual(con.execute('SELECT x FROM "t;"').fetchall(), [("a;\n'b",), ("/*c*/",)])
        con.close()

    def test_lexer_doubled_identifier_and_comments(self):
        pieces = list(matrix.statements(io.StringIO('/*a*/SELECT "a"";b", `a``;b`, [a]];b];-- ;\n;')))
        self.assertEqual(len(pieces), 1)
        self.assertTrue(pieces[0].endswith(";"))

    def test_lexer_errors_are_bounded(self):
        for sql in ("SELECT 'unterminated;", "/* bad", "SELECT 1"):
            with self.assertRaises(ValueError):
                list(matrix.statements(io.StringIO(sql)))
        with self.assertRaisesRegex(ValueError, "bounded limit"):
            list(matrix.statements(io.StringIO("SELECT '" + "x" * 100 + "';"), max_bytes=40))

    def test_exact_typed_fingerprint(self):
        payload, size = matrix.encode_row([None, 9007199254740993, -0.0, "aé"], ["int64", "int64", "float64", "text"])
        expected = b"R" + struct.pack(">I", 4) + b"\0" + b"\1" + struct.pack(">q", 9007199254740993) + b"\2" + struct.pack(">d", 0.0) + b"\3" + struct.pack(">Q", 3) + "aé".encode()
        self.assertEqual(payload, expected)
        self.assertEqual(size, 19)
        for value, kind in ((1.0, "int64"), (1, "float64"), (float("nan"), "float64"), (1 << 63, "int64"), (b"x", "text")):
            with self.assertRaises(ValueError):
                matrix.encode_row([value], [kind])

    def fixture(self, path):
        sql = "CREATE TABLE t(id BIGINT PRIMARY KEY NOT NULL, f DOUBLE, s TEXT); BEGIN; INSERT INTO t VALUES (9007199254740993,0.0,'a;é'),(9007199254740994,NULL,NULL); COMMIT;"
        path.write_text(sql)
        rows = [(9007199254740993, 0.0, "a;é"), (9007199254740994, None, None)]
        encoded = [matrix.encode_row(row, ["int64", "float64", "text"]) for row in rows]
        table = {"name": "t", "columns": ["id", "f", "s"], "logical_types": ["int64", "float64", "text"],
                 "order_by": ["id"], "rows": 2, "logical_bytes": sum(n for _, n in encoded),
                 "encoded_bytes": sum(len(p) for p, _ in encoded), "sha256": hashlib.sha256(b"".join(p for p, _ in encoded)).hexdigest()}
        return {"format": matrix.FORMAT, "dataset": "test", "tables": [table], "total_rows": 2,
                "total_logical_bytes": table["logical_bytes"], "sql_sha256": matrix.digest(path), "sql_bytes": path.stat().st_size}

    def test_sqlite_replay_and_both_fingerprints(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            sql = directory / "canonical.sql"
            manifest = self.fixture(sql)
            sidecar = directory / "manifest.json"
            sidecar.write_text(json.dumps(manifest))
            self.assertEqual(matrix.load_manifest(sidecar, sql), manifest)
            con = matrix.native_open("sqlite", directory / "db.sqlite", 16384)
            for statement in matrix.statements(io.StringIO(sql.read_text())):
                con.execute(statement)
            con.close()
            for maintain in (False, True):
                con = matrix.native_open("sqlite", directory / "db.sqlite", 16384)
                if maintain:
                    con.execute("VACUUM")
                phase = matrix.native_verify(con, manifest)
                self.assertEqual(phase["tables"][0]["sha256"], manifest["tables"][0]["sha256"])
                con.close()
            sql.write_text(sql.read_text() + " -- changed")
            with self.assertRaisesRegex(ValueError, "hash"):
                matrix.load_manifest(sidecar, sql)

    def test_native_cell_measures_only_verified_reopened_values(self):
        with tempfile.TemporaryDirectory() as temporary:
            cell = Path(temporary)
            sql = cell / "canonical.sql"
            manifest = self.fixture(sql)
            directory = cell / "database"
            directory.mkdir()
            report = matrix.native_cell(SimpleNamespace(sql=sql), manifest, "sqlite", 65536, cell, directory)
            self.assertEqual(report["sql_sha256"], manifest["sql_sha256"])
            self.assertEqual(report["statements"], 4)
            for phase in ("closed", "after_maintenance"):
                self.assertEqual(report[phase]["storage_info"]["page_size"], 65536)
                self.assertGreater(report[phase]["file_bytes"], 0)
                self.assertEqual(report[phase]["wal_files"], [])
                matrix.validate_phase(report[phase], manifest)

    def test_corruption_cannot_publish_size(self):
        with tempfile.TemporaryDirectory() as temporary:
            sql = Path(temporary) / "canonical.sql"
            manifest = self.fixture(sql)
            con = sqlite3.connect(":memory:")
            for statement in matrix.statements(io.StringIO(sql.read_text())):
                con.execute(statement)
            con.execute("UPDATE t SET s='corrupt' WHERE id=9007199254740993")
            with self.assertRaisesRegex(ValueError, "fingerprints"):
                matrix.native_verify(con, manifest)
            con.close()

    def test_full_bottom_level_gate(self):
        phase = {"page_size_bytes": 65536, "zstd_window_bytes": 65536, "bottom_level": 6,
                 "all_sstables_at_bottom": True, "maintenance_operation": "full_bottom_level_rewrite",
                 "levels": [{"FileCount": 0, "TotalSize": 0, "Files": []} for _ in range(7)]}
        matrix.validate_slop_metadata(phase, 65536, maintained=True)
        phase["levels"][0] = {"FileCount": 1, "TotalSize": 12, "Files": [{"Number": 1, "Size": 12}]}
        with self.assertRaisesRegex(ValueError, "above"):
            matrix.validate_slop_metadata(phase, 65536, maintained=True)


if __name__ == "__main__":
    unittest.main()
