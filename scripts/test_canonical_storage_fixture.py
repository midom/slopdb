import hashlib
import json
from pathlib import Path
import sqlite3
import struct
import tempfile
import unittest

import canonical_storage_fixture as fixture


class CanonicalFixtureTests(unittest.TestCase):
    def test_encoding_protocol_exact_int_null_unicode_and_negative_zero(self):
        row = [2**53 + 17, -0.0, "'雪;\n", None]
        expected = b"R" + struct.pack(">I", 4) + b"\x01" + struct.pack(">q", row[0]) + b"\x02" + struct.pack(">d", 0.0)
        text = row[2].encode("utf-8")
        expected += b"\x03" + struct.pack(">Q", len(text)) + text + b"\x00"
        self.assertEqual(fixture.row_bytes(row, ["int64", "float64", "text", "int64"]), expected)
        self.assertEqual(fixture.literal(-0.0, "float64"), "0.0")
        for value in [float("nan"), float("inf"), float("-inf")]:
            with self.assertRaises(ValueError):
                fixture.literal(value, "float64")
        for value in [2**63, -2**63 - 1, 3.0, True]:
            with self.assertRaises(ValueError):
                fixture.literal(value, "int64")
        with self.assertRaises(ValueError):
            fixture.literal("\x00", "text")

    def test_dump_roundtrip_quotes_newlines_semicolons_bounds_and_hash(self):
        with tempfile.TemporaryDirectory() as directory:
            connection = sqlite3.connect(Path(directory) / "reference.sqlite")
            schema = ["CREATE TABLE sample (k BIGINT NOT NULL PRIMARY KEY, x DOUBLE, t TEXT);"]
            connection.execute(schema[0])
            rows = [(2**53 + index, -0.0 if index == 0 else 1.25, "quoted '; -- 雪\nline") for index in range(4200)]
            connection.executemany("INSERT INTO sample VALUES (?,?,?)", reversed(rows))
            connection.commit()
            output = Path(directory) / "canonical.sql"
            result = fixture.dump(connection, output, "test", schema, {"sample": 4200}, {}, {})
            sql = output.read_text()
            self.assertEqual(sql.count("BEGIN;\n"), 2)
            self.assertEqual(sql.count("COMMIT;\n"), 2)
            self.assertEqual(sql.count('INSERT INTO "sample"'), 17)
            replay = sqlite3.connect(":memory:")
            replay.executescript(sql)
            self.assertEqual(replay.execute("SELECT * FROM sample ORDER BY k").fetchall(), rows)
            actual = hashlib.sha256()
            for row in rows:
                actual.update(fixture.row_bytes(row, ["int64", "float64", "text"]))
            self.assertEqual(result["tables"][0]["sha256"], actual.hexdigest())
            self.assertEqual(result["total_logical_bytes"], sum(16 + len(row[2].encode()) for row in rows))
            self.assertEqual(result["sql_sha256"], hashlib.sha256(output.read_bytes()).hexdigest())
            connection.close()
            replay.close()

    def test_portable_schema_requires_every_primary_key_component(self):
        sql = "CREATE TABLE a (x INTEGER, y INTEGER NOT NULL, f REAL, PRIMARY KEY(x,y));"
        portable = fixture.portable_schema(sql)
        self.assertEqual(portable, "CREATE TABLE a (x BIGINT NOT NULL, y BIGINT NOT NULL, f DOUBLE, PRIMARY KEY(x,y));")
        self.assertIn("i BIGINT NOT NULL PRIMARY KEY", fixture.portable_schema("CREATE TABLE item(i INTEGER PRIMARY KEY, p REAL);"))
        connection = sqlite3.connect(":memory:")
        connection.execute(portable)
        with self.assertRaises(sqlite3.IntegrityError):
            connection.execute("INSERT INTO a VALUES(NULL,1,1)")
        connection.close()

    def source(self, directory):
        source = Path(directory) / "seed.json"
        data = {"ddl": ["CREATE TABLE warehouse(w_id INTEGER PRIMARY KEY, ytd INTEGER NOT NULL);",
                        "CREATE TABLE history(h_id INTEGER PRIMARY KEY,w_id INTEGER,c_w_id INTEGER);",
                        "CREATE TABLE item(i_id INTEGER PRIMARY KEY,price INTEGER);"],
                "tables": [{"name": "warehouse", "columns": ["w_id", "ytd"], "rows": [[1, 12], [2, 13]]},
                           {"name": "history", "columns": ["h_id", "w_id", "c_w_id"], "rows": [[4, 1, 2]]},
                           {"name": "item", "columns": ["i_id", "price"], "rows": [[9, 2**53 + 19]]}]}
        source.write_text(json.dumps(data))
        return source

    def test_frozen_snapshot_replicates_once_then_replays_identical_sql(self):
        with tempfile.TemporaryDirectory() as directory:
            source = self.source(directory)
            output = Path(directory) / "fixture.sql"
            result = fixture.generate("tpcc", source, output, fixture.file_sha256(source), replicas=2)
            self.assertEqual(result["total_rows"], 7)
            self.assertEqual(result["format"], "slopdb-canonical-sql-v1")
            self.assertEqual(json.loads(Path(str(output) + ".manifest.json").read_text()), result)
            replay = sqlite3.connect(":memory:")
            replay.executescript(output.read_text())
            self.assertEqual(replay.execute("SELECT * FROM history ORDER BY h_id").fetchall(), [(4,1,2), (9,3,4)])
            self.assertEqual(replay.execute("SELECT price FROM item").fetchone()[0], 2**53 + 19)
            replay.close()
            with self.assertRaises(ValueError):
                fixture.generate("tpcc", source, output, fixture.file_sha256(source), replicas=2)
            second = Path(directory) / "second.sql"
            fixture.generate("tpcc", source, second, fixture.file_sha256(source), replicas=2)
            self.assertEqual(output.read_bytes(), second.read_bytes())

    def test_tpch_all_sources_pinned_schema_and_index_view_replay(self):
        with tempfile.TemporaryDirectory() as directory:
            directory = Path(directory)
            seed = sqlite3.connect(":memory:")
            seed.executescript(fixture.portable_schema((fixture.ROOT / "cmd/tpch/schema.sql").read_text()))
            columns, hashes, counts = {}, {}, {}
            for table in fixture.TABLES:
                names, kinds, _ = fixture.schema_info(seed, table)
                values = ["1" if kind == "int64" else "1.25" if kind == "float64" else "fixture text" for kind in kinds]
                (directory / f"{table}.tbl").write_text("|".join(values) + "|\n")
                columns[table], hashes[table], counts[table] = names, fixture.file_sha256(directory / f"{table}.tbl"), 1
            seed.close()
            (directory / "manifest.json").write_text(json.dumps({"columns": columns, "sha256": hashes, "rows": counts,
                                                                "scale_factor": 0.2, "generator": "test"}))
            output = directory / "tpch.sql"
            result = fixture.generate("tpch", directory, output)
            self.assertEqual(result["total_rows"], 8)
            replay = sqlite3.connect(":memory:")
            replay.executescript(output.read_text())
            self.assertEqual(replay.execute("SELECT count(*) FROM sqlite_master WHERE type='index' AND sql IS NOT NULL").fetchone()[0], 14)
            self.assertEqual(replay.execute("SELECT count(*) FROM sqlite_master WHERE name='tpch_revenue'").fetchone()[0], 1)
            for table in result["tables"]:
                values = replay.execute(f"SELECT * FROM {table['name']} ORDER BY {','.join(table['order_by'])}").fetchall()
                actual = hashlib.sha256(b"".join(fixture.row_bytes(row, table["logical_types"]) for row in values)).hexdigest()
                self.assertEqual(actual, table["sha256"])
            replay.close()
            (directory / "lineitem.tbl").write_text("tampered\n")
            invalid = directory / "invalid.sql"
            with self.assertRaisesRegex(ValueError, "SHA256 mismatch"):
                fixture.generate("tpch", directory, invalid)
            self.assertFalse(invalid.exists())
            self.assertFalse(Path(str(invalid) + ".manifest.json").exists())

    def test_invalid_hash_or_value_does_not_publish_any_output(self):
        with tempfile.TemporaryDirectory() as directory:
            source = self.source(directory)
            output = Path(directory) / "fixture.sql"
            for hash_value in [None, "0" * 64]:
                with self.assertRaises(ValueError):
                    fixture.generate("tpcc", source, output, hash_value)
                self.assertFalse(output.exists())
                self.assertFalse(Path(str(output) + ".manifest.json").exists())
            data = json.loads(source.read_text())
            data["tables"][2]["rows"][0][1] = 2**63
            source.write_text(json.dumps(data))
            with self.assertRaises(ValueError):
                fixture.generate("tpcc", source, output, fixture.file_sha256(source))
            self.assertFalse(output.exists())
            self.assertFalse(Path(str(output) + ".manifest.json").exists())


if __name__ == "__main__":
    unittest.main()
