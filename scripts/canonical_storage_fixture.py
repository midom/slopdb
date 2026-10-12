#!/usr/bin/env python3
"""Dump one frozen logical fixture to portable SQL for every storage comparison.

The SQLite staging database is generation evidence only. Each measured engine
must execute the exact resulting SQL bytes, then compare ordered typed row
hashes with the manifest before and after maintenance.
"""
import argparse
import hashlib
import json
import math
import os
from pathlib import Path
import re
import sqlite3
import struct
import tempfile

ROOT = Path(__file__).resolve().parents[1]
TABLES = ("region", "nation", "supplier", "customer", "part", "partsupp", "orders", "lineitem")
INSERT_ROWS = 256
TRANSACTION_ROWS = 4096
HASH_PROTOCOL = "slopdb-canonical-rows-v1"


def file_sha256(path):
    result = hashlib.sha256()
    with Path(path).open("rb") as source:
        for block in iter(lambda: source.read(1 << 20), b""):
            result.update(block)
    return result.hexdigest()


def identifier(name):
    if not re.fullmatch(r"[A-Za-z_][A-Za-z_0-9]*", name):
        raise ValueError(f"nonportable identifier: {name!r}")
    return '"' + name + '"'


def portable_schema(sql):
    # Frozen schemas have no quoted strings containing these type names.
    sql = re.sub(r"\bREAL\b", "DOUBLE", re.sub(r"\bINTEGER\b", "BIGINT", sql))
    if sql.startswith("CREATE TABLE"):
        keys = []
        for constraint in re.finditer(r"PRIMARY KEY\s*\(([^)]*)\)", sql):
            keys.extend(name.strip() for name in constraint[1].split(","))
        keys.extend(match[1] for match in re.finditer(r"(?:\(|,)\s*(\w+)\s+(?:BIGINT|DOUBLE|TEXT)\s+PRIMARY KEY", sql))
        for key in keys:
            pattern = r"(\b" + re.escape(key) + r"\s+(?:BIGINT|DOUBLE|TEXT)\b)([^,)]*)"
            def required(match):
                return match[0] if re.search(r"\bNOT NULL\b", match[2]) else match[1] + " NOT NULL" + match[2]
            sql = re.sub(pattern, required, sql, count=1)
    return sql


def logical_type(sql_type):
    return {"BIGINT": "int64", "DOUBLE": "float64", "TEXT": "text"}[sql_type.upper()]


def normalize(value, kind):
    if value is None:
        return None
    if kind == "int64":
        if isinstance(value, bool) or not isinstance(value, int) or not -(1 << 63) <= value < (1 << 63):
            raise ValueError(f"invalid int64: {value!r}")
        return value
    if kind == "float64":
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise ValueError(f"invalid float64: {value!r}")
        value = float(value)
        if not math.isfinite(value):
            raise ValueError("nonfinite float64")
        return 0.0 if value == 0 else value
    if kind == "text":
        if not isinstance(value, str) or "\x00" in value:
            raise ValueError("canonical text must be a string without NUL")
        value.encode("utf-8", errors="strict")
        return value
    raise ValueError(f"unknown logical type {kind}")


def row_bytes(row, kinds):
    if len(row) != len(kinds):
        raise ValueError("row width does not match schema")
    result = bytearray(b"R" + struct.pack(">I", len(kinds)))
    for value, kind in zip(row, kinds):
        value = normalize(value, kind)
        if value is None:
            result.append(0)
        elif kind == "int64":
            result.append(1)
            result.extend(struct.pack(">q", value))
        elif kind == "float64":
            result.append(2)
            result.extend(struct.pack(">d", value))
        else:
            encoded = value.encode("utf-8")
            result.append(3)
            result.extend(struct.pack(">Q", len(encoded)))
            result.extend(encoded)
    return bytes(result)


def encode_row(row, logical_types):
    """Encode one row with the shared v1 protocol; useful to independent loaders."""
    return row_bytes(row, logical_types)


def fingerprint_rows(rows, logical_types):
    """Fingerprint a canonically ordered row stream without retaining rows."""
    digest = hashlib.sha256()
    count = logical_bytes = encoded_bytes = 0
    for row in rows:
        encoded = encode_row(row, logical_types)
        digest.update(encoded)
        count += 1
        encoded_bytes += len(encoded)
        logical_bytes += sum(0 if value is None else len(value.encode("utf-8")) if kind == "text" else 8
                             for value, kind in zip(row, logical_types))
    return {"rows": count, "logical_bytes": logical_bytes, "encoded_bytes": encoded_bytes, "sha256": digest.hexdigest()}


def literal(value, kind):
    value = normalize(value, kind)
    if value is None:
        return "NULL"
    if kind == "int64":
        return str(value)
    if kind == "float64":
        return repr(value)
    return "'" + value.replace("'", "''") + "'"


def schema_info(connection, name):
    info = connection.execute(f"PRAGMA table_info({identifier(name)})").fetchall()
    columns = [col[1] for col in info]
    kinds = [logical_type(col[2]) for col in info]
    primary = sorted((col[5], col[1]) for col in info if col[5])
    # TPC-H partsupp has an indexed logical key instead of declared PRIMARY KEY.
    order = [name for _, name in primary] or (["ps_partkey", "ps_suppkey"] if name == "partsupp" else columns)
    return columns, kinds, order


def insert_rows(connection, table, rows):
    columns, kinds, _ = schema_info(connection, table)
    query = f"INSERT INTO {identifier(table)} ({','.join(map(identifier, columns))}) VALUES ({','.join('?' for _ in columns)})"
    batch = []
    for row in rows:
        if len(row) != len(kinds):
            raise ValueError(f"{table}: source row has incorrect width")
        batch.append([normalize(value, kind) for value, kind in zip(row, kinds)])
        if len(batch) == TRANSACTION_ROWS:
            connection.executemany(query, batch)
            connection.commit()
            batch.clear()
    if batch:
        connection.executemany(query, batch)
        connection.commit()


def stage_tpcc(connection, source, source_sha256, replicas):
    if replicas < 1:
        raise ValueError("replicas must be positive")
    actual = file_sha256(source)
    if actual != source_sha256:
        raise ValueError("TPC-C source SHA256 mismatch")
    data = json.loads(Path(source).read_text())
    schema = [portable_schema(sql) for sql in data["ddl"]]
    tables = {table["name"]: table for table in data["tables"]}
    if len(tables) != len(data["tables"]) or not {"warehouse", "history", "item"} <= tables.keys():
        raise ValueError("TPC-C source table inventory is invalid")
    warehouse_stride = max(row[0] for row in tables["warehouse"]["rows"])
    history_stride = max(row[0] for row in tables["history"]["rows"]) + 1
    for sql in schema:
        if sql.startswith("CREATE TABLE"):
            connection.execute(sql)
    expected = {}
    for name, table in sorted(tables.items()):
        columns, _, _ = schema_info(connection, name)
        if columns != table["columns"]:
            raise ValueError(f"{name}: source columns differ from schema")
        count = 1 if name == "item" else replicas
        def replicated_rows():
            for replica in range(count):
                for row in table["rows"]:
                    yield [value + replica * warehouse_stride if col == "w_id" or col.endswith("_w_id") else
                           value + replica * history_stride if col == "h_id" else value
                           for col, value in zip(columns, row)]
        insert_rows(connection, name, replicated_rows())
        expected[name] = len(table["rows"]) * count
    for sql in schema:
        if not sql.startswith("CREATE TABLE"):
            connection.execute(sql)
    connection.execute("ANALYZE")
    connection.commit()
    if file_sha256(source) != actual:
        raise ValueError("TPC-C source changed during generation")
    return schema, expected, {"snapshot_sha256": actual}, {
        "replicas": replicas, "warehouse_stride": warehouse_stride, "history_stride": history_stride}


def stage_tpch(connection, source):
    source = Path(source)
    manifest_path = source / "manifest.json"
    manifest = json.loads(manifest_path.read_text())
    hashes = {"manifest_sha256": file_sha256(manifest_path)}
    for table in TABLES:
        expected_hash = manifest["sha256"][table]
        actual = file_sha256(source / f"{table}.tbl")
        if actual != expected_hash:
            raise ValueError(f"{table}: TPC-H source SHA256 mismatch")
        hashes[table] = actual
    schema_paths = [ROOT / "cmd/tpch/schema.sql", ROOT / "cmd/tpch/indexes.sql", ROOT / "cmd/tpch/queries/q15-setup.sql"]
    # These committed DDL files contain no semicolons inside literals.
    schema_parts = [[portable_schema(sql.strip()) + ";" for sql in path.read_text().split(";") if sql.strip()] for path in schema_paths]
    for sql in schema_parts[0]:
        connection.execute(sql)
    for table in TABLES:
        columns, kinds, _ = schema_info(connection, table)
        if columns != manifest["columns"][table]:
            raise ValueError(f"{table}: source columns differ from schema")
        def rows():
            with (source / f"{table}.tbl").open(encoding="utf-8", newline="") as input_file:
                for line in input_file:
                    values = line.rstrip("\r\n").split("|")
                    if values[-1] == "":
                        values.pop()
                    if len(values) != len(kinds):
                        raise ValueError(f"{table}: source row has incorrect width")
                    yield [int(value) if kind == "int64" else float(value) if kind == "float64" else value
                           for value, kind in zip(values, kinds)]
        insert_rows(connection, table, rows())
        actual = connection.execute(f"SELECT COUNT(*) FROM {identifier(table)}").fetchone()[0]
        if actual != manifest["rows"][table]:
            raise ValueError(f"{table}: source row count mismatch")
    for sql in schema_parts[1] + schema_parts[2]:
        connection.execute(sql)
    connection.execute("ANALYZE")
    connection.commit()
    for table in TABLES:
        if file_sha256(source / f"{table}.tbl") != hashes[table]:
            raise ValueError(f"{table}: TPC-H source changed during generation")
    if file_sha256(manifest_path) != hashes["manifest_sha256"]:
        raise ValueError("TPC-H manifest changed during generation")
    hashes["schema_files"] = {str(path.relative_to(ROOT)): file_sha256(path) for path in schema_paths}
    return sum(schema_parts, []), manifest["rows"], hashes, {"scale_factor": manifest["scale_factor"], "generator": manifest["generator"]}


def dump(connection, output, dataset, schema, expected, hashes, config):
    manifest = {"version": 1, "format": "slopdb-canonical-sql-v1", "dataset": dataset, "hash_protocol": HASH_PROTOCOL, "schema_sql": schema + ["ANALYZE;"],
                "source_hashes": hashes, "generation_config": {**config, "insert_rows": INSERT_ROWS, "transaction_rows": TRANSACTION_ROWS},
                "tables": [], "total_rows": 0, "total_logical_bytes": 0, "total_encoded_bytes": 0}
    with Path(output).open("w", encoding="utf-8", newline="\n") as target:
        target.write("-- SlopDB canonical storage fixture v1: " + dataset + "\n")
        for sql in schema:
            if sql.startswith("CREATE TABLE"):
                target.write(sql + "\n")
        for name in sorted(expected):
            columns, kinds, order = schema_info(connection, name)
            digest = hashlib.sha256()
            count = logical_bytes = encoded_bytes = tx_rows = 0
            cursor = connection.execute(f"SELECT {','.join(map(identifier, columns))} FROM {identifier(name)} ORDER BY {','.join(map(identifier, order))}")
            query = f"INSERT INTO {identifier(name)} ({','.join(map(identifier, columns))}) VALUES\n"
            while True:
                rows = cursor.fetchmany(min(INSERT_ROWS, TRANSACTION_ROWS - tx_rows))
                if not rows:
                    break
                if tx_rows == 0:
                    target.write("BEGIN;\n")
                target.write(query)
                for index, row in enumerate(rows):
                    encoded = row_bytes(row, kinds)
                    digest.update(encoded)
                    encoded_bytes += len(encoded)
                    logical_bytes += sum(0 if value is None else len(value.encode("utf-8")) if kind == "text" else 8
                                         for value, kind in zip(row, kinds))
                    target.write("(" + ",".join(literal(value, kind) for value, kind in zip(row, kinds)) + ")" + (";\n" if index == len(rows) - 1 else ",\n"))
                count += len(rows)
                tx_rows += len(rows)
                if tx_rows == TRANSACTION_ROWS:
                    target.write("COMMIT;\n")
                    tx_rows = 0
            if tx_rows:
                target.write("COMMIT;\n")
            if count != expected[name]:
                raise ValueError(f"{name}: staged row count mismatch")
            manifest["tables"].append({"name": name, "columns": columns, "logical_types": kinds, "order_by": order,
                                       "rows": count, "logical_bytes": logical_bytes, "encoded_bytes": encoded_bytes, "sha256": digest.hexdigest()})
            manifest["total_rows"] += count
            manifest["total_logical_bytes"] += logical_bytes
            manifest["total_encoded_bytes"] += encoded_bytes
        for sql in schema:
            if not sql.startswith("CREATE TABLE"):
                target.write(sql + "\n")
        target.write("ANALYZE;\n")
        target.flush()
        os.fsync(target.fileno())
    manifest["sql_sha256"] = file_sha256(output)
    manifest["sql_bytes"] = Path(output).stat().st_size
    return manifest


def generate(dataset, source, output, source_sha256=None, replicas=128):
    output = Path(output)
    sidecar = Path(str(output) + ".manifest.json")
    output.parent.mkdir(parents=True, exist_ok=True)
    if output.exists() or sidecar.exists():
        raise ValueError("canonical output already exists; refusing to replace frozen evidence")
    # SQL is published first; only the atomically published sidecar marks success.
    # Failure removes our newly created SQL, so there is no valid partial fixture.
    with tempfile.TemporaryDirectory(prefix="canonical-stage-", dir=output.parent) as directory:
        directory = Path(directory)
        connection = sqlite3.connect(directory / "reference.sqlite")
        try:
            connection.execute("PRAGMA journal_mode=OFF")
            connection.execute("PRAGMA synchronous=OFF")
            connection.execute("PRAGMA cache_size=-32768")
            connection.execute("PRAGMA temp_store=FILE")
            if dataset == "tpcc":
                if not source_sha256 or not re.fullmatch("[0-9a-f]{64}", source_sha256):
                    raise ValueError("TPC-C requires a pinned source SHA256")
                staged = stage_tpcc(connection, source, source_sha256, replicas)
            elif dataset == "tpch":
                staged = stage_tpch(connection, source)
            else:
                raise ValueError("unknown dataset")
            manifest = dump(connection, directory / "fixture.sql", dataset, *staged)
            staged_manifest = directory / "manifest.json"
            with staged_manifest.open("w", encoding="utf-8") as target:
                json.dump(manifest, target, indent=2, sort_keys=True)
                target.write("\n")
                target.flush()
                os.fsync(target.fileno())
            # link() is no-clobber even against concurrent generation attempts.
            os.link(directory / "fixture.sql", output)
            sidecar_published = False
            try:
                os.link(staged_manifest, sidecar)
                sidecar_published = True
                parent_fd = os.open(output.parent, os.O_RDONLY)
                try:
                    os.fsync(parent_fd)
                finally:
                    os.close(parent_fd)
            except BaseException:
                if sidecar_published:
                    sidecar.unlink()
                output.unlink()
                raise
            return manifest
        finally:
            connection.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dataset", choices=("tpcc", "tpch"), required=True)
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--source-sha256")
    parser.add_argument("--replicas", type=int, default=128)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    result = generate(args.dataset, args.source, args.output, args.source_sha256, args.replicas)
    print(json.dumps({"dataset": args.dataset, "rows": result["total_rows"], "sql_bytes": result["sql_bytes"], "sql_sha256": result["sql_sha256"]}), flush=True)


if __name__ == "__main__":
    main()
