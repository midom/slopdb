#!/usr/bin/env python3
"""Regenerate scalar answers from an isolated Oracle MySQL 8 server.

No SlopDB process is used to produce expected values. Ordinary Go tests consume
only the checked-in fixture and need neither this script nor a MySQL install.
Use --collect to gather new candidates from the permanent function-family tests;
without it, the checked-in expression list remains unchanged.
"""
import argparse
import ast
import json
from pathlib import Path
import re
import subprocess
import xml.etree.ElementTree as ET

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUTPUT = ROOT / "mysqlserver/testdata/mysql-functions-mysql8.json"


def candidates():
    expressions = []
    for path in sorted((ROOT / "db").glob("mysql_function_*_test.go")) + sorted((ROOT / "sql").glob("mysql_function_*_test.go")):
        if "reference" in path.name:
            continue
        for match in re.finditer(r'"(?:[^"\\]|\\.)*"', path.read_text()):
            try:
                expression = ast.literal_eval(match.group())
            except (ValueError, SyntaxError):
                continue
            if not re.match(r"^[A-Z_][A-Z_0-9]*\(", expression):
                continue
            if "?" in expression or expression.startswith(("SELECT ", "VALUES ")):
                continue
            # Deliberate compatibility choices, volatile answers and allocation
            # policy tests are tested separately rather than oracle comparisons.
            if re.search(r"USING utf8(?:mb3)?\b", expression, re.I):
                continue
            if expression.startswith("TRIM(FROM "):
                continue  # supported native extension, not valid Oracle syntax
            if expression.startswith("DATE_ADD(") and "18446744073709551615" in expression:
                continue  # deliberate overflow rejection instead of signed wrap
            if re.match(r"^STRCMP\('a','a '\)$", expression):
                continue
            if re.search(r"\b(?:CURRENT_DATE|CURRENT_TIME|CURRENT_TIMESTAMP|UTC_DATE|UTC_TIME|UTC_TIMESTAMP|CURDATE|CURTIME|NOW|SYSDATE|LOCALTIME|LOCALTIMESTAMP)\b", expression):
                continue
            if "UUID()" in expression and expression not in ("IS_UUID(UUID())", "SUBSTRING(UUID(),15,1)", "UUID()=UUID()"):
                continue
            if "RANDOM_BYTES(" in expression and not expression.startswith("LENGTH("):
                continue
            if expression.startswith(("GET_LOCK(", "RELEASE_LOCK(", "IS_FREE_LOCK(", "LAST_INSERT_ID(", "ROW_COUNT(", "FOUND_ROWS(")):
                continue
            if expression.startswith("SLEEP(") and expression != "SLEEP(0)":
                continue
            if re.match(r"^(?:SPACE|LPAD|RPAD|REPEAT)\(", expression) and re.search(r"100000|18446744073709551615", expression):
                continue
            if expression.startswith("TO_BASE64(REPEAT(") or expression.startswith("UNCOMPRESS(X'00001000"):
                continue
            if expression.startswith("RAND(") and not re.match(r"^RAND\([0-9]+\)$", expression):
                continue
            if re.match(r"^(?:UNHEX|FROM_BASE64|AES_ENCRYPT|AES_DECRYPT|INET6_ATON|UUID_TO_BIN|CHAR)\(", expression):
                expression = "HEX(" + expression + ")"
            if re.match(r"^(?:LEFT|RIGHT|LPAD|RPAD|REVERSE|CONCAT_WS|ELT|INSERT|REPEAT|SUBSTR|MID|TRIM)\(", expression) and "X'" in expression:
                expression = "HEX(" + expression + ")"
            if expression.startswith("COMPRESS(") and expression != "COMPRESS(NULL)":
                continue  # legal zlib encoders can produce different byte streams
            expressions.append(expression)
    # Additional independently generated edge cases for complete family coverage.
    expressions += ["PI()", "RAND(3)", "ROUND(2.5)", "ROUND(2.5e0)",
                    "ROUND(-2.5)", "ROUND(-2.5e0)", "ROUND(12345678901234567890.12345,3)",
                    "TRUNCATE(12345678901234567890.12345,3)", "FLOOR(NULL)", "LOG(-1)",
                    "SQRT(-1)", "LN(0)", "MOD(10,0)", "POWER(0,-1)", "SIGN(NULL)",
                    "EXP(NULL)", "CRC32(NULL)", "HEX(NULL)", "ASCII('')",
                    "CHAR_LENGTH(NULL)", "REPEAT(NULL,2)", "CONCAT_WS(',',NULL,'a',NULL)",
                    "DATE_FORMAT(NULL,'%Y')", "TIMESTAMPDIFF(DAY,NULL,'2024-01-01')",
                    "EXTRACT(YEAR FROM NULL)", "TIME_TO_SEC(NULL)", "SEC_TO_TIME(NULL)",
                    "PERIOD_ADD(NULL,1)", "FROM_DAYS(NULL)", "FROM_UNIXTIME(NULL)",
                    "YEAR()", "LEFT('x')", "MD5()", "CONV('x',16)", "SHA2('x')"]
    return sorted(set(expressions))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--socket", required=True)
    parser.add_argument("--mysql-client", default="mysql")
    parser.add_argument("--user", default="root")
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--collect", action="store_true")
    options = parser.parse_args()

    def query(sql):
        command = [options.mysql_client, "--no-defaults", "--socket=" + options.socket,
                   "--user=" + options.user, "--xml", "--binary-as-hex",
                   "--execute", "SET time_zone='+00:00'; SET NAMES utf8mb4 COLLATE utf8mb4_bin; SET lc_time_names='en_US'; SET block_encryption_mode='aes-128-ecb'; " + sql]
        return subprocess.run(command, text=True, capture_output=True, check=False, timeout=10)

    version_result = query("SELECT VERSION()")
    if version_result.returncode:
        raise SystemExit(version_result.stderr)
    def cell(result):
        field = ET.fromstring(result.stdout).find("./row/field")
        if field is None:
            raise ValueError("reference returned no scalar")
        if field.get("{http://www.w3.org/2001/XMLSchema-instance}nil") == "true":
            return None
        return field.text or ""

    version = cell(version_result)
    if not version.startswith("8.") or "slopdb" in version.lower() or "mariadb" in version.lower():
        raise SystemExit("reference generation requires Oracle MySQL 8, got " + repr(version))
    if options.collect or not options.output.exists():
        expressions = candidates()
    else:
        expressions = [row["expression"] for row in json.loads(options.output.read_text())["cases"]]
    cases = []
    for expression in expressions:
        result = query("SELECT (" + expression + ")")
        if result.returncode:
            match = re.search(r"ERROR ([0-9]+)", result.stderr)
            cases.append({"expression": expression, "error": True,
                          "error_code": int(match.group(1)) if match else 0})
        else:
            value = cell(result)
            kind_result = query("SELECT JSON_TYPE(JSON_EXTRACT(JSON_ARRAY((" + expression + ")), '$[0]'))")
            if kind_result.returncode:
                raise RuntimeError("cannot classify reference expression " + expression + ": " + kind_result.stderr)
            kind = cell(kind_result)
            # Preserve the underlying DOUBLE value: e.g. PI()'s normal text
            # presentation rounds to six places while its binary value retains
            # full precision. The harness compares these numerically.
            if kind == "DOUBLE" and value is not None:
                precise = query("SELECT CAST((" + expression + ") AS DOUBLE)")
                if precise.returncode:
                    raise RuntimeError(precise.stderr)
                value = cell(precise)
            cases.append({"expression": expression, "kind": kind, "value": value})
    sql_mode = cell(query("SELECT @@SESSION.sql_mode"))
    fixture = {"source": {"engine": "Oracle MySQL", "version": version,
                          "time_zone": "+00:00", "character_set": "utf8mb4", "collation": "utf8mb4_bin",
                          "lc_time_names": "en_US", "sql_mode": sql_mode, "block_encryption_mode": "aes-128-ecb", "generator": "scripts/regenerate_mysql_function_references.py"},
               "cases": cases}
    options.output.parent.mkdir(parents=True, exist_ok=True)
    options.output.write_text(json.dumps(fixture, ensure_ascii=False, indent=2) + "\n")
    print(f"Wrote {len(cases)} Oracle MySQL cases to {options.output}")

if __name__ == "__main__":
    main()
