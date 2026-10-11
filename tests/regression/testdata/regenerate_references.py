"""Recompute checked-in SQL answers with the standard-library SQLite driver.

The existing query lists remain unchanged. To regenerate the randomized join
inputs and queries themselves, run generate_join_cases.py first.
"""

import json
from pathlib import Path
import sqlite3


def main():
    root = Path(__file__).resolve().parent
    for stem in ("sql_differential", "performance", "join_optimization"):
        path = root / f"{stem}_cases.json"
        cases = json.loads(path.read_text(encoding="utf-8"))
        with sqlite3.connect(":memory:") as connection:
            connection.executescript(
                (root / f"{stem}_setup.sql").read_text(encoding="utf-8")
            )
            for case in cases:
                query = case.get("query") or case["Query"]
                rows = [list(row) for row in connection.execute(query)]
                if "rows" in case:
                    case["rows"] = rows
                else:
                    case["Rows"] = rows
        path.write_text(
            json.dumps(cases, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
        print(f"{path.name}: {len(cases)} queries, SQLite {sqlite3.sqlite_version}")


if __name__ == "__main__":
    main()
