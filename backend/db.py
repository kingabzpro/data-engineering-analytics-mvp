from __future__ import annotations

from pathlib import Path

import duckdb

DB_PATH = Path("data") / "analytics.duckdb"
SQL_DIR = Path("backend") / "sql"


def get_connection() -> duckdb.DuckDBPyConnection:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(str(DB_PATH))


def create_tables(conn: duckdb.DuckDBPyConnection) -> None:
    schema_path = SQL_DIR / "raw_events.sql"
    conn.execute(schema_path.read_text(encoding="utf-8"))
