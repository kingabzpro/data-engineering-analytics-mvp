from __future__ import annotations

from pathlib import Path

import duckdb

DB_PATH = Path("data") / "analytics.duckdb"
SQL_DIR = Path("backend") / "sql"


def get_connection(db_path: Path | None = None) -> duckdb.DuckDBPyConnection:
    resolved_path = db_path or DB_PATH
    resolved_path.parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(str(resolved_path))


def create_tables(conn: duckdb.DuckDBPyConnection) -> None:
    schema_path = SQL_DIR / "raw_events.sql"
    conn.execute(schema_path.read_text(encoding="utf-8"))
