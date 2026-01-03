from __future__ import annotations

from pathlib import Path

from backend.db import create_tables, get_connection

SQL_DIR = Path("backend") / "sql"


def run_pipeline() -> None:
    conn = get_connection()
    create_tables(conn)
    transform_sql = (SQL_DIR / "010_fct_events.sql").read_text(encoding="utf-8")
    conn.execute(transform_sql)
    conn.close()


if __name__ == "__main__":
    run_pipeline()
