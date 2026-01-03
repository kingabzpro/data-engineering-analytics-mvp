from __future__ import annotations

from pathlib import Path

from backend.db import create_tables, get_connection
from backend.models import Metrics

SQL_DIR = Path("backend") / "sql"


def run_pipeline(db_path: Path | None = None) -> None:
    conn = get_connection(db_path)
    create_tables(conn)
    transform_sql = (SQL_DIR / "010_fct_events.sql").read_text(encoding="utf-8")
    conn.execute(transform_sql)
    conn.close()


def get_metrics(db_path: Path | None = None) -> Metrics:
    conn = get_connection(db_path)
    metrics_sql = (SQL_DIR / "020_metrics.sql").read_text(encoding="utf-8")
    row = conn.execute(metrics_sql).fetchone()
    conn.close()
    if row is None:
        return Metrics(total_events=0, unique_users=0, total_amount=0.0)
    return Metrics(
        total_events=int(row[0]),
        unique_users=int(row[1]),
        total_amount=float(row[2]),
    )


if __name__ == "__main__":
    run_pipeline()
