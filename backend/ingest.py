from __future__ import annotations

import argparse
import csv
import sys
from datetime import datetime
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(PROJECT_ROOT))

from backend.db import create_tables, get_connection  # noqa: E402

REQUIRED_COLUMNS = {"event_time", "user_id", "event_name", "category", "amount"}


def validate_csv(csv_path: Path) -> None:
    with csv_path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        fieldnames = reader.fieldnames or []
        missing = REQUIRED_COLUMNS - set(fieldnames)
        if missing:
            missing_list = ", ".join(sorted(missing))
            raise ValueError(f"Missing required columns: {missing_list}")

        for row_number, row in enumerate(reader, start=2):
            event_time = row.get("event_time")
            amount = row.get("amount")
            if not event_time:
                raise ValueError(f"Missing event_time at row {row_number}")
            if not amount:
                raise ValueError(f"Missing amount at row {row_number}")
            try:
                datetime.fromisoformat(event_time)
            except ValueError as exc:
                raise ValueError(
                    f"Invalid event_time at row {row_number}: {event_time}"
                ) from exc
            try:
                float(amount)
            except ValueError as exc:
                raise ValueError(
                    f"Invalid amount at row {row_number}: {amount}"
                ) from exc


def ingest_csv(csv_path: Path, db_path: Path | None = None) -> int:
    validate_csv(csv_path)
    conn = get_connection(db_path)
    create_tables(conn)
    conn.execute(
        """
        INSERT INTO raw_events
        SELECT source.*
        FROM read_csv_auto(
            ?,
            header=True,
            columns={
                'event_time': 'TIMESTAMP',
                'user_id': 'INTEGER',
                'event_name': 'VARCHAR',
                'category': 'VARCHAR',
                'amount': 'DOUBLE'
            }
        ) AS source
        WHERE NOT EXISTS (
            SELECT 1
            FROM raw_events AS target
            WHERE target.event_time = source.event_time
              AND target.user_id = source.user_id
              AND target.event_name = source.event_name
              AND target.category = source.category
              AND target.amount = source.amount
        )
        """,
        [str(csv_path)],
    )
    row_count = conn.execute("SELECT COUNT(*) FROM raw_events").fetchone()[0]
    conn.close()
    return int(row_count)


def main() -> None:
    parser = argparse.ArgumentParser(description="Ingest a CSV into DuckDB.")
    parser.add_argument(
        "--csv",
        dest="csv_path",
        default=str(Path("data") / "sample.csv"),
        help="Path to the CSV file to ingest.",
    )
    args = parser.parse_args()
    count = ingest_csv(Path(args.csv_path))
    print(f"raw_events row count: {count}")


if __name__ == "__main__":
    main()
