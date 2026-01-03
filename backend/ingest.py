from __future__ import annotations

import argparse
from pathlib import Path

from backend.db import create_tables, get_connection


def ingest_csv(csv_path: Path) -> int:
    conn = get_connection()
    create_tables(conn)
    conn.execute(
        """
        INSERT INTO raw_events
        SELECT *
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
