from __future__ import annotations

import argparse
from datetime import datetime, timezone
from pathlib import Path
import sqlite3

import pandas as pd


def normalize_columns(frame: pd.DataFrame) -> pd.DataFrame:
    renamed = {}
    for col in frame.columns:
        cleaned = (
            str(col)
            .strip()
            .lower()
            .replace(" ", "_")
            .replace("-", "_")
            .replace("/", "_")
        )
        renamed[col] = cleaned
    return frame.rename(columns=renamed)


def transform_sales(frame: pd.DataFrame) -> pd.DataFrame:
    frame = normalize_columns(frame)

    if "order_date" in frame.columns:
        frame["order_date"] = pd.to_datetime(frame["order_date"], errors="coerce")

    if "quantity" in frame.columns:
        frame["quantity"] = (
            pd.to_numeric(frame["quantity"], errors="coerce")
            .fillna(0)
            .astype("int64")
        )

    if "unit_price" in frame.columns:
        frame["unit_price"] = (
            pd.to_numeric(frame["unit_price"], errors="coerce").fillna(0.0)
        )

    if "quantity" in frame.columns and "unit_price" in frame.columns:
        frame["revenue"] = frame["quantity"] * frame["unit_price"]

    return frame


def compute_metrics(frame: pd.DataFrame) -> pd.DataFrame:
    total_revenue = float(frame.get("revenue", pd.Series(dtype="float64")).sum())
    total_orders = int(frame.get("order_id", pd.Series(dtype="object")).nunique())
    avg_order_value = total_revenue / total_orders if total_orders else 0.0

    generated_at = datetime.now(timezone.utc).isoformat()
    return pd.DataFrame(
        [
            {"metric": "total_revenue", "value": total_revenue, "generated_at": generated_at},
            {"metric": "total_orders", "value": total_orders, "generated_at": generated_at},
            {"metric": "avg_order_value", "value": avg_order_value, "generated_at": generated_at},
        ]
    )


def load_csv(path: Path) -> pd.DataFrame:
    return pd.read_csv(path)


def write_table(conn: sqlite3.Connection, table: str, frame: pd.DataFrame) -> None:
    frame.to_sql(table, conn, if_exists="replace", index=False)


def run_pipeline(csv_path: Path, db_path: Path) -> None:
    raw_frame = load_csv(csv_path)
    transformed = transform_sales(raw_frame.copy())
    metrics = compute_metrics(transformed)

    db_path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(db_path) as conn:
        write_table(conn, "raw_sales", raw_frame)
        write_table(conn, "sales", transformed)
        write_table(conn, "metrics", metrics)

    print(f"Loaded {len(raw_frame)} rows into raw_sales.")
    print(f"Loaded {len(transformed)} rows into sales.")
    print("Computed metrics: total_revenue, total_orders, avg_order_value.")
    print(f"Database written to {db_path}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="CSV to SQLite ETL for the MVP.")
    parser.add_argument(
        "--csv",
        dest="csv_path",
        default="data/sales.csv",
        help="Path to the source CSV file.",
    )
    parser.add_argument(
        "--db",
        dest="db_path",
        default="data/warehouse.db",
        help="Path to the SQLite database file.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    run_pipeline(Path(args.csv_path), Path(args.db_path))


if __name__ == "__main__":
    main()
