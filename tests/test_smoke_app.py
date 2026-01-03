from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(PROJECT_ROOT))

from backend.ingest import ingest_csv  # noqa: E402
from backend.pipeline import get_daily_counts, get_metrics, run_pipeline  # noqa: E402


def test_smoke_metrics_and_daily_counts(tmp_path: Path) -> None:
    db_path = tmp_path / "analytics.duckdb"
    csv_path = PROJECT_ROOT / "data" / "sample.csv"

    ingest_csv(csv_path, db_path=db_path)
    run_pipeline(db_path=db_path)

    metrics = get_metrics(db_path=db_path)
    assert metrics.total_events > 0
    assert metrics.unique_users > 0

    daily_counts = get_daily_counts(db_path=db_path)
    assert list(daily_counts.columns) == ["event_date", "daily_count"]
    assert daily_counts["daily_count"].sum() == metrics.total_events
