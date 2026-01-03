from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(PROJECT_ROOT))

from backend.ingest import ingest_csv  # noqa: E402
from backend.pipeline import get_metrics, run_pipeline  # noqa: E402


def test_metrics_shape_and_types(tmp_path: Path) -> None:
    db_path = tmp_path / "analytics.duckdb"
    csv_path = PROJECT_ROOT / "data" / "sample.csv"

    ingest_csv(csv_path, db_path=db_path)
    run_pipeline(db_path=db_path)
    metrics = get_metrics(db_path=db_path)

    payload = metrics.model_dump()
    assert set(payload.keys()) == {"total_events", "unique_users", "total_amount"}
    assert isinstance(metrics.total_events, int)
    assert isinstance(metrics.unique_users, int)
    assert isinstance(metrics.total_amount, float)
