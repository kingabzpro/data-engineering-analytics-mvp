from __future__ import annotations

import sys
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(PROJECT_ROOT))

from backend.ingest import ingest_csv  # noqa: E402


def test_ingest_idempotent(tmp_path: Path) -> None:
    db_path = tmp_path / "analytics.duckdb"
    csv_path = PROJECT_ROOT / "data" / "sample.csv"

    first_count = ingest_csv(csv_path, db_path=db_path)
    second_count = ingest_csv(csv_path, db_path=db_path)

    assert first_count == second_count == 50
