from __future__ import annotations

import sys
from pathlib import Path

import streamlit as st

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(PROJECT_ROOT))

from backend.pipeline import get_daily_counts, get_metrics, run_pipeline  # noqa: E402


def build_app() -> None:
    st.set_page_config(page_title="Data Engineering MVP", layout="wide")
    st.title("Data Engineering + Analytics MVP")

    run_pipeline()
    metrics = get_metrics()
    daily_counts = get_daily_counts()

    col1, col2, col3 = st.columns(3)
    col1.metric("Total Events", f"{metrics.total_events:,}")
    col2.metric("Unique Users", f"{metrics.unique_users:,}")
    col3.metric("Total Amount", f"{metrics.total_amount:,.2f}")

    st.subheader("Daily Event Count")
    if daily_counts.empty:
        st.info("No events available yet.")
    else:
        chart_data = daily_counts.set_index("event_date")
        st.line_chart(chart_data["daily_count"])


build_app()
