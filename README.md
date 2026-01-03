# Data Engineering + Analytics MVP

MVP only: CSV -> raw table -> transform -> 3 metrics -> Streamlit dashboard.

## Local setup (Python 3.11 + uv)

1) Install uv 0.9.21 (if not already installed):

```powershell
pip install uv==0.9.21
```

2) Create and activate a virtual environment with uv:

```powershell
uv venv .venv
.\.venv\Scripts\Activate.ps1
```

3) Install dependencies:

```powershell
uv pip install -e ".[dev]"
```

4) Ingest the sample CSV (prints row count):

```powershell
python backend\ingest.py --csv data\sample.csv
```

5) Run the Streamlit app:

```powershell
streamlit run app\app.py
```

## Local Demo

From the repo root:

```powershell
python backend\ingest.py --csv data\sample.csv
python backend\pipeline.py
streamlit run app\app.py
```

## Quick verification

Run the ingest twice; the row count should stay the same (3738):

```powershell
python backend\ingest.py --csv data\sample.csv
python backend\ingest.py --csv data\sample.csv
```

## Sanity checks

Confirm raw and fact row counts after running the pipeline:

```powershell
python backend\ingest.py --csv data\sample.csv
python backend\pipeline.py
python - <<'PY'
import duckdb

conn = duckdb.connect("data/analytics.duckdb")
raw_count = conn.execute("SELECT COUNT(*) FROM raw_events").fetchone()[0]
fct_count = conn.execute("SELECT COUNT(*) FROM fct_events").fetchone()[0]
print(f"raw_events count: {raw_count}")
print(f"fct_events count: {fct_count}")
conn.close()
PY
```

## Verify

Rebuild the DB from scratch, ingest, transform, and run tests:

```powershell
.\scripts\verify.ps1
```
