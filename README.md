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
