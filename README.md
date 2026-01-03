# Data Engineering + Analytics MVP

MVP only: CSV -> raw table -> transform -> 3 metrics -> Streamlit dashboard.

## Local setup (Python 3.11 + uv)

1) Create and activate a virtual environment:

```powershell
uv venv
.\.venv\Scripts\Activate.ps1
```

2) Install dependencies:

```powershell
uv pip install -e ".[dev]"
```

3) Ingest the sample CSV (prints row count):

```powershell
python backend\ingest.py --csv data\sample.csv
```

4) Run the Streamlit app:

```powershell
streamlit run app\app.py
```
