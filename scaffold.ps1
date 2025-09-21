# scaffold.ps1 — safe version without hash-literals
$ErrorActionPreference = 'Stop'

function Write-Text($path, $text) {
  $dir = Split-Path $path -Parent
  if ($dir -and -not (Test-Path $dir)) { New-Item -ItemType Directory -Force -Path $dir | Out-Null }
  $text | Set-Content -Path $path -Encoding UTF8
}

# --- Folders ---
$dirs = @(
  "ingestion_pipeline_AQ",
  "ingestion_pipeline_delphi",
  "transformation/dbt/models/silver",
  "transformation/dbt/models/gold",
  "transformation/dbt/seeds",
  "ml-model",
  "utils",
  ".github/workflows"
)
foreach ($d in $dirs) { New-Item -ItemType Directory -Force -Path $d | Out-Null }

# --- Files ---
Write-Text 'ingestion_pipeline_AQ/Dockerfile' @'
FROM python:3.11-slim
WORKDIR /app
COPY . /app
RUN pip install --no-cache-dir -r requirements.txt || true
CMD ["python","ingestion_openaq.py"]
'@

Write-Text 'ingestion_pipeline_AQ/cloudbuild.yaml' @'
steps:
  - name: 'gcr.io/cloud-builders/docker'
    args: ['build','-t','$_IMAGE','-f','ingestion_pipeline_AQ/Dockerfile','ingestion_pipeline_AQ']
images:
  - '$_IMAGE'
'@

Write-Text 'ingestion_pipeline_AQ/ingestion_openaq.py' @'
if __name__ == "__main__":
    print("ingestion_openaq.py - TODO")
'@

Write-Text 'ingestion_pipeline_AQ/backfill_openaq.py' @'
if __name__ == "__main__":
    print("backfill_openaq.py - TODO")
'@

Write-Text 'ingestion_pipeline_delphi/Dockerfile' @'
FROM python:3.11-slim
WORKDIR /app
COPY . /app
RUN pip install --no-cache-dir -r requirements.txt || true
CMD ["python","ingestion_delphi.py"]
'@

Write-Text 'ingestion_pipeline_delphi/cloudbuild.yaml' @'
steps:
  - name: 'gcr.io/cloud-builders/docker'
    args: ['build','-t','$_IMAGE','-f','ingestion_pipeline_delphi/Dockerfile','ingestion_pipeline_delphi']
images:
  - '$_IMAGE'
'@

Write-Text 'ingestion_pipeline_delphi/ingestion_delphi.py' @'
if __name__ == "__main__":
    print("ingestion_delphi.py - TODO")
'@

Write-Text 'transformation/dbt/models/gold/pm25_state_weekly.sql' @'
-- TODO: aggregate PM2.5 to state-week level
select 1 as dummy;
'@

Write-Text 'transformation/dbt/models/gold/ili_ca.sql' @'
-- TODO: California ILI weekly metric
select 1 as dummy;
'@

Write-Text 'transformation/dbt/models/silver/.gitkeep' ''

Write-Text 'transformation/dbt/seeds/dim_epiweeks.csv' @'
epiweek,start_date,end_date
2025-01,2025-01-01,2025-01-07
'@

Write-Text 'ml-model/train.py' @'
# Train model; save to GCS (pickle)
if __name__ == "__main__":
    print("train.py - TODO")
'@

Write-Text 'ml-model/batch_predict.py' @'
# Cloud Run Job: read BQ -> score -> write BQ
if __name__ == "__main__":
    print("batch_predict.py - TODO")
'@

Write-Text 'ml-model/serve_app.py' @'
from fastapi import FastAPI
app = FastAPI()

@app.get("/health")
def health():
    return {"status": "ok"}
'@

Write-Text 'ml-model/requirements.txt' @'
google-cloud-bigquery
google-cloud-storage
pandas
pyarrow
fastapi
uvicorn
scikit-learn
'@

Write-Text 'utils/epiweek_tools.py' @'
# Helpers for epiweek <-> date conversions
def to_epiweek(dt): ...
def from_epiweek(ew): ...
'@

Write-Text 'utils/bq_helpers.py' @'
# BigQuery helpers: upsert/merge, load, schema
def upsert_df(df, table_id, keys): ...
'@

Write-Text '.github/workflows/ci.yaml' @'
name: CI
on: [push, pull_request]
jobs:
  ci:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: "3.11"
      - name: Install tools
        run: pip install black flake8 pytest
      - name: Lint
        run: |
          black --check .
          flake8 .
      - name: Tests
        run: pytest -q || echo "no tests yet"
'@

Write-Text '.github/workflows/deploy_cloudrun.yaml' @'
name: Deploy Cloud Run (manual)
on:
  workflow_dispatch:
    inputs:
      service:
        description: "Service folder (e.g., ingestion_pipeline_AQ)"
        required: true
jobs:
  deploy:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - name: gcloud auth
        uses: google-github-actions/auth@v2
        with:
          workload_identity_provider: ${{ secrets.GCP_WIF_PROVIDER }}
          service_account: ${{ secrets.GCP_WIF_SA }}
      - name: Setup gcloud
        uses: google-github-actions/setup-gcloud@v2
      - name: Build & Deploy
        run: echo "Implement build/deploy here"
'@

Write-Text '.gitignore' @'
# Python
__pycache__/
*.py[cod]
*.egg-info/
.ipynb_checkpoints/

# Virtual envs
.venv/
venv/
env/

# OS/IDE
.vscode/
.DS_Store

# DBT
target/
dbt_packages/

# Local artifacts
*.log
'@

Write-Text 'README.md' @'
# PROJECT_STARFISHPRIME

Data platform for air quality + ILI forecasting.

## Structure
- `ingestion_pipeline_AQ`: OpenAQ recent + backfill
- `ingestion_pipeline_delphi`: FluView weekly upsert
- `transformation/dbt`: silver/gold models + seeds
- `ml-model`: training, batch predict, optional FastAPI
- `utils`: shared helpers
- `.github/workflows`: CI + optional deploy

## Quickstart
1. `python -m venv .venv && .\.venv\Scripts\Activate.ps1`
2. `pip install -r ml-model/requirements.txt`
3. `python ingestion_pipeline_AQ/ingestion_openaq.py`
'@

Write-Host "Scaffold complete."
