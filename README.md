# PROJECT_STARFISHPRIME

![PROJECT-STARFISHPRIME Logo](images/starfishprime_3d_text_cool.png)

Data platform for air quality + ILI forecasting.

## Structure
- `ingestion_pipeline_AQ`: OpenAQ recent + backfill
- `ingestion_pipeline_delphi`: FluView weekly upsert
- `transformation/dbt`: silver/gold models + seeds
- `ingestion_openmeteo.py`: temp 
- `ml-model`: training, batch predict, optional FastAPI
- `utils`: shared helpers
- `.github/workflows`: CI + optional deploy

## Quickstart
1. `\.\.venv\Scripts\Activate.ps1`
	- The repository already includes a pre-provisioned virtual environment with all GCP, Open-Meteo, Delphi, and analytics dependencies.
2. Export the BigQuery project/dataset defaults (adjust for your environment):
	```powershell
	$env:GOOGLE_CLOUD_PROJECT = 'your-gcp-project'
	$env:BQ_DATASET = 'raw'
	$env:BQ_LOCATION = 'europe-north2'
	```
3. Run ingestion jobs as needed:
	- OpenAQ: `python ingestion_pipeline_AQ/ingestion_openaq.py`
	- Open-Meteo: `python ingestion_pipeline_weather/ingestion_openmeteo.py`
	- Delphi FluView: `python ingestion_pipeline_delphi/ingestion_delphi.py`
4. Validate ingestion logic locally with pytest:
	```powershell
	Set-Item -Path Env:PYTHONPATH -Value (Get-Location)
	& '.\.venv\Scripts\python.exe' -m pytest tests
	```
5. Build silver/gold models with dbt (requires BigQuery access):
	```powershell
	cd transformation/transform_dbt
	$env:GOOGLE_CLOUD_PROJECT = 'your-gcp-project'
	$env:PROJECT = $env:GOOGLE_CLOUD_PROJECT
	$env:DBT_RAW = 'raw'
	$env:DBT_SILVER = 'silver'
	$env:DBT_GOLD = 'gold'
	$env:DBT_PROFILES_DIR = Join-Path (Get-Location) 'profiles'
	& '..\..\.venv\Scripts\dbt.exe' run
	```

## Cloud Run deployments via Cloud Build

Each ingestion pipeline ships with an idempotent Cloud Build configuration that now runs unit tests, builds the container image, and deploys/updates the corresponding Cloud Run job.

```powershell
gcloud builds submit --config ingestion_pipeline_weather/cloudbuild.yaml . \
  --substitutions=_IMAGE_TAG=$(git rev-parse --short HEAD),_SERVICE_ACCOUNT='cloud-run-ingest@your-project.iam.gserviceaccount.com'

gcloud builds submit --config ingestion_pipeline_delphi/cloudbuild.yaml . \
  --substitutions=_IMAGE_TAG=$(git rev-parse --short HEAD),_DELPHI_START_DATE='2022-01-03'

gcloud builds submit --config ingestion_pipeline_AQ/cloudbuild.yaml . \
  --substitutions=_IMAGE_TAG=$(git rev-parse --short HEAD)
```

Substitutions:

| Variable | Default | Notes |
| --- | --- | --- |
| `_REGION` | `europe-north2` | Cloud Run/Artifact Registry region |
| `_REPO` | `starfish-docker` | Artifact Registry repository |
| `_RAW_DATASET` | `raw` | BigQuery dataset target |
| `_OPENMETEO_TABLE` | `openmeteo_daily_ca` | Weather raw table |
| `_DELPHI_TABLE` | `fluview_ca_weekly` | FluView raw table |
| `_OPENAQ_TABLE` | `openaq_pm25_ca` | OpenAQ raw table |
| `_SERVICE_ACCOUNT` | `cloud-run-ingest@${PROJECT_ID}.iam.gserviceaccount.com` | Override with your job runtime SA |

Customize `job.env.yaml` files as needed; placeholders such as `${PROJECT_ID}` are resolved automatically during the Cloud Build deploy step.

## Nightly automation

A scheduled workflow lives in `.github/workflows/ingestion-nightly.yml`. Configure the following GitHub secrets to enable it:

- `GCP_PROJECT_ID`
- `GCP_WORKLOAD_IDENTITY_PROVIDER`
- `GCP_CLOUD_RUN_SA`

The workflow authenticates via Workload Identity Federation and executes the three Cloud Run jobs sequentially every day at 09:00 UTC.

## Continuous integration

`.github/workflows/ci.yml` runs on pull requests to `main`/`develop` and pushes to `main`. It installs pipeline dependencies, executes `pytest tests`, and performs `dbt parse` with placeholder configuration to keep the project in a deployable state.

## Verifying BigQuery loads manually

1. Trigger a job on demand (example for Open-Meteo):
	```powershell
	gcloud run jobs execute ingest-openmeteo --region=europe-north2 --project=your-gcp-project
	```
2. Tail the job logs (replace job name as needed) to observe per-point summaries and the final BigQuery row count emitted by the job:
	```powershell
	gcloud run jobs executions tail --region=europe-north2 --project=your-gcp-project --job=ingest-openmeteo
	```
3. Double-check the table contents directly:
	```powershell
	bq query --location=europe-north2 --nouse_legacy_sql "SELECT COUNT(*) AS row_count FROM `your-gcp-project.raw.openmeteo_daily_ca`"
	```
	Similar commands apply for `raw.fluview_ca_weekly` and `raw.openaq_pm25_ca`.

## Analytics models

- **New silver views**: `openaq_pm25_measurements` and `openaq_pm25_daily` expose PM2.5 measurements and daily aggregates from the raw OpenAQ ingestion table.
- **New gold view**: `openaq_pm25_with_weather` joins daily PM2.5 averages with matched weather metrics.
- **Data quality tests**: Custom dbt tests (`non_negative`, `unique_combination`) enforce non-negative pollution metrics and guard against duplicate grain issues. Flu/Weather joins now enforce non-negative `wili`/`ili` values.
