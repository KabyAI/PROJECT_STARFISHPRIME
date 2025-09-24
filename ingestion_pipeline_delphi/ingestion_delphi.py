import os, sys, time
from datetime import date, timedelta
import pandas as pd
import requests
from google.cloud import bigquery

PROJECT = os.getenv("GOOGLE_CLOUD_PROJECT") or os.getenv("PROJECT_ID")
if not PROJECT:
    print("FATAL: GOOGLE_CLOUD_PROJECT (or PROJECT_ID) is not set.", file=sys.stderr)
    sys.exit(1)

BQ_DATASET  = os.getenv("BQ_DATASET", "silver")
BQ_TABLE    = os.getenv("BQ_TABLE", "fluview_test")
BQ_LOCATION = os.getenv("BQ_LOCATION", "europe-north2")
REGION      = os.getenv("FLUVIEW_REGION", "ca").lower()         # "ca" or "state:ca"
WEEKS_BACK  = int(os.getenv("DELPHI_WEEKS_BACK", "104"))        # 2 years default

BASE = "https://api.delphi.cmu.edu/epidata/fluview/"

def epiweek_from_date(d: date) -> int:
    # Approx (ISO); Delphi also supports "latest", which we use primarily
    y, w, _ = d.isocalendar()
    return y*100 + w

def fetch_latest(region: str, weeks_back: int) -> pd.DataFrame:
    params = {"regions": region, "latest": weeks_back}
    r = requests.get(BASE, params=params, timeout=60)
    j = r.json()
    if j.get("result") != 1 or not j.get("epidata"):
        raise RuntimeError(f"Delphi latest failed: {j.get('message')}")
    return pd.DataFrame(j["epidata"])

def fetch_range(region: str, weeks_back: int) -> pd.DataFrame:
    today = date.today()
    start_ew = epiweek_from_date(today - timedelta(weeks=weeks_back))
    end_ew   = epiweek_from_date(today)
    params = {"regions": region, "epiweeks": f"{start_ew}-{end_ew}"}
    r = requests.get(BASE, params=params, timeout=60)
    j = r.json()
    if j.get("result") != 1 or not j.get("epidata"):
        raise RuntimeError(f"Delphi range failed: {j.get('message')}")
    return pd.DataFrame(j["epidata"])

def fetch_fluview(region: str, weeks_back: int) -> pd.DataFrame:
    # try "latest" first then fallback to explicit range
    try:
        df = fetch_latest(region, weeks_back)
    except Exception as e:
        print(f"[WARN] latest failed ({e}); falling back to epiweek range")
        df = fetch_range(region, weeks_back)

    keep = ["epiweek","region","wili","ili","num_ili","num_patients","release_date"]
    for k in keep:
        if k not in df.columns:
            df[k] = None
    out = df[keep].copy()
    out["epiweek"] = pd.to_numeric(out["epiweek"], errors="coerce").astype("Int64")
    out["release_date"] = pd.to_datetime(out["release_date"], errors="coerce", utc=True)
    return out.dropna(subset=["region","epiweek"])

def ensure_table(client: bigquery.Client, table_id: str):
    try:
        client.get_table(table_id); return
    except Exception:
        pass
    schema = [
        bigquery.SchemaField("region","STRING",mode="REQUIRED"),
        bigquery.SchemaField("epiweek","INT64",mode="REQUIRED"),
        bigquery.SchemaField("wili","FLOAT64"),
        bigquery.SchemaField("ili","FLOAT64"),
        bigquery.SchemaField("num_ili","INT64"),
        bigquery.SchemaField("num_patients","INT64"),
        bigquery.SchemaField("release_date","TIMESTAMP"),
        bigquery.SchemaField("inserted_at","TIMESTAMP",
                             default_value_expression="CURRENT_TIMESTAMP()"),
    ]
    tbl = bigquery.Table(table_id, schema=schema)
    client.create_table(tbl)
    print(f"Created {table_id}")

def merge_upsert(client: bigquery.Client, staging_id: str, table_id: str):
    sql = f"""
    MERGE `{table_id}` T
    USING `{staging_id}` S
    ON  T.region = S.region AND T.epiweek = S.epiweek
    WHEN MATCHED THEN UPDATE SET
      wili = S.wili, ili = S.ili, num_ili = S.num_ili, num_patients = S.num_patients,
      release_date = S.release_date, inserted_at = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN INSERT (
      region, epiweek, wili, ili, num_ili, num_patients, release_date, inserted_at
    ) VALUES (
      S.region, S.epiweek, S.wili, S.ili, S.num_ili, S.num_patients, S.release_date, CURRENT_TIMESTAMP()
    );
    """
    client.query(sql, location=BQ_LOCATION).result()

def main():
    client = bigquery.Client(project=PROJECT, location=BQ_LOCATION)
    region = REGION if REGION.startswith("state:") else REGION
    table_id = f"{PROJECT}.{BQ_DATASET}.{BQ_TABLE}"
    ensure_table(client, table_id)

    df = fetch_fluview(region, WEEKS_BACK)
    if df.empty:
        print("No FluView rows. Exit."); return

    print(f"FluView rows prepared: {len(df)}")
    stg = f"{PROJECT}.{BQ_DATASET}._fluview_stage_{int(time.time())}"
    load_job = client.load_table_from_dataframe(
        df, stg,
        job_config=bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE"),
        location=BQ_LOCATION,
    )
    load_job.result()
    merge_upsert(client, stg, table_id)
    client.delete_table(stg, not_found_ok=True)
    print(f"Upserted {len(df)} rows into {table_id}")

if __name__ == "__main__":
    main()
