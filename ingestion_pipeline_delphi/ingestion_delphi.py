import os
import sys
import time
import datetime as dt
from typing import List

import pandas as pd
import requests
from google.cloud import bigquery


PROJECT = os.getenv("GOOGLE_CLOUD_PROJECT") or os.getenv("PROJECT_ID")
if not PROJECT:
    print("FATAL: GOOGLE_CLOUD_PROJECT (or PROJECT_ID) is not set.", file=sys.stderr)
    sys.exit(1)

BQ_DATASET = os.getenv("BQ_DATASET", "silver")
BQ_TABLE = os.getenv("TABLE", "fluview_test")
BQ_LOCATION = os.getenv("BQ_LOCATION", "europe-north2")

# Fetch ~1 year by default (52 epiweeks). You can change with envs.
WEEKS_BACK = int(os.getenv("DELPHI_WEEKS_BACK", "52"))

# Delphi (EpiData) FluView API (v1 style)
DELPHI_BASE = "https://api.delphi.cmu.edu/epidata/fluview/"

def epiweek_from_date(d: dt.date) -> int:
    """
    Simple epiweek approximation: CDC epiweeks are Mon-Sun with special year boundaries.
    For operational ingestion we can use the delphi helper endpoints, but here we’ll
    compute a rough ISO-week based fallback that matches most weeks.
    """
    # Delphi encodes epiweek as YYYYWW (integer). We’ll use ISO week close enough.
    iso_year, iso_week, _ = d.isocalendar()
    return iso_year * 100 + iso_week


def fetch_fluview_recent(weeks_back: int = 52) -> pd.DataFrame:
    today = dt.date.today()
    end_ew = epiweek_from_date(today)
    start_ew = epiweek_from_date(today - dt.timedelta(weeks=weeks_back))

    params = {
        "regions": "us",                  # national level (use 'state:ca' etc. for state)
        "epiweeks": f"{start_ew}-{end_ew}"
    }
    r = requests.get(DELPHI_BASE, params=params, timeout=60)
    r.raise_for_status()
    payload = r.json()

    if payload.get("result") != 1:
        msg = payload.get("message", "Unknown error")
        raise RuntimeError(f"Delphi fluview error: {msg}")

    records = payload.get("epidata", [])
    if not records:
        return pd.DataFrame(columns=[
            "epiweek", "region", "wili", "ili", "num_ili", "num_patients", "release_date"
        ])

    df = pd.DataFrame.from_records(records)
    # Keep just the interesting bits; convert types
    keep = ["epiweek", "region", "wili", "ili", "num_ili", "num_patients", "release_date"]
    for k in keep:
        if k not in df.columns:
            df[k] = None
    df = df[keep].copy()
    df["release_date"] = pd.to_datetime(df["release_date"], errors="coerce", utc=True)
    df["epiweek"] = pd.to_numeric(df["epiweek"], errors="coerce").astype("Int64")

    # key: (region, epiweek)
    df = df.dropna(subset=["region", "epiweek"])
    return df


def ensure_table(client: bigquery.Client, full_table_id: str) -> None:
    try:
        client.get_table(full_table_id)
        return
    except Exception:
        pass

    schema = [
        bigquery.SchemaField("region", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("epiweek", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("wili", "FLOAT64"),
        bigquery.SchemaField("ili", "FLOAT64"),
        bigquery.SchemaField("num_ili", "INT64"),
        bigquery.SchemaField("num_patients", "INT64"),
        bigquery.SchemaField("release_date", "TIMESTAMP"),
        bigquery.SchemaField("inserted_at", "TIMESTAMP", default_value_expression="CURRENT_TIMESTAMP()"),
    ]
    table = bigquery.Table(full_table_id, schema=schema)
    client.create_table(table)
    print(f"Created table {full_table_id}")


def merge_upsert(client: bigquery.Client, staging_id: str, target_id: str) -> None:
    sql = f"""
    MERGE `{target_id}` T
    USING `{staging_id}` S
    ON  T.region = S.region AND T.epiweek = S.epiweek
    WHEN MATCHED THEN UPDATE SET
        wili = S.wili,
        ili = S.ili,
        num_ili = S.num_ili,
        num_patients = S.num_patients,
        release_date = S.release_date,
        inserted_at = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN INSERT (
        region, epiweek, wili, ili, num_ili, num_patients, release_date, inserted_at
    ) VALUES (
        S.region, S.epiweek, S.wili, S.ili, S.num_ili, S.num_patients, S.release_date, CURRENT_TIMESTAMP()
    );
    """
    job = client.query(sql, location=BQ_LOCATION)
    job.result()


def main():
    client = bigquery.Client(project=PROJECT, location=BQ_LOCATION)
    full_table_id = f"{PROJECT}.{BQ_DATASET}.{BQ_TABLE}"
    ensure_table(client, full_table_id)

    df = fetch_fluview_recent(WEEKS_BACK)
    if df.empty:
        print("No FluView rows fetched. Exiting gracefully.")
        return

    print(f"FluView rows prepared: {len(df)}")

    staging_id = f"{PROJECT}.{BQ_DATASET}._fluview_stage_{int(time.time())}"
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
    load_job = client.load_table_from_dataframe(df, staging_id, job_config=job_config, location=BQ_LOCATION)
    load_job.result()

    merge_upsert(client, staging_id, full_table_id)
    client.delete_table(staging_id, not_found_ok=True)

    print(f"Upserted {len(df)} rows into {full_table_id}")


if __name__ == "__main__":
    main()
