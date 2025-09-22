# Pull FluView (ILI) for California and upsert into BigQuery (idempotent).
# Table schema: region STRING, epiweek INT64, wili FLOAT64, inserted_at TIMESTAMP

import os
import sys
from datetime import datetime, timezone
from typing import List, Dict, Any

import requests
import pandas as pd
from google.cloud import bigquery


# ---- Config from env ----
PROJECT     = os.environ["GOOGLE_CLOUD_PROJECT"]           # set by your job update
DATASET     = os.environ.get("BQ_DATASET", "silver")
TABLE_NAME  = os.environ.get("TABLE", "fluview_test")
BQ_LOCATION = os.environ.get("BQ_LOCATION", "EU")          # 'EU' (dataset) or a regional location
REGION_CODE = os.environ.get("FLUVIEW_REGION", "ca")       # 'ca' = California

# Week range (Delphi epiweeks). Default: 202401-202452 (this year).
EPIWEEK_FROM = os.environ.get("FLUVIEW_FROM", "202401")
EPIWEEK_TO   = os.environ.get("FLUVIEW_TO",   "202452")

TABLE_FQN    = f"{PROJECT}.{DATASET}.{TABLE_NAME}"


def _fetch_delphi(region: str, ew_from: str, ew_to: str) -> List[Dict[str, Any]]:
    params = {
        "source": "fluview",
        "regions": region,
        "epiweeks": f"{ew_from}-{ew_to}",
    }
    r = requests.get("https://api.delphi.cmu.edu/epidata/api.php", params=params, timeout=60)
    r.raise_for_status()
    data = r.json()

    if data.get("result") != 1 or "epidata" not in data:
        # If result is not OK, return empty
        return []

    rows = []
    for it in data["epidata"]:
        # Expected fields: region (str), epiweek (int), wili (float)
        region = it.get("region")
        epiweek = it.get("epiweek")
        wili = it.get("wili")
        if region is None or epiweek is None or wili is None:
            continue
        rows.append({"region": region, "epiweek": int(epiweek), "wili": float(wili)})

    return rows


def main():
    print(f"Fetching FluView for {REGION_CODE} epiweeks {EPIWEEK_FROM}-{EPIWEEK_TO}")

    rows = _fetch_delphi(REGION_CODE, EPIWEEK_FROM, EPIWEEK_TO)
    if not rows:
        print("Delphi returned no rows for the requested window.")
        return

    df = pd.DataFrame(rows)
    df["inserted_at"] = datetime.now(timezone.utc)

    print(f"FluView rows prepared: {len(df)}")

    bq = bigquery.Client(project=PROJECT, location=BQ_LOCATION)

    # Stage then MERGE
    stg_table_id = f"{PROJECT}.{DATASET}._stg_fluview"
    target_table = TABLE_FQN

    schema = [
        bigquery.SchemaField("region", "STRING"),
        bigquery.SchemaField("epiweek", "INT64"),
        bigquery.SchemaField("wili", "FLOAT64"),
        bigquery.SchemaField("inserted_at", "TIMESTAMP"),
    ]

    # Ensure staging exists fresh
    stg_table = bigquery.Table(stg_table_id, schema=schema)
    stg_table = bq.create_table(stg_table, exists_ok=True)
    bq.delete_table(stg_table, not_found_ok=True)
    stg_table = bq.create_table(bigquery.Table(stg_table_id, schema=schema))

    job = bq.load_table_from_dataframe(df, stg_table_id)
    job.result()

    # Ensure target exists
    try:
        bq.get_table(target_table)
    except Exception:
        tgt = bigquery.Table(target_table, schema=schema)
        bq.create_table(tgt)

    merge_sql = f"""
    MERGE `{target_table}` T
    USING `{stg_table_id}` S
    ON T.region = S.region
       AND T.epiweek = S.epiweek
    WHEN MATCHED THEN UPDATE SET
      wili = S.wili,
      inserted_at = S.inserted_at
    WHEN NOT MATCHED THEN
      INSERT (region, epiweek, wili, inserted_at)
      VALUES (S.region, S.epiweek, S.wili, S.inserted_at)
    """
    bq.query(merge_sql).result()

    bq.delete_table(stg_table, not_found_ok=True)
    print(f"Upserted {len(df)} rows into {target_table}")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"FATAL: {e}", file=sys.stderr)
        raise
