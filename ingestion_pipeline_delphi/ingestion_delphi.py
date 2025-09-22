# ingestion_pipeline_delphi/ingestion_delphi.py
# Pull FluView (ILI) for California and upsert into BigQuery (idempotent).

import os
from datetime import datetime, timezone
import requests
import pandas as pd
from google.cloud import bigquery

# --------- Config from environment ----------
PROJECT     = os.environ["GOOGLE_CLOUD_PROJECT"]
DATASET     = os.environ.get("BQ_DATASET", "silver")
TABLE_NAME  = os.environ.get("TABLE", "fluview_test")
BQ_LOCATION = os.environ.get("BQ_LOCATION")  # optional; defaults to dataset location
# -----------------------------------------------------------

FINAL_TABLE = f"{PROJECT}.{DATASET}.{TABLE_NAME}"

def fetch_fluview_df() -> pd.DataFrame:
    # CA region, 2024 epiweeks through 2025 epiweeks; adjust as you like
    params = {"source": "fluview", "regions": "ca", "epiweeks": "202401-202552"}
    r = requests.get("https://api.delphi.cmu.edu/epidata/api.php", params=params, timeout=60)
    r.raise_for_status()
    js = r.json()
    if js.get("result") != 1:
        raise RuntimeError(f"Delphi API error: {js}")

    rows = []
    for rec in js.get("epidata", []):
        rows.append({
            "region": rec.get("region"),
            "epiweek": int(rec.get("epiweek")),
            "wili": float(rec.get("wili")) if rec.get("wili") is not None else None,
        })
    df = pd.DataFrame(rows)
    if df.empty:
        return pd.DataFrame(columns=["region","epiweek","wili","inserted_at"])
    df["inserted_at"] = datetime.now(timezone.utc).isoformat()
    return df[["region","epiweek","wili","inserted_at"]]

def upsert_to_bq(df: pd.DataFrame):
    bq = bigquery.Client(project=PROJECT)
    stg_table_id = f"{PROJECT}.{DATASET}._fluview_stg"

    # Load staging
    job = bq.load_table_from_dataframe(df, stg_table_id)
    job.result()

    merge_sql = f"""
    MERGE `{FINAL_TABLE}` T
    USING `{stg_table_id}` S
    ON  T.region = S.region
    AND T.epiweek = S.epiweek
    WHEN MATCHED THEN UPDATE SET
      wili        = S.wili,
      inserted_at = S.inserted_at
    WHEN NOT MATCHED THEN INSERT (region, epiweek, wili, inserted_at)
      VALUES (S.region, S.epiweek, S.wili, S.inserted_at)
    """
    bq.query(merge_sql, location=BQ_LOCATION).result()
    bq.delete_table(stg_table_id, not_found_ok=True)

def main():
    print(
        f"[Delphi] project={PROJECT} dataset={DATASET} table={FINAL_TABLE} "
        f"bq_loc={BQ_LOCATION or 'dataset default'}"
    )
    df = fetch_fluview_df()
    print(f"[Delphi] rows prepared: {len(df)}")

    if not df.empty:
        upsert_to_bq(df)
        print(f"[Delphi] upserted {len(df)} rows into {FINAL_TABLE}")
    else:
        print("[Delphi] nothing to upsert (no rows returned).")

if __name__ == "__main__":
    main()
