# ingestion_pipeline_delphi/ingestion_delphi.py
# Pull FluView (ILI) for California and upsert into BigQuery (idempotent).

import os, time
from datetime import datetime, timezone
import requests
from google.cloud import bigquery

PROJECT = os.environ["GOOGLE_CLOUD_PROJECT"]
DATASET = os.environ.get("BQ_DATASET", "silver")
TABLE   = f"{PROJECT}.{DATASET}.fluview_test"

def main():
    
    # Small recent range; adjust as you like
    params = {"source": "fluview", "regions": "ca", "epiweeks": "202401-202452"}
    r = requests.get("https://api.delphi.cmu.edu/epidata/api.php", params=params, timeout=60)
    r.raise_for_status()
    js = r.json()
    if js.get("result") != 1:
        raise SystemExit(f"FluView API error: {js.get('message')}")

    now = datetime.now(timezone.utc).isoformat()
    rows_out = []
    for d in js.get("epidata", []) or []:
        try:
            rows_out.append({
                "region": d["region"],
                "epiweek": int(d["epiweek"]),
                "wili": float(d["wili"]),
                "inserted_at": now
            })
        except Exception:
            continue

    print(f"FluView rows prepared: {len(rows_out)}")
    if not rows_out:
        return

    bq = bigquery.Client(project=PROJECT)
    stg = f"{PROJECT}.{DATASET}._stg_flu_{int(time.time())}"
    bq.load_table_from_json(rows_out, stg).result()

    merge_sql = f"""
    MERGE `{TABLE}` T
    USING `{stg}` S
    ON T.region = S.region AND T.epiweek = S.epiweek
    WHEN MATCHED THEN UPDATE SET
      wili = S.wili,
      inserted_at = S.inserted_at
    WHEN NOT MATCHED THEN
      INSERT (region, epiweek, wili, inserted_at)
      VALUES (S.region, S.epiweek, S.wili, S.inserted_at)
    """
    bq.query(merge_sql).result()
    bq.delete_table(stg, not_found_ok=True)
    print(f"Upserted {len(rows_out)} rows into {TABLE}")

if __name__ == "__main__":
    main()
