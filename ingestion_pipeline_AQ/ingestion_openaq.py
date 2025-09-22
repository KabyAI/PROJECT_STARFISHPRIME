# ingestion_pipeline_AQ/ingestion_openaq.py
# Pull OpenAQ PM2.5 daily aggregates for a known sensor (demo)
# and upsert into BigQuery (idempotent MERGE).

import os, time, random
from datetime import datetime, timedelta, timezone
import requests
from google.cloud import bigquery

PROJECT = os.environ["GOOGLE_CLOUD_PROJECT"]
DATASET = os.environ.get("BQ_DATASET", "silver")
TABLE   = f"{PROJECT}.{DATASET}.openaq_pm25_test"

API_KEY = os.environ["OPENAQ_API_KEY"]  # set this in your shell
BASE    = "https://api.openaq.org/v3"
HEADERS = {"accept": "application/json", "X-API-Key": API_KEY}

def polite_get(url, params, tries=0):
    r = requests.get(url, headers=HEADERS, params=params, timeout=60)
    if r.status_code in (429, 500, 502, 503, 504) and tries < 6:
        ra = r.headers.get("Retry-After")
        sleep_s = int(ra) if ra and ra.isdigit() else min(2 ** (tries + 1), 30) + random.random()
        time.sleep(sleep_s)
        return polite_get(url, params, tries + 1)
    r.raise_for_status()
    return r.json()

def main():
    # Known-good sensor for a quick smoke test; change later if you want.
    sensor_id = 957

    # Pull a tiny window (yesterday only)
    today = datetime.now(timezone.utc).date()
    date_from = (today - timedelta(days=2)).isoformat()
    date_to   = (today - timedelta(days=1)).isoformat()

    data = polite_get(f"{BASE}/sensors/{sensor_id}/days",
                      {"date_from": date_from, "date_to": date_to, "limit": 100})

    now = datetime.utcnow().isoformat()
    rows_out = []
    for r in data.get("results", []) or []:
        value = (r.get("summary") or {}).get("avg", r.get("value"))
        unit  = (r.get("parameter") or {}).get("units") or r.get("unit")
        start = ((r.get("period") or {}).get("datetimeFrom") or {}).get("utc")
        if value is not None and start:
            rows_out.append({
                "sensor_id": int(sensor_id),
                "period_start_utc": start,
                "value": float(value),
                "unit": unit,
                "inserted_at": now
            })

    print(f"OpenAQ rows prepared: {len(rows_out)}")
    if not rows_out:
        return

    bq = bigquery.Client(project=PROJECT)

    # 1) Load to a short-lived staging table
    stg = f"{PROJECT}.{DATASET}._stg_openaq_{int(time.time())}"
    bq.load_table_from_json(rows_out, stg).result()

    # 2) MERGE into target by (sensor_id, period_start_utc) → idempotent
    merge_sql = f"""
    MERGE `{TABLE}` T
    USING `{stg}` S
    ON T.sensor_id = S.sensor_id AND T.period_start_utc = S.period_start_utc
    WHEN MATCHED THEN UPDATE SET
      value = S.value,
      unit = COALESCE(S.unit, T.unit),
      inserted_at = S.inserted_at
    WHEN NOT MATCHED THEN
      INSERT (sensor_id, period_start_utc, value, unit, inserted_at)
      VALUES (S.sensor_id, S.period_start_utc, S.value, S.unit, S.inserted_at)
    """
    bq.query(merge_sql).result()
    bq.delete_table(stg, not_found_ok=True)
    print(f"Upserted {len(rows_out)} rows into {TABLE}")

if __name__ == "__main__":
    main()
