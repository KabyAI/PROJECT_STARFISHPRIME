# ingestion_pipeline_AQ/ingestion_openaq.py
# Fetch daily PM2.5 for a specific OpenAQ location and upsert into BigQuery (idempotent).

import os
from datetime import datetime, timedelta, timezone
import requests
import pandas as pd
from google.cloud import bigquery

# --------- Config from environment ----------
PROJECT      = os.environ["GOOGLE_CLOUD_PROJECT"]
DATASET      = os.environ.get("BQ_DATASET", "silver")
TABLE_NAME   = os.environ.get("TABLE", "openaq_pm25_test")  # final table name inside DATASET
BQ_LOCATION  = os.environ.get("BQ_LOCATION")                # e.g. "europe-north2" (optional)
OPENAQ_KEY   = os.environ.get("OPENAQ_API_KEY")             # provided via Secret Manager mapping

# OpenAQ specifics (edit if you want)
LOCATION_ID  = int(os.environ.get("OPENAQ_LOCATION_ID", "957"))  # OpenAQ location_id to pull
PARAMETER    = os.environ.get("OPENAQ_PARAMETER", "pm25")        # e.g. pm25

# Date window: default to "yesterday UTC" (full day)
# You can override via: OPENAQ_DAYS_AGO_START / OPENAQ_DAYS_AGO_END (integers)
_days_ago_start = int(os.environ.get("OPENAQ_DAYS_AGO_START", "1"))  # start days ago (inclusive)
_days_ago_end   = int(os.environ.get("OPENAQ_DAYS_AGO_END", "1"))    # end days ago (inclusive)
# -----------------------------------------------------------

FINAL_TABLE = f"{PROJECT}.{DATASET}.{TABLE_NAME}"

def _yesterday_utc_bounds():
    # full UTC day: 00:00:00 to 23:59:59 of "days_ago_start"
    # if start==end, it's just one day.
    today = datetime.now(timezone.utc).date()
    start_day = today - timedelta(days=_days_ago_start)
    end_day   = today - timedelta(days=_days_ago_end)
    if end_day < start_day:
        # swap if user set reversed values
        start_day, end_day = end_day, start_day
    start_dt = datetime(start_day.year, start_day.month, start_day.day, 0, 0, 0, tzinfo=timezone.utc)
    end_dt   = datetime(end_day.year,   end_day.month,   end_day.day,   23, 59, 59, tzinfo=timezone.utc)
    return start_dt, end_dt

def fetch_openaq(location_id: int, parameter: str, date_from: datetime, date_to: datetime) -> pd.DataFrame:
    """
    Pulls OpenAQ measurements over the window and returns a DataFrame aggregated to daily average.
    Columns returned: date (DATE), location_id (INT64), parameter (STRING), value (FLOAT64), inserted_at (TIMESTAMP)
    """
    headers = {}
    if OPENAQ_KEY:
        headers["X-API-Key"] = OPENAQ_KEY

    base = "https://api.openaq.org/v3/measurements"
    # Pull in pages (OpenAQ caps page size; a single day usually small, but paginate just in case)
    page = 1
    per_page = 1000
    rows = []
    while True:
        params = {
            "location_id": location_id,
            "parameter": parameter,
            "date_from": date_from.isoformat().replace("+00:00", "Z"),
            "date_to": date_to.isoformat().replace("+00:00", "Z"),
            "limit": per_page,
            "page": page,
        }
        r = requests.get(base, headers=headers, params=params, timeout=60)
        r.raise_for_status()
        payload = r.json()
        results = payload.get("results", [])
        for item in results:
            # item["date"]["utc"] like "2025-09-21T12:00:00+00:00"
            ts_utc = item["date"]["utc"]
            # Normalize to date
            day = datetime.fromisoformat(ts_utc.replace("Z", "+00:00")).date()
            rows.append({
                "date": day.isoformat(),
                "location_id": item.get("location", location_id),
                "parameter": item.get("parameter", parameter),
                "value": float(item.get("value")) if item.get("value") is not None else None,
            })
        meta = payload.get("meta", {})
        found = meta.get("found", 0)
        page_count = (found + per_page - 1) // per_page if found else (1 if results else 0)
        if page >= page_count or not results:
            break
        page += 1

    if not rows:
        return pd.DataFrame(columns=["date","location_id","parameter","value","inserted_at"])

    df = pd.DataFrame(rows)
    # daily average per day
    grp = (
        df.groupby(["date", "location_id", "parameter"], as_index=False)["value"]
          .mean()
          .rename(columns={"value": "pm25_avg"})
    )
    grp["inserted_at"] = datetime.now(timezone.utc).isoformat()
    return grp[["date", "location_id", "parameter", "pm25_avg", "inserted_at"]]

def upsert_to_bq(df: pd.DataFrame):
    bq = bigquery.Client(project=PROJECT)
    stg_table_id = f"{PROJECT}.{DATASET}._openaq_pm25_stg"

    # Create staging table & load
    job = bq.load_table_from_dataframe(df, stg_table_id)
    job.result()  # wait

    merge_sql = f"""
    MERGE `{FINAL_TABLE}` T
    USING `{stg_table_id}` S
    ON  T.date = S.date
    AND T.location_id = S.location_id
    AND T.parameter = S.parameter
    WHEN MATCHED THEN UPDATE SET
      pm25_avg    = S.pm25_avg,
      inserted_at = S.inserted_at
    WHEN NOT MATCHED THEN INSERT (date, location_id, parameter, pm25_avg, inserted_at)
      VALUES (S.date, S.location_id, S.parameter, S.pm25_avg, S.inserted_at)
    """
    bq.query(merge_sql, location=BQ_LOCATION).result()
    bq.delete_table(stg_table_id, not_found_ok=True)

def main():
    date_from, date_to = _yesterday_utc_bounds()
    print(
        f"[OpenAQ] project={PROJECT} dataset={DATASET} table={FINAL_TABLE} "
        f"window={date_from.isoformat()} → {date_to.isoformat()} (UTC), "
        f"location_id={LOCATION_ID}, param={PARAMETER}, bq_loc={BQ_LOCATION or 'dataset default'}"
    )

    df = fetch_openaq(LOCATION_ID, PARAMETER, date_from, date_to)
    print(f"[OpenAQ] rows prepared: {len(df)}")

    if not df.empty:
        upsert_to_bq(df)
        print(f"[OpenAQ] upserted {len(df)} rows into {FINAL_TABLE}")
    else:
        print("[OpenAQ] nothing to upsert (no rows returned).")

if __name__ == "__main__":
    main()
