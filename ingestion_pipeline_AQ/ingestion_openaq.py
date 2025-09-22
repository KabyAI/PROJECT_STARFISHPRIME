# ingestion_pipeline_AQ/ingestion_openaq.py
# Fetch daily PM2.5 for an OpenAQ location and upsert into BigQuery (idempotent).

import os
from datetime import datetime, timedelta, timezone
import requests
import pandas as pd
from google.cloud import bigquery


# ============== Config (env) ==============
PROJECT      = os.environ["GOOGLE_CLOUD_PROJECT"]
DATASET      = os.environ.get("BQ_DATASET", "silver")
TABLE_NAME   = os.environ.get("TABLE", "openaq_pm25_test")
FINAL_TABLE  = f"{PROJECT}.{DATASET}.{TABLE_NAME}"

# BQ job location (optional). If unset, BQ uses the dataset’s location.
BQ_LOCATION  = os.environ.get("BQ_LOCATION")

# OpenAQ auth (Secret Manager mapping on the job)
OPENAQ_KEY   = os.environ.get("OPENAQ_API_KEY")

# OpenAQ query settings
LOCATION_ID  = int(os.environ.get("OPENAQ_LOCATION_ID", "957"))
PARAMETER    = os.environ.get("OPENAQ_PARAMETER", "pm25")

# Date window (UTC). Defaults to *yesterday only*.
# Override with OPENAQ_DAYS_AGO_START / OPENAQ_DAYS_AGO_END if desired.
DAYS_AGO_START = int(os.environ.get("OPENAQ_DAYS_AGO_START", "1"))
DAYS_AGO_END   = int(os.environ.get("OPENAQ_DAYS_AGO_END", "1"))
# =========================================


def compute_window_utc() -> tuple[datetime, datetime]:
    """Return [inclusive day start, inclusive day end] in UTC."""
    today = datetime.now(timezone.utc).date()
    start_day = today - timedelta(days=DAYS_AGO_START)
    end_day   = today - timedelta(days=DAYS_AGO_END)
    if end_day < start_day:
        start_day, end_day = end_day, start_day
    start_dt = datetime(start_day.year, start_day.month, start_day.day, 0, 0, 0, tzinfo=timezone.utc)
    end_dt   = datetime(end_day.year,   end_day.month,   end_day.day,   23, 59, 59, tzinfo=timezone.utc)
    return start_dt, end_dt


def fetch_openaq(location_id: int, parameter: str, date_from: datetime, date_to: datetime) -> pd.DataFrame:
    """Fetch measurements and aggregate to daily avg."""
    headers = {}
    if OPENAQ_KEY:
        headers["X-API-Key"] = OPENAQ_KEY

    base = "https://api.openaq.org/v3/measurements"
    per_page = 1000
    page = 1
    rows: list[dict] = []

    while True:
        params = {
            "location_id": location_id,
            "parameter": parameter,
            "date_from": date_from.isoformat().replace("+00:00", "Z"),
            "date_to":   date_to.isoformat().replace("+00:00", "Z"),
            "limit": per_page,
            "page": page,
        }
        r = requests.get(base, headers=headers, params=params, timeout=60)
        r.raise_for_status()
        payload = r.json()
        results = payload.get("results", [])

        for it in results:
            ts_utc = it["date"]["utc"]  # e.g. 2025-09-21T12:00:00+00:00
            day = datetime.fromisoformat(ts_utc.replace("Z", "+00:00")).date()
            rows.append({
                "date": day.isoformat(),
                "location_id": it.get("location", location_id),
                "parameter": it.get("parameter", parameter),
                "value": float(it.get("value")) if it.get("value") is not None else None,
            })

        meta = payload.get("meta", {})
        found = meta.get("found", 0)
        page_count = (found + per_page - 1) // per_page if found else (1 if results else 0)
        if page >= page_count or not results:
            break
        page += 1

    if not rows:
        return pd.DataFrame(columns=["date","location_id","parameter","pm25_avg","inserted_at"])

    df = pd.DataFrame(rows)
    out = (
        df.groupby(["date", "location_id", "parameter"], as_index=False)["value"]
          .mean()
          .rename(columns={"value": "pm25_avg"})
    )
    out["inserted_at"] = datetime.now(timezone.utc).isoformat()
    return out[["date","location_id","parameter","pm25_avg","inserted_at"]]


def upsert_to_bq(df: pd.DataFrame):
    bq = bigquery.Client(project=PROJECT)
    staging = f"{PROJECT}.{DATASET}._openaq_pm25_stg"

    load = bq.load_table_from_dataframe(df, staging)
    load.result()

    merge_sql = f"""
    MERGE `{FINAL_TABLE}` T
    USING `{staging}` S
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
    bq.delete_table(staging, not_found_ok=True)


def main():
    # Compute the window **before** any logging to avoid UnboundLocalError.
    date_from, date_to = compute_window_utc()

    print(
        f"[OpenAQ] project={PROJECT} dataset={DATASET} table={FINAL_TABLE} "
        f"window={date_from.isoformat()} → {date_to.isoformat()} (UTC), "
        f"location_id={LOCATION_ID}, param={PARAMETER}, "
        f"bq_loc={BQ_LOCATION or 'dataset default'}"
    )

    df = fetch_openaq(LOCATION_ID, PARAMETER, date_from, date_to)
    print(f"[OpenAQ] rows prepared: {len(df)}")

    if df.empty:
        print("[OpenAQ] nothing to upsert (no rows returned).")
        return

    upsert_to_bq(df)
    print(f"[OpenAQ] upserted {len(df)} rows into {FINAL_TABLE}")


if __name__ == "__main__":
    main()
