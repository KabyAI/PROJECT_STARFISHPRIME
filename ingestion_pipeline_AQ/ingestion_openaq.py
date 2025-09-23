import os
import sys
import math
import time
import datetime as dt
from typing import List, Dict

import pandas as pd
import requests
from google.cloud import bigquery

# -----------------------------
# Configuration (env-driven)
# -----------------------------
PROJECT = os.getenv("GOOGLE_CLOUD_PROJECT") or os.getenv("PROJECT_ID")
if not PROJECT:
    print("FATAL: GOOGLE_CLOUD_PROJECT (or PROJECT_ID) is not set.", file=sys.stderr)
    sys.exit(1)

BQ_DATASET  = os.getenv("BQ_DATASET", "silver")
BQ_TABLE    = os.getenv("TABLE", "openaq_pm25")
BQ_LOCATION = os.getenv("BQ_LOCATION", "europe-north2")

# OpenAQ v3 settings
OPENAQ_API_KEY   = os.getenv("OPENAQ_API_KEY")   # required in production
OPENAQ_BASE      = "https://api.openaq.org/v3"
OPENAQ_LOCATION_ID = int(os.getenv("OPENAQ_LOCATION_ID", "957"))  # sample location
OPENAQ_PARAMETER   = os.getenv("OPENAQ_PARAMETER", "pm25")

# Day window (UTC): inclusive day range (start..end) in "days ago"
START_DAYS_AGO = int(os.getenv("OPENAQ_DAYS_AGO_START", "1"))
END_DAYS_AGO   = int(os.getenv("OPENAQ_DAYS_AGO_END", "1"))
if START_DAYS_AGO < END_DAYS_AGO:
    START_DAYS_AGO, END_DAYS_AGO = END_DAYS_AGO, START_DAYS_AGO

# -----------------------------
# Helpers
# -----------------------------
def day_bounds_utc(days_ago: int) -> tuple[dt.datetime, dt.datetime]:
    """Return 00:00:00Z and 23:59:59Z for the UTC day that is `days_ago` before today."""
    today_utc = dt.datetime.now(dt.UTC).date()
    day = today_utc - dt.timedelta(days=days_ago)
    start = dt.datetime(day.year, day.month, day.day, 0, 0, 0, tzinfo=dt.UTC)
    end   = dt.datetime(day.year, day.month, day.day, 23, 59, 59, tzinfo=dt.UTC)
    return start, end

def _pick_dt_utc(item: Dict) -> str | None:
    """
    Prefer OpenAQ v3 'date.utc'; fall back to 'period.datetimeTo.utc' (seen in your smoketest),
    then a few common aliases.
    Returns an ISO8601 UTC string or None.
    """
    # 1) canonical
    v = (item.get("date") or {}).get("utc")
    if isinstance(v, str) and v:
        return v

    # 2) period window
    period = item.get("period") or {}
    dt_to = (period.get("datetimeTo") or {}).get("utc")
    if isinstance(dt_to, str) and dt_to:
        return dt_to
    dt_from = (period.get("datetimeFrom") or {}).get("utc")
    if isinstance(dt_from, str) and dt_from:
        return dt_from

    # 3) other aliases occasionally present
    for k in ("datetime_utc", "date_utc", "timestamp", "datetime"):
        v = item.get(k)
        if isinstance(v, str) and v:
            return v

    return None

def fetch_openaq_day(location_id: int, parameter: str,
                     date_from: dt.datetime, date_to: dt.datetime) -> pd.DataFrame:
    """
    OpenAQ v3 /measurements for one UTC-day window with pagination.
    """
    headers = {}
    if OPENAQ_API_KEY:
        headers["X-API-Key"] = OPENAQ_API_KEY

    limit = 1000
    page  = 1
    rows: List[Dict] = []

    while True:
        params = {
            "location_id": location_id,
            "parameter": parameter,
            "date_from": date_from.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "date_to":   date_to.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "limit": limit,
            "page":  page,
        }
        url = f"{OPENAQ_BASE}/measurements"
        r = requests.get(url, headers=headers, params=params, timeout=60)

        # If a day has no data, v3 often returns 404; treat as empty page.
        if r.status_code == 404:
            break
        r.raise_for_status()
        payload = r.json()

        data = payload.get("results", [])
        if not data:
            break

        for item in data:
            when = _pick_dt_utc(item)
            rows.append({
                "location_id":   (item.get("location") or {}).get("id") or location_id,
                "location_name": (item.get("location") or {}).get("name"),
                "parameter":     item.get("parameter"),
                "unit":          item.get("unit"),
                "value":         item.get("value"),
                "datetime_utc":  when,
                "coordinates_lat": (item.get("coordinates") or {}).get("latitude"),
                "coordinates_lon": (item.get("coordinates") or {}).get("longitude"),
                "source_name":   item.get("sourceName"),
            })

        meta  = payload.get("meta", {})
        found = meta.get("found")
        if found is None:
            if len(data) < limit:
                break
        else:
            pages = math.ceil(int(found) / limit)
            if page >= pages:
                break
        page += 1

    if not rows:
        return pd.DataFrame(columns=[
            "location_id","location_name","parameter","unit","value",
            "datetime_utc","coordinates_lat","coordinates_lon","source_name"
        ])

    df = pd.DataFrame(rows)

    # Cast / clean
    df["datetime_utc"] = pd.to_datetime(df["datetime_utc"], utc=True, errors="coerce")
    df["location_id"]  = pd.to_numeric(df["location_id"], errors="coerce").astype("Int64")

    # Key must exist
    df = df.dropna(subset=["location_id", "parameter", "datetime_utc"])

    # (Optional) Helpful fields for later joins
    df["date"]    = df["datetime_utc"].dt.date
    df["epiweek"] = df["datetime_utc"].dt.isocalendar().week.astype("Int64") + \
                    (df["datetime_utc"].dt.isocalendar().year.astype("Int64") * 100)
    return df

def ensure_table(client: bigquery.Client, full_table_id: str) -> None:
    """Create the table if it does not exist (idempotent)."""
    try:
        client.get_table(full_table_id)
        return
    except Exception:
        pass

    schema = [
        bigquery.SchemaField("location_id",    "INT64",    mode="REQUIRED"),
        bigquery.SchemaField("location_name",  "STRING"),
        bigquery.SchemaField("parameter",      "STRING",   mode="REQUIRED"),
        bigquery.SchemaField("unit",           "STRING"),
        bigquery.SchemaField("value",          "FLOAT64"),
        bigquery.SchemaField("datetime_utc",   "TIMESTAMP",mode="REQUIRED"),
        bigquery.SchemaField("coordinates_lat","FLOAT64"),
        bigquery.SchemaField("coordinates_lon","FLOAT64"),
        bigquery.SchemaField("source_name",    "STRING"),
        # convenience fields to ease downstream joins/aggregations
        bigquery.SchemaField("date",           "DATE"),
        bigquery.SchemaField("epiweek",        "INT64"),
        bigquery.SchemaField("inserted_at",    "TIMESTAMP",
                             default_value_expression="CURRENT_TIMESTAMP()"),
    ]
    table = bigquery.Table(full_table_id, schema=schema)
    client.create_table(table)
    print(f"Created table {full_table_id}")

def merge_upsert(client: bigquery.Client, staging_id: str, target_id: str) -> None:
    """
    Upsert using (location_id, parameter, datetime_utc) as the key.
    """
    sql = f"""
    MERGE `{target_id}` T
    USING `{staging_id}` S
      ON  T.location_id  = S.location_id
      AND T.parameter    = S.parameter
      AND T.datetime_utc = S.datetime_utc
    WHEN MATCHED THEN UPDATE SET
      location_name  = S.location_name,
      unit           = S.unit,
      value          = S.value,
      coordinates_lat= S.coordinates_lat,
      coordinates_lon= S.coordinates_lon,
      source_name    = S.source_name,
      date           = S.date,
      epiweek        = S.epiweek,
      inserted_at    = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN
      INSERT (location_id, location_name, parameter, unit, value,
              datetime_utc, coordinates_lat, coordinates_lon, source_name,
              date, epiweek, inserted_at)
      VALUES (S.location_id, S.location_name, S.parameter, S.unit, S.value,
              S.datetime_utc, S.coordinates_lat, S.coordinates_lon, S.source_name,
              S.date, S.epiweek, CURRENT_TIMESTAMP());
    """
    client.query(sql, location=BQ_LOCATION).result()

def main():
    client = bigquery.Client(project=PROJECT, location=BQ_LOCATION)
    full_table_id = f"{PROJECT}.{BQ_DATASET}.{BQ_TABLE}"
    ensure_table(client, full_table_id)

    all_days: List[pd.DataFrame] = []
    for d in range(START_DAYS_AGO, END_DAYS_AGO - 1, -1):
        start_dt, end_dt = day_bounds_utc(d)
        print(f"[OpenAQ] {start_dt.isoformat()} → {end_dt.isoformat()} UTC "
              f"(loc_id={OPENAQ_LOCATION_ID}, param={OPENAQ_PARAMETER})")
        df = fetch_openaq_day(OPENAQ_LOCATION_ID, OPENAQ_PARAMETER, start_dt, end_dt)
        if not df.empty:
            all_days.append(df)

    if not all_days:
        print("No OpenAQ rows fetched for the requested window. Exiting gracefully.")
        return

    df_all = pd.concat(all_days, ignore_index=True).drop_duplicates(
        subset=["location_id","parameter","datetime_utc"]
    )
    print(f"OpenAQ rows prepared: {len(df_all)}")

    staging_id = f"{PROJECT}.{BQ_DATASET}._openaq_stage_{int(time.time())}"
    job_cfg = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
    client.load_table_from_dataframe(df_all, staging_id, job_config=job_cfg,
                                     location=BQ_LOCATION).result()

    merge_upsert(client, staging_id, full_table_id)
    client.delete_table(staging_id, not_found_ok=True)
    print(f"Upserted {len(df_all)} rows into {full_table_id}")

if __name__ == "__main__":
    main()
