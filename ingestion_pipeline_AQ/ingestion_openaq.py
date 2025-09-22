# Pull daily PM2.5 from OpenAQ for a single sensor and upsert into BigQuery.
# Table schema: sensor_id INT64, period_start_utc TIMESTAMP, value FLOAT64, unit STRING, inserted_at TIMESTAMP

import os
import sys
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any

import requests
import pandas as pd
from google.cloud import bigquery


# ---- Config from env (Cloud Run Jobs will pass these) ----
PROJECT         = os.environ["GOOGLE_CLOUD_PROJECT"]                # set by your job update
DATASET         = os.environ.get("BQ_DATASET", "silver")
TABLE_NAME      = os.environ.get("TABLE", "openaq_pm25_test")
BQ_LOCATION     = os.environ.get("BQ_LOCATION", "EU")               # 'EU' (dataset) or a regional location
OPENAQ_API_KEY  = os.environ.get("OPENAQ_API_KEY")                  # from Secret Manager; may be None (public rate limits)
SENSOR_ID       = int(os.environ.get("SENSOR_ID", "957"))           # default to 957 as you used
# Backfill window (inclusive): default = yesterday only
START_DAYS_AGO  = int(os.environ.get("START_DAYS_AGO", "1"))
END_DAYS_AGO    = int(os.environ.get("END_DAYS_AGO", "1"))

TABLE_FQN       = f"{PROJECT}.{DATASET}.{TABLE_NAME}"


def _headers() -> Dict[str, str]:
    h = {"Accept": "application/json"}
    if OPENAQ_API_KEY:
        # OpenAQ v3 supports key via header 'X-API-Key'
        h["X-API-Key"] = OPENAQ_API_KEY
    return h


def _date_range_utc(start_days_ago: int, end_days_ago: int):
    """Return (date_from_iso, date_to_iso) where to = end-of-day (23:59:59) UTC."""
    if end_days_ago < start_days_ago:
        # swap for safety
        start_days_ago, end_days_ago = end_days_ago, start_days_ago

    today = datetime.now(timezone.utc).date()
    d_from = today - timedelta(days=start_days_ago)
    d_to   = today - timedelta(days=end_days_ago)

    start_dt = datetime(d_from.year, d_from.month, d_from.day, 0, 0, 0, tzinfo=timezone.utc)
    end_dt   = datetime(d_to.year,   d_to.month,   d_to.day,   23, 59, 59, tzinfo=timezone.utc)
    return start_dt.isoformat().replace("+00:00", "Z"), end_dt.isoformat().replace("+00:00", "Z")


def _fetch_openaq(sensor_id: int, date_from_iso: str, date_to_iso: str) -> List[Dict[str, Any]]:
    """
    Query OpenAQ for measurements for the given sensor over [date_from, date_to].

    We try the v3 Sensor Measurements endpoint first, falling back to a generic
    measurements endpoint if needed. We normalize output to a list of dicts:
    { 'timestamp': <iso-utc>, 'value': float, 'unit': 'µg/m³' }
    """
    # Try v3: /v3/sensors/{id}/measurements
    url_candidates = [
        f"https://api.openaq.org/v3/sensors/{sensor_id}/measurements",
        # Fallbacks (keep same params)
        "https://api.openaq.org/v3/measurements",
        "https://api.openaq.org/v2/measurements",
    ]

    params_base = {
        "parameter": "pm25",
        "date_from": date_from_iso,
        "date_to":   date_to_iso,
        "limit":     1000,   # small window, should be plenty
        "page":      1,
        "order_by":  "datetime",
        "sort":      "asc",
        # Some deployments accept 'temporal=day' but many do raw samples; we’ll aggregate ourselves.
    }

    results: List[Dict[str, Any]] = []

    for url in url_candidates:
        params = dict(params_base)
        # If using generic endpoint, we must filter by sensor/location
        if "{sensor_id}" not in url and "sensors/" not in url:
            # Some APIs use location_id instead of sensor_id; try both
            params["sensor_id"] = sensor_id
            params.setdefault("location_id", sensor_id)

        try:
            r = requests.get(url, params=params, headers=_headers(), timeout=60)
            r.raise_for_status()
            data = r.json()

            # Normalize various shapes:
            items = []
            if isinstance(data, dict):
                if "results" in data and isinstance(data["results"], list):
                    items = data["results"]
                elif "data" in data and isinstance(data["data"], list):
                    items = data["data"]
                elif "results" in data and isinstance(data["results"], dict) and "measurements" in data["results"]:
                    items = data["results"]["measurements"]

            if not items:
                continue

            normalized = []
            for it in items:
                # Common shapes we've seen:
                # v3 example fields: datetime / date.utc, value, unit
                ts = (
                    it.get("datetime")
                    or (it.get("date") or {}).get("utc")
                    or it.get("timestamp")
                )
                val = it.get("value")
                unit = it.get("unit") or "µg/m³"
                if ts is None or val is None:
                    continue
                normalized.append({"timestamp": ts, "value": float(val), "unit": unit})

            if normalized:
                results.extend(normalized)
                break  # success with this URL; stop trying fallbacks

        except Exception as e:
            # Move to next candidate
            continue

    return results


def _aggregate_daily(rows: List[Dict[str, Any]]) -> pd.DataFrame:
    """
    Convert raw measurements to daily averages.
    Output columns: sensor_id, period_start_utc, value, unit, inserted_at
    """
    if not rows:
        return pd.DataFrame(columns=["sensor_id", "period_start_utc", "value", "unit", "inserted_at"])

    df = pd.DataFrame(rows)
    # Parse timestamps -> UTC datetime
    df["ts"] = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")
    df = df.dropna(subset=["ts"])

    # Daily bucket
    df["day"] = df["ts"].dt.floor("D")
    agg = df.groupby("day")["value"].mean().reset_index()
    agg["unit"] = df["unit"].dropna().iloc[0] if not df["unit"].dropna().empty else "µg/m³"

    agg = agg.rename(columns={"day": "period_start_utc"})
    agg["sensor_id"] = SENSOR_ID
    agg["inserted_at"] = datetime.now(timezone.utc)

    return agg[["sensor_id", "period_start_utc", "value", "unit", "inserted_at"]]


def main():
    date_from_iso, date_to_iso = _date_range_utc(START_DAYS_AGO, END_DAYS_AGO)
    print(f"Fetching OpenAQ measurements for sensor={SENSOR_ID} from {date_from_iso} to {date_to_iso} (UTC)")

    rows = _fetch_openaq(SENSOR_ID, date_from_iso, date_to_iso)
    if not rows:
        print("OpenAQ returned no rows for the requested window.")
        return

    df_daily = _aggregate_daily(rows)
    print(f"OpenAQ rows prepared (daily): {len(df_daily)}")

    bq = bigquery.Client(project=PROJECT, location=BQ_LOCATION)

    # Create a small staging table, load dataframe, MERGE into target
    stg_table_id = f"{PROJECT}.{DATASET}._stg_openaq_pm25"
    target_table = TABLE_FQN

    # Ensure staging table exists with desired schema
    schema = [
        bigquery.SchemaField("sensor_id", "INT64"),
        bigquery.SchemaField("period_start_utc", "TIMESTAMP"),
        bigquery.SchemaField("value", "FLOAT64"),
        bigquery.SchemaField("unit", "STRING"),
        bigquery.SchemaField("inserted_at", "TIMESTAMP"),
    ]

    # Create (or replace) the staging table
    stg_table = bigquery.Table(stg_table_id, schema=schema)
    stg_table = bq.create_table(stg_table, exists_ok=True)

    # Truncate staging (replace with empty then load)
    bq.delete_table(stg_table, not_found_ok=True)
    stg_table = bq.create_table(bigquery.Table(stg_table_id, schema=schema))

    job = bq.load_table_from_dataframe(df_daily, stg_table_id)
    job.result()

    # MERGE into target (idempotent on sensor_id + period_start_utc)
    # Create target if not exists
    try:
        bq.get_table(target_table)
    except Exception:
        tgt = bigquery.Table(target_table, schema=schema)
        bq.create_table(tgt)

    merge_sql = f"""
    MERGE `{target_table}` T
    USING `{stg_table_id}` S
    ON T.sensor_id = S.sensor_id
       AND T.period_start_utc = S.period_start_utc
    WHEN MATCHED THEN UPDATE SET
      value = S.value,
      unit = S.unit,
      inserted_at = S.inserted_at
    WHEN NOT MATCHED THEN
      INSERT (sensor_id, period_start_utc, value, unit, inserted_at)
      VALUES (S.sensor_id, S.period_start_utc, S.value, S.unit, S.inserted_at)
    """
    bq.query(merge_sql).result()

    # Clean up staging
    bq.delete_table(stg_table, not_found_ok=True)

    print(f"Upserted {len(df_daily)} rows into {target_table}")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        # Force non-zero exit so Cloud Run marks as failed and shows the exception in logs
        print(f"FATAL: {e}", file=sys.stderr)
        raise
