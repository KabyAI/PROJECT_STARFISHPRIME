#!/usr/bin/env python3
"""
OpenAQ PM2.5 -> BigQuery (idempotent upsert).

- Uses v3 /measurements with bbox + date window to minimize API calls.
- Retries politely on 429/5xx with exponential backoff.
- Loads to a staging table, then MERGEs into target.
- Tweak behavior via env vars (see ENV section below).
"""

from __future__ import annotations

import os
import math
import time
import json
import datetime as dt
from dataclasses import dataclass
from typing import Iterable, List, Dict, Optional

import pandas as pd
import requests
from requests.adapters import HTTPAdapter, Retry
from google.cloud import bigquery


# -----------------------------
# ENV (with sensible defaults)
# -----------------------------
PROJECT          = os.environ.get("GOOGLE_CLOUD_PROJECT", "")
BQ_DATASET       = os.environ.get("BQ_DATASET", "silver")
BQ_LOCATION      = os.environ.get("BQ_LOCATION", "europe-north2")
BQ_TABLE         = os.environ.get("BQ_TABLE", "openaq_pm25")

# OpenAQ filters
OPENAQ_API_KEY   = os.environ.get("OPENAQ_API_KEY") or os.environ.get("OPENAQ_KEY") or os.environ.get("OPENAQ_TOKEN")
OPENAQ_PARAMETER = os.environ.get("OPENAQ_PARAMETER", "pm25")          # "pm25" or parameter_id=2
OPENAQ_COUNTRY   = os.environ.get("OPENAQ_COUNTRY", "US")
OPENAQ_STATE     = os.environ.get("OPENAQ_STATE", "California")        # optional hint, not required when using bbox
# Window relative to today UTC: [start, end] inclusive (e.g., 2..1 means “yesterday only”)
DAYS_AGO_START   = int(os.environ.get("OPENAQ_DAYS_AGO_START", "1"))
DAYS_AGO_END     = int(os.environ.get("OPENAQ_DAYS_AGO_END", "0"))

# Optional: one or more bounding boxes to constrain the pull (comma-separated bboxes separated by ';')
# Format per OpenAQ: "lon1,lat1,lon2,lat2" (SW lon, SW lat, NE lon, NE lat)
# Defaults target dense-population slices for CA: LA, SF City, San Diego
BBOXES = os.environ.get(
    "OPENAQ_BBOXES",
    ";".join([
        "-118.6681,33.7037,-117.6462,34.3373",  # Los Angeles
        "-122.5136,37.7080,-122.3569,37.8324",  # San Francisco (city slice)
        "-117.282,32.615,-116.908,33.023",      # San Diego
    ])
).split(";")

# Optional: hard-pin parameter id; OpenAQ v3 uses id=2 for PM2.5
PARAMETER_ID = 2 if OPENAQ_PARAMETER.lower() in ("pm25", "pm2.5") else None

# Tuning
OPENAQ_BASE_URL  = os.environ.get("OPENAQ_BASE_URL", "https://api.openaq.org/v3")
PAGE_LIMIT       = int(os.environ.get("OPENAQ_PAGE_LIMIT", "1000"))    # max allowed by API
HTTP_TIMEOUT_S   = float(os.environ.get("HTTP_TIMEOUT_S", "20"))
MAX_PAGES        = int(os.environ.get("MAX_PAGES", "2000"))            # safety valve


# -----------------------------
# Helpers
# -----------------------------
@dataclass
class DayWindow:
    start: dt.datetime
    end: dt.datetime


def utc_day_bounds(days_ago: int) -> DayWindow:
    """Return [00:00:00Z, 23:59:59Z] for the UTC day `days_ago` before today."""
    day = (dt.datetime.utcnow().date() - dt.timedelta(days=days_ago))
    start = dt.datetime(day.year, day.month, day.day, 0, 0, 0, tzinfo=dt.timezone.utc)
    end   = dt.datetime(day.year, day.month, day.day, 23, 59, 59, tzinfo=dt.timezone.utc)
    return DayWindow(start, end)


def requests_session_with_retries() -> requests.Session:
    # Retries on 429 & the usual 5xx; backoff capped to ~20s
    retry = Retry(
        total=8,
        read=8,
        connect=8,
        backoff_factor=0.7,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET"]),
        respect_retry_after_header=True,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=20, pool_maxsize=20)
    sess = requests.Session()
    sess.mount("https://", adapter)
    sess.headers.update({"User-Agent": "starfishprime-openaq/1.0"})
    if OPENAQ_API_KEY:
        sess.headers.update({"X-API-Key": OPENAQ_API_KEY})
    return sess


def dt_to_iso_z(d: dt.datetime) -> str:
    return d.astimezone(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


# -----------------------------
# OpenAQ pull (bbox + day)
# -----------------------------
def fetch_measurements_bbox_day(
    sess: requests.Session,
    bbox: str,
    date_from: dt.datetime,
    date_to: dt.datetime,
    parameter_id: Optional[int] = PARAMETER_ID,
    iso: Optional[str] = OPENAQ_COUNTRY,
) -> pd.DataFrame:
    """
    Pull PM2.5 measurements inside a bbox for a UTC day window using /v3/measurements.
    Returns a normalized DataFrame.
    """
    rows: List[Dict] = []

    page = 1
    while True:
        params = {
            "bbox": bbox,                   # "lon1,lat1,lon2,lat2"
            "limit": PAGE_LIMIT,
            "page": page,
            "date_from": dt_to_iso_z(date_from),
            "date_to": dt_to_iso_z(date_to),
            "sort": "desc",
        }
        if parameter_id is not None:
            params["parameter_id"] = parameter_id
        if iso:
            params["iso"] = iso

        url = f"{OPENAQ_BASE_URL}/measurements"
        r = sess.get(url, params=params, timeout=HTTP_TIMEOUT_S)
        r.raise_for_status()
        payload = r.json()

        results = payload.get("results") or []
        for m in results:
            # Shape (per docs): location object + fields on the measurement
            loc = m.get("location") or {}
            coords = (loc.get("coordinates") or m.get("coordinates") or {})
            when = (
                m.get("datetime_utc")
                or (m.get("datetime") or {}).get("utc")
                or m.get("datetime")
                or m.get("date_utc")
                or (m.get("date") or {}).get("utc")
            )

            rows.append({
                "location_id":      loc.get("id") or m.get("locations_id") or m.get("locationId"),
                "location_name":    loc.get("name") or m.get("location_name") or m.get("name"),
                "latitude":         coords.get("latitude"),
                "longitude":        coords.get("longitude"),
                "parameter_id":     m.get("parameter_id") or (m.get("parameter") or {}).get("id"),
                "parameter":        (m.get("parameter") or {}).get("code") or OPENAQ_PARAMETER,
                "value":            m.get("value"),
                "unit":             m.get("unit"),
                "datetime_utc":     when,
                "provider_id":      m.get("provider_id") or loc.get("provider_id"),
                "bbox":             bbox,
            })

        meta = payload.get("meta") or {}
        pages = int(meta.get("pages") or 1)
        if page >= pages:
            break
        page += 1
        if page > MAX_PAGES:
            break

    if not rows:
        return pd.DataFrame(columns=[
            "location_id","location_name","latitude","longitude",
            "parameter_id","parameter","value","unit","datetime_utc","provider_id","bbox"
        ])
    df = pd.DataFrame(rows)
    # normalize timestamp for BQ (TIMESTAMP)
    if "datetime_utc" in df.columns:
        df["datetime_utc"] = pd.to_datetime(df["datetime_utc"], utc=True, errors="coerce")
    # only pm2.5 just in case; keep id==2 or code "pm25"/"pm2.5"
    def _is_pm25(row) -> bool:
        pid = row.get("parameter_id")
        code = str(row.get("parameter") or "").lower()
        return (pid == 2) or (code in ("pm25", "pm2.5"))
    df = df[df.apply(_is_pm25, axis=1)]
    df = df.dropna(subset=["location_id", "datetime_utc", "value"])
    return df


def fetch_all(sess: requests.Session) -> pd.DataFrame:
    """Fetch across the configured day window(s) and bboxes, return a single DataFrame."""
    # Build list of day windows inclusive
    start = min(DAYS_AGO_START, DAYS_AGO_END)
    end   = max(DAYS_AGO_START, DAYS_AGO_END)
    windows: List[DayWindow] = [utc_day_bounds(n) for n in range(start, end + 1)]

    frames: List[pd.DataFrame] = []
    for bbox in BBOXES:
        bbox = bbox.strip()
        if not bbox:
            continue
        for w in windows:
            df = fetch_measurements_bbox_day(
                sess=sess,
                bbox=bbox,
                date_from=w.start,
                date_to=w.end,
            )
            if not df.empty:
                frames.append(df)
            # be nice to the API between windows
            time.sleep(0.25)

    if not frames:
        return pd.DataFrame(columns=[
            "location_id","location_name","latitude","longitude",
            "parameter_id","parameter","value","unit","datetime_utc","provider_id","bbox"
        ])
    df_all = pd.concat(frames, ignore_index=True)
    # de-dupe: same reading may appear across pages/windows if your window overlaps
    df_all = df_all.drop_duplicates(subset=["location_id", "datetime_utc"], keep="last")
    return df_all


# -----------------------------
# BigQuery upsert
# -----------------------------
SCHEMA = [
    bigquery.SchemaField("location_id",  "INT64"),
    bigquery.SchemaField("location_name","STRING"),
    bigquery.SchemaField("latitude",     "FLOAT64"),
    bigquery.SchemaField("longitude",    "FLOAT64"),
    bigquery.SchemaField("parameter_id", "INT64"),
    bigquery.SchemaField("parameter",    "STRING"),
    bigquery.SchemaField("value",        "FLOAT64"),
    bigquery.SchemaField("unit",         "STRING"),
    bigquery.SchemaField("datetime_utc", "TIMESTAMP"),
    bigquery.SchemaField("provider_id",  "INT64"),
    bigquery.SchemaField("bbox",         "STRING"),
    bigquery.SchemaField("_ingested_at", "TIMESTAMP"),
]


def ensure_table(client: bigquery.Client, table_id: str) -> None:
    try:
        client.get_table(table_id)
    except Exception:
        tbl = bigquery.Table(table_id, schema=SCHEMA)
        tbl.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="datetime_utc"
        )
        tbl.clustering_fields = ["location_id"]
        client.create_table(tbl)


def merge_upsert(client: bigquery.Client, staging_id: str, target_id: str) -> None:
    query = f"""
    MERGE `{target_id}` T
    USING `{staging_id}` S
    ON T.location_id = S.location_id
       AND T.datetime_utc = S.datetime_utc
    WHEN MATCHED THEN
      UPDATE SET
        location_name = S.location_name,
        latitude      = S.latitude,
        longitude     = S.longitude,
        parameter_id  = S.parameter_id,
        parameter     = S.parameter,
        value         = S.value,
        unit          = S.unit,
        provider_id   = S.provider_id,
        bbox          = S.bbox,
        _ingested_at  = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN
      INSERT (
        location_id, location_name, latitude, longitude,
        parameter_id, parameter, value, unit,
        datetime_utc, provider_id, bbox, _ingested_at
      ) VALUES (
        S.location_id, S.location_name, S.latitude, S.longitude,
        S.parameter_id, S.parameter, S.value, S.unit,
        S.datetime_utc, S.provider_id, S.bbox, CURRENT_TIMESTAMP()
      )
    """
    client.query(query, location=BQ_LOCATION).result()


def main() -> None:
    if not PROJECT:
        raise RuntimeError("GOOGLE_CLOUD_PROJECT is required")
    if not OPENAQ_API_KEY:
        # The service runs fine without a key for some endpoints,
        # but a key prevents stricter rate limits — warn loudly.
        print("WARNING: OPENAQ_API_KEY not set; you may hit 429 rate limiting more often.", flush=True)

    client = bigquery.Client(project=PROJECT, location=BQ_LOCATION)

    session = requests_session_with_retries()
    df_all = fetch_all(session)

    if df_all.empty:
        print("No rows fetched from OpenAQ (window/bbox may have no recent PM2.5 or you were rate-limited).")
        return

    # final shaping + add _ingested_at
    df_all = df_all.copy()
    df_all["_ingested_at"] = pd.Timestamp.utcnow(tz="UTC")

    target_id = f"{PROJECT}.{BQ_DATASET}.{BQ_TABLE}"
    ensure_table(client, target_id)

    # Load to a staging table then MERGE (idempotent)
    staging_id = f"{PROJECT}.{BQ_DATASET}._openaq_stage_{int(time.time())}"
    job_cfg = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
        schema=SCHEMA,
    )
    load = client.load_table_from_dataframe(df_all, staging_id, job_config=job_cfg, location=BQ_LOCATION)
    load.result()

    merge_upsert(client, staging_id, target_id)
    client.delete_table(staging_id, not_found_ok=True)
    print(f"Upserted {len(df_all):,} rows into {target_id}")


if __name__ == "__main__":
    main()
