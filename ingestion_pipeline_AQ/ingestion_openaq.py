#!/usr/bin/env python3
# ingestion_openaq.py — OpenAQ v3 → BigQuery (silver)
# - Multiple bbox sweep for PM2.5
# - Primary: /sensors/{id}/measurements (time-bounded)
# - Fallback: /locations/{id}/latest (extracts period.datetimeTo.utc etc.)
# - Writes rows to BQ as TIMESTAMP (datetime_utc)
#
# Env (all strings):
#   GOOGLE_CLOUD_PROJECT  (required)
#   BQ_DATASET            (default: "silver")
#   BQ_TABLE              (default: "openaq_pm25_test")
#   BQ_LOCATION           (default: "US")  e.g., "europe-north2"
#   OPENAQ_API_KEY        (required in Cloud Run via secret -> env)
#   BBOXES                (default LA|SF|SD) "lonW,latS,lonE,latN|..."
#   PER_BBOX              (default: "10")
#   HOURS                 (default: "48")   look-back window
#   SLEEP_MS              (default: "250")
#   USE_LATEST_FALLBACK   (default: "1")    "0" to disable
#   DEBUG                 (default: "0")
#
# Table schema (created if missing):
#   value FLOAT64
#   sensor_id INT64
#   location_id INT64
#   location_name STRING
#   latitude FLOAT64
#   longitude FLOAT64
#   unit STRING
#   datetime_utc TIMESTAMP
#   parameter STRING         -- "pm25"
#   region STRING            -- "ca" (to match Delphi naming you used)
#   country STRING           -- "us"
#   bbox STRING
#   source STRING            -- "openaq_v3"
#   ingested_at TIMESTAMP

from __future__ import annotations

import os
import sys
import time
import json
import datetime as dt
from typing import Dict, Any, List, Tuple, Iterable, Optional

import requests
import pandas as pd
from google.cloud import bigquery

OPENAQ_BASE = "https://api.openaq.org/v3"

# ----------------------- tiny util -----------------------
def _now_utc() -> dt.datetime:
    return dt.datetime.now(dt.UTC).replace(microsecond=0)

def _to_iso_z(ts: dt.datetime) -> str:
    # returns RFC3339 ending in Z
    return ts.astimezone(dt.UTC).isoformat().replace("+00:00", "Z")

def _parse_ts_maybe(s: Optional[str]) -> Optional[dt.datetime]:
    if not s or not isinstance(s, str):
        return None
    try:
        if s.endswith("Z"):
            s = s.replace("Z", "+00:00")
        return dt.datetime.fromisoformat(s).astimezone(dt.UTC)
    except Exception:
        return None

# ------------------- HTTP with simple backoff -------------
def get_with_backoff(
    s: requests.Session,
    url: str,
    *,
    params: Optional[Dict[str, Any]] = None,
    max_tries: int = 6,
    base_delay: float = 0.6,
    debug: bool = False,
):
    delay = base_delay
    for i in range(1, max_tries + 1):
        r = s.get(url, params=params, timeout=30)
        if r.status_code in (429, 500, 502, 503, 504):
            if i == max_tries:
                r.raise_for_status()
            if debug:
                print(f"  RETRY {i}/{max_tries} {r.status_code} for {url}", flush=True)
            time.sleep(delay)
            delay = min(10.0, round(delay * 1.8, 2))
            continue
        r.raise_for_status()
        return r

# ------------------- OpenAQ helpers (v3) ------------------
def list_locations_in_bbox(s: requests.Session, bbox: str, *, debug: bool = False) -> List[dict]:
    # PM2.5 only
    params = {"bbox": bbox, "parameters_id": 2, "limit": 1000, "page": 1}
    r = get_with_backoff(s, f"{OPENAQ_BASE}/locations", params=params, debug=debug)
    return r.json().get("results", []) or []

def list_pm25_sensors_for_location(s: requests.Session, location_id: int, *, debug: bool = False) -> List[dict]:
    url = f"{OPENAQ_BASE}/locations/{location_id}/sensors"
    r = get_with_backoff(s, url, params={"limit": 100, "page": 1}, debug=debug)
    sensors = r.json().get("results", []) or []
    out = []
    for sr in sensors:
        pid = sr.get("parameter_id") or (sr.get("parameter") or {}).get("id")
        code = (sr.get("parameter") or {}).get("code")
        is_pm25 = (pid == 2) or (isinstance(code, str) and code.lower().replace(" ", "") in {"pm25", "pm2.5", "pm2_5"})
        if not is_pm25:
            continue
        unit_hint = sr.get("unit") or (sr.get("parameter") or {}).get("preferredUnit") or (sr.get("parameter") or {}).get("unit")
        if unit_hint:
            sr["_unit_hint"] = unit_hint
        out.append(sr)
    return out

def _pick_datetime(m: Dict[str, Any]) -> Optional[str]:
    # try direct fields first
    for k in ("datetime_utc", "datetime", "date_utc", "timestamp", "observed_at", "observedAt"):
        v = m.get(k)
        if isinstance(v, str) and v:
            return v
    # period.{datetimeTo,datetimeFrom}.utc
    per = m.get("period") or {}
    if isinstance(per, dict):
        for node in ("datetimeTo", "datetimeFrom"):
            d = per.get(node) or {}
            if isinstance(d, dict):
                v = d.get("utc") or d.get("UTC") or d.get("iso")
                if isinstance(v, str) and v:
                    return v
    # v2-ish nested date
    d = m.get("date") or {}
    if isinstance(d, dict):
        for k in ("utc", "UTC", "iso"):
            v = d.get(k)
            if isinstance(v, str) and v:
                return v
    return None

def latest_measurement_for_sensor(
    s: requests.Session, sensor_id: int, date_from_iso: str, date_to_iso: str, *, debug: bool = False
) -> Tuple[Optional[float], Optional[str], Optional[str], Optional[dict]]:
    url = f"{OPENAQ_BASE}/sensors/{sensor_id}/measurements"
    params = {"date_from": date_from_iso, "date_to": date_to_iso, "limit": 1, "sort": "desc"}
    r = get_with_backoff(s, url, params=params, debug=debug)
    results = r.json().get("results", []) or []
    if not results:
        return None, None, None, None
    m = results[0]
    return m.get("value"), m.get("unit"), _pick_datetime(m), m

def latest_from_location(s: requests.Session, location_id: int | str, *, debug: bool = False) -> Optional[dict]:
    r = get_with_backoff(s, f"{OPENAQ_BASE}/locations/{location_id}/latest", debug=debug)
    loc = (r.json().get("results") or [{}])[0]
    ms = loc.get("measurements") or []
    if not ms:
        return None
    # prefer pm25
    def is_pm25(m):
        pid = m.get("parameter_id") or (m.get("parameter") or {}).get("id")
        code = (m.get("parameter") or {}).get("code")
        return pid == 2 or (isinstance(code, str) and code.lower().replace(" ", "") in {"pm25", "pm2.5", "pm2_5"})
    pm = [m for m in ms if is_pm25(m)]
    return pm[0] if pm else ms[0]

# ------------------- BigQuery I/O -------------------------
def ensure_table(client: bigquery.Client, table_ref: bigquery.TableReference) -> None:
    from google.api_core.exceptions import NotFound

    try:
        client.get_table(table_ref)
        return
    except NotFound:
        schema = [
            bigquery.SchemaField("value", "FLOAT"),
            bigquery.SchemaField("sensor_id", "INT64"),
            bigquery.SchemaField("location_id", "INT64"),
            bigquery.SchemaField("location_name", "STRING"),
            bigquery.SchemaField("latitude", "FLOAT"),
            bigquery.SchemaField("longitude", "FLOAT"),
            bigquery.SchemaField("unit", "STRING"),
            bigquery.SchemaField("datetime_utc", "TIMESTAMP"),
            bigquery.SchemaField("parameter", "STRING"),
            bigquery.SchemaField("region", "STRING"),
            bigquery.SchemaField("country", "STRING"),
            bigquery.SchemaField("bbox", "STRING"),
            bigquery.SchemaField("source", "STRING"),
            bigquery.SchemaField("ingested_at", "TIMESTAMP"),
        ]
        table = bigquery.Table(table_ref, schema=schema)
        client.create_table(table)

def write_rows_bq(
    rows: List[Dict[str, Any]],
    project_id: str,
    dataset_id: str,
    table_id: str,
    bq_location: str,
) -> int:
    if not rows:
        return 0
    df = pd.DataFrame(rows)
    client = bigquery.Client(project=project_id, location=bq_location or None)
    table_ref = bigquery.DatasetReference(project_id, dataset_id).table(table_id)
    ensure_table(client, table_ref)

    job = client.load_table_from_dataframe(
        df, table_ref, job_config=bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
    )
    job.result()
    return len(rows)

# ------------------- main sweep ---------------------------
def run() -> int:
    # env
    project_id = os.getenv("GOOGLE_CLOUD_PROJECT", "")
    dataset_id = os.getenv("BQ_DATASET", "silver")
    table_id = os.getenv("BQ_TABLE", "openaq_pm25_test")
    bq_location = os.getenv("BQ_LOCATION", "US")

    api_key = os.getenv("OPENAQ_API_KEY", "")
    if not project_id or not api_key:
        print("Missing GOOGLE_CLOUD_PROJECT and/or OPENAQ_API_KEY.", file=sys.stderr)
        return 2

    # default three CA metros
    default_bboxes = [
        "-118.6681,33.7037,-117.6462,34.3373",  # Los Angeles
        "-122.5136,37.7080,-122.3569,37.8324",  # San Francisco
        "-117.282,32.615,-116.908,33.023",      # San Diego
    ]
    bboxes_raw = os.getenv("BBOXES", "|".join(default_bboxes))
    bboxes = [b.strip() for b in bboxes_raw.split("|") if b.strip()]

    per_bbox = int(os.getenv("PER_BBOX", "10"))
    hours = int(os.getenv("HOURS", "48"))
    sleep_ms = int(os.getenv("SLEEP_MS", "250"))
    use_latest_fallback = os.getenv("USE_LATEST_FALLBACK", "1") not in {"0", "false", "False"}
    debug = os.getenv("DEBUG", "0") not in {"0", "false", "False"}

    # HTTP session
    s = requests.Session()
    s.headers.update({"X-API-Key": api_key, "Accept": "application/json", "User-Agent": "starfishprime-ingest/1.0"})

    # window
    to_ = _now_utc()
    frm = to_ - dt.timedelta(hours=hours)
    date_from = _to_iso_z(frm)
    date_to = _to_iso_z(to_)

    rows: List[Dict[str, Any]] = []

    for bbox in bboxes:
        print(f"Finding PM2.5 locations in bbox: {bbox}", flush=True)
        try:
            locs = list_locations_in_bbox(s, bbox, debug=debug)
        except requests.HTTPError as e:
            print(f"  ERROR listing locations for bbox {bbox}: {e}", flush=True)
            continue

        # filter obvious temporary/test sites
        filtered = []
        for L in locs:
            name = str(L.get("name", "")).lower()
            if any(x in name for x in ("ebam", "temporary", "decommissioned", "pilot", "test", "unit", "mammoth", "gbuapcd")):
                continue
            filtered.append(L)

        picked: List[dict] = []
        for L in filtered:
            if len(picked) >= per_bbox:
                break
            lid = L.get("id")
            try:
                sensors = list_pm25_sensors_for_location(s, lid, debug=debug)
            except requests.HTTPError as e:
                print(f"  WARN sensors for location {lid}: {e}", flush=True)
                continue
            for sen in sensors:
                if len(picked) >= per_bbox:
                    break
                sen["_location"] = L
                picked.append(sen)

        # query latest in window (+ fallback)
        for sen in picked:
            sid = int(sen.get("id"))
            loc = sen.get("_location") or {}
            loc_id = int(loc.get("id"))
            loc_name = loc.get("name", "")
            coords = loc.get("coordinates") or {}
            lat = coords.get("latitude")
            lon = coords.get("longitude")

            try:
                val, unit, when_s, raw = latest_measurement_for_sensor(s, sid, date_from, date_to, debug=debug)
            except requests.HTTPError as e:
                print(f"  WARN measurement for sensor {sid}: {e}", flush=True)
                val, unit, when_s, raw = None, None, None, None

            # fallback to /locations/{id}/latest
            if val is None and use_latest_fallback:
                try:
                    m = latest_from_location(s, loc_id, debug=debug)
                except requests.HTTPError as e:
                    m = None
                    if debug:
                        print(f"  DEBUG fallback latest for location {loc_id} failed: {e}", flush=True)
                if m:
                    val = m.get("value")
                    unit = unit or m.get("unit") or (m.get("parameter") or {}).get("preferredUnit") \
                           or (m.get("parameter") or {}).get("unit") or sen.get("_unit_hint") or "µg/m³"
                    when_s = _pick_datetime(m)

            if val is None:
                time.sleep(sleep_ms / 1000.0)
                continue

            # normalize timestamp for BQ
            ts = _parse_ts_maybe(when_s)
            if not ts:
                # as last resort, stamp with window end (not ideal, but consistent)
                ts = to_

            rows.append({
                "value": float(val) if isinstance(val, (int, float)) else None,
                "sensor_id": sid,
                "location_id": loc_id,
                "location_name": loc_name,
                "latitude": float(lat) if isinstance(lat, (int, float)) else None,
                "longitude": float(lon) if isinstance(lon, (int, float)) else None,
                "unit": unit or "µg/m³",
                "datetime_utc": ts,              # pandas→BQ will map to TIMESTAMP
                "parameter": "pm25",
                "region": "ca",                   # matches your Delphi region naming
                "country": "us",
                "bbox": bbox,
                "source": "openaq_v3",
                "ingested_at": _now_utc(),
            })

            time.sleep(sleep_ms / 1000.0)

    if not rows:
        print("No OpenAQ rows fetched for the requested sweep. Exiting gracefully.", flush=True)
        return 0

    # Sort by datetime desc just for cleanliness
    rows.sort(key=lambda r: r["datetime_utc"] or dt.datetime.min.replace(tzinfo=dt.UTC), reverse=True)

    n = write_rows_bq(rows, project_id, dataset_id, table_id, bq_location)
    print(f"Wrote {n} rows to {project_id}.{dataset_id}.{table_id} ({bq_location}).", flush=True)
    return 0


if __name__ == "__main__":
    try:
        sys.exit(run())
    except Exception as e:
        # Log + non-zero exit so Cloud Run marks failure
        print(f"FATAL: {e}", file=sys.stderr, flush=True)
        # helpful for debugging in logs
        try:
            import traceback
            traceback.print_exc()
        except Exception:
            pass
        sys.exit(1)
