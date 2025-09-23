#!/usr/bin/env python3
from __future__ import annotations
import os
import sys
import time
import math
import json
import datetime as dt
from typing import Any, Dict, List, Optional, Iterable

import requests
from google.cloud import bigquery

OPENAQ_BASE = "https://api.openaq.org/v3"

# -------- env --------
PROJECT_ID      = os.getenv("GOOGLE_CLOUD_PROJECT", "")
BQ_DATASET      = os.getenv("BQ_DATASET", "silver")              # <- keep writing to silver
BQ_TABLE        = os.getenv("BQ_TABLE", "openaq_pm25_test")
BQ_LOCATION     = os.getenv("BQ_LOCATION", "europe-north2")
OPENAQ_API_KEY  = os.getenv("OPENAQ_API_KEY", "")                # set via Secret
# Pipe-separated list of bboxes (lon1,lat1,lon2,lat2)
# Default: LA, SF, SD (same as your smoketest)
BBOXES          = os.getenv("BBOXES", "-118.6681,33.7037,-117.6462,34.3373|"
                                      "-122.5136,37.7080,-122.3569,37.8324|"
                                      "-117.282,32.615,-116.908,33.023")
PER_BBOX        = int(os.getenv("PER_BBOX", "10"))               # locations to sample per bbox
SLEEP_MS        = int(os.getenv("SLEEP_MS", "250"))              # delay between API calls
PARAMETER_ID    = int(os.getenv("PARAMETER_ID", "2"))            # 2 = pm2.5
PARAMETER_CODE  = os.getenv("PARAMETER", "pm25")

# -------- requests session with backoff --------
def build_session() -> requests.Session:
    s = requests.Session()
    if OPENAQ_API_KEY:
        s.headers.update({"X-API-Key": OPENAQ_API_KEY})
    s.headers.update({"Accept": "application/json", "User-Agent": "starfishprime-ingestor/1.0"})
    adapter = requests.adapters.HTTPAdapter(pool_connections=8, pool_maxsize=8, max_retries=3)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    return s

def get_with_backoff(s: requests.Session, url: str, params=None, max_tries=6, base_delay=0.6):
    delay = base_delay
    for i in range(1, max_tries + 1):
        r = s.get(url, params=params, timeout=30)
        if r.status_code in (429, 500, 502, 503, 504):
            if i == max_tries:
                r.raise_for_status()
            time.sleep(delay)
            delay = min(12.0, round(delay * 1.8, 2))
            continue
        r.raise_for_status()
        return r

# -------- OpenAQ helpers (same spirit as local_smoketest) --------
def list_locations_in_bbox(s: requests.Session, bbox: str, limit=1000) -> List[dict]:
    params = {"bbox": bbox, "parameters_id": PARAMETER_ID, "limit": limit, "page": 1}
    r = get_with_backoff(s, f"{OPENAQ_BASE}/locations", params=params)
    return r.json().get("results", [])

def list_pm25_sensors_for_location(s: requests.Session, location_id: int, per_location_limit=50) -> List[dict]:
    url = f"{OPENAQ_BASE}/locations/{location_id}/sensors"
    params = {"limit": per_location_limit, "page": 1}
    r = get_with_backoff(s, url, params=params)
    sensors = r.json().get("results", [])
    out = []
    for sr in sensors:
        pid = sr.get("parameter_id") or (sr.get("parameter") or {}).get("id")
        code = (sr.get("parameter") or {}).get("code") or ""
        if pid == PARAMETER_ID or str(code).lower().replace(" ", "") in {"pm25", "pm2.5", "pm2_5"}:
            unit_hint = (sr.get("unit")
                         or (sr.get("parameter") or {}).get("preferredUnit")
                         or (sr.get("parameter") or {}).get("unit"))
            if unit_hint:
                sr["_unit_hint"] = unit_hint
            out.append(sr)
    return out

def latest_for_location(s: requests.Session, location_id: int) -> dict:
    r = get_with_backoff(s, f"{OPENAQ_BASE}/locations/{location_id}/latest", params={"limit": 1})
    j = r.json().get("results")
    if isinstance(j, list) and j:
        return j[0]
    return j or {}

def _pick_datetime_from_measurement(m: dict) -> Optional[str]:
    """
    Robust timestamp extraction for 'latest' payloads.
    Prefer explicit fields; fall back to period/coverage datetimeTo.utc.
    """
    if not isinstance(m, dict):
        return None
    # explicit
    for k in ("datetime_utc", "datetime", "date_utc", "timestamp", "observed_at", "observedAt"):
        v = m.get(k)
        if isinstance(v, str) and v:
            return v
    # nested 'date'
    d = m.get("date") or {}
    for k in ("utc", "UTC", "iso"):
        v = d.get(k)
        if isinstance(v, str) and v:
            return v
    # period datetimeTo.utc
    period = m.get("period") or {}
    dt_to = (period.get("datetimeTo") or {}).get("utc")
    if isinstance(dt_to, str) and dt_to:
        return dt_to
    # coverage datetimeTo.utc
    cov = m.get("coverage") or {}
    dt_to2 = (cov.get("datetimeTo") or {}).get("utc")
    if isinstance(dt_to2, str) and dt_to2:
        return dt_to2
    # period datetimeFrom.utc (last resort)
    dt_from = (period.get("datetimeFrom") or {}).get("utc")
    if isinstance(dt_from, str) and dt_from:
        return dt_from
    return None

def is_pm25(m: dict) -> bool:
    pid = m.get("parameter_id") or (m.get("parameter") or {}).get("id")
    code = (m.get("parameter") or {}).get("code") or m.get("parameter") or ""
    if pid == PARAMETER_ID:
        return True
    if isinstance(code, str) and code.lower().replace(" ", "") in {"pm25", "pm2.5", "pm2_5"}:
        return True
    return False

# -------- BigQuery --------
def ensure_table(bq: bigquery.Client, table_fq: str):
    table = bigquery.Table(table_fq)
    table.schema = [
        bigquery.SchemaField("location_id", "INT64"),
        bigquery.SchemaField("sensor_id",   "INT64"),
        bigquery.SchemaField("parameter",   "STRING"),
        bigquery.SchemaField("value",       "FLOAT"),
        bigquery.SchemaField("unit",        "STRING"),
        bigquery.SchemaField("datetime_utc","TIMESTAMP"),
        bigquery.SchemaField("location_name","STRING"),
        bigquery.SchemaField("lat",         "FLOAT"),
        bigquery.SchemaField("lon",         "FLOAT"),
        bigquery.SchemaField("ingested_at", "TIMESTAMP"),
        bigquery.SchemaField("source",      "STRING"),
    ]
    table.time_partitioning = bigquery.TimePartitioning(field="datetime_utc")
    try:
        bq.get_table(table_fq)
    except Exception:
        bq.create_table(table)

def upsert_rows(bq: bigquery.Client, table_fq: str, rows: List[Dict[str, Any]]):
    if not rows:
        return 0
    # Stage rows to a temp table (loads are cheaper), then MERGE into target
    job_id = f"openaq_stage_{int(time.time())}"
    stage_table = f"{table_fq}_stage_{job_id}"
    schema = [
        bigquery.SchemaField("location_id", "INT64"),
        bigquery.SchemaField("sensor_id",   "INT64"),
        bigquery.SchemaField("parameter",   "STRING"),
        bigquery.SchemaField("value",       "FLOAT"),
        bigquery.SchemaField("unit",        "STRING"),
        bigquery.SchemaField("datetime_utc","TIMESTAMP"),
        bigquery.SchemaField("location_name","STRING"),
        bigquery.SchemaField("lat",         "FLOAT"),
        bigquery.SchemaField("lon",         "FLOAT"),
        bigquery.SchemaField("ingested_at", "TIMESTAMP"),
        bigquery.SchemaField("source",      "STRING"),
    ]
    bq.create_table(bigquery.Table(stage_table, schema=schema))
    bq.load_table_from_json(rows, stage_table).result()

    merge_sql = f"""
    MERGE `{table_fq}` T
    USING `{stage_table}` S
    ON T.location_id = S.location_id
       AND T.parameter = S.parameter
       AND T.datetime_utc = S.datetime_utc
    WHEN MATCHED THEN UPDATE SET
        T.value = S.value, T.unit = S.unit, T.sensor_id = S.sensor_id,
        T.location_name = S.location_name, T.lat = S.lat, T.lon = S.lon,
        T.ingested_at = S.ingested_at, T.source = S.source
    WHEN NOT MATCHED THEN INSERT ROW;
    """
    bq.query(merge_sql, location=BQ_LOCATION).result()
    bq.delete_table(stage_table, not_found_ok=True)
    return len(rows)

# -------- main sweep --------
def pick_locations(locs: List[dict], n: int) -> List[dict]:
    out = []
    for L in locs:
        name = str(L.get("name", "")).lower()
        if any(x in name for x in ("ebam","temporary","decommissioned","pilot","test","unit","mammoth","gbuapcd")):
            continue
        out.append(L)
    return out[:n]

def run() -> int:
    if not PROJECT_ID:
        print("GOOGLE_CLOUD_PROJECT not set", file=sys.stderr)
        return 2
    if not OPENAQ_API_KEY:
        print("OPENAQ_API_KEY missing (secret)", file=sys.stderr)
        return 2

    client = bigquery.Client(project=PROJECT_ID, location=BQ_LOCATION)
    table_fq = f"{PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}"
    ensure_table(client, table_fq)

    s = build_session()
    bboxes = [b.strip() for b in BBOXES.split("|") if b.strip()]
    rows: List[Dict[str, Any]] = []
    now = dt.datetime.now(dt.UTC)

    for bbox in bboxes:
        print(f"Finding PM2.5 locations in bbox: {bbox}", flush=True)
        try:
            locs = list_locations_in_bbox(s, bbox)
        except requests.HTTPError as e:
            print(f"  ERROR listing locations: {e}", flush=True)
            continue

        chosen = pick_locations(locs, PER_BBOX)
        for loc in chosen:
            loc_id = loc.get("id")
            loc_name = loc.get("name", "")
            coords = loc.get("coordinates") or {}
            lat = coords.get("latitude")
            lon = coords.get("longitude")

            # Grab sensors to infer unit if latest lacks it
            unit_hint = None
            try:
                sens = list_pm25_sensors_for_location(s, loc_id, per_location_limit=20)
                if sens:
                    unit_hint = sens[0].get("_unit_hint")
            except requests.HTTPError:
                pass

            try:
                latest = latest_for_location(s, loc_id)
            except requests.HTTPError as e:
                print(f"  WARN latest for location {loc_id}: {e}", flush=True)
                continue

            # measurements[]: pick PM2.5 and extract value + datetime
            ms = latest.get("measurements") or []
            pm25 = [m for m in ms if is_pm25(m)]
            if not pm25:
                time.sleep(SLEEP_MS/1000.0)
                continue

            # choose the most recent by parsed timestamp
            def k(m: dict):
                sdt = _pick_datetime_from_measurement(m) or ""
                try:
                    if sdt.endswith("Z"):
                        return dt.datetime.fromisoformat(sdt.replace("Z","+00:00"))
                    return dt.datetime.fromisoformat(sdt).astimezone(dt.UTC)
                except Exception:
                    return dt.datetime.min.replace(tzinfo=dt.UTC)

            best = max(pm25, key=k)
            when = _pick_datetime_from_measurement(best)
            if not when:
                # skip if we truly couldn't find a timestamp
                time.sleep(SLEEP_MS/1000.0)
                continue

            val  = best.get("value")
            unit = best.get("unit") or unit_hint or "µg/m³"
            sid  = best.get("sensor_id") or best.get("sensorId")

            rows.append({
                "location_id":  loc_id,
                "sensor_id":    sid,
                "parameter":    PARAMETER_CODE,
                "value":        val,
                "unit":         unit,
                "datetime_utc": when,          # TIMESTAMP column accepts RFC3339
                "location_name": loc_name,
                "lat":          lat,
                "lon":          lon,
                "ingested_at":  now.isoformat(),
                "source":       "openaq_latest_v3",
            })

            time.sleep(SLEEP_MS/1000.0)

    if not rows:
        print("No OpenAQ rows fetched for the requested sweep. Exiting gracefully.")
        return 0

    n = upsert_rows(client, table_fq, rows)
    print(f"Upserted {n} OpenAQ 'latest' rows into {table_fq}")
    return 0

if __name__ == "__main__":
    sys.exit(run())
