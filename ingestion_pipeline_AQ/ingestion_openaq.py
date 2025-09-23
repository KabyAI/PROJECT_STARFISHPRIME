#!/usr/bin/env python3
"""
OpenAQ v3 -> BigQuery ingestion (PM2.5, multi-bbox, multi-sensor).

Env vars (typical for your job.env.yaml):
  GOOGLE_CLOUD_PROJECT   e.g. project-starfishprime-001
  BQ_DATASET             e.g. silver
  BQ_TABLE               e.g. openaq_pm25_test
  BQ_LOCATION            e.g. europe-north2
  OPENAQ_API_KEY         secret (injected via --set-secrets)
  BBOXES                 pipe-separated bboxes: "lon1,lat1,lon2,lat2|..."
  PER_BBOX               how many sensors to sample per bbox (default 20)
  HOURS                  lookback window in hours (default 720)
  DAYS_FALLBACK          if no rows in HOURS, look back this many days (default 3650)
  SLEEP_MS               delay between /measurements calls (default 250)
  DEBUG                  "1" to print sample raw payload when no datetime is found

NOTE: This script *does not* call /locations/{id}/latest. It uses the stable
      /sensors/{id}/measurements endpoint like your working smoketest.
"""

from __future__ import annotations
import os
import sys
import time
import json
import math
import datetime as dt
from typing import Any, Dict, Iterable, List, Optional, Tuple

import requests
from google.cloud import bigquery

OPENAQ_BASE = "https://api.openaq.org/v3"


# ----------------------- HTTP helper -----------------------
def get_with_backoff(
    s: requests.Session,
    url: str,
    params: Dict[str, Any] | None = None,
    max_tries: int = 6,
    base_delay: float = 0.5,
) -> requests.Response:
    delay = base_delay
    for i in range(1, max_tries + 1):
        r = s.get(url, params=params, timeout=30)
        if r.status_code in (429, 500, 502, 503, 504):
            if i == max_tries:
                r.raise_for_status()
            time.sleep(delay)
            delay = min(10.0, round(delay * 1.8, 2))
            continue
        r.raise_for_status()
        return r


# ----------------------- OpenAQ helpers -----------------------
def list_locations_in_bbox(
    s: requests.Session, bbox: str, limit: int = 1000
) -> List[Dict[str, Any]]:
    params = {
        "bbox": bbox,
        "parameters_id": 2,  # PM2.5
        "limit": limit,
        "page": 1,
    }
    r = get_with_backoff(s, f"{OPENAQ_BASE}/locations", params=params)
    return r.json().get("results", [])


def list_pm25_sensors_for_location(
    s: requests.Session, location_id: int, per_location_limit: int = 50
) -> List[Dict[str, Any]]:
    url = f"{OPENAQ_BASE}/locations/{location_id}/sensors"
    params = {"limit": per_location_limit, "page": 1}
    r = get_with_backoff(s, url, params=params)
    sensors = r.json().get("results", [])
    out: List[Dict[str, Any]] = []

    for sr in sensors:
        pid = sr.get("parameter_id") or (sr.get("parameter") or {}).get("id") or sr.get("parameterId")
        pcode = (sr.get("parameter") or {}).get("code") or sr.get("parameterCode") or ""
        is_pm25 = (pid == 2) or (str(pcode).lower() in ("pm25", "pm2.5", "pm2_5"))
        if not is_pm25:
            continue

        unit_hint = (
            sr.get("unit")
            or (sr.get("parameter") or {}).get("preferredUnit")
            or (sr.get("parameter") or {}).get("unit")
        )
        if unit_hint:
            sr["_unit_hint"] = unit_hint

        out.append(sr)
    return out


def _pick_datetime(m: Dict[str, Any]) -> Optional[str]:
    """Coalesce UTC timestamp from an OpenAQ measurement object."""
    if not isinstance(m, dict):
        return None

    # 1) canonical v3 pattern: "datetime" object/dict or string
    dt_obj = m.get("datetime")
    if isinstance(dt_obj, dict):
        utc = dt_obj.get("utc") or dt_obj.get("UTC")
        if isinstance(utc, str) and utc:
            return utc
    elif isinstance(dt_obj, str) and dt_obj:
        return dt_obj

    # 2) "period" block: prefer end of interval
    period = m.get("period")
    if isinstance(period, dict):
        dto = period.get("datetimeTo")
        if isinstance(dto, dict):
            utc = dto.get("utc") or dto.get("UTC")
            if isinstance(utc, str) and utc:
                return utc
        dfrom = period.get("datetimeFrom")
        if isinstance(dfrom, dict):
            utc = dfrom.get("utc") or dfrom.get("UTC")
            if isinstance(utc, str) and utc:
                return utc

    # 3) other flat keys sometimes present
    for k in ("datetime_utc", "date_utc", "observed_at", "observedAt", "lastUpdated", "last_updated", "time", "timestamp"):
        v = m.get(k)
        if isinstance(v, str) and v:
            return v

    # 4) nested date object (older harmonized shape)
    date_obj = m.get("date")
    if isinstance(date_obj, dict):
        for k in ("utc", "UTC", "iso"):
            v = date_obj.get(k)
            if isinstance(v, str) and v:
                return v

    # 5) some feeds return strings in period.to / period.end
    if isinstance(period, dict):
        v = period.get("to") or period.get("end")
        if isinstance(v, str) and v:
            return v

    return None


def latest_measurement_for_sensor(
    s: requests.Session, sensor_id: int, date_from_iso: str, date_to_iso: str
) -> Tuple[Optional[float], Optional[str], Optional[str], Optional[Dict[str, Any]]]:
    url = f"{OPENAQ_BASE}/sensors/{sensor_id}/measurements"
    params = {
        "date_from": date_from_iso,
        "date_to": date_to_iso,
        "limit": 1,
        "sort": "desc",
    }
    r = get_with_backoff(s, url, params=params)
    results = r.json().get("results", [])
    if not results:
        return None, None, None, None
    m = results[0]
    value = m.get("value")
    unit = m.get("unit") or (m.get("parameter") or {}).get("units")
    when = _pick_datetime(m)
    return value, unit, when, m


# ----------------------- Core sweep -----------------------
def sweep_openaq(
    api_key: str,
    bboxes: Iterable[str],
    per_bbox: int,
    hours: int,
    sleep_ms: int,
    debug: bool = False,
) -> List[Dict[str, Any]]:
    headers = {"X-API-Key": api_key} if api_key else {}
    s = requests.Session()
    s.headers.update(headers)

    # time window
    now_utc = dt.datetime.now(dt.UTC).replace(microsecond=0)
    frm = now_utc - dt.timedelta(hours=hours)
    date_from = frm.isoformat().replace("+00:00", "Z")
    date_to = now_utc.isoformat().replace("+00:00", "Z")

    rows: List[Dict[str, Any]] = []

    for bbox in bboxes:
        print(f"Finding PM2.5 locations in bbox: {bbox}", flush=True)
        try:
            locs = list_locations_in_bbox(s, bbox)
        except requests.HTTPError as e:
            print(f"  ERROR listing locations for bbox {bbox}: {e}", flush=True)
            continue

        # filter obvious temporary/test sites
        filtered: List[Dict[str, Any]] = []
        for L in locs:
            name = str(L.get("name", "")).lower()
            if any(x in name for x in ("ebam", "temporary", "decommissioned", "pilot", "test", "unit", "mammoth", "gbuapcd")):
                continue
            filtered.append(L)

        # gather up to per_bbox sensors
        picked: List[Dict[str, Any]] = []
        for L in filtered:
            if len(picked) >= per_bbox:
                break
            lid = L["id"]
            try:
                sensors = list_pm25_sensors_for_location(s, lid)
            except requests.HTTPError as e:
                print(f"  WARN sensors for location {lid}: {e}", flush=True)
                continue

            for sen in sensors:
                if len(picked) >= per_bbox:
                    break
                sen["_location"] = L
                picked.append(sen)

        # get latest measurement per sensor
        for sen in picked:
            sid = sen["id"]
            loc = sen["_location"]
            loc_id = loc["id"]
            loc_name = loc.get("name", "")
            coords = loc.get("coordinates") or {}
            lat = coords.get("latitude")
            lon = coords.get("longitude")

            try:
                val, unit, when, raw = latest_measurement_for_sensor(s, sid, date_from, date_to)
            except requests.HTTPError as e:
                print(f"  WARN measurement for sensor {sid}: {e}", flush=True)
                continue

            if not unit:
                unit = (
                    sen.get("_unit_hint")
                    or (sen.get("parameter") or {}).get("preferredUnit")
                    or (sen.get("parameter") or {}).get("unit")
                )
                pid = sen.get("parameter_id") or (sen.get("parameter") or {}).get("id")
                if not unit and pid == 2:
                    unit = "µg/m³"

            if val is not None:
                if not when and debug and raw:
                    print(f"  DEBUG sensor {sid}: no datetime found; sample raw result:\n    {raw}", flush=True)
                rows.append(
                    {
                        "SensorId": sid,
                        "LocationId": loc_id,
                        "LocationName": loc_name,
                        "Lat": lat,
                        "Lon": lon,
                        "Value": val,
                        "Unit": unit,
                        "DateTimeUTC": when,  # string ISO; we’ll coerce to TIMESTAMP on load
                    }
                )
            time.sleep(sleep_ms / 1000.0)

    return rows


# ----------------------- BigQuery I/O -----------------------
def ensure_table(client: bigquery.Client, dataset: str, table: str, location: str) -> bigquery.Table:
    ds_ref = bigquery.DatasetReference(client.project, dataset)
    try:
        client.get_dataset(ds_ref)
    except Exception:
        # dataset should exist already in your setup; create if missing
        ds = bigquery.Dataset(ds_ref)
        ds.location = location
        client.create_dataset(ds, exists_ok=True)

    table_id = f"{client.project}.{dataset}.{table}"
    schema = [
        bigquery.SchemaField("SensorId", "INT64"),
        bigquery.SchemaField("LocationId", "INT64"),
        bigquery.SchemaField("LocationName", "STRING"),
        bigquery.SchemaField("Lat", "FLOAT64"),
        bigquery.SchemaField("Lon", "FLOAT64"),
        bigquery.SchemaField("Value", "FLOAT64"),
        bigquery.SchemaField("Unit", "STRING"),
        bigquery.SchemaField("DateTimeUTC", "TIMESTAMP"),
    ]
    table_ref = bigquery.Table(table_id, schema=schema)
    return client.create_table(table_ref, exists_ok=True)


def load_rows(
    client: bigquery.Client,
    dataset: str,
    table: str,
    location: str,
    rows: List[Dict[str, Any]],
) -> None:
    if not rows:
        print("No OpenAQ rows fetched for the requested sweep. Exiting gracefully.")
        return

    ensure_table(client, dataset, table, location)

    # Convert DateTimeUTC (str|None) -> None or RFC3339 for TIMESTAMP
    def to_rfc3339(s: Optional[str]) -> Optional[str]:
        if not s:
            return None
        # Trust Z; otherwise add Z if naive
        if s.endswith("Z") or "+" in s:
            return s
        return s + "Z"

    payload = []
    for r in rows:
        rec = dict(r)
        rec["DateTimeUTC"] = to_rfc3339(r.get("DateTimeUTC"))
        payload.append(rec)

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        schema_update_options=[bigquery.SchemaUpdateOption.ALLOW_FIELD_RELAXATION],
    )

    table_id = f"{client.project}.{dataset}.{table}"
    # stream via a tempfile-like bytes buffer
    body = "\n".join(json.dumps(x, ensure_ascii=False) for x in payload).encode("utf-8")
    load_job = client.load_table_from_file(
        file_obj=os.BytesIO(body),  # type: ignore
        destination=table_id,
        job_config=job_config,
        location=location,
    )
    load_job.result()
    print(f"Loaded {len(rows)} rows into {table_id}.")


# ----------------------- Entry -----------------------
def main() -> None:
    project = os.environ.get("GOOGLE_CLOUD_PROJECT") or os.environ.get("PROJECT_ID")
    dataset = os.environ.get("BQ_DATASET", "silver")
    table = os.environ.get("BQ_TABLE", "openaq_pm25_test")
    location = os.environ.get("BQ_LOCATION", "europe-north2")
    api_key = os.environ.get("OPENAQ_API_KEY", "")

    if not project:
        print("GOOGLE_CLOUD_PROJECT is required.", file=sys.stderr)
        sys.exit(2)
    if not api_key:
        print("OPENAQ_API_KEY is not set (secret).", file=sys.stderr)
        sys.exit(2)

    # BBOXES parsing (pipe-separated)
    raw_bboxes = os.environ.get("BBOXES", "").strip()
    if not raw_bboxes:
        # sensible default to your three metros
        raw_bboxes = (
            "-118.6681,33.7037,-117.6462,34.3373|"
            "-122.5136,37.7080,-122.3569,37.8324|"
            "-117.282,32.615,-116.908,33.023"
        )
    bboxes = [b.strip() for b in raw_bboxes.split("|") if b.strip()]

    per_bbox = int(os.environ.get("PER_BBOX", "20"))
    hours = int(os.environ.get("HOURS", "720"))
    days_fallback = int(os.environ.get("DAYS_FALLBACK", "3650"))
    sleep_ms = int(os.environ.get("SLEEP_MS", "250"))
    debug = os.environ.get("DEBUG", "0") == "1"

    # First sweep in the configured HOURS window
    rows = sweep_openaq(api_key, bboxes, per_bbox, hours, sleep_ms, debug=debug)

    # Optional fallback: if no rows, try a large historical window
    if not rows and days_fallback and days_fallback > 0:
        print(f"No rows in HOURS={hours}. Falling back to DAYS_FALLBACK={days_fallback}.", flush=True)
        # Recompute window by days
        now_utc = dt.datetime.now(dt.UTC).replace(microsecond=0)
        frm = now_utc - dt.timedelta(days=days_fallback)
        os.environ["HOURS"] = str(max(1, days_fallback * 24))
        rows = sweep_openaq(api_key, bboxes, per_bbox, days_fallback * 24, sleep_ms, debug=debug)

    # Load to BigQuery
    client = bigquery.Client(project=project, location=location)
    load_rows(client, dataset, table, location, rows)


if __name__ == "__main__":
    main()
