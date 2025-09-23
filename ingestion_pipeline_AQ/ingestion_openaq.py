#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import io
import json
import sys
import time
import datetime as dt
from typing import Iterable, List, Tuple, Optional, Dict, Any

import requests
from google.cloud import bigquery

OPENAQ_BASE = "https://api.openaq.org/v3"

# --------------- HTTP with backoff (429/5xx) -----------------
def get_with_backoff(
    s: requests.Session,
    url: str,
    params: Optional[dict] = None,
    max_tries: int = 6,
    base_delay: float = 0.6,
):
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


# --------------- OpenAQ helpers (v3) -------------------------
def list_locations_in_bbox(s: requests.Session, bbox: str, limit=1000) -> List[dict]:
    params = {"bbox": bbox, "parameters_id": 2, "limit": limit, "page": 1}  # PM2.5
    r = get_with_backoff(s, f"{OPENAQ_BASE}/locations", params=params)
    return r.json().get("results", [])


def list_pm25_sensors_for_location(
    s: requests.Session, location_id: int, per_location_limit=50
) -> List[dict]:
    url = f"{OPENAQ_BASE}/locations/{location_id}/sensors"
    params = {"limit": per_location_limit, "page": 1}
    r = get_with_backoff(s, url, params=params)
    sensors = r.json().get("results", [])
    out = []
    for sr in sensors:
        pid = sr.get("parameter_id") or (sr.get("parameter") or {}).get("id")
        pcode = (sr.get("parameter") or {}).get("code") or ""
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
    dt_obj = m.get("datetime")
    if isinstance(dt_obj, dict):
        utc = dt_obj.get("utc") or dt_obj.get("UTC")
        if isinstance(utc, str) and utc:
            return utc
    elif isinstance(dt_obj, str) and dt_obj:
        return dt_obj
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
    for k in ("datetime_utc","date_utc","observed_at","observedAt","lastUpdated","last_updated","time","timestamp"):
        v = m.get(k)
        if isinstance(v, str) and v:
            return v
    date_obj = m.get("date")
    if isinstance(date_obj, dict):
        for k in ("utc", "UTC", "iso"):
            v = date_obj.get(k)
            if isinstance(v, str) and v:
                return v
    if isinstance(period, dict):
        v = period.get("to") or period.get("end")
        if isinstance(v, str) and v:
            return v
    return None


def latest_measurement_for_sensor(
    s: requests.Session, sensor_id: int, date_from_iso: str, date_to_iso: str
) -> Tuple[Optional[float], Optional[str], Optional[str]]:
    url = f"{OPENAQ_BASE}/sensors/{sensor_id}/measurements"
    params = {"date_from": date_from_iso, "date_to": date_to_iso, "limit": 1, "sort": "desc"}
    r = get_with_backoff(s, url, params=params)
    results = r.json().get("results", [])
    if not results:
        return None, None, None
    m = results[0]
    value = m.get("value")
    unit = m.get("unit") or (m.get("parameter") or {}).get("units")
    when = _pick_datetime(m)
    return value, unit, when


def latest_for_location(
    s: requests.Session, location_id: int
) -> Tuple[Optional[float], Optional[str], Optional[str]]:
    url = f"{OPENAQ_BASE}/locations/{location_id}/latest"
    params = {"limit": 1}
    r = get_with_backoff(s, url, params=params)
    results = r.json().get("results", [])
    if not results:
        return None, None, None
    m = results[0]
    pid = m.get("parameter_id") or (m.get("parameter") or {}).get("id")
    pcode = (m.get("parameter") or {}).get("code") or ""
    if not (pid == 2 or str(pcode).lower() in ("pm25", "pm2.5", "pm2_5")):
        return None, None, None
    value = m.get("value")
    unit = m.get("unit") or (m.get("parameter") or {}).get("units")
    when = _pick_datetime(m)
    return value, unit, when


# --------------- Collection (multi-bbox sweep) ---------------
def collect_rows(
    bboxes: Iterable[str],
    per_bbox: int,
    hours: int,
    days_fallback: int,
    sleep_ms: int,
    api_key: str,
    debug: bool = False,
) -> List[Tuple]:
    headers = {"X-API-Key": api_key} if api_key else {}
    s = requests.Session()
    s.headers.update(headers)

    to_ = dt.datetime.now(dt.UTC).replace(microsecond=0)
    frm = to_ - dt.timedelta(hours=hours)
    date_from = frm.isoformat().replace("+00:00", "Z")
    date_to = to_.isoformat().replace("+00:00", "Z")
    wide_from = (to_ - dt.timedelta(days=days_fallback)).isoformat().replace("+00:00", "Z")

    rows: List[Tuple] = []

    for bbox in bboxes:
        print(f"Finding PM2.5 locations in bbox: {bbox}", flush=True)
        try:
            locs = list_locations_in_bbox(s, bbox)
        except requests.HTTPError as e:
            print(f"  ERROR listing locations for bbox {bbox}: {e}", flush=True)
            continue

        filtered = []
        for L in locs:
            name = str(L.get("name", "")).lower()
            if any(x in name for x in ("ebam","temporary","decommissioned","pilot","test","unit","mammoth","gbuapcd")):
                continue
            filtered.append(L)

        picked: List[dict] = []
        for L in filtered:
            if len(picked) >= per_bbox:
                break
            lid = L.get("id")
            if lid is None:
                continue
            try:
                sensors = list_pm25_sensors_for_location(s, int(lid))
            except requests.HTTPError as e:
                print(f"  WARN sensors for location {lid}: {e}", flush=True)
                continue
            for sen in sensors:
                if len(picked) >= per_bbox:
                    break
                sen["_location"] = L
                picked.append(sen)

        for sen in picked:
            sid = sen.get("id")
            if sid is None:
                continue
            loc = sen["_location"]
            loc_id = loc.get("id")
            loc_name = loc.get("name", "")
            coords = loc.get("coordinates") or {}
            lat = coords.get("latitude")
            lon = coords.get("longitude")

            try:
                val, unit, when = latest_measurement_for_sensor(s, int(sid), date_from, date_to)
            except requests.HTTPError as e:
                print(f"  WARN measurement for sensor {sid}: {e}", flush=True)
                val = unit = when = None

            if val is None or when is None:
                try:
                    v2, u2, w2 = latest_measurement_for_sensor(s, int(sid), wide_from, date_to)
                except requests.HTTPError:
                    v2 = u2 = w2 = None
                if v2 is not None and w2 is not None:
                    val, unit, when = v2, u2, w2

            if val is None or when is None:
                try:
                    lv, lu, lw = latest_for_location(s, int(loc_id))
                except requests.HTTPError:
                    lv = lu = lw = None
                if lv is not None and lw is not None:
                    val, unit, when = lv, lu, lw

            if val is not None:
                if not unit:
                    unit = (
                        sen.get("_unit_hint")
                        or (sen.get("parameter") or {}).get("preferredUnit")
                        or (sen.get("parameter") or {}).get("unit")
                        or ("µg/m³" if (sen.get("parameter_id") == 2) else None)
                    )
                rows.append((sid, loc_id, loc_name, lat, lon, val, unit, when))
            time.sleep(sleep_ms / 1000.0)

    return rows


# --------------- BigQuery load + MERGE -----------------------
def load_rows(
    client: bigquery.Client,
    dataset: str,
    target_table: str,
    location: str,
    rows: List[Tuple],
):
    """
    rows: (sensor_id, location_id, location_name, lat, lon, value, unit, when_iso)
    Upload NDJSON -> staging (autodetect) -> MERGE -> target (snake_case).
    """
    if not rows:
        print("No OpenAQ rows fetched for the requested sweep. Exiting gracefully.", flush=True)
        return

    # 1) upload NDJSON to staging
    staging = f"_stg_openaq_{int(time.time())}"
    staging_fq = f"{client.project}.{dataset}.{staging}"

    payload = []
    for sid, loc_id, loc_name, lat, lon, val, unit, when in rows:
        payload.append(
            {
                "SensorId": sid,
                "LocationId": loc_id,
                "LocationName": loc_name,
                "Lat": lat,
                "Lon": lon,
                "Value": val,
                "Unit": unit,
                "DateTimeUTC": when,
            }
        )
    buf = io.BytesIO(("\n".join(json.dumps(x, ensure_ascii=False) for x in payload)).encode("utf-8"))

    load_job = client.load_table_from_file(
        file_obj=buf,
        destination=staging_fq,
        job_config=bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            autodetect=True,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        ),
        location=location,
    )
    load_job.result()
    print(f"[BQ] Loaded {load_job.output_rows} rows to {staging_fq}", flush=True)

    # 2) ensure target exists (snake_case), partitioned by date(datetime_utc)
    target_fq = f"{client.project}.{dataset}.{target_table}"
    ensure_sql = f"""
    CREATE TABLE IF NOT EXISTS `{target_fq}` (
      sensor_id     INT64,
      location_id   INT64,
      location_name STRING,
      latitude      FLOAT64,
      longitude     FLOAT64,
      value         FLOAT64,
      unit          STRING,
      datetime_utc  TIMESTAMP
    )
    PARTITION BY DATE(datetime_utc)
    CLUSTER BY location_id, sensor_id;
    """
    client.query(ensure_sql, location=location).result()

    # 3) MERGE from staging (PascalCase -> snake_case)
    merge_sql = f"""
    MERGE `{target_fq}` T
    USING (
      SELECT
        CAST(SensorId     AS INT64)   AS sensor_id,
        CAST(LocationId   AS INT64)   AS location_id,
        CAST(LocationName AS STRING)  AS location_name,
        CAST(Lat          AS FLOAT64) AS latitude,
        CAST(Lon          AS FLOAT64) AS longitude,
        CAST(Value        AS FLOAT64) AS value,
        CAST(Unit         AS STRING)  AS unit,
        TIMESTAMP(DateTimeUTC)        AS datetime_utc
      FROM `{staging_fq}`
      WHERE DateTimeUTC IS NOT NULL
    ) S
    ON  T.sensor_id    = S.sensor_id
    AND T.location_id  = S.location_id
    AND T.datetime_utc = S.datetime_utc
    WHEN MATCHED THEN
      UPDATE SET
        location_name = S.location_name,
        latitude      = S.latitude,
        longitude     = S.longitude,
        value         = S.value,
        unit          = S.unit
    WHEN NOT MATCHED THEN
      INSERT (sensor_id, location_id, location_name, latitude, longitude, value, unit, datetime_utc)
      VALUES (S.sensor_id, S.location_id, S.location_name, S.latitude, S.longitude, S.value, S.unit, S.datetime_utc);
    """
    client.query(merge_sql, location=location).result()
    print(f"[BQ] MERGE into {target_fq} completed.", flush=True)


# --------------- main ----------------------------------------
def parse_bboxes(env_str: Optional[str]) -> List[str]:
    if not env_str:
        return [
            "-118.6681,33.7037,-117.6462,34.3373",  # LA
            "-122.5136,37.7080,-122.3569,37.8324",  # SF
            "-117.282,32.615,-116.908,33.023",      # SD
        ]
    parts = [p.strip() for p in env_str.split("|") if p.strip()]
    return parts or []


def env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except Exception:
        return default


def env_bool(name: str, default: bool = False) -> bool:
    v = (os.getenv(name) or "").strip().lower()
    if not v:
        return default
    return v in ("1", "true", "t", "yes", "y", "on")


def main():
    project = os.getenv("GOOGLE_CLOUD_PROJECT") or os.getenv("PROJECT_ID")
    dataset = os.getenv("BQ_DATASET", "silver")
    table = os.getenv("BQ_TABLE", "openaq_pm25_test")
    location = os.getenv("BQ_LOCATION", "europe-north2")

    api_key = os.getenv("OPENAQ_API_KEY", "")
    if not api_key:
        print("Missing OPENAQ_API_KEY (use Secret Manager binding).", file=sys.stderr)
        sys.exit(2)

    bboxes = parse_bboxes(os.getenv("BBOXES"))
    per_bbox = env_int("PER_BBOX", 20)
    hours = env_int("HOURS", 720)              # 30 days
    days_fallback = env_int("DAYS_FALLBACK", 3650)  # ~10 years
    sleep_ms = env_int("SLEEP_MS", 250)
    debug = env_bool("DEBUG", False)

    if not project:
        print("Missing GOOGLE_CLOUD_PROJECT.", file=sys.stderr)
        sys.exit(2)

    rows = collect_rows(
        bboxes=bboxes,
        per_bbox=per_bbox,
        hours=hours,
        days_fallback=days_fallback,
        sleep_ms=sleep_ms,
        api_key=api_key,
        debug=debug,
    )

    if not rows:
        print("No OpenAQ rows fetched for the requested sweep. Exiting gracefully.", flush=True)
        sys.exit(0)

    client = bigquery.Client(project=project)
    load_rows(client, dataset, table, location, rows)


if __name__ == "__main__":
    main()
