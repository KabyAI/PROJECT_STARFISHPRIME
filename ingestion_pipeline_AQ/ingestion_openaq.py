#!/usr/bin/env python3
"""
OpenAQ v3 → BigQuery (multi-bbox PM2.5 “latest” sweep with robust datetime extraction)

Reads env:
  GOOGLE_CLOUD_PROJECT   (required)
  BQ_DATASET             (default: silver)
  BQ_TABLE               (default: openaq_pm25_test)
  BQ_LOCATION            (default: europe-north2)
  OPENAQ_API_KEY         (required; provide via Secret)
  BBOXES                 (e.g. "-118.66,33.70,-117.64,34.33|-122.51,37.71,-122.36,37.83|-117.282,32.615,-116.908,33.023")
  PER_BBOX               (default: 10)
  HOURS                  (default: 48)     # main window for /measurements
  DAYS_FALLBACK          (default: 3650)   # /days fallback lookback in days (set large; we sort desc+limit=1)
  SLEEP_MS               (default: 250)
  DEBUG                  ("1" to enable extra prints)

Behavior:
  For each bbox:
    1) /locations?bbox=…&parameters_id=2
    2) /locations/{id}/sensors (filter parameter=pm25)
    3) For up to PER_BBOX sensors:
         a) /sensors/{id}/measurements (date_from .. date_to, sort=desc, limit=1)
         b) if none: /sensors/{id}/days (date_from .. date_to in YYYY-MM-DD, sort=desc, limit=1)
         c) if none: /locations/{loc_id}/latest (pick pm25 measurement)
  Rows merged to BQ on (sensor_id, datetime_utc).
"""

from __future__ import annotations
import os
import sys
import time
import datetime as dt
from typing import Dict, Any, Iterable, List, Optional, Tuple

import requests
from google.cloud import bigquery

OPENAQ_BASE = "https://api.openaq.org/v3"


# ──────────────────────────── HTTP helper with backoff ─────────────────────────
def get_with_backoff(
    s: requests.Session,
    url: str,
    params: Optional[dict] = None,
    max_tries: int = 6,
    base_delay: float = 0.5,
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


# ───────────────────────────── OpenAQ API helpers ──────────────────────────────
def list_locations_in_bbox(
    s: requests.Session, bbox: str, limit: int = 1000, debug: bool = False
) -> List[dict]:
    params = {
        "bbox": bbox,
        "parameters_id": 2,  # PM2.5
        "limit": limit,
        "page": 1,
    }
    r = get_with_backoff(s, f"{OPENAQ_BASE}/locations", params=params)
    data = r.json()
    results = data.get("results", [])
    if debug:
        print(f"  locations found: {len(results)}", flush=True)
    return results


def list_pm25_sensors_for_location(
    s: requests.Session, location_id: int, per_location_limit: int = 50, debug: bool = False
) -> List[dict]:
    url = f"{OPENAQ_BASE}/locations/{location_id}/sensors"
    params = {"limit": per_location_limit, "page": 1}
    r = get_with_backoff(s, url, params=params)
    sensors = r.json().get("results", []) or []

    out = []
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

    if debug:
        print(f"    location {location_id}: sensors(pm25)={len(out)}", flush=True)
    return out


def _pick_datetime(m: Dict[str, Any]) -> Optional[str]:
    """
    Coalesce a UTC timestamp from a measurement dict.

    Priority:
      1) datetime.utc (if present) or datetime (string)
      2) period.datetimeTo.utc (end of interval), then period.datetimeFrom.utc
      3) known flat fields
      4) date.utc / date.iso
      5) period.to / period.end
    """
    if not isinstance(m, dict):
        return None

    # (1) datetime object/string
    dt_obj = m.get("datetime")
    if isinstance(dt_obj, dict):
        utc = dt_obj.get("utc") or dt_obj.get("UTC")
        if isinstance(utc, str) and utc:
            return utc
    elif isinstance(dt_obj, str) and dt_obj:
        return dt_obj

    # (2) period.*.utc
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

    # (3) flat fields seen in the wild
    for k in ("datetime_utc", "date_utc", "observed_at", "observedAt", "lastUpdated", "last_updated", "time", "timestamp"):
        v = m.get(k)
        if isinstance(v, str) and v:
            return v

    # (4) nested "date" object
    date_obj = m.get("date")
    if isinstance(date_obj, dict):
        for k in ("utc", "UTC", "iso"):
            v = date_obj.get(k)
            if isinstance(v, str) and v:
                return v

    # (5) period.to / period.end
    if isinstance(period, dict):
        v = period.get("to") or period.get("end")
        if isinstance(v, str) and v:
            return v

    return None


def latest_from_measurements(
    s: requests.Session, sensor_id: int, date_from_iso: str, date_to_iso: str
) -> Tuple[Optional[float], Optional[str], Optional[str], Optional[Dict[str, Any]]]:
    url = f"{OPENAQ_BASE}/sensors/{sensor_id}/measurements"
    params = {"date_from": date_from_iso, "date_to": date_to_iso, "limit": 1, "sort": "desc"}
    r = get_with_backoff(s, url, params=params)
    res = r.json().get("results", []) or []
    if not res:
        return None, None, None, None
    m = res[0]
    value = m.get("value")
    unit = m.get("unit") or (m.get("parameter") or {}).get("units")
    when = _pick_datetime(m)
    return value, unit, when, m


def latest_from_days(
    s: requests.Session, sensor_id: int, date_from_day: str, date_to_day: str
) -> Tuple[Optional[float], Optional[str], Optional[str], Optional[Dict[str, Any]]]:
    """Use /sensors/{id}/days as a fallback; take the most recent day."""
    url = f"{OPENAQ_BASE}/sensors/{sensor_id}/days"
    params = {"date_from": date_from_day, "date_to": date_to_day, "limit": 1, "sort": "desc"}
    r = get_with_backoff(s, url, params=params)
    res = r.json().get("results", []) or []
    if not res:
        return None, None, None, None
    m = res[0]
    value = m.get("value")
    unit = m.get("unit") or (m.get("parameter") or {}).get("units")
    when = _pick_datetime(m)
    # If only a date is present, normalize to midnight UTC
    if when and len(when) == 10 and when.count("-") == 2:
        when = f"{when}T00:00:00Z"
    return value, unit, when, m


def latest_from_location_latest(
    s: requests.Session, location_id: int
) -> Tuple[Optional[float], Optional[str], Optional[str], Optional[Dict[str, Any]]]:
    """Final fallback: /locations/{id}/latest → pick pm25 measurement."""
    url = f"{OPENAQ_BASE}/locations/{location_id}/latest"
    r = get_with_backoff(s, url)
    j = r.json()
    loc = None
    if isinstance(j.get("results"), list) and j["results"]:
        loc = j["results"][0]
    elif isinstance(j.get("results"), dict):
        loc = j["results"]
    if not loc:
        return None, None, None, None
    ms = loc.get("measurements") or []
    # choose a pm25 measurement
    pm25 = []
    for m in ms:
        pid = m.get("parameter_id") or (m.get("parameter") or {}).get("id") or m.get("parameterId")
        code = (m.get("parameter") or {}).get("code") or m.get("parameter") or m.get("parameterCode")
        if pid == 2 or (isinstance(code, str) and code.lower().replace(" ", "") in {"pm25", "pm2.5", "pm2_5"}):
            pm25.append(m)
    if not pm25:
        return None, None, None, None
    # pick the one with the latest datetime
    def dt_key(m: Dict[str, Any]):
        s = _pick_datetime(m)
        if not s:
            return dt.datetime.min.replace(tzinfo=dt.UTC)
        try:
            if s.endswith("Z"):
                return dt.datetime.fromisoformat(s.replace("Z", "+00:00"))
            return dt.datetime.fromisoformat(s).astimezone(dt.UTC)
        except Exception:
            return dt.datetime.min.replace(tzinfo=dt.UTC)

    m_best = max(pm25, key=dt_key)
    value = m_best.get("value")
    unit = m_best.get("unit") or (m_best.get("parameter") or {}).get("units")
    when = _pick_datetime(m_best)
    return value, unit, when, m_best


# ─────────────────────────────────── BQ helpers ────────────────────────────────
def bq_upsert_rows(
    client: bigquery.Client,
    dataset: str,
    table: str,
    location: str,
    rows: List[Dict[str, Any]],
) -> None:
    """
    Staging table load + MERGE into dataset.table on (sensor_id, datetime_utc).
    Skips rows with null datetime_utc.
    """
    target = f"{client.project}.{dataset}.{table}"
    staging = f"{dataset}._stg_openaq_{int(time.time())}"

    # Ensure target table exists
    schema = [
        bigquery.SchemaField("sensor_id", "INT64"),
        bigquery.SchemaField("location_id", "INT64"),
        bigquery.SchemaField("location_name", "STRING"),
        bigquery.SchemaField("lat", "FLOAT64"),
        bigquery.SchemaField("lon", "FLOAT64"),
        bigquery.SchemaField("value", "FLOAT64"),
        bigquery.SchemaField("unit", "STRING"),
        bigquery.SchemaField("datetime_utc", "TIMESTAMP"),
        bigquery.SchemaField("fetched_at", "TIMESTAMP"),
        bigquery.SchemaField("source", "STRING"),
    ]
    try:
        client.get_table(target)
    except Exception:
        tbl = bigquery.Table(target, schema=schema)
        tbl = client.create_table(tbl)
        print(f"Created table {target}", flush=True)

    # Create staging and load
    stg_tbl = bigquery.Table(f"{client.project}.{staging}", schema=schema)
    stg_tbl = client.create_table(stg_tbl)
    # filter out rows without datetime_utc (MERGE key would be null)
    clean = [r for r in rows if r.get("datetime_utc")]
    if not clean:
        print("  No rows with datetime_utc; nothing to load.", flush=True)
        client.delete_table(staging, not_found_ok=True)
        return

    job = client.load_table_from_json(clean, stg_tbl, location=location)
    job.result()
    print(f"  Loaded {len(clean)} rows into {staging}", flush=True)

    # MERGE (sensor_id, datetime_utc) as key; update value/unit/location_name/coords
    merge_sql = f"""
    MERGE `{target}` T
    USING `{client.project}.{staging}` S
    ON T.sensor_id = S.sensor_id AND T.datetime_utc = S.datetime_utc
    WHEN MATCHED THEN UPDATE SET
      T.value = S.value,
      T.unit = S.unit,
      T.location_id = S.location_id,
      T.location_name = S.location_name,
      T.lat = S.lat,
      T.lon = S.lon,
      T.fetched_at = S.fetched_at,
      T.source = S.source
    WHEN NOT MATCHED THEN INSERT (
      sensor_id, location_id, location_name, lat, lon,
      value, unit, datetime_utc, fetched_at, source
    ) VALUES (
      S.sensor_id, S.location_id, S.location_name, S.lat, S.lon,
      S.value, S.unit, S.datetime_utc, S.fetched_at, S.source
    )
    """
    qjob = client.query(merge_sql, location=location)
    qjob.result()
    print(f"  MERGE complete into {target}", flush=True)

    client.delete_table(staging, not_found_ok=True)


# ───────────────────────────────────── Main flow ───────────────────────────────
def parse_env() -> Dict[str, Any]:
    project = os.getenv("GOOGLE_CLOUD_PROJECT")
    if not project:
        print("GOOGLE_CLOUD_PROJECT is required", file=sys.stderr)
        sys.exit(2)

    dataset = os.getenv("BQ_DATASET", "silver")
    table = os.getenv("BQ_TABLE", "openaq_pm25_test")
    location = os.getenv("BQ_LOCATION", "europe-north2")

    api_key = os.getenv("OPENAQ_API_KEY") or ""
    if not api_key:
        print("OPENAQ_API_KEY is required (secret).", file=sys.stderr)
        sys.exit(2)

    bboxes_env = os.getenv("BBOXES", "")
    if not bboxes_env:
        # default to LA/SF/SD
        bboxes_env = "-118.6681,33.7037,-117.6462,34.3373|-122.5136,37.7080,-122.3569,37.8324|-117.282,32.615,-116.908,33.023"
    bboxes = [b.strip() for b in bboxes_env.split("|") if b.strip()]

    per_bbox = int(os.getenv("PER_BBOX", "10"))
    hours = int(os.getenv("HOURS", "48"))
    days_fb = int(os.getenv("DAYS_FALLBACK", "3650"))
    sleep_ms = int(os.getenv("SLEEP_MS", "250"))
    debug = os.getenv("DEBUG", "").strip() in ("1", "true", "True", "YES", "yes")

    return dict(
        project=project,
        dataset=dataset,
        table=table,
        location=location,
        api_key=api_key,
        bboxes=bboxes,
        per_bbox=per_bbox,
        hours=hours,
        days_fb=days_fb,
        sleep_ms=sleep_ms,
        debug=debug,
    )


def run_sweep(cfg: Dict[str, Any]) -> List[Dict[str, Any]]:
    api_key = cfg["api_key"]
    headers = {"X-API-Key": api_key, "Accept": "application/json"}
    s = requests.Session()
    s.headers.update(headers)

    debug = cfg["debug"]
    per_bbox = cfg["per_bbox"]
    sleep_ms = cfg["sleep_ms"]

    # Main window (UTC)
    to_ = dt.datetime.now(dt.UTC).replace(microsecond=0)
    frm = to_ - dt.timedelta(hours=cfg["hours"])
    date_from = frm.isoformat().replace("+00:00", "Z")
    date_to = to_.isoformat().replace("+00:00", "Z")

    # Days fallback range (broad; we ask for latest via sort desc limit 1)
    df_from = (to_ - dt.timedelta(days=cfg["days_fb"])).date().isoformat()
    df_to = to_.date().isoformat()

    rows: List[Dict[str, Any]] = []
    picked_count_total = 0

    for bbox in cfg["bboxes"]:
        print(f"Finding PM2.5 locations in bbox: {bbox}", flush=True)
        try:
            locs = list_locations_in_bbox(s, bbox, debug=debug)
        except requests.HTTPError as e:
            print(f"  ERROR /locations for bbox {bbox}: {e}", flush=True)
            continue

        # filter out obvious temporary/test sites (loosened: removed "unit")
        filtered = []
        for L in locs:
            nm = str(L.get("name", "")).lower()
            if any(x in nm for x in ("ebam", "temporary", "decommissioned", "pilot", "test", "mammoth", "gbuapcd")):
                continue
            filtered.append(L)
        if debug:
            print(f"  after filter: {len(filtered)}", flush=True)

        picked: List[Dict[str, Any]] = []
        for L in filtered:
            if len(picked) >= per_bbox:
                break
            lid = L["id"]
            try:
                sensors = list_pm25_sensors_for_location(s, lid, debug=debug)
            except requests.HTTPError as e:
                print(f"  WARN /sensors for location {lid}: {e}", flush=True)
                continue

            for sen in sensors:
                if len(picked) >= per_bbox:
                    break
                sen["_location"] = L
                picked.append(sen)

        picked_count_total += len(picked)

        # For each picked sensor, do the 3-step cascade
        wrote = 0
        for sen in picked:
            sid = sen["id"]
            loc = sen["_location"]
            loc_id = loc["id"]
            loc_name = loc.get("name", "")
            coords = loc.get("coordinates") or {}
            lat = coords.get("latitude")
            lon = coords.get("longitude")

            if debug:
                print(f"      sensor {sid}: trying measurements {date_from}..{date_to}", flush=True)

            val = unit = when = None
            raw = None

            # (a) measurements
            try:
                val, unit, when, raw = latest_from_measurements(s, sid, date_from, date_to)
            except requests.HTTPError as e:
                print(f"  WARN /measurements sensor {sid}: {e}", flush=True)

            # (b) days fallback
            if val is None or when is None:
                try:
                    v2, u2, w2, raw2 = latest_from_days(s, sid, df_from, df_to)
                    if v2 is not None and w2:
                        val, unit, when, raw = v2, (unit or u2), w2, raw2
                except requests.HTTPError as e:
                    print(f"  WARN /days sensor {sid}: {e}", flush=True)

            # (c) locations/{id}/latest fallback
            if val is None or when is None:
                try:
                    v3, u3, w3, raw3 = latest_from_location_latest(s, loc_id)
                    if v3 is not None and w3:
                        val, unit, when, raw = v3, (unit or u3), w3, raw3
                except requests.HTTPError as e:
                    print(f"  WARN /locations/latest loc {loc_id}: {e}", flush=True)

            # Fill unit if still missing (PM2.5 default µg/m³)
            if not unit:
                unit = (
                    sen.get("_unit_hint")
                    or (sen.get("parameter") or {}).get("preferredUnit")
                    or (sen.get("parameter") or {}).get("unit")
                    or "µg/m³"
                )

            if val is None or when is None:
                if debug:
                    print(f"      sensor {sid}: no data in measurements/days/latest", flush=True)
                time.sleep(sleep_ms / 1000.0)
                continue

            # Prepare row
            rows.append(
                {
                    "sensor_id": int(sid),
                    "location_id": int(loc_id),
                    "location_name": str(loc_name),
                    "lat": float(lat) if lat is not None else None,
                    "lon": float(lon) if lon is not None else None,
                    "value": float(val),
                    "unit": str(unit) if unit is not None else None,
                    "datetime_utc": when,
                    "fetched_at": dt.datetime.now(dt.UTC).isoformat(),
                    "source": "openaq_v3",
                }
            )
            wrote += 1
            time.sleep(sleep_ms / 1000.0)

        print(f"  -> picked {len(picked)} sensors, wrote {wrote} rows from this bbox.", flush=True)

    print(f"TOTAL picked sensors: {picked_count_total}, total rows: {len(rows)}", flush=True)
    return rows


def main():
    cfg = parse_env()
    print(f"API key present: {bool(cfg['api_key'])}", flush=True)  # sanity (doesn't print the key)
    rows = run_sweep(cfg)

    if not rows:
        print("No OpenAQ rows fetched for the requested sweep. Exiting gracefully.", flush=True)
        sys.exit(0)

    client = bigquery.Client(project=cfg["project"])
    bq_upsert_rows(
        client=client,
        dataset=cfg["dataset"],
        table=cfg["table"],
        location=cfg["location"],
        rows=rows,
    )
    print("Done.", flush=True)


if __name__ == "__main__":
    main()
