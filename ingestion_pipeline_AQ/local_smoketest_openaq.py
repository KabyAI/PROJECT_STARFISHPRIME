#!/usr/bin/env python3
import argparse
import datetime as dt
import os
import sys
import time
from typing import Iterable, List, Tuple, Optional, Dict, Any

import requests

OPENAQ_BASE = "https://api.openaq.org/v3"

# ---------- retry helper (handles 429/5xx) ----------
def get_with_backoff(s: requests.Session, url: str, params=None, max_tries=6, base_delay=0.5):
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

# ---------- API helpers (v3) ----------
def list_locations_in_bbox(s: requests.Session, bbox: str, limit=1000) -> List[dict]:
    params = {
        "bbox": bbox,
        "parameters_id": 2,  # PM2.5
        "limit": limit,
        "page": 1,
    }
    r = get_with_backoff(s, f"{OPENAQ_BASE}/locations", params=params)
    data = r.json()
    return data.get("results", [])

def list_pm25_sensors_for_location(s: requests.Session, location_id: int, per_location_limit=50) -> List[dict]:
    url = f"{OPENAQ_BASE}/locations/{location_id}/sensors"
    params = {"limit": per_location_limit, "page": 1}
    r = get_with_backoff(s, url, params=params)
    sensors = r.json().get("results", [])

    out = []
    for sr in sensors:
        pid = (
            sr.get("parameter_id")
            or (sr.get("parameter") or {}).get("id")
            or sr.get("parameterId")
        )
        pcode = (
            (sr.get("parameter") or {}).get("code")
            or sr.get("parameterCode")
            or ""
        )
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
    """
    Coalesce a UTC timestamp from a measurement dict.

    Priority:
      1) datetime.utc (if present)
      2) period.datetimeTo.utc  (end of interval)
         fallback: period.datetimeFrom.utc
      3) common flat fields
      4) date.utc
      5) period.to / period.end
    """
    if not isinstance(m, dict):
        return None

    # 1) canonical v3 pattern: datetime object with utc/local, or a string
    dt_obj = m.get("datetime")
    if isinstance(dt_obj, dict):
        utc = dt_obj.get("utc") or dt_obj.get("UTC")
        if isinstance(utc, str) and utc:
            return utc
    elif isinstance(dt_obj, str) and dt_obj:
        return dt_obj

    # 2) period.*.utc (your debug output shows this shape)
    period = m.get("period")
    if isinstance(period, dict):
        # prefer the end of the interval
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

    # 3) other flat fields seen in the wild
    for k in ("datetime_utc", "date_utc", "observed_at", "observedAt", "lastUpdated", "last_updated", "time", "timestamp"):
        v = m.get(k)
        if isinstance(v, str) and v:
            return v

    # 4) nested "date" object (older harmonized shape)
    date_obj = m.get("date")
    if isinstance(date_obj, dict):
        for k in ("utc", "UTC", "iso"):
            v = date_obj.get(k)
            if isinstance(v, str) and v:
                return v

    # 5) some feeds return period.to / period.end as strings
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
    # prefer unit on measurement; else we'll fallback later
    unit = m.get("unit") or (m.get("parameter") or {}).get("units")
    when = _pick_datetime(m)
    return value, unit, when, m

# ---------- main smoketest flow ----------
def run_smoketest(
    bboxes: Iterable[str],
    per_bbox: int,
    hours: int,
    sleep_ms: int,
    api_key: str,
    debug: bool = False,
) -> List[Tuple]:
    headers = {"X-API-Key": api_key} if api_key else {}
    s = requests.Session()
    s.headers.update(headers)

    # timezone-aware UTC (no deprecation warnings)
    to_ = dt.datetime.now(dt.UTC).replace(microsecond=0)
    frm = to_ - dt.timedelta(hours=hours)
    date_from = frm.isoformat().replace("+00:00", "Z")
    date_to = to_.isoformat().replace("+00:00", "Z")

    rows: List[Tuple] = []

    for bbox in bboxes:
        print(f"Finding PM2.5 locations in bbox: {bbox}", flush=True)
        try:
            locs = list_locations_in_bbox(s, bbox)
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

        # gather up to per_bbox sensors
        picked = []
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

        # pull the latest measurement per picked sensor
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

            # robust unit fallback for PM2.5
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
                rows.append((sid, loc_id, loc_name, lat, lon, val, unit, when))
            time.sleep(sleep_ms / 1000.0)

    return rows

def main():
    p = argparse.ArgumentParser(description="OpenAQ v3 local smoketest for PM2.5 (CA metros).")
    p.add_argument("--api-key", default=None, help="OpenAQ API key. If omitted, uses OPENAQ_API_KEY env.")
    p.add_argument("--per-bbox", type=int, default=10, help="Max sensors per bbox to query.")
    p.add_argument("--hours", type=int, default=48, help="Lookback window in hours.")
    p.add_argument("--sleep-ms", type=int, default=250, help="Delay between sensor calls (ms).")
    p.add_argument("--debug", action="store_true", help="Print one raw measurement if datetime is missing.")
    args = p.parse_args()

    api_key = args.api_key or os.getenv("OPENAQ_API_KEY") or ""
    if not api_key:
        print("No API key provided. Use --api-key or set OPENAQ_API_KEY.", file=sys.stderr)
        sys.exit(2)

    bboxes = [
        "-118.6681,33.7037,-117.6462,34.3373",  # LA
        "-122.5136,37.7080,-122.3569,37.8324",  # SF
        "-117.282,32.615,-116.908,33.023",      # SD
    ]

    rows = run_smoketest(bboxes, args.per_bbox, args.hours, args.sleep_ms, api_key, debug=args.debug)
    if not rows:
        print("No PM2.5 rows returned (rate-limit or no recent data). Try re-running.")
        return

    rows.sort(key=lambda r: (r[7] or ""), reverse=True)
    print("\nSensorId  LocationId  LocationName                         Lat        Lon        Value  Unit         DateTimeUTC")
    print("-" * 120)
    for sid, loc_id, loc_name, lat, lon, val, unit, when in rows:
        print(f"{sid:<8}  {loc_id:<10}  {str(loc_name)[:33]:<33}  "
              f"{str(lat or ''):<10} {str(lon or ''):<10}  {val:<6}  {str(unit or ''):<11}  {when or ''}")

if __name__ == "__main__":
    main()
