#!/usr/bin/env python3
import argparse
import datetime as dt
import os
import sys
import time
from typing import Iterable, List, Tuple, Optional, Dict, Any
from dotenv import load_dotenv
load_dotenv()
import requests

OPENAQ_BASE = "https://api.openaq.org/v3"


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


def list_locations_in_bbox(s: requests.Session, bbox: str, limit=1000) -> List[dict]:
    params = {
        "bbox": bbox,
        "parameters": "pm25",  # Filter for PM2.5 locations
        "limit": limit,
        "page": 1,
    }
    r = get_with_backoff(s, f"{OPENAQ_BASE}/locations", params=params)
    data = r.json()
    return data.get("results", [])


def latest_measurement_for_location(
    s: requests.Session, location_id: int
) -> Tuple[Optional[float], Optional[str], Optional[str]]:
    url = f"{OPENAQ_BASE}/locations/{location_id}/latest"
    r = get_with_backoff(s, url)
    data = r.json()
    results = data.get("results", [])
    if not results:
        return None, None, None
    for m in results[0].get("measurements", []):
        if m.get("parameter") == "pm25":
            value = m.get("value")
            unit = m.get("unit")
            when = m.get("lastUpdated") or m.get("last_updated") or m.get("date", {}).get("utc")
            return value, unit, when
    return None, None, None


def run_smoketest(
    bboxes: Iterable[str], per_bbox: int, sleep_ms: int, api_key: str, debug: bool = False
) -> List[Tuple]:
    headers = {"X-API-Key": api_key} if api_key else {}
    s = requests.Session()
    s.headers.update(headers)

    rows: List[Tuple] = []

    for bbox in bboxes:
        print(f"Finding PM2.5 locations in bbox: {bbox}", flush=True)
        try:
            locs = list_locations_in_bbox(s, bbox, limit=per_bbox * 3)
        except requests.HTTPError as e:
            print(f"  ERROR listing locations for bbox {bbox}: {e}", flush=True)
            continue

        filtered = []
        for L in locs:
            name = str(L.get("name", "")).lower()
            if any(x in name for x in ("ebam", "temporary", "decommissioned", "pilot", "test", "unit", "mammoth", "gbuapcd")):
                continue
            filtered.append(L)
            if len(filtered) >= per_bbox:
                break

        for loc in filtered:
            loc_id = loc["id"]
            loc_name = loc.get("name", "")
            coords = loc.get("coordinates") or {}
            lat = coords.get("latitude")
            lon = coords.get("longitude")

            try:
                val, unit, when = latest_measurement_for_location(s, loc_id)
            except requests.HTTPError as e:
                print(f"  WARN latest measurement for location {loc_id}: {e}", flush=True)
                continue

            if val is not None:
                rows.append((loc_id, loc_name, lat, lon, val, unit, when))
            else:
                if debug:
                    print(f"  DEBUG no latest PM2.5 measurement for location {loc_id} ({loc_name})", flush=True)

            time.sleep(sleep_ms / 1000.0)

    return rows


def main():
    p = argparse.ArgumentParser(description="OpenAQ v3 local smoketest for PM2.5 (CA metros).")
    p.add_argument("--api-key", default=None, help="OpenAQ API key. If omitted, uses OPENAQ_API_KEY env.")
    p.add_argument("--per-bbox", type=int, default=10, help="Max locations per bbox to query.")
    p.add_argument("--sleep-ms", type=int, default=250, help="Delay between location calls (ms).")
    p.add_argument("--debug", action="store_true", help="Print debug info.")
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

    rows = run_smoketest(bboxes, args.per_bbox, args.sleep_ms, api_key, debug=args.debug)
    if not rows:
        print("No PM2.5 rows returned (rate-limit or no recent data). Try re-running.")
        return

    rows.sort(key=lambda r: r[6] or "", reverse=True)
    print("\nLocationId  LocationName                       Lat         Lon         Value  Unit      DateTimeUTC")
    print("-" * 100)
    for loc_id, loc_name, lat, lon, val, unit, when in rows:
        print(f"{loc_id:<11} {str(loc_name)[:30]:<30} {str(lat or ''):<11} {str(lon or ''):<11} {val:<6} {str(unit or ''):<9} {when or ''}")


if __name__ == "__main__":
    main()
