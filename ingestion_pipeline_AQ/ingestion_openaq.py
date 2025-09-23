#!/usr/bin/env python3
# OpenAQ v3  -> BigQuery (silver)
# Robust multi-bbox sweep with 3 cascaded sources:
#  1) sensors/{id}/measurements (time-bounded by HOURS)
#  2) sensors/{id}/days (bounded by DAYS_FALLBACK)  <-- new
#  3) locations/{id}/latest                          <-- new
#
# Writes to TIMESTAMP(datetime_utc) in BigQuery.
#
# Env:
#   GOOGLE_CLOUD_PROJECT (required)
#   OPENAQ_API_KEY       (required)
#   BQ_DATASET           default "silver"
#   BQ_TABLE             default "openaq_pm25_test"
#   BQ_LOCATION          default "US" (e.g., "europe-north2")
#   BBOXES               default LA|SF|SD  (lonW,latS,lonE,latN|...)
#   PER_BBOX             default "10"
#   HOURS                default "48"
#   DAYS_FALLBACK        default "365"  -- how many days to look back in /days
#   SLEEP_MS             default "250"
#   DEBUG                default "0"
#
# Schema created if missing.

from __future__ import annotations
import os, sys, time, json
import datetime as dt
from typing import Any, Dict, List, Optional, Tuple

import requests
import pandas as pd
from google.cloud import bigquery

OPENAQ_BASE = "https://api.openaq.org/v3"

# ---------------- utils ----------------
def now_utc() -> dt.datetime:
    return dt.datetime.now(dt.UTC).replace(microsecond=0)

def to_iso_z(t: dt.datetime) -> str:
    return t.astimezone(dt.UTC).isoformat().replace("+00:00", "Z")

def parse_dt_maybe(s: Optional[str]) -> Optional[dt.datetime]:
    if not s or not isinstance(s, str):
        return None
    try:
        return dt.datetime.fromisoformat(s.replace("Z", "+00:00")).astimezone(dt.UTC)
    except Exception:
        return None

# ---------------- http with backoff -------------
def get_json(
    s: requests.Session,
    url: str,
    *,
    params: Optional[Dict[str, Any]] = None,
    max_tries: int = 6,
    base_delay: float = 0.6,
    debug: bool = False,
) -> Dict[str, Any]:
    delay = base_delay
    for i in range(1, max_tries + 1):
        r = s.get(url, params=params, timeout=30)
        if r.status_code in (429, 500, 502, 503, 504):
            if i == max_tries:
                r.raise_for_status()
            if debug:
                print(f"  RETRY {i}/{max_tries} {r.status_code} {url}", flush=True)
            time.sleep(delay)
            delay = min(10.0, round(delay * 1.8, 2))
            continue
        r.raise_for_status()
        return r.json()
    return {}

# -------------- openaq helpers -----------------
def list_locations_in_bbox(s: requests.Session, bbox: str, *, debug=False) -> List[dict]:
    params = {"bbox": bbox, "parameters_id": 2, "limit": 1000, "page": 1}
    return (get_json(s, f"{OPENAQ_BASE}/locations", params=params, debug=debug).get("results") or [])

def list_pm25_sensors_for_location(s: requests.Session, location_id: int, *, debug=False) -> List[dict]:
    res = get_json(s, f"{OPENAQ_BASE}/locations/{location_id}/sensors",
                   params={"limit": 100, "page": 1}, debug=debug).get("results") or []
    out = []
    for sr in res:
        pid = sr.get("parameter_id") or (sr.get("parameter") or {}).get("id")
        code = (sr.get("parameter") or {}).get("code", "")
        is_pm25 = (pid == 2) or (str(code).lower().replace(" ", "") in {"pm25","pm2.5","pm2_5"})
        if not is_pm25:
            continue
        unit_hint = sr.get("unit") or (sr.get("parameter") or {}).get("preferredUnit") or (sr.get("parameter") or {}).get("unit")
        if unit_hint: sr["_unit_hint"] = unit_hint
        out.append(sr)
    return out

def pick_datetime(m: Dict[str, Any]) -> Optional[str]:
    # 1) explicit datetime
    dtb = m.get("datetime")
    if isinstance(dtb, dict):
        u = dtb.get("utc") or dtb.get("UTC")
        if isinstance(u, str) and u: return u
    elif isinstance(dtb, str) and dtb:
        return dtb
    # 2) period.{datetimeTo|datetimeFrom}.utc
    per = m.get("period")
    if isinstance(per, dict):
        for key in ("datetimeTo", "datetimeFrom"):
            node = per.get(key)
            if isinstance(node, dict):
                u = node.get("utc") or node.get("UTC") or node.get("iso")
                if isinstance(u, str) and u:
                    return u
        for k in ("to","end"):
            v = per.get(k)
            if isinstance(v, str) and v:
                return v
    # 3) various flat keys
    for k in ("datetime_utc","date_utc","observed_at","observedAt","timestamp","time","lastUpdated","last_updated"):
        v = m.get(k)
        if isinstance(v, str) and v:
            return v
    # 4) nested date
    d = m.get("date")
    if isinstance(d, dict):
        for k in ("utc","UTC","iso"):
            v = d.get(k)
            if isinstance(v, str) and v:
                return v
    return None

def latest_measurement(s: requests.Session, sensor_id: int, date_from_iso: str, date_to_iso: str, *, debug=False):
    data = get_json(
        s, f"{OPENAQ_BASE}/sensors/{sensor_id}/measurements",
        params={"date_from": date_from_iso, "date_to": date_to_iso, "limit": 1, "sort": "desc"},
        debug=debug
    )
    res = data.get("results") or []
    if not res:
        return None, None, None, None
    m = res[0]
    return m.get("value"), m.get("unit"), pick_datetime(m), m

def latest_days_fallback(s: requests.Session, sensor_id: int, days_back: int, *, debug=False):
    # /sensors/{id}/days?date_from=YYYY-MM-DD&date_to=YYYY-MM-DD
    to_ = now_utc().date()
    frm = to_ - dt.timedelta(days=days_back)
    data = get_json(
        s, f"{OPENAQ_BASE}/sensors/{sensor_id}/days",
        params={"date_from": frm.isoformat(), "date_to": to_.isoformat(), "limit": 100, "page": 1},
        debug=debug
    )
    res = data.get("results") or []
    if not res:
        return None, None, None, None
    # pick newest row by date (API returns ascending sometimes)
    res.sort(key=lambda r: r.get("date") or r.get("day") or "", reverse=True)
    drow = res[0]
    value = drow.get("value") or drow.get("avg") or drow.get("mean") or drow.get("median")
    unit = drow.get("unit") or "µg/m³"
    day = drow.get("date") or drow.get("day")
    when = None
    if isinstance(day, str) and day:
        # synthesize end-of-day UTC
        try:
            when = dt.datetime.fromisoformat(day).date().strftime("%Y-%m-%d") + "T23:59:59Z"
        except Exception:
            pass
    return value, unit, when, drow

def latest_at_location(s: requests.Session, location_id: int, *, debug=False):
    data = get_json(s, f"{OPENAQ_BASE}/locations/{location_id}/latest", debug=debug)
    loc = (data.get("results") or [{}])[0]
    ms = loc.get("measurements") or []
    if not ms:
        return None, None, None, None
    def is_pm25(m):
        pid = m.get("parameter_id") or (m.get("parameter") or {}).get("id")
        code = (m.get("parameter") or {}).get("code")
        return pid == 2 or (isinstance(code, str) and code.lower().replace(" ", "") in {"pm25","pm2.5","pm2_5"})
    cand = [m for m in ms if is_pm25(m)] or [ms[0]]
    m = cand[0]
    value = m.get("value")
    unit = m.get("unit") or (m.get("parameter") or {}).get("preferredUnit") or (m.get("parameter") or {}).get("unit") or "µg/m³"
    when = pick_datetime(m)
    return value, unit, when, m

# --------------- bq --------------------
def ensure_table(client: bigquery.Client, table_ref: bigquery.TableReference):
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
        client.create_table(bigquery.Table(table_ref, schema=schema))

def load_rows(project_id: str, dataset: str, table: str, location: str, rows: List[Dict[str, Any]]) -> int:
    if not rows:
        return 0
    df = pd.DataFrame(rows)
    client = bigquery.Client(project=project_id, location=location or None)
    tref = bigquery.DatasetReference(project_id, dataset).table(table)
    ensure_table(client, tref)
    job = client.load_table_from_dataframe(df, tref, job_config=bigquery.LoadJobConfig(write_disposition="WRITE_APPEND"))
    job.result()
    return len(rows)

# -------------- main -------------------
def run() -> int:
    project = os.getenv("GOOGLE_CLOUD_PROJECT", "")
    api_key = os.getenv("OPENAQ_API_KEY", "")
    if not project or not api_key:
        print("Missing GOOGLE_CLOUD_PROJECT or OPENAQ_API_KEY.", file=sys.stderr)
        return 2

    dataset = os.getenv("BQ_DATASET", "silver")
    table = os.getenv("BQ_TABLE", "openaq_pm25_test")
    bq_loc = os.getenv("BQ_LOCATION", "US")

    default_bboxes = [
        "-118.6681,33.7037,-117.6462,34.3373",  # LA
        "-122.5136,37.7080,-122.3569,37.8324",  # SF
        "-117.282,32.615,-116.908,33.023",      # SD
    ]
    bboxes = [b.strip() for b in os.getenv("BBOXES", "|".join(default_bboxes)).split("|") if b.strip()]
    per_bbox = int(os.getenv("PER_BBOX", "10"))
    hours = int(os.getenv("HOURS", "48"))
    days_fallback = int(os.getenv("DAYS_FALLBACK", "365"))
    sleep_ms = int(os.getenv("SLEEP_MS", "250"))
    debug = os.getenv("DEBUG", "0") not in {"0","false","False"}

    s = requests.Session()
    s.headers.update({"X-API-Key": api_key, "Accept": "application/json", "User-Agent": "starfishprime-ingest/1.1"})

    to_ = now_utc()
    frm = to_ - dt.timedelta(hours=hours)
    date_from = to_iso_z(frm)
    date_to = to_iso_z(to_)

    all_rows: List[Dict[str, Any]] = []

    for bbox in bboxes:
        print(f"Finding PM2.5 locations in bbox: {bbox}", flush=True)
        locs = list_locations_in_bbox(s, bbox, debug=debug)

        # filter out noisy/temporary
        filtered = []
        for L in locs:
            nm = str(L.get("name","")).lower()
            if any(x in nm for x in ("ebam","temporary","decommissioned","pilot","test","unit","mammoth","gbuapcd")):
                continue
            filtered.append(L)

        picked: List[dict] = []
        for L in filtered:
            if len(picked) >= per_bbox: break
            lid = int(L.get("id"))
            sensors = list_pm25_sensors_for_location(s, lid, debug=debug)
            for sen in sensors:
                if len(picked) >= per_bbox: break
                sen["_location"] = L
                picked.append(sen)

        got = 0
        for sen in picked:
            sid = int(sen.get("id"))
            loc = sen.get("_location") or {}
            lid = int(loc.get("id"))
            lname = loc.get("name","")
            coords = loc.get("coordinates") or {}
            lat, lon = coords.get("latitude"), coords.get("longitude")

            # 1) measurements in the window
            val, unit, when_s, _raw = latest_measurement(s, sid, date_from, date_to, debug=debug)

            # 2) fallback: days
            if val is None:
                val, unit, when_s, _ = latest_days_fallback(s, sid, days_back=days_fallback, debug=debug)

            # 3) fallback: latest at location
            if val is None:
                val, unit, when_s, _ = latest_at_location(s, lid, debug=debug)

            if val is None:
                time.sleep(sleep_ms/1000.0)
                continue

            ts = parse_dt_maybe(when_s) or to_
            if not unit:
                unit = sen.get("_unit_hint") or (sen.get("parameter") or {}).get("preferredUnit") \
                       or (sen.get("parameter") or {}).get("unit") or "µg/m³"

            all_rows.append({
                "value": float(val) if isinstance(val,(int,float)) else None,
                "sensor_id": sid,
                "location_id": lid,
                "location_name": lname,
                "latitude": float(lat) if isinstance(lat,(int,float)) else None,
                "longitude": float(lon) if isinstance(lon,(int,float)) else None,
                "unit": unit,
                "datetime_utc": ts,
                "parameter": "pm25",
                "region": "ca",
                "country": "us",
                "bbox": bbox,
                "source": "openaq_v3",
                "ingested_at": now_utc(),
            })
            got += 1
            time.sleep(sleep_ms/1000.0)

        print(f"  -> picked {len(picked)} sensors, wrote {got} rows from this bbox.", flush=True)

    if not all_rows:
        print("No OpenAQ rows fetched for the requested sweep. Exiting gracefully.", flush=True)
        return 0

    all_rows.sort(key=lambda r: r["datetime_utc"] or dt.datetime.min.replace(tzinfo=dt.UTC), reverse=True)
    n = load_rows(project, dataset, table, bq_loc, all_rows)
    print(f"Wrote {n} rows to {project}.{dataset}.{table} ({bq_loc}).", flush=True)
    return 0

if __name__ == "__main__":
    try:
        sys.exit(run())
    except Exception as e:
        print(f"FATAL: {e}", file=sys.stderr, flush=True)
        import traceback; traceback.print_exc()
        sys.exit(1)
