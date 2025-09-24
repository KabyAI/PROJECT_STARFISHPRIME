import os, sys, time, math
from datetime import datetime, timedelta, date
from typing import List, Tuple
import pandas as pd
import requests
from google.cloud import bigquery

PROJECT = os.getenv("GOOGLE_CLOUD_PROJECT") or os.getenv("PROJECT_ID")
if not PROJECT:
    print("FATAL: GOOGLE_CLOUD_PROJECT (or PROJECT_ID) is not set.", file=sys.stderr)
    sys.exit(1)

BQ_DATASET     = os.getenv("BQ_DATASET", "silver")
BQ_TABLE_POINTS= os.getenv("BQ_TABLE_POINTS", "weather_points_daily")
BQ_LOCATION    = os.getenv("BQ_LOCATION", "europe-north2")
POINTS         = os.getenv("POINTS", "")
DAYS_BACK      = int(os.getenv("DAYS_BACK", "31"))
START_DATE_S   = os.getenv("START_DATE", "").strip()
TZ             = os.getenv("TIMEZONE", "America/Los_Angeles")

BASE = "https://api.open-meteo.com/v1/forecast"

def parse_points(s: str) -> List[Tuple[float,float,str]]:
    pts = []
    labels = ["sj","sac","sd","la","sf","fresno"]
    for i, part in enumerate([p for p in s.split(";") if p.strip()]):
        lat_s, lon_s = part.split(",")
        pts.append((float(lat_s), float(lon_s), labels[i] if i < len(labels) else f"p{i+1}"))
    return pts

def daterange(start: date, end: date) -> List[str]:
    d = start
    out = []
    while d <= end:
        out.append(d.isoformat())
        d += timedelta(days=1)
    return out

def fetch_daily(lat: float, lon: float, tz: str, d0: date, d1: date) -> pd.DataFrame:
    params = {
        "latitude": lat, "longitude": lon,
        "timezone": tz,
        "daily": ["temperature_2m_mean","temperature_2m_max","temperature_2m_min"],
        "start_date": d0.isoformat(),
        "end_date": d1.isoformat(),
    }
    r = requests.get(BASE, params=params, timeout=60)
    r.raise_for_status()
    j = r.json()
    if "daily" not in j:
        return pd.DataFrame()
    daily = j["daily"]
    df = pd.DataFrame({
        "date": pd.to_datetime(daily["time"]).dt.date,
        "temp_mean": daily.get("temperature_2m_mean"),
        "temp_max": daily.get("temperature_2m_max"),
        "temp_min": daily.get("temperature_2m_min"),
    })
    df["lat"] = lat
    df["lon"] = lon
    return df

def ensure_points_table(client: bigquery.Client, table_id: str):
    try:
        client.get_table(table_id); return
    except Exception:
        pass
    schema = [
        bigquery.SchemaField("date", "DATE", mode="REQUIRED"),
        bigquery.SchemaField("region_label", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("point_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("lat", "FLOAT64"),
        bigquery.SchemaField("lon", "FLOAT64"),
        bigquery.SchemaField("temp_mean", "FLOAT64"),
        bigquery.SchemaField("temp_max", "FLOAT64"),
        bigquery.SchemaField("temp_min", "FLOAT64"),
        bigquery.SchemaField("inserted_at", "TIMESTAMP",
                             default_value_expression="CURRENT_TIMESTAMP()"),
    ]
    tbl = bigquery.Table(table_id, schema=schema)
    tbl.time_partitioning = bigquery.TimePartitioning(field="date")
    client.create_table(tbl)
    print(f"Created {table_id}")

def upsert_points(client: bigquery.Client, df: pd.DataFrame, table_id: str):
    stg = f"{table_id.replace('.', '._stg_')}_{int(time.time())}"
    job = client.load_table_from_dataframe(
        df,
        stg,
        job_config=bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE"),
        location=BQ_LOCATION,
    )
    job.result()

    sql = f"""
    MERGE `{table_id}` T
    USING `{stg}` S
    ON  T.date = S.date AND T.point_id = S.point_id
    WHEN MATCHED THEN UPDATE SET
      temp_mean = S.temp_mean,
      temp_max  = S.temp_max,
      temp_min  = S.temp_min,
      lat       = S.lat,
      lon       = S.lon,
      inserted_at = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN INSERT (
      date, region_label, point_id, lat, lon, temp_mean, temp_max, temp_min, inserted_at
    ) VALUES (
      S.date, S.region_label, S.point_id, S.lat, S.lon, S.temp_mean, S.temp_max, S.temp_min, CURRENT_TIMESTAMP()
    );
    """
    client.query(sql, location=BQ_LOCATION).result()
    client.delete_table(stg, not_found_ok=True)

def main():
    client = bigquery.Client(project=PROJECT, location=BQ_LOCATION)
    table_id = f"{PROJECT}.{BQ_DATASET}.{BQ_TABLE_POINTS}"
    ensure_points_table(client, table_id)

    pts = parse_points(POINTS)
    if not pts:
        print("No POINTS provided.", file=sys.stderr); sys.exit(1)

    today = date.today()
    if START_DATE_S:
        start_d = date.fromisoformat(START_DATE_S)
    else:
        start_d = today - timedelta(days=DAYS_BACK)
    end_d = today

    rows = []
    for lat, lon, pid in pts:
        df = fetch_daily(lat, lon, TZ, start_d, end_d)
        if df.empty: continue
        df["region_label"] = "ca"
        df["point_id"] = pid
        rows.append(df)

    if not rows:
        print("No weather rows fetched. Exiting."); return

    df_all = pd.concat(rows, ignore_index=True)
    # Keep raw per-point in silver
    upsert_points(client, df_all, table_id)
    print(f"Upserted {len(df_all)} weather point-days into {table_id}")

if __name__ == "__main__":
    main()
