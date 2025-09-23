import os, sys, time, json, math
from datetime import date, timedelta, datetime
import requests
import pandas as pd
from google.cloud import bigquery

PROJECT   = os.environ["GOOGLE_CLOUD_PROJECT"]
BQ_DATASET= os.environ.get("BQ_DATASET","silver")
BQ_TABLE  = os.environ.get("BQ_TABLE","weather_ca_daily")
BQ_LOC    = os.environ.get("BQ_LOCATION","europe-north2")
TIMEZONE  = os.environ.get("TIMEZONE","America/Los_Angeles")

POINTS    = os.environ.get("POINTS","34.05,-118.24").split(";")
DAYS_BACK = int(os.environ.get("DAYS_BACK","31"))
START_DATE= os.environ.get("START_DATE","").strip()

API_URL   = "https://api.open-meteo.com/v1/forecast"
DAILY_VARS= "temperature_2m_max,temperature_2m_min,temperature_2m_mean"

def daterange(start: date, end: date):
    for n in range(int((end - start).days) + 1):
        yield start + timedelta(n)

def fetch_point(lat, lon, start_d: date, end_d: date):
    params = {
        "latitude": lat, "longitude": lon,
        "daily": DAILY_VARS,
        "timezone": TIMEZONE,
        "start_date": start_d.isoformat(),
        "end_date": end_d.isoformat(),
    }
    r = requests.get(API_URL, params=params, timeout=30)
    r.raise_for_status()
    j = r.json()
    d = j.get("daily", {})
    df = pd.DataFrame(d)
    # expected columns: time, temperature_2m_max, temperature_2m_min, temperature_2m_mean
    if df.empty: return df
    df.rename(columns={"time":"date"}, inplace=True)
    df["date"] = pd.to_datetime(df["date"]).dt.date
    df["lat"] = float(lat); df["lon"] = float(lon)
    return df

def ensure_table(client: bigquery.Client, table_id: str):
    from google.cloud.bigquery import SchemaField, TimePartitioning
    try:
        client.get_table(table_id)
        return
    except Exception:
        schema = [
            bigquery.SchemaField("date","DATE",mode="REQUIRED"),
            bigquery.SchemaField("region","STRING",mode="REQUIRED"),
            bigquery.SchemaField("temp_mean","FLOAT"),
            bigquery.SchemaField("temp_max","FLOAT"),
            bigquery.SchemaField("temp_min","FLOAT"),
            bigquery.SchemaField("inserted_at","TIMESTAMP"),
        ]
        table = bigquery.Table(table_id, schema=schema)
        table.time_partitioning = bigquery.TimePartitioning(field="date")
        client.create_table(table)
        print(f"Created {table_id}", file=sys.stderr)

def upsert(client: bigquery.Client, df: pd.DataFrame, table_id: str):
    stg = f"{table_id}__stg"
    job = client.load_table_from_dataframe(df, stg, job_config=bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE"))
    job.result()
    sql = f"""
    MERGE `{table_id}` T
    USING (
      SELECT DATE(date) AS date, 'ca' AS region,
             AVG(temperature_2m_mean) AS temp_mean,
             AVG(temperature_2m_max)  AS temp_max,
             AVG(temperature_2m_min)  AS temp_min,
             CURRENT_TIMESTAMP() AS inserted_at
      FROM `{stg}`
      GROUP BY date
    ) S
    ON T.date = S.date AND T.region = S.region
    WHEN MATCHED THEN UPDATE SET
      temp_mean=S.temp_mean, temp_max=S.temp_max, temp_min=S.temp_min, inserted_at=S.inserted_at
    WHEN NOT MATCHED THEN
      INSERT (date,region,temp_mean,temp_max,temp_min,inserted_at)
      VALUES (S.date,S.region,S.temp_mean,S.temp_max,S.temp_min,S.inserted_at)
    """
    client.query(sql, location=BQ_LOC).result()
    client.delete_table(stg, not_found_ok=True)

def main():
    client = bigquery.Client(project=PROJECT)
    table_id = f"{PROJECT}.{BQ_DATASET}.{BQ_TABLE}"
    ensure_table(client, table_id)

    today = date.today()
    if START_DATE:
        start_d = date.fromisoformat(START_DATE)
    else:
        start_d = today - timedelta(days=DAYS_BACK)
    end_d = today

    all_rows = []
    for p in POINTS:
        lat, lon = p.split(",")
        df = fetch_point(lat.strip(), lon.strip(), start_d, end_d)
        if not df.empty: all_rows.append(df)
        time.sleep(0.2)  # be nice

    if not all_rows:
        print("No data fetched", file=sys.stderr); return

    df_all = pd.concat(all_rows).reset_index(drop=True)
    upsert(client, df_all, table_id)
    print(f"Upserted {len(df_all)} rows into {table_id}")

if __name__ == "__main__":
    main()
