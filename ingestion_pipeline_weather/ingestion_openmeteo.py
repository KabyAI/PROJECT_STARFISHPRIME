"""Open-Meteo ingestion job with gap-aware monthly chunking and BigQuery MERGE."""

from __future__ import annotations

import dataclasses
import hashlib
import json
import os
import random
import sys
import time
import uuid
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Dict, Iterable, List, Sequence, Tuple

import pandas as pd
import pytz
import requests
from google.api_core.exceptions import NotFound
from google.cloud import bigquery

BASE_URL = "https://archive-api.open-meteo.com/v1/archive"
DEFAULT_POINTS = [
    (37.34, -121.89, "sj"),
    (38.58, -121.49, "sac"),
    (32.72, -117.16, "sd"),
    (34.05, -118.24, "la"),
    (37.77, -122.42, "sf"),
    (36.74, -119.78, "fresno"),
]
DAILY_METRICS = "temperature_2m_max,temperature_2m_min,temperature_2m_mean"

PROJECT = os.getenv("GOOGLE_CLOUD_PROJECT") or os.getenv("PROJECT_ID")

BQ_DATASET = os.getenv("BQ_DATASET", "raw")
BQ_TABLE_POINTS = os.getenv("BQ_TABLE_POINTS", "openmeteo_daily_ca")
BQ_LOCATION = os.getenv("BQ_LOCATION", "europe-north2")
POINTS_RAW = os.getenv("POINTS", "")
REGION_LABEL = os.getenv("REGION_LABEL", "CA")
START_DATE_S = os.getenv("START_DATE", "2022-01-01").strip()
END_DATE_S = os.getenv("END_DATE", "").strip()
TZ_NAME = os.getenv("TIMEZONE", "America/Los_Angeles")
REQUEST_SLEEP_SECONDS = float(os.getenv("REQUEST_SLEEP_SECONDS", "0.6"))
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "5"))
CACHE_TTL_SECONDS = int(os.getenv("CACHE_TTL_SECONDS", str(6 * 3600)))
CACHE_DIR = Path(os.getenv("CACHE_DIR", "/tmp/openmeteo-cache"))
IMAGE_TAG = os.getenv("IMAGE_TAG", "unknown")
COMMIT_SHA = os.getenv("COMMIT_SHA", "unknown")


def log(severity: str, message: str, **kwargs: object) -> None:
    entry = {
        "severity": severity,
        "message": message,
        "timestamp": datetime.now(UTC).isoformat(),
        "image_tag": IMAGE_TAG,
        "commit_sha": COMMIT_SHA,
        **kwargs,
    }
    print(json.dumps(entry, default=str))


@dataclass(frozen=True)
class Point:
    lat: float
    lon: float
    point_id: str
    region_label: str


def parse_points(raw: str) -> List[Point]:
    """Parse POINTS env string into Point objects."""

    if not raw:
        return [Point(lat, lon, pid, REGION_LABEL) for lat, lon, pid in DEFAULT_POINTS]

    chunks = [chunk.strip() for chunk in raw.split(";") if chunk.strip()]
    points: List[Point] = []

    for idx, chunk in enumerate(chunks):
        parts = [p.strip() for p in chunk.split(",")]
        if len(parts) < 2:
            log("ERROR", "Invalid POINTS chunk", chunk=chunk)
            continue

        try:
            lat = float(parts[0])
            lon = float(parts[1])
        except ValueError:
            log("ERROR", "Unable to parse lat/lon", chunk=chunk)
            continue

        point_id = parts[2] if len(parts) >= 3 and parts[2] else (
            DEFAULT_POINTS[idx][2] if idx < len(DEFAULT_POINTS) else f"p{idx+1}"
        )
        region = parts[3] if len(parts) >= 4 and parts[3] else REGION_LABEL
        points.append(Point(lat=lat, lon=lon, point_id=point_id, region_label=region))

    if not points:
        return [Point(lat, lon, pid, REGION_LABEL) for lat, lon, pid in DEFAULT_POINTS]

    return points


def resolve_date_range(start_s: str, end_s: str | None = None) -> Tuple[date, date]:
    """Return inclusive start/end dates, allowing optional END_DATE override."""
    """Return inclusive start/end dates, defaulting END_DATE to today."""
    if not start_s:
        raise ValueError("START_DATE must be provided")

    try:
        start_date = date.fromisoformat(start_s)
    except ValueError as exc:
        raise ValueError(f"Invalid START_DATE: {start_s}") from exc

    if end_s:
        try:
            end_date = date.fromisoformat(end_s)
        except ValueError as exc:
            raise ValueError(f"Invalid END_DATE: {end_s}") from exc
    else:
        end_date = date.today()

    if end_date < start_date:
        raise ValueError("END_DATE cannot be before START_DATE")

    return start_date, end_date


def month_windows(start: date, end: date, tz_name: str) -> List[Tuple[date, date]]:
    tz = pytz.timezone(tz_name)
    windows: List[Tuple[date, date]] = []

    # Normalize start/end to timezone-aware dates to respect DST boundaries
    current = tz.localize(datetime.combine(start, datetime.min.time())).date()
    last = tz.localize(datetime.combine(end, datetime.min.time())).date()

    while current <= last:
        next_month = (current.replace(day=1) + timedelta(days=32)).replace(day=1)
        chunk_end = min(last, next_month - timedelta(days=1))
        windows.append((current, chunk_end))
        current = next_month

    return windows


def contiguous_ranges(dates: Sequence[date]) -> List[Tuple[date, date]]:
    if not dates:
        return []

    sorted_dates = sorted(dates)
    ranges: List[Tuple[date, date]] = []
    start = prev = sorted_dates[0]

    for current in sorted_dates[1:]:
        if current == prev + timedelta(days=1):
            prev = current
            continue
        ranges.append((start, prev))
        start = prev = current

    ranges.append((start, prev))
    return ranges


class ResponseCache:
    def __init__(self, directory: Path, ttl_seconds: int) -> None:
        self.directory = directory
        self.ttl = ttl_seconds
        self.directory.mkdir(parents=True, exist_ok=True)

    def _path(self, key: str) -> Path:
        digest = hashlib.sha256(key.encode("utf-8")).hexdigest()
        return self.directory / f"{digest}.json"

    def get(self, key: str) -> Dict[str, object] | None:
        path = self._path(key)
        if not path.exists():
            return None
        if time.time() - path.stat().st_mtime > self.ttl:
            return None
        try:
            with path.open("r", encoding="utf-8") as handle:
                return json.load(handle)
        except json.JSONDecodeError:
            return None

    def set(self, key: str, value: Dict[str, object]) -> None:
        path = self._path(key)
        tmp = path.with_suffix(".tmp")
        with tmp.open("w", encoding="utf-8") as handle:
            json.dump(value, handle)
        tmp.replace(path)


class OpenMeteoFetcher:
    def __init__(self, session: requests.Session, cache: ResponseCache | None = None) -> None:
        self.session = session
        self.cache = cache

    def fetch(self, point: Point, start: date, end: date, timezone: str) -> Dict[str, object] | None:
        params = {
            "latitude": point.lat,
            "longitude": point.lon,
            "start_date": start.isoformat(),
            "end_date": end.isoformat(),
            "timezone": timezone,
            "daily": DAILY_METRICS,
        }
        cache_key = json.dumps(
            {
                "point": dataclasses.asdict(point),
                "start": params["start_date"],
                "end": params["end_date"],
            },
            sort_keys=True,
        )

        if self.cache:
            cached = self.cache.get(cache_key)
            if cached:
                return cached

        retries = 0
        while True:
            try:
                response = self.session.get(BASE_URL, params=params, timeout=90)
                if response.status_code in {400, 429}:
                    raise requests.HTTPError(response.text, response=response)
                response.raise_for_status()
                payload = response.json()
                if self.cache:
                    self.cache.set(cache_key, payload)
                time.sleep(max(0.0, REQUEST_SLEEP_SECONDS))
                return payload
            except requests.HTTPError as exc:
                retries += 1
                status = exc.response.status_code if exc.response else None
                if retries > MAX_RETRIES:
                    log(
                        "ERROR",
                        "API fetch failed after max retries",
                        point=point.point_id,
                        start=start,
                        end=end,
                        status=status,
                        error_body=exc.response.text if exc.response else "No response body",
                    )
                    return None
                backoff = min(8.0, (2 ** retries) + random.random())
                log(
                    "WARNING",
                    "Retrying request",
                    point=point.point_id,
                    start=start,
                    end=end,
                    attempt=retries,
                    status=status,
                )
                time.sleep(backoff)
            except requests.RequestException as exc:
                retries += 1
                if retries > MAX_RETRIES:
                    log(
                        "ERROR",
                        "Request error",
                        point=point.point_id,
                        start=start,
                        end=end,
                        error=str(exc),
                    )
                    return None
                backoff = min(8.0, (2 ** retries) + random.random())
                log(
                    "WARNING",
                    "Retrying request",
                    point=point.point_id,
                    start=start,
                    end=end,
                    attempt=retries,
                )
                time.sleep(backoff)


def dataframe_from_payload(point: Point, payload: Dict[str, object]) -> pd.DataFrame:
    daily = payload.get("daily") or {}
    times = daily.get("time") or []
    if not times:
        return pd.DataFrame()

    parsed_times = pd.to_datetime(times)
    df = pd.DataFrame(
        {
            "date": parsed_times.date,
            "temp_max": daily.get("temperature_2m_max"),
            "temp_min": daily.get("temperature_2m_min"),
            "temp_mean": daily.get("temperature_2m_mean"),
        }
    )
    df["point_id"] = point.point_id
    df["region_label"] = point.region_label
    df["lat"] = point.lat
    df["lon"] = point.lon
    df["source_url"] = BASE_URL
    return df


def ensure_table(client: bigquery.Client, project: str, dataset: str, table: str) -> None:
    table_id = f"{project}.{dataset}.{table}"
    schema = [
        bigquery.SchemaField("date", "DATE", mode="REQUIRED"),
        bigquery.SchemaField("region_label", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("point_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("lat", "FLOAT64", mode="REQUIRED"),
        bigquery.SchemaField("lon", "FLOAT64", mode="REQUIRED"),
        bigquery.SchemaField("temp_min", "FLOAT64"),
        bigquery.SchemaField("temp_max", "FLOAT64"),
        bigquery.SchemaField("temp_mean", "FLOAT64"),
        bigquery.SchemaField(
            "inserted_at",
            "TIMESTAMP",
            mode="REQUIRED",
            default_value_expression="CURRENT_TIMESTAMP()",
        ),
        bigquery.SchemaField("source_url", "STRING"),
        bigquery.SchemaField("chunk_start", "DATE"),
        bigquery.SchemaField("chunk_end", "DATE"),
    ]

    try:
        existing = client.get_table(table_id)
        updates: List[str] = []
        existing_fields = {field.name: field for field in existing.schema}
        for field in schema:
            current = existing_fields.get(field.name)
            if current is None:
                existing.schema = list(existing.schema) + [field]
                updates.append("schema")
            elif current.field_type != field.field_type or current.mode != field.mode:
                log(
                    "ERROR",
                    "Existing table has incompatible field",
                    table=table_id,
                    field=field.name,
                    expected_type=field.field_type,
                    current_type=current.field_type,
                )
                raise SystemExit(1)

        expected_partition = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="date",
        )
        if not existing.time_partitioning or existing.time_partitioning.field != "date":
            existing.time_partitioning = expected_partition
            updates.append("time_partitioning")

        expected_clusters = ["point_id"]
        if tuple(existing.clustering_fields or []) != tuple(expected_clusters):
            existing.clustering_fields = expected_clusters
            updates.append("clustering_fields")

        if updates:
            client.update_table(existing, list(set(updates)))
            log("INFO", "Table updated", table=table_id, updates=list(set(updates)))
        else:
            log("INFO", "Table exists with expected schema", table=table_id)
    except NotFound:
        table_ref = bigquery.Table(table_id, schema=schema)
        table_ref.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="date",
        )
        table_ref.clustering_fields = ["point_id"]
        client.create_table(table_ref)
        log("INFO", "Created table", table=table_id)


def existing_dates(client: bigquery.Client, table_id: str, point_id: str, start: date, end: date) -> set[date]:
    query = f"""
        SELECT date
        FROM `{table_id}`
        WHERE point_id = @point_id AND date BETWEEN @start AND @end
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("point_id", "STRING", point_id),
            bigquery.ScalarQueryParameter("start", "DATE", start.isoformat()),
            bigquery.ScalarQueryParameter("end", "DATE", end.isoformat()),
        ]
    )
    result = client.query(query, job_config=job_config, location=BQ_LOCATION).result()
    return {row["date"] for row in result}


def table_row_count(client: bigquery.Client, table_id: str) -> int:
    query = f"SELECT COUNT(*) AS row_count FROM `{table_id}`"
    result = client.query(query, location=BQ_LOCATION).result()
    row = next(iter(result), None)
    return int(row["row_count"]) if row else 0


def missing_date_ranges(
    client: bigquery.Client,
    table_id: str,
    point_id: str,
    start: date,
    end: date,
) -> List[Tuple[date, date]]:
    total_days = (end - start).days + 1
    if total_days <= 0:
        return []

    existing = existing_dates(client, table_id, point_id, start, end)
    if len(existing) == total_days:
        return []

    missing = [
        start + timedelta(days=i)
        for i in range(total_days)
        if (start + timedelta(days=i)) not in existing
    ]
    return contiguous_ranges(missing)


def upsert_rows(
    client: bigquery.Client,
    project: str,
    dataset: str,
    table: str,
    dataframe: pd.DataFrame,
) -> int:
    if dataframe.empty:
        return 0

    table_id = f"{project}.{dataset}.{table}"
    staging_id = f"{project}.{dataset}._openmeteo_stage_{uuid.uuid4().hex}"

    load_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
        schema=[
            bigquery.SchemaField("date", "DATE"),
            bigquery.SchemaField("region_label", "STRING"),
            bigquery.SchemaField("point_id", "STRING"),
            bigquery.SchemaField("lat", "FLOAT64"),
            bigquery.SchemaField("lon", "FLOAT64"),
            bigquery.SchemaField("temp_min", "FLOAT64"),
            bigquery.SchemaField("temp_max", "FLOAT64"),
            bigquery.SchemaField("temp_mean", "FLOAT64"),
            bigquery.SchemaField("source_url", "STRING"),
            bigquery.SchemaField("chunk_start", "DATE"),
            bigquery.SchemaField("chunk_end", "DATE"),
        ],
    )

    load_job = client.load_table_from_dataframe(
        dataframe,
        staging_id,
        job_config=load_config,
        location=BQ_LOCATION,
    )
    load_job.result()

    merge_sql = f"""
    MERGE `{table_id}` T
    USING `{staging_id}` S
    ON T.date = S.date AND T.point_id = S.point_id
    WHEN MATCHED THEN UPDATE SET
      region_label = S.region_label,
      lat = S.lat,
      lon = S.lon,
      temp_min = S.temp_min,
      temp_max = S.temp_max,
      temp_mean = S.temp_mean,
      source_url = S.source_url,
      chunk_start = S.chunk_start,
      chunk_end = S.chunk_end,
      inserted_at = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN INSERT (
      date, region_label, point_id, lat, lon, temp_min, temp_max, temp_mean,
      inserted_at, source_url, chunk_start, chunk_end
    ) VALUES (
      S.date, S.region_label, S.point_id, S.lat, S.lon, S.temp_min, S.temp_max,
      S.temp_mean, CURRENT_TIMESTAMP(), S.source_url, S.chunk_start, S.chunk_end
    )
    """

    merge_job = client.query(merge_sql, location=BQ_LOCATION)
    merge_job.result()
    affected = merge_job.num_dml_affected_rows or 0

    client.delete_table(staging_id, not_found_ok=True)
    return int(affected)


def summarize_missing(missed: List[Tuple[Point, date, date]]) -> None:
    if not missed:
        log("INFO", "All requested windows ingested")
        return

    summary = [
        {
            "point_id": point.point_id,
            "start": start.isoformat(),
            "end": end.isoformat(),
        }
        for point, start, end in missed
    ]
    log("WARNING", "Some windows failed to ingest", missing_windows=summary)


def run_ingestion() -> None:
    if not PROJECT:
        log("CRITICAL", "GOOGLE_CLOUD_PROJECT (or PROJECT_ID) must be set")
        raise SystemExit(1)

    log(
        "INFO",
        "Starting Open-Meteo ingestion",
        project=PROJECT,
        dataset=BQ_DATASET,
        table=BQ_TABLE_POINTS,
        location=BQ_LOCATION,
    )

    client = bigquery.Client(project=PROJECT, location=BQ_LOCATION)
    ensure_table(client, PROJECT, BQ_DATASET, BQ_TABLE_POINTS)

    points = parse_points(POINTS_RAW)
    if not points:
        log("ERROR", "No valid points configured")
        raise SystemExit(1)

    try:
        start_date, end_date = resolve_date_range(START_DATE_S, END_DATE_S or None)
    except ValueError as exc:
        log(
            "ERROR",
            str(exc),
            start_date=START_DATE_S or None,
            end_date=END_DATE_S or None,
        )
        raise SystemExit(1)

    log(
        "INFO",
        "Resolved ingestion range",
        start_date=start_date.isoformat(),
        end_date=end_date.isoformat(),
        start_week=start_date.isocalendar().week,
        end_week=end_date.isocalendar().week,
        total_days=(end_date - start_date).days + 1,
    )

    windows = month_windows(start_date, end_date, TZ_NAME)

    cache = ResponseCache(CACHE_DIR, CACHE_TTL_SECONDS)
    session = requests.Session()
    fetcher = OpenMeteoFetcher(session, cache=cache)

    total_rows = 0
    missed_windows: List[Tuple[Point, date, date]] = []
    table_id = f"{PROJECT}.{BQ_DATASET}.{BQ_TABLE_POINTS}"

    for point in points:
        point_frames: List[pd.DataFrame] = []
        for window_start, window_end in windows:
            missing_ranges = missing_date_ranges(client, table_id, point.point_id, window_start, window_end)
            if not missing_ranges:
                continue

            log(
                "INFO",
                "Processing window",
                point_id=point.point_id,
                start=window_start,
                end=window_end,
                missing_ranges=[
                    {"start": s.isoformat(), "end": e.isoformat()} for s, e in missing_ranges
                ],
            )

            for chunk_start, chunk_end in missing_ranges:
                payload = fetcher.fetch(point, chunk_start, chunk_end, TZ_NAME)
                if payload is None:
                    missed_windows.append((point, chunk_start, chunk_end))
                    continue

                df = dataframe_from_payload(point, payload)
                if df.empty:
                    log(
                        "WARNING",
                        "Empty payload returned",
                        point_id=point.point_id,
                        start=chunk_start,
                        end=chunk_end,
                    )
                    continue

                df["chunk_start"] = chunk_start
                df["chunk_end"] = chunk_end
                point_frames.append(df)

        if point_frames:
            combined = pd.concat(point_frames, ignore_index=True)
            rows_written = upsert_rows(client, PROJECT, BQ_DATASET, BQ_TABLE_POINTS, combined)
            total_rows += len(combined)
            log(
                "INFO",
                "Point ingestion completed",
                point_id=point.point_id,
                rows_loaded=len(combined),
                rows_affected=rows_written,
            )
        else:
            log("INFO", "No missing data for point", point_id=point.point_id)

    summarize_missing(missed_windows)
    log(
        "INFO",
        "Ingestion finished",
        total_rows=total_rows,
        points=len(points),
    )

    try:
        row_count = table_row_count(client, table_id)
        log("INFO", "BigQuery table row count", table=table_id, row_count=row_count)
    except Exception as exc:  # pragma: no cover - best effort logging
        log("WARNING", "Failed to fetch table row count", table=table_id, error=str(exc))


def main(argv: Iterable[str] | None = None) -> None:
    try:
        run_ingestion()
    except Exception as exc:
        log("CRITICAL", "Unhandled exception in ingestion", error=str(exc))
        raise


if __name__ == "__main__":
    main()

