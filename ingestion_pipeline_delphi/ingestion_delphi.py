"""Delphi FluView ingestion job with gap-aware weekly MERGE into BigQuery."""

from __future__ import annotations

import hashlib
import json
import os
import random
import time
import uuid
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Dict, Iterable, List, Sequence, Tuple

import pandas as pd
import requests
from google.api_core.exceptions import NotFound
from google.cloud import bigquery

BASE_URL = "https://api.delphi.cmu.edu/epidata/fluview/"
DEFAULT_REGIONS = ["ca", "state:ca"]

PROJECT = os.getenv("GOOGLE_CLOUD_PROJECT") or os.getenv("PROJECT_ID")

BQ_DATASET = os.getenv("BQ_DATASET", "raw")
BQ_TABLE = os.getenv("BQ_TABLE", "fluview_ca_weekly")
BQ_LOCATION = os.getenv("BQ_LOCATION", "europe-north2")
REGIONS_RAW = os.getenv("REGIONS", ",".join(DEFAULT_REGIONS))
START_DATE_S = os.getenv("START_DATE", "2022-01-01").strip()
END_DATE_S = os.getenv("END_DATE", "").strip()
REQUEST_SLEEP_SECONDS = float(os.getenv("REQUEST_SLEEP_SECONDS", "0.3"))
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "5"))
CACHE_TTL_SECONDS = int(os.getenv("CACHE_TTL_SECONDS", str(3 * 3600)))
CACHE_DIR = Path(os.getenv("CACHE_DIR", "/tmp/fluview-cache"))
MAX_WEEKS_PER_CHUNK = int(os.getenv("MAX_WEEKS_PER_CHUNK", "26"))
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


def parse_regions(raw: str) -> List[str]:
    regions = [part.strip().lower() for part in raw.split(",") if part.strip()]
    return regions or DEFAULT_REGIONS


def align_to_monday(value: date) -> date:
    return value - timedelta(days=value.weekday())


def resolve_date_range(start_s: str, end_s: str | None = None) -> Tuple[date, date]:
    """Return inclusive dates aligned to Mondays, allowing optional END_DATE override."""

    if not start_s:
        raise ValueError("START_DATE must be provided")

    try:
        start_date = align_to_monday(date.fromisoformat(start_s))
    except ValueError as exc:
        raise ValueError(f"Invalid START_DATE: {start_s}") from exc

    if end_s:
        try:
            end_date = align_to_monday(date.fromisoformat(end_s))
        except ValueError as exc:
            raise ValueError(f"Invalid END_DATE: {end_s}") from exc
    else:
        end_date = align_to_monday(date.today())

    if end_date < start_date:
        raise ValueError("END_DATE cannot be before START_DATE")

    return start_date, end_date


def to_epiweek(dt: date) -> int:
    return int(dt.strftime("%G%V"))


def epiweek_to_date(epiweek: int) -> date:
    year = epiweek // 100
    week = epiweek % 100
    return datetime.fromisocalendar(year, week, 1).date()


def advance_epiweek(epiweek: int, weeks: int = 1) -> int:
    return to_epiweek(epiweek_to_date(epiweek) + timedelta(weeks=weeks))


def generate_epiweek_numbers(start_epiweek: int, end_epiweek: int) -> Iterable[int]:
    current = start_epiweek
    while True:
        yield current
        if current == end_epiweek:
            break
        current = advance_epiweek(current, 1)


def contiguous_epiweek_ranges(epiweeks: Sequence[int]) -> List[Tuple[int, int]]:
    if not epiweeks:
        return []

    sorted_weeks = sorted(epiweeks, key=lambda ew: epiweek_to_date(ew))
    ranges: List[Tuple[int, int]] = []
    start = prev = sorted_weeks[0]

    for current in sorted_weeks[1:]:
        if current == advance_epiweek(prev, 1):
            prev = current
            continue
        ranges.append((start, prev))
        start = prev = current

    ranges.append((start, prev))
    return ranges


def chunk_epiweek_range(start_epiweek: int, end_epiweek: int, max_size: int) -> List[Tuple[int, int]]:
    if max_size <= 0:
        raise ValueError("max_size must be positive")

    ranges: List[Tuple[int, int]] = []
    current_start = start_epiweek
    end_date = epiweek_to_date(end_epiweek)

    while True:
        current_start_date = epiweek_to_date(current_start)
        chunk_end_date = current_start_date + timedelta(weeks=max_size - 1)
        if chunk_end_date > end_date:
            chunk_end = end_epiweek
        else:
            chunk_end = to_epiweek(chunk_end_date)
            if epiweek_to_date(chunk_end) > end_date:
                chunk_end = end_epiweek

        ranges.append((current_start, chunk_end))

        if chunk_end == end_epiweek:
            break
        current_start = advance_epiweek(chunk_end, 1)

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


class FluviewFetcher:
    def __init__(self, session: requests.Session, cache: ResponseCache | None = None) -> None:
        self.session = session
        self.cache = cache

    def fetch(self, region: str, start_epiweek: int, end_epiweek: int) -> Dict[str, object] | None:
        params = {
            "regions": region,
            "epiweeks": f"{start_epiweek}-{end_epiweek}",
        }
        cache_key = json.dumps({
            "region": region,
            "start": start_epiweek,
            "end": end_epiweek,
        }, sort_keys=True)

        if self.cache:
            cached = self.cache.get(cache_key)
            if cached:
                return cached

        retries = 0
        while True:
            try:
                response = self.session.get(BASE_URL, params=params, timeout=90)
                if response.status_code in {400, 429, 500, 502, 503, 504}:
                    raise requests.HTTPError(response.text, response=response)
                response.raise_for_status()
                payload = response.json()
                if payload.get("result") != 1:
                    log(
                        "ERROR",
                        "Delphi API returned error payload",
                        region=region,
                        start_epiweek=start_epiweek,
                        end_epiweek=end_epiweek,
                        message=payload.get("message"),
                    )
                    return None
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
                        "Fluview fetch failed after retries",
                        region=region,
                        start_epiweek=start_epiweek,
                        end_epiweek=end_epiweek,
                        status=status,
                    )
                    return None
                backoff = min(8.0, (2 ** retries) + random.random())
                log(
                    "WARNING",
                    "Retrying Fluview request",
                    region=region,
                    start_epiweek=start_epiweek,
                    end_epiweek=end_epiweek,
                    attempt=retries,
                    status=status,
                )
                time.sleep(backoff)
            except requests.RequestException as exc:
                retries += 1
                if retries > MAX_RETRIES:
                    log(
                        "ERROR",
                        "Fluview request error",
                        region=region,
                        start_epiweek=start_epiweek,
                        end_epiweek=end_epiweek,
                        error=str(exc),
                    )
                    return None
                backoff = min(8.0, (2 ** retries) + random.random())
                log(
                    "WARNING",
                    "Retrying Fluview request",
                    region=region,
                    start_epiweek=start_epiweek,
                    end_epiweek=end_epiweek,
                    attempt=retries,
                )
                time.sleep(backoff)


def dataframe_from_payload(region: str, payload: Dict[str, object], chunk_start: int, chunk_end: int) -> pd.DataFrame:
    records = payload.get("epidata") or []
    if not records:
        return pd.DataFrame()

    df = pd.DataFrame(records)
    df["region"] = region

    expected_columns = [
        "region",
        "epiweek",
        "wili",
        "ili",
        "num_ili",
        "num_patients",
        "release_date",
    ]
    for column in expected_columns:
        if column not in df.columns:
            df[column] = None

    df = df[expected_columns]
    df["epiweek"] = pd.to_numeric(df["epiweek"], errors="coerce").astype("Int64")
    df = df.dropna(subset=["region", "epiweek"])

    for column in ("wili", "ili"):
        df[column] = pd.to_numeric(df[column], errors="coerce")

    for column in ("num_ili", "num_patients"):
        df[column] = pd.to_numeric(df[column], errors="coerce").astype("Int64")

    df["release_date"] = pd.to_datetime(df["release_date"], errors="coerce", utc=True)

    df = df[(df["epiweek"] >= chunk_start) & (df["epiweek"] <= chunk_end)]
    if df.empty:
        return df

    week_starts = [epiweek_to_date(int(ew)) for ew in df["epiweek"].tolist()]
    df["week_start"] = week_starts
    df["source_url"] = BASE_URL
    df["chunk_start"] = chunk_start
    df["chunk_end"] = chunk_end
    df.reset_index(drop=True, inplace=True)
    return df


def table_schema() -> List[bigquery.SchemaField]:
    return [
        bigquery.SchemaField("region", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("epiweek", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("week_start", "DATE", mode="REQUIRED"),
        bigquery.SchemaField("wili", "FLOAT64"),
        bigquery.SchemaField("ili", "FLOAT64"),
        bigquery.SchemaField("num_ili", "INT64"),
        bigquery.SchemaField("num_patients", "INT64"),
        bigquery.SchemaField("release_date", "TIMESTAMP"),
        bigquery.SchemaField("source_url", "STRING"),
        bigquery.SchemaField("chunk_start", "INT64"),
        bigquery.SchemaField("chunk_end", "INT64"),
        bigquery.SchemaField(
            "inserted_at",
            "TIMESTAMP",
            mode="REQUIRED",
            default_value_expression="CURRENT_TIMESTAMP()",
        ),
    ]


def ensure_table(client: bigquery.Client, project: str, dataset: str, table: str) -> str:
    table_id = f"{project}.{dataset}.{table}"
    schema = table_schema()

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
            field="week_start",
        )
        if not existing.time_partitioning or existing.time_partitioning.field != "week_start":
            existing.time_partitioning = expected_partition
            updates.append("time_partitioning")

        expected_clusters = ["region", "epiweek"]
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
            field="week_start",
        )
        table_ref.clustering_fields = ["region", "epiweek"]
        client.create_table(table_ref)
        log("INFO", "Table created", table=table_id)

    return table_id


def table_row_count(client: bigquery.Client, table_id: str) -> int:
    query = f"SELECT COUNT(*) AS row_count FROM `{table_id}`"
    result = client.query(query, location=BQ_LOCATION).result()
    row = next(iter(result), None)
    return int(row["row_count"]) if row else 0


def merge_upsert(client: bigquery.Client, staging_id: str, table_id: str) -> None:
    sql = f"""
    MERGE `{table_id}` T
    USING `{staging_id}` S
    ON  T.region = S.region AND T.epiweek = S.epiweek
    WHEN MATCHED THEN UPDATE SET
      week_start = S.week_start,
      wili = S.wili,
      ili = S.ili,
      num_ili = S.num_ili,
      num_patients = S.num_patients,
      release_date = S.release_date,
      source_url = S.source_url,
      chunk_start = S.chunk_start,
      chunk_end = S.chunk_end,
      inserted_at = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN INSERT (
      region,
      epiweek,
      week_start,
      wili,
      ili,
      num_ili,
      num_patients,
      release_date,
      source_url,
      chunk_start,
      chunk_end,
      inserted_at
    ) VALUES (
      S.region,
      S.epiweek,
      S.week_start,
      S.wili,
      S.ili,
      S.num_ili,
      S.num_patients,
      S.release_date,
      S.source_url,
      S.chunk_start,
      S.chunk_end,
      CURRENT_TIMESTAMP()
    );
    """
    client.query(sql, location=BQ_LOCATION).result()


def existing_epiweeks(
    client: bigquery.Client,
    table_id: str,
    region: str,
    start_epiweek: int,
    end_epiweek: int,
) -> set[int]:
    query = f"""
        SELECT DISTINCT epiweek
        FROM `{table_id}`
        WHERE region = @region AND epiweek BETWEEN @start AND @end
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("region", "STRING", region),
            bigquery.ScalarQueryParameter("start", "INT64", start_epiweek),
            bigquery.ScalarQueryParameter("end", "INT64", end_epiweek),
        ]
    )
    result = client.query(query, job_config=job_config, location=BQ_LOCATION).result()
    return {int(row["epiweek"]) for row in result}


def missing_epiweek_chunks(
    existing: set[int],
    start_epiweek: int,
    end_epiweek: int,
    max_size: int,
) -> List[Tuple[int, int]]:
    all_weeks = list(generate_epiweek_numbers(start_epiweek, end_epiweek))
    missing = [week for week in all_weeks if week not in existing]
    if not missing:
        return []

    ranges = contiguous_epiweek_ranges(missing)
    chunks: List[Tuple[int, int]] = []
    for start, end in ranges:
        chunks.extend(chunk_epiweek_range(start, end, max_size))
    return chunks


def upsert_rows(client: bigquery.Client, df: pd.DataFrame, table_id: str) -> int:
    if df.empty:
        return 0

    staging_table = f"{PROJECT}.{BQ_DATASET}._fluview_stage_{uuid.uuid4().hex}"
    load_df = df.copy()
    load_df["epiweek"] = load_df["epiweek"].astype("int64")
    load_df["chunk_start"] = load_df["chunk_start"].astype("int64")
    load_df["chunk_end"] = load_df["chunk_end"].astype("int64")
    load_df["week_start"] = pd.to_datetime(load_df["week_start"]).dt.date

    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
        schema=[
            bigquery.SchemaField("region", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("epiweek", "INT64", mode="REQUIRED"),
            bigquery.SchemaField("week_start", "DATE", mode="REQUIRED"),
            bigquery.SchemaField("wili", "FLOAT64"),
            bigquery.SchemaField("ili", "FLOAT64"),
            bigquery.SchemaField("num_ili", "INT64"),
            bigquery.SchemaField("num_patients", "INT64"),
            bigquery.SchemaField("release_date", "TIMESTAMP"),
            bigquery.SchemaField("source_url", "STRING"),
            bigquery.SchemaField("chunk_start", "INT64"),
            bigquery.SchemaField("chunk_end", "INT64"),
        ],
    )

    try:
        load_job = client.load_table_from_dataframe(
            load_df,
            staging_table,
            job_config=job_config,
            location=BQ_LOCATION,
        )
        load_job.result()
        merge_upsert(client, staging_table, table_id)
    finally:
        client.delete_table(staging_table, not_found_ok=True)

    return int(len(load_df))


def ingest_region(
    client: bigquery.Client,
    fetcher: FluviewFetcher,
    region: str,
    start_epiweek: int,
    end_epiweek: int,
    table_id: str,
) -> int:
    existing = existing_epiweeks(client, table_id, region, start_epiweek, end_epiweek)
    chunks = missing_epiweek_chunks(existing, start_epiweek, end_epiweek, MAX_WEEKS_PER_CHUNK)

    if not chunks:
        log(
            "INFO",
            "Region already up to date",
            region=region,
            start_epiweek=start_epiweek,
            end_epiweek=end_epiweek,
        )
        return 0

    frames: List[pd.DataFrame] = []
    for chunk_start, chunk_end in chunks:
        log(
            "INFO",
            "Fetching chunk",
            region=region,
            chunk_start=chunk_start,
            chunk_end=chunk_end,
        )
        payload = fetcher.fetch(region, chunk_start, chunk_end)
        if payload is None:
            log(
                "ERROR",
                "Failed to fetch chunk",
                region=region,
                chunk_start=chunk_start,
                chunk_end=chunk_end,
            )
            continue

        frame = dataframe_from_payload(region, payload, chunk_start, chunk_end)
        if frame.empty:
            log(
                "WARNING",
                "Chunk returned no rows",
                region=region,
                chunk_start=chunk_start,
                chunk_end=chunk_end,
            )
            continue

        frames.append(frame)

    if not frames:
        log(
            "WARNING",
            "No data fetched for region",
            region=region,
            start_epiweek=start_epiweek,
            end_epiweek=end_epiweek,
        )
        return 0

    combined = pd.concat(frames, ignore_index=True)
    combined.sort_values(["region", "epiweek", "release_date"], inplace=True)
    combined.drop_duplicates(subset=["region", "epiweek"], keep="last", inplace=True)
    combined.reset_index(drop=True, inplace=True)

    rows = upsert_rows(client, combined, table_id)
    log(
        "INFO",
        "Region upsert complete",
        region=region,
        rows=rows,
        chunks=len(chunks),
        start_epiweek=start_epiweek,
        end_epiweek=end_epiweek,
    )
    return rows


def run_ingestion() -> int:
    if not PROJECT:
        raise SystemExit("GOOGLE_CLOUD_PROJECT must be set")

    regions = parse_regions(REGIONS_RAW)
    start_date, end_date = resolve_date_range(START_DATE_S, END_DATE_S or None)
    start_epiweek = to_epiweek(start_date)
    end_epiweek = to_epiweek(end_date)

    client = bigquery.Client(project=PROJECT, location=BQ_LOCATION)
    table_id = ensure_table(client, PROJECT, BQ_DATASET, BQ_TABLE)

    cache = ResponseCache(CACHE_DIR, CACHE_TTL_SECONDS) if CACHE_TTL_SECONDS > 0 else None
    fetcher = FluviewFetcher(requests.Session(), cache)

    total_rows = 0
    for region in regions:
        total_rows += ingest_region(client, fetcher, region, start_epiweek, end_epiweek, table_id)

    log(
        "INFO",
        "Ingestion finished",
        total_rows=total_rows,
        regions=regions,
        start_epiweek=start_epiweek,
        end_epiweek=end_epiweek,
    )

    try:
        row_count = table_row_count(client, table_id)
        log("INFO", "BigQuery table row count", table=table_id, row_count=row_count)
    except Exception as exc:  # pragma: no cover - best effort logging
        log("WARNING", "Failed to fetch table row count", table=table_id, error=str(exc))
    return total_rows


def main() -> None:
    run_ingestion()


if __name__ == "__main__":
    main()

