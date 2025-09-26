"""Utility to ensure the `raw` BigQuery dataset and canonical ingestion tables exist.

Run locally or in CI before deploying ingestion jobs:

    python infrastructure/create_raw_tables.py \
        --project project-starfishprime-001 \
        --location europe-north2
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from typing import Iterable

from google.api_core.exceptions import BadRequest, NotFound
from google.cloud import bigquery

RAW_DATASET = "raw"
OPENMETEO_TABLE = "openmeteo_daily_ca"
FLUVIEW_TABLE = "fluview_ca_weekly"


def log(severity: str, message: str, **kwargs) -> None:
    entry = {
        "severity": severity,
        "message": message,
        "timestamp": datetime.utcnow().isoformat() + "Z",
        **kwargs,
    }
    print(json.dumps(entry))


def ensure_dataset(client: bigquery.Client, dataset_id: str, location: str) -> None:
    dataset_ref = bigquery.Dataset(f"{client.project}.{dataset_id}")
    dataset_ref.location = location
    try:
        dataset = client.get_dataset(dataset_ref)
        if dataset.location != location:
            log(
                "ERROR",
                "Dataset exists in different location",
                dataset=dataset.dataset_id,
                current_location=dataset.location,
                expected_location=location,
            )
            raise SystemExit(1)
        log(
            "INFO",
            "Dataset already exists",
            dataset=dataset_id,
            project=client.project,
            location=dataset.location,
        )
    except NotFound:
        client.create_dataset(dataset_ref)
        log(
            "INFO",
            "Created dataset",
            dataset=dataset_id,
            project=client.project,
            location=location,
        )


def _missing_fields(expected: list[bigquery.SchemaField], existing: list[bigquery.SchemaField]) -> list[bigquery.SchemaField]:
    existing_names = {field.name: field for field in existing}
    missing: list[bigquery.SchemaField] = []
    for field in expected:
        current = existing_names.get(field.name)
        if current is None:
            missing.append(field)
        elif (
            current.field_type != field.field_type
            or current.mode != field.mode
        ):
            log(
                "ERROR",
                "Existing field has incompatible type/mode",
                field=field.name,
                expected_type=field.field_type,
                expected_mode=field.mode,
                current_type=current.field_type,
                current_mode=current.mode,
            )
            raise SystemExit(1)
    return missing


def ensure_table(client: bigquery.Client, dataset_id: str, table: bigquery.Table) -> None:
    table_id = f"{client.project}.{dataset_id}.{table.table_id.split('.')[-1]}"
    try:
        existing = client.get_table(table_id)
        missing = _missing_fields(table.schema, list(existing.schema))

        needs_update = False
        if missing:
            updated_schema = list(existing.schema) + missing
            existing.schema = updated_schema
            needs_update = True

        if existing.time_partitioning is None or existing.time_partitioning.field != table.time_partitioning.field:
            existing.time_partitioning = table.time_partitioning
            needs_update = True

        current_clusters = tuple(existing.clustering_fields or [])
        expected_clusters = tuple(table.clustering_fields or [])
        if current_clusters != expected_clusters:
            existing.clustering_fields = expected_clusters
            needs_update = True

        if needs_update:
            try:
                client.update_table(existing, ["schema", "time_partitioning", "clustering_fields"])
                log("INFO", "Table updated", table=table_id, updated_fields=[f.name for f in missing])
            except BadRequest as exc:
                log("ERROR", "Failed to update existing table", table=table_id, error=str(exc))
                raise
        else:
            log("INFO", "Table already exists", table=table_id)
    except NotFound:
        client.create_table(table)
        log("INFO", "Created table", table=table_id)


def openmeteo_schema(dataset_id: str, project: str) -> bigquery.Table:
    table_id = f"{project}.{dataset_id}.{OPENMETEO_TABLE}"
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
    table = bigquery.Table(table_id, schema=schema)
    table.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY,
        field="date",
    )
    table.clustering_fields = ["point_id"]
    table.require_partition_filter = False
    return table


def fluview_schema(dataset_id: str, project: str) -> bigquery.Table:
    table_id = f"{project}.{dataset_id}.{FLUVIEW_TABLE}"
    schema = [
        bigquery.SchemaField("epiweek", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("issue", "INT64"),
        bigquery.SchemaField("region", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("wili", "FLOAT64"),
        bigquery.SchemaField("ili", "FLOAT64"),
        bigquery.SchemaField("num_ili", "INT64"),
        bigquery.SchemaField("num_patients", "INT64"),
        bigquery.SchemaField("num_providers", "INT64"),
        bigquery.SchemaField("release_date", "DATE"),
        bigquery.SchemaField("week_start_date", "DATE"),
        bigquery.SchemaField(
            "inserted_at",
            "TIMESTAMP",
            mode="REQUIRED",
            default_value_expression="CURRENT_TIMESTAMP()",
        ),
        bigquery.SchemaField("source_url", "STRING"),
    ]
    table = bigquery.Table(table_id, schema=schema)
    table.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY,
        field="week_start_date",
    )
    table.clustering_fields = ["region"]
    table.require_partition_filter = False
    return table


def parse_args(argv: Iterable[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Ensure raw BigQuery dataset/tables exist")
    parser.add_argument("--project", required=True, help="GCP project ID")
    parser.add_argument(
        "--location",
        default="europe-north2",
        help="BigQuery location; defaults to europe-north2",
    )
    parser.add_argument(
        "--dataset",
        default=RAW_DATASET,
        help="Dataset name to create if missing; defaults to 'raw'",
    )
    return parser.parse_args(argv)


def main(argv: Iterable[str] | None = None) -> int:
    args = parse_args(argv or sys.argv[1:])
    client = bigquery.Client(project=args.project, location=args.location)

    ensure_dataset(client, args.dataset, args.location)

    openmeteo_table = openmeteo_schema(args.dataset, args.project)
    fluview_table = fluview_schema(args.dataset, args.project)

    for table in (openmeteo_table, fluview_table):
        ensure_table(client, args.dataset, table)

    log(
        "INFO",
        "Raw landing setup complete",
        dataset=args.dataset,
        project=args.project,
        tables=[OPENMETEO_TABLE, FLUVIEW_TABLE],
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
