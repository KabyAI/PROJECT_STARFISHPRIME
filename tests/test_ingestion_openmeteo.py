import json
from datetime import date
from pathlib import Path

import pandas as pd
import pytest
import requests

from ingestion_pipeline_weather.ingestion_openmeteo import (
    OpenMeteoFetcher,
    Point,
    ResponseCache,
    contiguous_ranges,
    dataframe_from_payload,
    month_windows,
    missing_date_ranges,
    parse_points,
    resolve_date_range,
)


class DummyResponse:
    def __init__(self, status_code: int, payload: dict | None = None) -> None:
        self.status_code = status_code
        self._payload = payload or {}
        self.text = json.dumps(self._payload)

    def json(self) -> dict:
        return self._payload

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise requests.HTTPError(self.text, response=self)


class DummySession:
    def __init__(self, payload: dict) -> None:
        self._payload = payload
        self.calls = 0

    def get(self, url: str, params: dict, timeout: int) -> DummyResponse:
        self.calls += 1
        if self.calls == 1:
            response = DummyResponse(429, {"error": "too many requests"})
            raise requests.HTTPError(response.text, response=response)
        return DummyResponse(200, self._payload)


@pytest.fixture
def cache(tmp_path: Path) -> ResponseCache:
    return ResponseCache(tmp_path / "cache", ttl_seconds=3600)


def test_month_windows_generates_month_boundaries() -> None:
    windows = month_windows(date(2024, 1, 15), date(2024, 3, 10), "America/Los_Angeles")
    assert windows == [
        (date(2024, 1, 15), date(2024, 1, 31)),
        (date(2024, 2, 1), date(2024, 2, 29)),
        (date(2024, 3, 1), date(2024, 3, 10)),
    ]


def test_contiguous_ranges_splits_gaps() -> None:
    ranges = contiguous_ranges([date(2024, 1, d) for d in (1, 2, 4, 5, 7)])
    assert ranges == [
        (date(2024, 1, 1), date(2024, 1, 2)),
        (date(2024, 1, 4), date(2024, 1, 5)),
        (date(2024, 1, 7), date(2024, 1, 7)),
    ]


def test_parse_points_defaults_when_empty() -> None:
    points = parse_points("")
    assert len(points) == 6
    assert {p.point_id for p in points} == {"sj", "sac", "sd", "la", "sf", "fresno"}


def test_resolve_date_range_uses_today_when_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    class FrozenDate(date):
        @classmethod
        def today(cls) -> "FrozenDate":  # type: ignore[override]
            return cls(2024, 1, 10)

    monkeypatch.setattr(
        "ingestion_pipeline_weather.ingestion_openmeteo.date",
        FrozenDate,
    )

    start, end = resolve_date_range("2024-01-01")
    assert start == date(2024, 1, 1)
    assert end == date(2024, 1, 10)


def test_resolve_date_range_validates_bounds() -> None:
    with pytest.raises(ValueError):
        resolve_date_range("2024-02-01", "2024-01-31")


def test_missing_date_ranges_identifies_gaps(monkeypatch: pytest.MonkeyPatch) -> None:
    window_start = date(2024, 1, 1)
    window_end = date(2024, 1, 6)

    observed_args: dict[str, object] = {}

    def fake_existing_dates(client: object, table_id: str, point_id: str, start: date, end: date) -> set[date]:
        observed_args.update({
            "client": client,
            "table_id": table_id,
            "point_id": point_id,
            "start": start,
            "end": end,
        })
        return {date(2024, 1, 1), date(2024, 1, 2), date(2024, 1, 4)}

    monkeypatch.setattr(
        "ingestion_pipeline_weather.ingestion_openmeteo.existing_dates",
        fake_existing_dates,
    )

    missing = missing_date_ranges(object(), "project.raw.table", "point-1", window_start, window_end)

    assert missing == [
        (date(2024, 1, 3), date(2024, 1, 3)),
        (date(2024, 1, 5), date(2024, 1, 6)),
    ]
    assert observed_args["table_id"] == "project.raw.table"
    assert observed_args["point_id"] == "point-1"
    assert observed_args["start"] == window_start
    assert observed_args["end"] == window_end


def test_fetcher_retries_and_caches(monkeypatch: pytest.MonkeyPatch, cache: ResponseCache) -> None:
    payload = {
        "daily": {
            "time": ["2024-01-01"],
            "temperature_2m_max": [10.0],
            "temperature_2m_min": [5.0],
            "temperature_2m_mean": [7.5],
        }
    }
    session = DummySession(payload)
    fetcher = OpenMeteoFetcher(session, cache=cache)

    monkeypatch.setattr("ingestion_pipeline_weather.ingestion_openmeteo.time.sleep", lambda _: None)

    point = Point(37.0, -122.0, "test", "CA")
    result = fetcher.fetch(point, date(2024, 1, 1), date(2024, 1, 1), "America/Los_Angeles")
    assert result == payload
    assert session.calls == 2

    session.calls = 0
    cached = fetcher.fetch(point, date(2024, 1, 1), date(2024, 1, 1), "America/Los_Angeles")
    assert cached == payload
    assert session.calls == 0


def test_dataframe_from_payload_shapes_rows() -> None:
    point = Point(37.0, -122.0, "ap", "CA")
    payload = {
        "daily": {
            "time": ["2024-01-01", "2024-01-02"],
            "temperature_2m_max": [12, 14],
            "temperature_2m_min": [5, 7],
            "temperature_2m_mean": [8, 10],
        }
    }
    df = dataframe_from_payload(point, payload)

    assert list(df.columns) == [
        "date",
        "temp_max",
        "temp_min",
        "temp_mean",
        "point_id",
        "region_label",
        "lat",
        "lon",
        "source_url",
    ]
    assert len(df) == 2
    jan_2_row = df.loc[df["date"] == pd.to_datetime("2024-01-02").date()].iloc[0]
    assert jan_2_row["temp_mean"] == 10
    assert jan_2_row["point_id"] == "ap"
