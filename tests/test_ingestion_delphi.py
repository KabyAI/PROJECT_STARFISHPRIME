import json
from datetime import UTC, date
from pathlib import Path
from typing import Any

import pandas as pd
import pytest
import requests

from ingestion_pipeline_delphi.ingestion_delphi import (
    ResponseCache,
    FluviewFetcher,
    dataframe_from_payload,
    generate_epiweek_numbers,
    missing_epiweek_chunks,
    parse_regions,
    resolve_date_range,
    to_epiweek,
    advance_epiweek,
)


class DummyResponse:
    def __init__(self, status_code: int, payload: dict[str, Any] | None = None) -> None:
        self.status_code = status_code
        self._payload = payload or {}
        self.text = json.dumps(self._payload)

    def json(self) -> dict[str, Any]:
        return self._payload

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise requests.HTTPError(self.text, response=self)


class DummySession:
    def __init__(self, payload: dict[str, Any]) -> None:
        self._payload = payload
        self.calls = 0

    def get(self, url: str, params: dict[str, Any], timeout: int) -> DummyResponse:
        self.calls += 1
        if self.calls == 1:
            error = DummyResponse(429, {"result": 0, "message": "rate limit"})
            raise requests.HTTPError(error.text, response=error)
        return DummyResponse(200, self._payload)


@pytest.fixture
def cache(tmp_path: Path) -> ResponseCache:
    return ResponseCache(tmp_path / "cache", ttl_seconds=3600)


def test_parse_regions_defaults_when_empty() -> None:
    assert parse_regions("") == ["ca", "state:ca"]


def test_resolve_date_range_aligns_to_monday(monkeypatch: pytest.MonkeyPatch) -> None:
    class FrozenDate(date):
        @classmethod
        def today(cls) -> "FrozenDate":  # type: ignore[override]
            return cls(2024, 1, 11)

    monkeypatch.setattr("ingestion_pipeline_delphi.ingestion_delphi.date", FrozenDate)

    start, end = resolve_date_range("2024-01-03")
    assert start == date(2024, 1, 1)
    assert end == date(2024, 1, 8)


def test_generate_epiweek_numbers_is_inclusive() -> None:
    start_epiweek = to_epiweek(date(2024, 1, 1))
    end_epiweek = advance_epiweek(start_epiweek, 2)

    weeks = list(generate_epiweek_numbers(start_epiweek, end_epiweek))
    assert weeks[0] == start_epiweek
    assert weeks[-1] == end_epiweek
    assert len(weeks) == 3


def test_missing_epiweek_chunks_respects_chunk_size() -> None:
    start_epiweek = to_epiweek(date(2024, 1, 1))
    end_epiweek = advance_epiweek(start_epiweek, 7)

    chunks = missing_epiweek_chunks(set(), start_epiweek, end_epiweek, max_size=3)

    assert len(chunks) > 1

    collected: list[int] = []
    for chunk_start, chunk_end in chunks:
        span = list(generate_epiweek_numbers(chunk_start, chunk_end))
        assert len(span) <= 3
        collected.extend(span)

    expected = list(generate_epiweek_numbers(start_epiweek, end_epiweek))
    assert collected == expected


def test_dataframe_from_payload_converts_types() -> None:
    start_epiweek = to_epiweek(date(2024, 1, 1))
    payload = {
        "epidata": [
            {
                "epiweek": start_epiweek,
                "wili": "1.2",
                "ili": "0.9",
                "num_ili": "5",
                "num_patients": "200",
                "release_date": "2024-01-15",
            }
        ]
    }

    df = dataframe_from_payload("ca", payload, start_epiweek, start_epiweek)

    assert list(df.columns) == [
        "region",
        "epiweek",
        "wili",
        "ili",
        "num_ili",
        "num_patients",
        "release_date",
        "week_start",
        "source_url",
        "chunk_start",
        "chunk_end",
    ]
    assert df.loc[0, "week_start"] == date(2024, 1, 1)
    assert df.loc[0, "chunk_start"] == start_epiweek
    assert df.loc[0, "chunk_end"] == start_epiweek
    assert df.loc[0, "release_date"].tzinfo is UTC


def test_fetcher_retries_and_uses_cache(
    monkeypatch: pytest.MonkeyPatch,
    cache: ResponseCache,
) -> None:
    payload = {"result": 1, "epidata": [{"epiweek": 202401}]}
    session = DummySession(payload)
    fetcher = FluviewFetcher(session, cache=cache)

    monkeypatch.setattr("ingestion_pipeline_delphi.ingestion_delphi.time.sleep", lambda _: None)
    monkeypatch.setattr("ingestion_pipeline_delphi.ingestion_delphi.random.random", lambda: 0.0)
    monkeypatch.setattr("ingestion_pipeline_delphi.ingestion_delphi.REQUEST_SLEEP_SECONDS", 0.0)

    result = fetcher.fetch("ca", 202401, 202402)
    assert result == payload
    assert session.calls == 2

    session.calls = 0
    cached = fetcher.fetch("ca", 202401, 202402)
    assert cached == payload
    assert session.calls == 0
