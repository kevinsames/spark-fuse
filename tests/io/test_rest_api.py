from __future__ import annotations

from collections import deque
from typing import Any, Deque, Dict, Tuple

import pytest

from spark_fuse.io.rest_api import RestAPIReader
from spark_fuse.io import rest_api as rest_mod


class _FakeResponse:
    def __init__(self, status_code: int, payload: Any):
        self.status_code = status_code
        self._payload = payload

    def json(self) -> Any:
        return self._payload


def _patch_session(monkeypatch, responses: Dict[str, Deque[Tuple[int, Any]]], calls: list) -> None:
    class _FakeSession:
        instances: list["_FakeSession"] = []

        def __init__(self):
            self.headers: Dict[str, str] = {}
            self.__class__.instances.append(self)

        def get(self, url: str, timeout: float, **kwargs: Any) -> _FakeResponse:
            calls.append({"url": url, "timeout": timeout, "kwargs": kwargs})
            queue = responses.get(url)
            if not queue:
                raise AssertionError(f"Unexpected request to {url}")
            try:
                status, payload = queue.popleft()
            except IndexError as exc:
                raise AssertionError(f"No responses remaining for {url}") from exc
            return _FakeResponse(status, payload)

    monkeypatch.setattr(rest_mod.requests, "Session", _FakeSession)
    return _FakeSession


@pytest.mark.usefixtures("spark")
def test_rest_api_reader_query_pagination(spark, monkeypatch):
    base_url = "https://example.com/items"
    data_pages = [
        [{"id": 1, "name": "alpha"}, {"id": 2, "name": "beta"}],
        [{"id": 3, "name": "gamma"}],
    ]
    responses: Dict[str, Deque[Tuple[int, Any]]] = {}
    for idx, records in enumerate(data_pages, start=1):
        url = rest_mod._merge_query_params(base_url, {"page": idx, "limit": 2})
        responses[url] = deque([(200, {"data": records, "meta": {"page": idx}})])

    calls: list = []
    fake_cls = _patch_session(monkeypatch, responses, calls)

    reader = RestAPIReader()
    df = reader.read(
        spark,
        base_url,
        source_config={
            "params": {"limit": 2},
            "records_field": "data",
            "pagination": {"mode": "query", "param": "page", "start": 1, "stop": 2},
            "parallelism": 1,
        },
    )

    rows = {(row.id, row.name) for row in df.select("id", "name").collect()}
    assert rows == {(1, "alpha"), (2, "beta"), (3, "gamma")}
    assert len(calls) == 2
    assert len(fake_cls.instances) == 1


@pytest.mark.usefixtures("spark")
def test_rest_api_reader_response_pagination_next_links(spark, monkeypatch):
    base_url = "https://example.com/pokemon"
    first_url = rest_mod._merge_query_params(base_url, {"limit": 2})
    second_url = rest_mod._merge_query_params(base_url, {"limit": 2, "offset": 2})

    responses: Dict[str, Deque[Tuple[int, Any]]] = {
        first_url: deque(
            [
                (
                    200,
                    {
                        "results": [
                            {"name": "bulbasaur", "url": "https://pokeapi.co/api/v2/pokemon/1/"},
                            {"name": "ivysaur", "url": "https://pokeapi.co/api/v2/pokemon/2/"},
                        ],
                        "next": second_url,
                    },
                )
            ]
        ),
        second_url: deque(
            [
                (
                    200,
                    {
                        "results": [
                            {"name": "venusaur", "url": "https://pokeapi.co/api/v2/pokemon/3/"},
                            {"name": "charmander", "url": "https://pokeapi.co/api/v2/pokemon/4/"},
                        ],
                        "next": None,
                    },
                )
            ]
        ),
    }

    calls: list = []
    fake_cls = _patch_session(monkeypatch, responses, calls)

    reader = RestAPIReader()
    df = reader.read(
        spark,
        base_url,
        source_config={
            "params": {"limit": 2},
            "records_field": "results",
            "pagination": {"mode": "response", "field": "next", "max_pages": 3},
            "parallelism": 1,
            "headers": {"User-Agent": "spark-fuse-tests"},
        },
    )

    ordered = [row.name for row in df.orderBy("name").collect()]
    assert ordered == ["bulbasaur", "charmander", "ivysaur", "venusaur"]
    assert len(calls) == 2
    assert len(fake_cls.instances) == 1
    assert fake_cls.instances[0].headers.get("User-Agent") == "spark-fuse-tests"
