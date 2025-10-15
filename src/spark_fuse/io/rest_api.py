from __future__ import annotations

import functools
import json
import logging
import time
from typing import Any, Dict, Iterable, Iterator, Mapping, MutableMapping, Optional, Sequence
from urllib.parse import urljoin

import requests
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType

from .base import Connector
from .registry import register_connector


_LOGGER = logging.getLogger(__name__)
_DEFAULT_RECORD_KEYS: Sequence[str] = ("data", "results", "items", "value")


def _merge_query_params(url: str, params: Optional[Mapping[str, Any]]) -> str:
    if not params:
        return url
    req = requests.Request("GET", url, params=params)
    prepared = req.prepare()
    return prepared.url


def _get_nested_value(payload: Any, path: str) -> Any:
    current = payload
    for part in path.split("."):
        if isinstance(current, Mapping):
            current = current.get(part)
        else:
            return None
    return current


def _extract_records(payload: Any, records_field: Optional[Sequence[str]]) -> Sequence[Any]:
    if records_field:
        path = (
            ".".join(records_field)
            if isinstance(records_field, (list, tuple))
            else str(records_field)
        )
        data = _get_nested_value(payload, path)
    else:
        data = None
        if isinstance(payload, list):
            data = payload
        elif isinstance(payload, Mapping):
            for key in _DEFAULT_RECORD_KEYS:
                candidate = payload.get(key)
                if isinstance(candidate, list):
                    data = candidate
                    break
            if data is None:
                data = payload
    if data is None:
        return []
    if isinstance(data, list):
        return data
    if isinstance(data, Mapping):
        return [data]
    return [data]


def _ensure_dict(record: Any) -> Dict[str, Any]:
    if isinstance(record, MutableMapping):
        return dict(record)
    if isinstance(record, Mapping):
        return dict(record.items())
    return {"value": record}


def _iter_page_values(pagination: Mapping[str, Any]) -> Iterator[Any]:
    explicit = pagination.get("values")
    if explicit is not None:
        for value in explicit:
            yield value
        return

    start = pagination.get("start", 1)
    stop = pagination.get("stop")
    step = pagination.get("step", 1)
    max_pages = pagination.get("max_pages")
    if stop is None and max_pages is None:
        raise ValueError("query pagination requires 'stop', 'max_pages', or explicit 'values'")

    count = 0
    value = start
    while True:
        if max_pages is not None and count >= max_pages:
            break
        if stop is not None:
            if step > 0 and value > stop:
                break
            if step < 0 and value < stop:
                break
        yield value
        count += 1
        value += step


def _perform_request(
    session: requests.Session,
    url: str,
    *,
    timeout: float,
    max_retries: int,
    backoff_factor: float,
    request_kwargs: Mapping[str, Any],
) -> Optional[Any]:
    attempts = max(max_retries, 0) + 1
    for attempt in range(attempts):
        try:
            response = session.get(url, timeout=timeout, **request_kwargs)
            if 200 <= response.status_code < 300:
                try:
                    return response.json()
                except ValueError:
                    _LOGGER.error("Failed to decode JSON response from %s", url)
                    return None
            _LOGGER.warning("Received HTTP %s from %s", response.status_code, url)
        except requests.RequestException as exc:
            _LOGGER.warning(
                "Request to %s failed on attempt %s/%s: %s", url, attempt + 1, attempts, exc
            )
        if attempt < attempts - 1:
            delay = backoff_factor * (2**attempt)
            if delay > 0:
                time.sleep(delay)
    _LOGGER.error("Exhausted retries fetching %s", url)
    return None


def _fetch_single(
    session: requests.Session,
    url: str,
    *,
    timeout: float,
    max_retries: int,
    backoff_factor: float,
    request_kwargs: Mapping[str, Any],
    records_field: Optional[Sequence[str]],
) -> Iterator[str]:
    payload = _perform_request(
        session,
        url,
        timeout=timeout,
        max_retries=max_retries,
        backoff_factor=backoff_factor,
        request_kwargs=request_kwargs,
    )
    if payload is None:
        return
    records = _extract_records(payload, records_field)
    for record in records:
        yield json.dumps(_ensure_dict(record))


def _fetch_with_response_pagination(
    session: requests.Session,
    item: Mapping[str, Any],
    *,
    timeout: float,
    max_retries: int,
    backoff_factor: float,
    request_kwargs: Mapping[str, Any],
    records_field: Optional[Sequence[str]],
) -> Iterator[str]:
    pagination = item["pagination"]
    next_field = pagination.get("field", "next")
    if isinstance(next_field, (list, tuple)):
        next_path = ".".join(str(part) for part in next_field)
    else:
        next_path = str(next_field)
    max_pages = pagination.get("max_pages")
    current_url = item["url"]
    page = 0
    seen_urls = set()
    while current_url:
        if current_url in seen_urls:
            _LOGGER.warning("Detected pagination loop for %s, stopping iteration", current_url)
            break
        seen_urls.add(current_url)
        if max_pages is not None and page >= max_pages:
            break
        page += 1
        payload = _perform_request(
            session,
            current_url,
            timeout=timeout,
            max_retries=max_retries,
            backoff_factor=backoff_factor,
            request_kwargs=request_kwargs,
        )
        if payload is None:
            break
        records = _extract_records(payload, records_field)
        for record in records:
            yield json.dumps(_ensure_dict(record))
        next_value = _get_nested_value(payload, next_path) if next_path else None
        if not next_value:
            break
        current_url = urljoin(current_url, str(next_value))


def _map_partition_fetch(
    partition: Iterable[Mapping[str, Any]],
    *,
    timeout: float,
    max_retries: int,
    backoff_factor: float,
    headers: Mapping[str, str],
    request_kwargs: Mapping[str, Any],
    records_field: Optional[Sequence[str]],
) -> Iterator[str]:
    session = requests.Session()
    if headers:
        session.headers.update(headers)
    for item in partition:
        mode = item.get("mode", "single")
        if mode == "response":
            yield from _fetch_with_response_pagination(
                session,
                item,
                timeout=timeout,
                max_retries=max_retries,
                backoff_factor=backoff_factor,
                request_kwargs=request_kwargs,
                records_field=records_field,
            )
        else:
            yield from _fetch_single(
                session,
                item["url"],
                timeout=timeout,
                max_retries=max_retries,
                backoff_factor=backoff_factor,
                request_kwargs=request_kwargs,
                records_field=records_field,
            )


@register_connector
class RestAPIReader(Connector):
    """Connector that loads JSON payloads from REST APIs into Spark DataFrames."""

    name = "rest"

    def validate_path(self, path: str) -> bool:
        return isinstance(path, str) and path.startswith(("http://", "https://"))

    def read(
        self,
        spark: SparkSession,
        source: Any,
        *,
        fmt: Optional[str] = None,
        schema: Optional[StructType] = None,
        source_config: Optional[Mapping[str, Any]] = None,
        options: Optional[Mapping[str, Any]] = None,
        headers: Optional[Mapping[str, str]] = None,
        **kwargs: Any,
    ) -> DataFrame:
        config: Dict[str, Any] = {}
        if source_config:
            config.update(source_config)
        if options:
            config.update(options)
        if kwargs:
            config.update(kwargs)

        records_field = config.get("records_field")
        if isinstance(records_field, str):
            records_field = records_field.split(".")
        infer_schema = config.get("infer_schema", schema is None)
        if not infer_schema and schema is None:
            raise ValueError("schema must be provided when infer_schema=False for REST API reads")

        request_timeout = float(config.get("request_timeout", 30.0))
        max_retries = int(config.get("max_retries", 3))
        backoff_factor = float(config.get("retry_backoff", 0.5))

        base_headers: Dict[str, str] = {}
        if isinstance(config.get("headers"), Mapping):
            base_headers.update({str(k): str(v) for k, v in dict(config["headers"]).items()})
        if headers:
            base_headers.update({str(k): str(v) for k, v in dict(headers).items()})

        request_kwargs: Dict[str, Any] = {}
        if isinstance(config.get("request_kwargs"), Mapping):
            request_kwargs.update(config["request_kwargs"])

        pagination = config.get("pagination")
        params = dict(config.get("params", {})) if isinstance(config.get("params"), Mapping) else {}

        work_items = self._prepare_work_items(source, params, pagination)

        if not work_items:
            empty_schema = schema or StructType([])
            return spark.createDataFrame(spark.sparkContext.emptyRDD(), empty_schema)

        parallelism = config.get("parallelism")
        sc = spark.sparkContext
        default_parallelism = sc.defaultParallelism or 1
        if parallelism is None:
            parallelism = min(len(work_items), default_parallelism) or 1
        else:
            parallelism = max(int(parallelism), 1)

        fetch_config = {
            "timeout": request_timeout,
            "max_retries": max_retries,
            "backoff_factor": backoff_factor,
            "headers": base_headers,
            "request_kwargs": request_kwargs,
            "records_field": records_field,
        }

        payload_rdd = sc.parallelize(work_items, numSlices=parallelism).mapPartitions(
            functools.partial(_map_partition_fetch, **fetch_config)
        )

        if payload_rdd.isEmpty():
            empty_schema = schema or StructType([])
            return spark.createDataFrame(sc.emptyRDD(), empty_schema)

        reader = spark.read
        spark_options = config.get("spark_options")
        if schema is not None:
            reader = reader.schema(schema)
        elif not infer_schema:
            reader = reader.schema(StructType([]))
        if isinstance(spark_options, Mapping):
            reader = reader.options(**dict(spark_options))
        return reader.json(payload_rdd)

    def _prepare_work_items(
        self,
        source: Any,
        params: Mapping[str, Any],
        pagination: Optional[Mapping[str, Any]],
    ) -> Sequence[Dict[str, Any]]:
        if isinstance(source, str):
            base_url = source
            if not self.validate_path(base_url):
                raise ValueError(f"Invalid REST endpoint: {base_url}")
            return self._prepare_for_single_endpoint(base_url, params, pagination)
        if isinstance(source, Sequence) and not isinstance(source, (str, bytes)):
            items = []
            for url in source:
                if not isinstance(url, str) or not self.validate_path(url):
                    raise ValueError(f"Invalid REST endpoint: {url}")
                items.append({"mode": "single", "url": url})
            return items
        raise TypeError("source must be a string URL or a sequence of URLs for RestAPIReader")

    def _prepare_for_single_endpoint(
        self,
        base_url: str,
        params: Mapping[str, Any],
        pagination: Optional[Mapping[str, Any]],
    ) -> Sequence[Dict[str, Any]]:
        merged_params = dict(params)
        url_with_params = _merge_query_params(base_url, merged_params)
        if not pagination:
            return [{"mode": "single", "url": url_with_params}]

        mode = pagination.get("mode", "query").lower()
        if mode in {"query", "page"}:
            page_param = pagination.get("param", "page")
            extra_params = dict(pagination.get("extra_params", {}))
            page_size_param = pagination.get("page_size_param")
            if page_size_param and "page_size" in pagination:
                extra_params[page_size_param] = pagination["page_size"]
            items: list[Dict[str, Any]] = []
            for value in _iter_page_values(pagination):
                page_params = dict(merged_params)
                page_params.update(extra_params)
                page_params[page_param] = value
                items.append({"mode": "single", "url": _merge_query_params(base_url, page_params)})
            return items
        if mode in {"response", "link"}:
            pagination_conf = dict(pagination)
            pagination_conf.setdefault("field", "next")
            return [
                {
                    "mode": "response",
                    "url": url_with_params,
                    "pagination": pagination_conf,
                }
            ]
        raise ValueError(f"Unsupported pagination mode: {mode}")

    def write(
        self,
        df: DataFrame,
        path: str,
        *,
        fmt: Optional[str] = None,
        mode: str = "error",
        **options: Any,
    ) -> None:
        """Writing back to REST APIs is not supported."""
        raise NotImplementedError("RestAPIReader does not support write operations")
