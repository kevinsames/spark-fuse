"""Qdrant read path: configuration, scrolling, schema inference, and reader."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Dict, Iterator, Mapping, MutableMapping, Optional, Sequence

import time

import requests
from pyspark.sql import Row, SparkSession
from pyspark.sql.datasource import DataSourceReader, InputPartition
from pyspark.sql.types import StructType
from pyspark.sql.types import _infer_schema, _merge_type

from .._http import normalize_jsonable as _normalize_jsonable
from .._http import validate_http_url as _validate_http_url

_LOGGER = logging.getLogger(__name__)

_DEFAULT_PAGE_SIZE = 128


def _normalize_payload_option(value: Any) -> Any:
    if value is None:
        return True
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        trimmed = value.strip()
        return [trimmed] if trimmed else False
    if isinstance(value, Sequence):
        return [str(v) for v in value]
    if isinstance(value, Mapping):
        return _normalize_jsonable(value)
    return bool(value)


def _normalize_vectors_option(value: Any) -> Any:
    if value is None:
        return False
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        trimmed = value.strip()
        return [trimmed] if trimmed else False
    if isinstance(value, Sequence):
        return [str(v) for v in value]
    return bool(value)


def _scroll_url(endpoint: str, collection: str) -> str:
    base = endpoint.rstrip("/")
    return f"{base}/collections/{collection}/points/scroll"


def _should_include_vectors(option: Any) -> bool:
    return bool(option)


def _should_include_payload(option: Any) -> bool:
    if isinstance(option, bool):
        return option
    return option is not False and option is not None


def _normalize_point(
    point: Any,
    *,
    include_payload: bool,
    include_vectors: bool,
) -> Dict[str, Any]:
    if not isinstance(point, MutableMapping):
        return {"value": point}
    row: Dict[str, Any] = {}
    for key, value in point.items():
        if key == "payload" and not include_payload:
            continue
        if key == "vector" and not include_vectors:
            continue
        row[str(key)] = value
    return row


def _perform_scroll_request(
    session: requests.Session,
    url: str,
    payload: Mapping[str, Any],
    *,
    timeout: float,
    max_retries: int,
    backoff_factor: float,
) -> Mapping[str, Any]:
    attempts = max(max_retries, 0) + 1
    for attempt in range(attempts):
        try:
            response = session.post(url, json=payload, timeout=timeout)
            if 200 <= response.status_code < 300:
                try:
                    return response.json()
                except ValueError as exc:
                    raise ValueError(f"Failed to decode Qdrant response JSON: {exc}") from exc
            _LOGGER.warning(
                "Qdrant scroll returned HTTP %s for %s (attempt %s/%s)",
                response.status_code,
                url,
                attempt + 1,
                attempts,
            )
        except requests.RequestException as exc:
            _LOGGER.warning(
                "Qdrant scroll request failed on attempt %s/%s: %s",
                attempt + 1,
                attempts,
                exc,
            )
        if attempt < attempts - 1:
            delay = backoff_factor * (2**attempt)
            if delay > 0:
                time.sleep(delay)
    raise RuntimeError(f"Qdrant scroll failed after {attempts} attempts for {url}")


@dataclass
class _QdrantResolvedConfig:
    endpoint: str
    collection: str
    api_key: Optional[str]
    headers: Mapping[str, str]
    timeout: float
    max_retries: int
    backoff_factor: float
    with_payload: Any
    with_vectors: Any
    include_payload: bool
    include_vectors: bool
    limit: Optional[int]
    page_size: int
    max_pages: Optional[int]
    filter: Optional[Mapping[str, Any]]
    offset: Optional[Any]
    infer_schema: bool

    @staticmethod
    def from_dict(data: Mapping[str, Any]) -> "_QdrantResolvedConfig":
        endpoint = data.get("endpoint")
        if not endpoint or not _validate_http_url(str(endpoint)):
            raise ValueError("Qdrant endpoint must start with http:// or https://")
        endpoint_str = str(endpoint).rstrip("/")
        collection = str(data.get("collection") or "").strip()
        if not collection:
            raise ValueError("Qdrant collection name must be provided")

        api_key = data.get("api_key")
        if api_key is not None:
            api_key = str(api_key)

        headers_value: Dict[str, str] = {}
        if isinstance(data.get("headers"), Mapping):
            headers_value.update({str(k): str(v) for k, v in data["headers"].items()})

        timeout = float(data.get("timeout", 30.0))
        max_retries = int(data.get("max_retries", 3))
        backoff_factor = float(data.get("backoff_factor", 0.5))

        with_payload = _normalize_payload_option(data.get("with_payload", True))
        include_payload = _should_include_payload(with_payload)
        with_vectors = _normalize_vectors_option(data.get("with_vectors", False))
        include_vectors = _should_include_vectors(with_vectors)

        limit_value = data.get("limit")
        limit = int(limit_value) if limit_value is not None else None
        if limit is not None and limit <= 0:
            raise ValueError("limit must be positive when provided")

        page_size = int(data.get("page_size", _DEFAULT_PAGE_SIZE))
        if page_size <= 0:
            raise ValueError("page_size must be a positive integer")
        if limit is not None:
            page_size = min(page_size, limit)

        max_pages_value = data.get("max_pages")
        max_pages = int(max_pages_value) if max_pages_value is not None else None
        if max_pages is not None and max_pages <= 0:
            raise ValueError("max_pages must be positive when provided")

        filter_value = data.get("filter")
        if filter_value is not None and not isinstance(filter_value, Mapping):
            raise TypeError("filter must be a mapping when provided")
        filter_payload = (
            _normalize_jsonable(filter_value) if isinstance(filter_value, Mapping) else None
        )

        offset = data.get("offset")
        infer_schema = bool(data.get("infer_schema", True))

        return _QdrantResolvedConfig(
            endpoint=endpoint_str,
            collection=collection,
            api_key=api_key,
            headers=headers_value,
            timeout=timeout,
            max_retries=max_retries,
            backoff_factor=backoff_factor,
            with_payload=with_payload,
            with_vectors=with_vectors,
            include_payload=include_payload,
            include_vectors=include_vectors,
            limit=limit,
            page_size=page_size,
            max_pages=max_pages,
            filter=filter_payload,
            offset=offset,
            infer_schema=infer_schema,
        )


def _iter_points(config: _QdrantResolvedConfig) -> Iterator[Dict[str, Any]]:
    session = requests.Session()
    session.headers.update(config.headers)
    if config.api_key:
        session.headers.setdefault("api-key", config.api_key)
    url = _scroll_url(config.endpoint, config.collection)

    remaining = config.limit
    page = 0
    offset = config.offset

    try:
        while True:
            if remaining is not None and remaining <= 0:
                break

            request_limit = config.page_size
            if remaining is not None:
                request_limit = min(request_limit, remaining)

            payload: Dict[str, Any] = {
                "limit": request_limit,
                "with_payload": config.with_payload,
                "with_vectors": config.with_vectors,
            }
            if config.filter is not None:
                payload["filter"] = config.filter
            if offset is not None:
                payload["offset"] = offset

            response = _perform_scroll_request(
                session,
                url,
                payload,
                timeout=config.timeout,
                max_retries=config.max_retries,
                backoff_factor=config.backoff_factor,
            )

            status = response.get("status")
            if status and str(status).lower() != "ok":
                raise RuntimeError(f"Qdrant returned a non-ok status: {status}")
            result = response.get("result")
            if not isinstance(result, Mapping):
                raise ValueError("Invalid Qdrant response: missing result object")

            points = result.get("points") or []
            if not isinstance(points, Sequence):
                raise ValueError("Invalid Qdrant response: result.points must be a sequence")

            for point in points:
                yield _normalize_point(
                    point,
                    include_payload=config.include_payload,
                    include_vectors=config.include_vectors,
                )
                if remaining is not None:
                    remaining -= 1
                    if remaining <= 0:
                        break

            if remaining is not None and remaining <= 0:
                break

            next_offset = (
                result.get("next_page_offset")
                or result.get("next_offset")
                or result.get("next_page")
            )
            page += 1
            if not next_offset:
                break
            if config.max_pages is not None and page >= config.max_pages:
                break
            offset = next_offset
    finally:
        session.close()


def _infer_schema_from_points(config: _QdrantResolvedConfig) -> StructType:
    schema: Optional[StructType] = None
    for record in _iter_points(config):
        inferred = _infer_schema(record, infer_dict_as_struct=True)
        schema = inferred if schema is None else _merge_type(schema, inferred)
    return schema or StructType([])


class _QdrantInputPartition(InputPartition):
    def __init__(self) -> None:
        super().__init__(None)


class QdrantDataSourceReader(DataSourceReader):
    def __init__(self, config: _QdrantResolvedConfig, schema: StructType) -> None:
        self._config = config
        self._schema = schema
        self._field_names = schema.fieldNames()

    def partitions(self) -> Sequence[InputPartition]:
        return [_QdrantInputPartition()]

    def read(self, partition: InputPartition) -> Iterator[Dict[str, Any]]:
        return (self._dict_to_row(record) for record in _iter_points(self._config))

    def _dict_to_row(self, record: Mapping[str, Any]) -> Row:
        data = {name: record.get(name) for name in self._field_names}
        return Row(**data)


def build_qdrant_config(
    spark: SparkSession,
    endpoint: Any,
    *,
    collection: Optional[str] = None,
    schema: Optional[StructType] = None,
    source_config: Optional[Mapping[str, Any]] = None,
    headers: Optional[Mapping[str, str]] = None,
    **kwargs: Any,
) -> Dict[str, Any]:
    """Build the options payload consumed by the Qdrant data source."""

    config: Dict[str, Any] = {}
    for mapping in (source_config, kwargs):
        if mapping:
            config.update(mapping)

    endpoint_str = str(endpoint)
    if not _validate_http_url(endpoint_str):
        raise ValueError("endpoint must start with http:// or https:// for Qdrant reads")

    collection_name = collection or config.get("collection")
    if not collection_name or not str(collection_name).strip():
        raise ValueError("collection must be provided for Qdrant reads")
    config["collection"] = str(collection_name).strip()

    infer_schema = bool(config.get("infer_schema", schema is None))
    if not infer_schema and schema is None:
        raise ValueError("schema must be provided when infer_schema=False for Qdrant reads")

    base_headers: Dict[str, str] = {}
    for header_map in (config.get("headers"), headers):
        if isinstance(header_map, Mapping):
            base_headers.update({str(k): str(v) for k, v in header_map.items()})

    limit_value = config.get("limit")
    if limit_value is not None:
        limit_value = int(limit_value)
        if limit_value <= 0:
            raise ValueError("limit must be positive when provided")
        config["limit"] = limit_value

    page_size = int(config.get("page_size", _DEFAULT_PAGE_SIZE))
    if page_size <= 0:
        raise ValueError("page_size must be a positive integer")
    if limit_value is not None:
        page_size = min(page_size, int(limit_value))
    config["page_size"] = page_size

    max_pages = config.get("max_pages")
    if max_pages is not None:
        max_pages = int(max_pages)
        if max_pages <= 0:
            raise ValueError("max_pages must be positive when provided")
        config["max_pages"] = max_pages

    filter_value = config.get("filter")
    if filter_value is not None and not isinstance(filter_value, Mapping):
        raise TypeError("filter must be a mapping when provided")
    if isinstance(filter_value, Mapping):
        config["filter"] = _normalize_jsonable(filter_value)

    config_payload = {
        "endpoint": endpoint_str.rstrip("/"),
        "collection": config["collection"],
        "api_key": config.get("api_key"),
        "headers": base_headers,
        "timeout": float(config.get("timeout", 30.0)),
        "max_retries": int(config.get("max_retries", 3)),
        "backoff_factor": float(config.get("backoff_factor", 0.5)),
        "with_payload": _normalize_payload_option(config.get("with_payload", True)),
        "with_vectors": _normalize_vectors_option(config.get("with_vectors", False)),
        "limit": config.get("limit"),
        "page_size": config["page_size"],
        "max_pages": config.get("max_pages"),
        "filter": config.get("filter"),
        "offset": config.get("offset"),
        "infer_schema": infer_schema,
    }

    return config_payload
