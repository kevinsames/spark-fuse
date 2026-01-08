"""REST API data source implementation for PySpark."""

from __future__ import annotations

import json
import logging
import math
import time
from dataclasses import dataclass
from typing import Any, Dict, Iterator, List, Mapping, MutableMapping, Optional, Sequence
from urllib.parse import urljoin

import requests
from pyspark.sql import Row, SparkSession
from pyspark.sql.datasource import DataSource, DataSourceReader, InputPartition
from pyspark.sql.types import StructType
from pyspark.sql.types import _infer_schema, _merge_type

_LOGGER = logging.getLogger(__name__)
_DEFAULT_RECORD_KEYS: Sequence[str] = ("data", "results", "items", "value")
REST_API_CONFIG_OPTION = "spark.fuse.rest.config"
REST_API_SCHEMA_OPTION = "spark.fuse.rest.schema"
_REGISTERED_SESSIONS: set[str] = set()
REST_API_FORMAT = "spark-fuse-rest"


def register_rest_data_source(spark: SparkSession) -> None:
    session_id = spark.sparkContext.applicationId
    if session_id in _REGISTERED_SESSIONS:
        return
    spark.dataSource.register(RestAPIDataSource)
    _REGISTERED_SESSIONS.add(session_id)


def _merge_query_params(url: str, params: Optional[Mapping[str, Any]]) -> str:
    if not params:
        return url
    req = requests.Request("GET", url, params=params)
    prepared = req.prepare()
    return prepared.url


def _get_nested_value(payload: Any, path: Sequence[str]) -> Any:
    current = payload
    for part in path:
        if isinstance(current, Mapping):
            current = current.get(part)
        else:
            return None
    return current


def _extract_records(payload: Any, records_field: Optional[Sequence[str]]) -> Sequence[Any]:
    if records_field:
        data = _get_nested_value(payload, records_field)
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
    request_type: str,
    request_kwargs: Mapping[str, Any],
) -> Optional[Any]:
    attempts = max(max_retries, 0) + 1
    method = request_type.upper()
    for attempt in range(attempts):
        try:
            response = session.request(method, url, timeout=timeout, **request_kwargs)
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
    request_type: str,
    records_field: Optional[Sequence[str]],
    include_response_payload: bool,
    response_payload_field: Optional[str],
) -> Iterator[Dict[str, Any]]:
    payload = _perform_request(
        session,
        url,
        timeout=timeout,
        max_retries=max_retries,
        backoff_factor=backoff_factor,
        request_type=request_type,
        request_kwargs=request_kwargs,
    )
    if payload is None:
        return
    response_value: Optional[Any] = payload if include_response_payload else None
    records = _extract_records(payload, records_field)
    for record in records:
        row = _ensure_dict(record)
        if response_value is not None and response_payload_field:
            row[response_payload_field] = response_value
        yield row


def _fetch_with_response_pagination(
    session: requests.Session,
    item: Mapping[str, Any],
    *,
    timeout: float,
    max_retries: int,
    backoff_factor: float,
    request_kwargs: Mapping[str, Any],
    request_type: str,
    records_field: Optional[Sequence[str]],
    include_response_payload: bool,
    response_payload_field: Optional[str],
) -> Iterator[Dict[str, Any]]:
    pagination = item["pagination"]
    next_path = _as_pagination_field(pagination.get("field"))
    max_pages = pagination.get("max_pages")
    current_url = item["url"]
    page = 0
    while current_url:
        page += 1
        if max_pages is not None and page > max_pages:
            break
        payload = _perform_request(
            session,
            current_url,
            timeout=timeout,
            max_retries=max_retries,
            backoff_factor=backoff_factor,
            request_type=request_type,
            request_kwargs=request_kwargs,
        )
        if payload is None:
            break
        response_value: Optional[Any] = payload if include_response_payload else None
        records = _extract_records(payload, records_field)
        for record in records:
            row = _ensure_dict(record)
            if response_value is not None and response_payload_field:
                row[response_payload_field] = response_value
            yield row
        next_value = _get_nested_value(payload, next_path) if next_path else None
        if not next_value:
            break
        if isinstance(next_value, str):
            if next_value.startswith("http://") or next_value.startswith("https://"):
                current_url = next_value
            else:
                current_url = urljoin(current_url, next_value)
        else:
            current_url = None


def _fetch_with_token_pagination(
    session: requests.Session,
    item: Mapping[str, Any],
    *,
    timeout: float,
    max_retries: int,
    backoff_factor: float,
    request_kwargs: Mapping[str, Any],
    request_type: str,
    records_field: Optional[Sequence[str]],
    include_response_payload: bool,
    response_payload_field: Optional[str],
) -> Iterator[Dict[str, Any]]:
    pagination = item["pagination"]
    token_param = item["token_param"]
    base_url = item["url"]
    base_params = dict(item.get("params") or {})
    token_path = _as_pagination_field(pagination.get("field"))
    max_pages = pagination.get("max_pages")
    token_value = base_params.get(token_param)
    page = 0
    while True:
        page += 1
        if max_pages is not None and page > max_pages:
            break
        params = dict(base_params)
        if token_value is not None and token_value != "":
            params[token_param] = token_value
        else:
            params.pop(token_param, None)
        current_url = _merge_query_params(base_url, params)
        payload = _perform_request(
            session,
            current_url,
            timeout=timeout,
            max_retries=max_retries,
            backoff_factor=backoff_factor,
            request_type=request_type,
            request_kwargs=request_kwargs,
        )
        if payload is None:
            break
        response_value: Optional[Any] = payload if include_response_payload else None
        records = _extract_records(payload, records_field)
        for record in records:
            row = _ensure_dict(record)
            if response_value is not None and response_payload_field:
                row[response_payload_field] = response_value
            yield row
        next_token = _get_nested_value(payload, token_path) if token_path else None
        if next_token is None or next_token == "":
            break
        token_value = next_token


def _normalize_jsonable(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(k): _normalize_jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_normalize_jsonable(v) for v in value]
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    return str(value)


def _as_records_field(value: Optional[Any]) -> Optional[List[str]]:
    if value is None:
        return None
    if isinstance(value, str):
        trimmed = value.strip()
        if not trimmed:
            return None
        return trimmed.split(".")
    if isinstance(value, Sequence):
        return [str(part) for part in value]
    raise TypeError("records_field must be a string or sequence of path segments")


def _as_pagination_field(value: Optional[Any]) -> Optional[List[str]]:
    if value is None:
        return None
    if isinstance(value, str):
        trimmed = value.strip()
        if not trimmed:
            return None
        return trimmed.split(".")
    if isinstance(value, Sequence):
        return [str(part) for part in value]
    return [str(value)]


@dataclass
class _RestAPIResolvedConfig:
    sources: List[str]
    params: Mapping[str, Any]
    pagination: Optional[Mapping[str, Any]]
    records_field: Optional[List[str]]
    request_type: str
    request_kwargs: Mapping[str, Any]
    headers: Mapping[str, str]
    timeout: float
    max_retries: int
    backoff_factor: float
    include_response_payload: bool
    response_payload_field: Optional[str]
    parallelism: int
    infer_schema: bool

    @staticmethod
    def from_dict(data: Mapping[str, Any]) -> "_RestAPIResolvedConfig":
        sources_value = data.get("sources") or []
        if isinstance(sources_value, str):
            sources = [sources_value]
        elif isinstance(sources_value, Sequence):
            sources = [str(item) for item in sources_value]
        else:
            raise TypeError("sources must be a string or a sequence of URLs")
        params = data.get("params")
        if isinstance(params, Mapping):
            params_value: Mapping[str, Any] = params
        else:
            params_value = {}
        pagination = data.get("pagination")
        if pagination is not None and not isinstance(pagination, Mapping):
            raise TypeError("pagination configuration must be a mapping when provided")
        records_field = _as_records_field(data.get("records_field"))
        request_type = str(data.get("request_type", "GET")).upper()
        request_kwargs = data.get("request_kwargs")
        if not isinstance(request_kwargs, Mapping):
            request_kwargs = {}
        headers = data.get("headers")
        if isinstance(headers, Mapping):
            headers_value = {str(k): str(v) for k, v in headers.items()}
        else:
            headers_value = {}
        timeout = float(data.get("timeout", 30.0))
        max_retries = int(data.get("max_retries", 3))
        backoff_factor = float(data.get("backoff_factor", 0.5))
        include_response_payload = bool(data.get("include_response_payload", False))
        response_payload_field = data.get("response_payload_field")
        if response_payload_field is not None:
            response_payload_field = str(response_payload_field)
        parallelism = max(int(data.get("parallelism", len(sources) or 1)), 1)
        infer_schema = bool(data.get("infer_schema", True))
        return _RestAPIResolvedConfig(
            sources=sources,
            params=params_value,
            pagination=pagination,
            records_field=records_field,
            request_type=request_type,
            request_kwargs=request_kwargs,
            headers=headers_value,
            timeout=timeout,
            max_retries=max_retries,
            backoff_factor=backoff_factor,
            include_response_payload=include_response_payload,
            response_payload_field=response_payload_field,
            parallelism=parallelism,
            infer_schema=infer_schema,
        )


def _prepare_work_items(
    config: _RestAPIResolvedConfig,
) -> List[Dict[str, Any]]:
    if not config.sources:
        raise ValueError("REST connector requires at least one source URL")
    if len(config.sources) > 1:
        return [{"mode": "single", "url": url} for url in config.sources]
    base_url = config.sources[0]
    if config.pagination:
        mode = str(config.pagination.get("mode", "query")).lower()
        if mode in {"query", "page"}:
            page_param = config.pagination.get("param", "page")
            extra_params = dict(config.pagination.get("extra_params", {}))
            page_size_param = config.pagination.get("page_size_param")
            if page_size_param and "page_size" in config.pagination:
                extra_params[page_size_param] = config.pagination["page_size"]
            items: List[Dict[str, Any]] = []
            for value in _iter_page_values(config.pagination):
                page_params = dict(config.params)
                page_params.update(extra_params)
                page_params[page_param] = value
                items.append({"mode": "single", "url": _merge_query_params(base_url, page_params)})
            return items
        if mode in {"token", "cursor"}:
            token_param = config.pagination.get("param")
            if not token_param:
                raise ValueError("token pagination requires 'param'")
            field_value = config.pagination.get("field")
            if field_value is None or (isinstance(field_value, str) and not field_value.strip()):
                raise ValueError("token pagination requires 'field'")
            extra_params = dict(config.pagination.get("extra_params", {}))
            page_size_param = config.pagination.get("page_size_param")
            if page_size_param and "page_size" in config.pagination:
                extra_params[page_size_param] = config.pagination["page_size"]
            base_params = dict(config.params)
            base_params.update(extra_params)
            start_value = config.pagination.get("start")
            if start_value is not None:
                base_params[token_param] = start_value
            return [
                {
                    "mode": "token",
                    "url": base_url,
                    "params": base_params,
                    "pagination": config.pagination,
                    "token_param": str(token_param),
                }
            ]
        if mode in {"response", "link"}:
            return [
                {
                    "mode": "response",
                    "url": _merge_query_params(base_url, config.params),
                    "pagination": config.pagination,
                }
            ]
        raise ValueError("Unsupported pagination mode: %s" % config.pagination.get("mode"))
    return [{"mode": "single", "url": _merge_query_params(base_url, config.params)}]


def _chunk_work_items(items: List[Dict[str, Any]], parallelism: int) -> List[List[Dict[str, Any]]]:
    if not items:
        return []
    parallelism = max(parallelism, 1)
    chunk_size = max(int(math.ceil(len(items) / parallelism)), 1)
    chunks: List[List[Dict[str, Any]]] = []
    for idx in range(0, len(items), chunk_size):
        chunks.append(items[idx : idx + chunk_size])
    return chunks


def _iter_records_for_items(
    items: Sequence[Mapping[str, Any]],
    config: _RestAPIResolvedConfig,
) -> Iterator[Dict[str, Any]]:
    session = requests.Session()
    session.headers.update(config.headers)
    try:
        for item in items:
            mode = item.get("mode")
            if mode == "single":
                yield from _fetch_single(
                    session,
                    item["url"],
                    timeout=config.timeout,
                    max_retries=config.max_retries,
                    backoff_factor=config.backoff_factor,
                    request_kwargs=config.request_kwargs,
                    request_type=config.request_type,
                    records_field=config.records_field,
                    include_response_payload=config.include_response_payload,
                    response_payload_field=config.response_payload_field,
                )
            elif mode == "response":
                yield from _fetch_with_response_pagination(
                    session,
                    item,
                    timeout=config.timeout,
                    max_retries=config.max_retries,
                    backoff_factor=config.backoff_factor,
                    request_kwargs=config.request_kwargs,
                    request_type=config.request_type,
                    records_field=config.records_field,
                    include_response_payload=config.include_response_payload,
                    response_payload_field=config.response_payload_field,
                )
            elif mode == "token":
                yield from _fetch_with_token_pagination(
                    session,
                    item,
                    timeout=config.timeout,
                    max_retries=config.max_retries,
                    backoff_factor=config.backoff_factor,
                    request_kwargs=config.request_kwargs,
                    request_type=config.request_type,
                    records_field=config.records_field,
                    include_response_payload=config.include_response_payload,
                    response_payload_field=config.response_payload_field,
                )
            else:
                raise ValueError(f"Unsupported work item mode: {mode}")
    finally:
        session.close()


def _infer_schema_from_items(
    items: Sequence[Mapping[str, Any]],
    config: _RestAPIResolvedConfig,
) -> StructType:
    schema: Optional[StructType] = None
    for record in _iter_records_for_items(items, config):
        inferred = _infer_schema(record, infer_dict_as_struct=True)
        schema = inferred if schema is None else _merge_type(schema, inferred)
    return schema or StructType([])


class _RestAPIInputPartition(InputPartition):
    def __init__(self, work_items: Sequence[Mapping[str, Any]]):
        super().__init__(list(work_items))


class RestAPIDataSourceReader(DataSourceReader):
    def __init__(
        self,
        config: _RestAPIResolvedConfig,
        schema: StructType,
        partitions: Sequence[_RestAPIInputPartition],
    ) -> None:
        self._config = config
        self._schema = schema
        self._partitions = list(partitions)
        self._field_names = schema.fieldNames()

    def partitions(self) -> Sequence[InputPartition]:
        return self._partitions

    def read(self, partition: InputPartition) -> Iterator[Dict[str, Any]]:
        items = partition.value if partition is not None else None
        if not items:
            return iter(())
        return (
            self._dict_to_row(record) for record in _iter_records_for_items(items, self._config)
        )

    def _dict_to_row(self, record: Mapping[str, Any]) -> Row:
        data = {name: record.get(name) for name in self._field_names}
        return Row(**data)


class RestAPIDataSource(DataSource):
    """Python data source powering the REST connector."""

    @classmethod
    def name(cls) -> str:  # pragma: no cover - trivial accessor
        return "spark-fuse-rest"

    def __init__(self, options: Mapping[str, str]) -> None:
        super().__init__(options)
        raw_config = options.get(REST_API_CONFIG_OPTION)
        if not raw_config:
            raise ValueError("REST data source requires the config option to be provided")
        self._config = _RestAPIResolvedConfig.from_dict(json.loads(raw_config))
        schema_json = options.get(REST_API_SCHEMA_OPTION)
        self._user_schema = StructType.fromJson(json.loads(schema_json)) if schema_json else None
        self._work_items: Optional[List[Dict[str, Any]]] = None
        self._schema_cache: Optional[StructType] = None
        self._partitions: Optional[List[_RestAPIInputPartition]] = None

    def schema(self) -> StructType:
        if self._user_schema is not None:
            return self._user_schema
        if not self._config.infer_schema:
            raise ValueError("infer_schema is disabled; provide an explicit schema")
        if self._schema_cache is None:
            self._ensure_work_items()
            items = self._work_items or []
            if not items:
                self._schema_cache = StructType([])
            else:
                self._schema_cache = _infer_schema_from_items(items, self._config)
        return self._schema_cache

    def reader(self, schema: StructType) -> RestAPIDataSourceReader:
        self._ensure_partitions()
        partitions = self._partitions or []
        return RestAPIDataSourceReader(self._config, schema, partitions)

    def _ensure_work_items(self) -> None:
        if self._work_items is None:
            self._work_items = _prepare_work_items(self._config)

    def _ensure_partitions(self) -> None:
        if self._partitions is not None:
            return
        self._ensure_work_items()
        items = self._work_items or []
        chunks = _chunk_work_items(items, self._config.parallelism) if items else []
        self._partitions = [_RestAPIInputPartition(chunk) for chunk in chunks]
        if not self._partitions:
            self._partitions = [_RestAPIInputPartition([])]


def _validate_http_url(value: str) -> bool:
    return isinstance(value, str) and value.startswith(("http://", "https://"))


def build_rest_api_config(
    spark: SparkSession,
    source: Any,
    *,
    schema: Optional[StructType] = None,
    source_config: Optional[Mapping[str, Any]] = None,
    options: Optional[Mapping[str, Any]] = None,
    headers: Optional[Mapping[str, str]] = None,
    **kwargs: Any,
) -> Dict[str, Any]:
    """Build the options payload consumed by the REST data source."""

    config: Dict[str, Any] = {}
    for mapping in (source_config, options, kwargs):
        if mapping:
            config.update(mapping)

    records_field = config.get("records_field")
    if isinstance(records_field, str):
        records_path = records_field.split(".") if records_field else None
    elif isinstance(records_field, Sequence):
        records_path = [str(part) for part in records_field]
    elif records_field is None:
        records_path = None
    else:
        raise TypeError("records_field must be a string or sequence")

    infer_schema = bool(config.get("infer_schema", schema is None))
    if not infer_schema and schema is None:
        raise ValueError("schema must be provided when infer_schema=False for REST API reads")

    request_timeout = float(config.get("request_timeout", 30.0))
    max_retries = int(config.get("max_retries", 3))
    backoff_factor = float(config.get("retry_backoff", 0.5))

    base_headers: Dict[str, str] = {}
    for header_map in (config.get("headers"), headers):
        if isinstance(header_map, Mapping):
            base_headers.update({str(k): str(v) for k, v in header_map.items()})

    request_kwargs: Dict[str, Any] = {}
    if isinstance(config.get("request_kwargs"), Mapping):
        request_kwargs.update(config["request_kwargs"])

    request_type = str(config.get("request_type", "GET")).upper()
    if request_type not in {"GET", "POST"}:
        raise ValueError("request_type must be either 'GET' or 'POST'")

    request_body = config.get("request_body")
    if request_body is not None and request_type != "POST":
        raise ValueError("request_body is only supported when request_type='POST'")

    if request_body is not None:
        body_mode = config.get("request_body_type")
        if body_mode is None:
            body_mode = "json" if isinstance(request_body, Mapping) else "data"
        body_mode = str(body_mode).lower()
        if body_mode == "json":
            request_kwargs.setdefault("json", request_body)
        elif body_mode in {"data", "form"}:
            request_kwargs.setdefault("data", request_body)
        elif body_mode in {"raw", "content"}:
            request_kwargs.setdefault("data", request_body)
        else:
            raise ValueError(
                "request_body_type must be one of {'json', 'data', 'form', 'raw', 'content'}"
            )

    pagination = config.get("pagination")
    if pagination is not None and not isinstance(pagination, Mapping):
        raise TypeError("pagination configuration must be a mapping when provided")
    params = (
        dict(config.get("params", {}))
        if isinstance(config.get("params"), Mapping)
        else config.get("params", {})
    )
    if params and not isinstance(params, Mapping):
        raise TypeError("params configuration must be a mapping if provided")

    include_response_payload = bool(config.get("include_response_payload", False))
    response_payload_field: Optional[str] = None
    if include_response_payload:
        response_payload_field = str(config.get("response_payload_field", "response_payload"))
        if not response_payload_field:
            raise ValueError("response_payload_field must be a non-empty string when enabled")

    work_source: List[str]
    if isinstance(source, str):
        work_source = [source]
    elif isinstance(source, Sequence) and not isinstance(source, (str, bytes)):
        work_source = [str(url) for url in source]
    else:
        raise TypeError("source must be a string URL or a sequence of URLs for REST reads")
    for url in work_source:
        if not _validate_http_url(url):
            raise ValueError(f"Invalid REST endpoint: {url}")

    spark_parallelism = config.get("parallelism")
    if spark_parallelism is None:
        spark_parallelism = spark.sparkContext.defaultParallelism or 1

    payload_config = {
        "sources": work_source,
        "params": params or {},
        "pagination": pagination,
        "records_field": records_path,
        "request_type": request_type,
        "request_kwargs": _normalize_jsonable(request_kwargs),
        "headers": base_headers,
        "timeout": request_timeout,
        "max_retries": max_retries,
        "backoff_factor": backoff_factor,
        "include_response_payload": include_response_payload,
        "response_payload_field": response_payload_field,
        "parallelism": int(spark_parallelism),
        "infer_schema": infer_schema,
    }

    return payload_config


__all__ = [
    "RestAPIDataSource",
    "register_rest_data_source",
    "build_rest_api_config",
    "REST_API_FORMAT",
    "REST_API_CONFIG_OPTION",
    "REST_API_SCHEMA_OPTION",
]
