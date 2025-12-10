"""Qdrant data source implemented against the HTTP API."""

from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Dict, Iterable, Iterator, Mapping, MutableMapping, Optional, Sequence, Set

import requests
from pyspark.sql import Row, SparkSession
from pyspark.sql.datasource import (
    DataSource,
    DataSourceReader,
    DataSourceWriter,
    InputPartition,
    WriterCommitMessage,
)
from pyspark.sql.types import StructType
from pyspark.sql.types import _infer_schema, _merge_type

_LOGGER = logging.getLogger(__name__)

QDRANT_CONFIG_OPTION = "spark.fuse.qdrant.config"
QDRANT_SCHEMA_OPTION = "spark.fuse.qdrant.schema"
QDRANT_FORMAT = "spark-fuse-qdrant"
_REGISTERED_SESSIONS: set[str] = set()
_DEFAULT_PAGE_SIZE = 128


def register_qdrant_data_source(spark: SparkSession) -> None:
    session_id = spark.sparkContext.applicationId
    if session_id in _REGISTERED_SESSIONS:
        return
    spark.dataSource.register(QdrantDataSource)
    _REGISTERED_SESSIONS.add(session_id)


def _normalize_jsonable(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(k): _normalize_jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_normalize_jsonable(v) for v in value]
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    return str(value)


def _validate_http_url(value: str) -> bool:
    return isinstance(value, str) and value.startswith(("http://", "https://"))


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


def _points_url(endpoint: str, collection: str) -> str:
    base = endpoint.rstrip("/")
    return f"{base}/collections/{collection}/points"


def _should_include_vectors(option: Any) -> bool:
    return bool(option)


def _should_include_payload(option: Any) -> bool:
    if isinstance(option, bool):
        return option
    return option is not False and option is not None


def _coerce_float(value: Any) -> float:
    """Convert numeric-like values to float, raising a clear error for invalid entries."""

    if isinstance(value, (float, int, Decimal)):
        return float(value)
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            raise ValueError("Vector entries cannot be empty strings")
        try:
            return float(stripped)
        except ValueError as exc:
            raise TypeError(f"Vector entries must be numeric; got string '{value}'") from exc
    if hasattr(value, "item"):  # numpy scalar
        try:
            return float(value)
        except Exception:
            pass
    raise TypeError(f"Vector entries must be numeric; got {type(value).__name__}: {value}")


def _normalize_vector_value(vector: Any) -> Any:
    """Normalize vectors for Qdrant: sequences of numbers or mapping of named vectors."""

    # Spark MLlib DenseVector / SparseVector expose toArray()
    if hasattr(vector, "toArray"):
        try:
            vector = vector.toArray().tolist()
        except Exception:
            vector = vector.toArray()
    elif hasattr(vector, "tolist") and not isinstance(vector, (str, bytes, bytearray)):
        # numpy arrays, pandas objects
        try:
            vector = vector.tolist()
        except Exception:
            pass

    if isinstance(vector, Mapping):
        return {str(k): _normalize_vector_value(v) for k, v in vector.items()}

    if isinstance(vector, Sequence) and not isinstance(vector, (str, bytes, bytearray)):
        return [_coerce_float(v) for v in vector]

    raise TypeError(
        "Vector must be a sequence of numbers (or mapping of named vectors); "
        f"got {type(vector).__name__}"
    )


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


class QdrantDataSource(DataSource):
    """Python data source that scrolls a Qdrant collection."""

    @classmethod
    def name(cls) -> str:  # pragma: no cover - trivial accessor
        return QDRANT_FORMAT

    def __init__(self, options: Mapping[str, str]) -> None:
        super().__init__(options)
        raw_config = options.get(QDRANT_CONFIG_OPTION)
        if not raw_config:
            raise ValueError("Qdrant data source requires the config option to be provided")
        self._raw_config = json.loads(raw_config)
        schema_json = options.get(QDRANT_SCHEMA_OPTION)
        self._user_schema = StructType.fromJson(json.loads(schema_json)) if schema_json else None
        self._schema_cache: Optional[StructType] = None
        self._read_config: Optional[_QdrantResolvedConfig] = None
        self._write_config: Optional["_QdrantWriteConfig"] = None

    def schema(self) -> StructType:
        if self._user_schema is not None:
            return self._user_schema
        config = self._get_read_config()
        if not config.infer_schema:
            raise ValueError("infer_schema is disabled; provide an explicit schema")
        if self._schema_cache is None:
            self._schema_cache = _infer_schema_from_points(config)
        return self._schema_cache

    def reader(self, schema: StructType) -> QdrantDataSourceReader:
        return QdrantDataSourceReader(self._get_read_config(), schema)

    def writer(self, schema: StructType, overwrite: bool) -> "_QdrantDataSourceWriter":
        return _QdrantDataSourceWriter(self._get_write_config())

    def _get_read_config(self) -> _QdrantResolvedConfig:
        if self._read_config is None:
            self._read_config = _QdrantResolvedConfig.from_dict(self._raw_config)
        return self._read_config

    def _get_write_config(self) -> "_QdrantWriteConfig":
        if self._write_config is None:
            self._write_config = _QdrantWriteConfig.from_dict(self._raw_config)
        return self._write_config


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


@dataclass
class _QdrantWriteConfig:
    endpoint: str
    collection: str
    api_key: Optional[str]
    headers: Mapping[str, str]
    timeout: float
    max_retries: int
    backoff_factor: float
    batch_size: int
    wait: bool
    id_field: Optional[str]
    vector_field: str
    payload_fields: Optional[Sequence[str]]
    create_collection: bool
    distance: str
    payload_format: str
    write_method: str

    @staticmethod
    def from_dict(data: Mapping[str, Any]) -> "_QdrantWriteConfig":
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
        batch_size = int(data.get("batch_size", 128))
        if batch_size <= 0:
            raise ValueError("batch_size must be a positive integer")
        wait = bool(data.get("wait", True))

        id_field = data.get("id_field", "id")
        if id_field is not None:
            id_field = str(id_field).strip()
            if not id_field:
                id_field = None

        vector_field = str(data.get("vector_field", "vector"))
        if not vector_field:
            raise ValueError("vector_field must be provided for Qdrant writes")

        payload_fields_value = data.get("payload_fields")
        if payload_fields_value is not None:
            if isinstance(payload_fields_value, str):
                payload_fields_value = [payload_fields_value]
            elif isinstance(payload_fields_value, Sequence):
                payload_fields_value = [str(v) for v in payload_fields_value]
            else:
                raise TypeError("payload_fields must be a string or sequence when provided")

        create_collection = bool(data.get("create_collection", False))
        distance = str(data.get("distance", "Cosine"))
        payload_format = str(data.get("payload_format", "auto")).lower()
        if payload_format not in {"auto", "points", "batch"}:
            raise ValueError("payload_format must be one of: auto, points, batch")
        write_method = str(data.get("write_method", "auto")).lower()
        if write_method not in {"auto", "post", "put"}:
            raise ValueError("write_method must be one of: auto, post, put")

        return _QdrantWriteConfig(
            endpoint=endpoint_str,
            collection=collection,
            api_key=api_key,
            headers=headers_value,
            timeout=timeout,
            max_retries=max_retries,
            backoff_factor=backoff_factor,
            batch_size=batch_size,
            wait=wait,
            id_field=id_field,
            vector_field=vector_field,
            payload_fields=payload_fields_value,
            create_collection=create_collection,
            distance=distance,
            payload_format=payload_format,
            write_method=write_method,
        )


def _perform_points_request(
    session: requests.Session,
    url: str,
    payload: Mapping[str, Any],
    *,
    timeout: float,
    max_retries: int,
    backoff_factor: float,
    method: str = "POST",
) -> Mapping[str, Any]:
    attempts = max(max_retries, 0) + 1
    last_error_detail: Optional[str] = None
    for attempt in range(attempts):
        try:
            response = session.request(method, url, json=payload, timeout=timeout)
            if 200 <= response.status_code < 300:
                try:
                    return response.json()
                except ValueError as exc:
                    raise ValueError(f"Failed to decode Qdrant response JSON: {exc}") from exc
            try:
                body_preview = response.text[:500]
            except Exception:
                body_preview = "<response body unavailable>"
            last_error_detail = f"HTTP {response.status_code}; body preview: {body_preview}"
            _LOGGER.warning(
                "Qdrant points write returned HTTP %s for %s (attempt %s/%s)",
                response.status_code,
                url,
                attempt + 1,
                attempts,
            )
        except requests.RequestException as exc:
            last_error_detail = str(exc)
            _LOGGER.warning(
                "Qdrant points request failed on attempt %s/%s: %s",
                attempt + 1,
                attempts,
                exc,
            )
        if attempt < attempts - 1:
            delay = backoff_factor * (2**attempt)
            if delay > 0:
                time.sleep(delay)
    error_message = f"Qdrant points write failed after {attempts} attempts for {url}"
    if last_error_detail:
        error_message = f"{error_message} (last error: {last_error_detail})"
    raise RuntimeError(error_message)


def _perform_collection_request(
    session: requests.Session,
    method: str,
    url: str,
    *,
    json_body: Optional[Mapping[str, Any]] = None,
    timeout: float,
) -> requests.Response:
    response = session.request(method, url, json=json_body, timeout=timeout)
    return response


def _vectors_payload_from_point(point: Mapping[str, Any], *, distance: str) -> Mapping[str, Any]:
    vector = point.get("vector")
    if isinstance(vector, Mapping):
        vectors: Dict[str, Any] = {}
        for name, value in vector.items():
            if not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray)):
                raise TypeError(
                    "Named vectors must be sequences of numbers; "
                    f"got {type(value).__name__} for '{name}'"
                )
            if len(value) == 0:
                raise ValueError(f"Named vector '{name}' cannot be empty")
            vectors[str(name)] = {"size": len(value), "distance": distance}
        if not vectors:
            raise ValueError("No named vectors provided for collection creation")
        return vectors

    if isinstance(vector, Sequence) and not isinstance(vector, (str, bytes, bytearray)):
        if len(vector) == 0:
            raise ValueError("Vector cannot be empty for collection creation")
        return {"size": len(vector), "distance": distance}

    raise TypeError("Unable to derive vectors schema from provided point")


def _ensure_collection_exists(
    session: requests.Session,
    config: _QdrantWriteConfig,
    sample_point: Mapping[str, Any],
) -> None:
    if not config.create_collection:
        return

    url = f"{config.endpoint}/collections/{config.collection}"
    response = _perform_collection_request(session, "GET", url, timeout=config.timeout)
    if response.status_code < 300:
        return
    if response.status_code != 404:
        body = response.text[:200] if response.text else ""
        raise RuntimeError(
            f"Failed to check Qdrant collection '{config.collection}': "
            f"HTTP {response.status_code} {body}"
        )

    vectors_payload = _vectors_payload_from_point(sample_point, distance=config.distance)
    create_payload: Dict[str, Any] = {"vectors": vectors_payload}
    _LOGGER.info(
        "Creating Qdrant collection '%s' with vectors schema derived from first record",
        config.collection,
    )
    response = _perform_collection_request(
        session,
        "PUT",
        url,
        json_body=create_payload,
        timeout=config.timeout,
    )
    if not (200 <= response.status_code < 300):
        body = response.text[:500] if response.text else ""
        raise RuntimeError(
            f"Failed to create Qdrant collection '{config.collection}': "
            f"HTTP {response.status_code} {body}"
        )


def _build_points_batch_payload(
    batch: Sequence[Mapping[str, Any]],
    *,
    wait: bool,
) -> Dict[str, Any]:
    """Construct the Qdrant PointsBatch payload (ids/vectors/payloads arrays)."""

    ids: list[Any] = []
    vectors: list[Any] = []
    payloads: list[Any] = []
    for point in batch:
        ids.append(point.get("id"))
        vectors.append(point.get("vector"))
        payloads.append(point.get("payload"))

    batch_payload: Dict[str, Any] = {"ids": ids, "vectors": vectors}
    if any(p is not None for p in payloads):
        batch_payload["payloads"] = payloads

    return {"batch": batch_payload, "wait": wait}


def _build_flat_batch_payload(
    batch: Sequence[Mapping[str, Any]],
    *,
    wait: bool,
) -> Dict[str, Any]:
    """Construct legacy-compatible batch payload without the 'batch' envelope."""

    ids: list[Any] = []
    vectors: list[Any] = []
    payloads: list[Any] = []
    for point in batch:
        ids.append(point.get("id"))
        vectors.append(point.get("vector"))
        payloads.append(point.get("payload"))

    payload: Dict[str, Any] = {"ids": ids, "vectors": vectors, "wait": wait}
    if any(p is not None for p in payloads):
        payload["payloads"] = payloads
    return payload


def _extract_payload_fields(
    record: Mapping[str, Any],
    *,
    id_field: Optional[str],
    vector_field: str,
    payload_fields: Optional[Sequence[str]],
) -> Mapping[str, Any]:
    payload: Dict[str, Any] = {}
    if payload_fields is None:
        skip: Set[str] = {vector_field}
        if id_field:
            skip.add(id_field)
        for key, value in record.items():
            if key in skip:
                continue
            payload[key] = value
    else:
        for key in payload_fields:
            if key in record:
                payload[key] = record[key]
    return payload


def _point_from_record(record: Mapping[str, Any], config: _QdrantWriteConfig) -> Dict[str, Any]:
    vector_raw = record.get(config.vector_field)
    if vector_raw is None:
        raise ValueError(f"Missing vector field '{config.vector_field}' in record: {record}")
    try:
        vector = _normalize_vector_value(vector_raw)
    except Exception as exc:
        raise TypeError(f"Failed to normalize vector field '{config.vector_field}': {exc}") from exc

    point: Dict[str, Any] = {"vector": vector}
    if config.id_field:
        if config.id_field not in record:
            raise ValueError(f"Missing id field '{config.id_field}' in record: {record}")
        if record[config.id_field] is None:
            raise ValueError(f"ID field '{config.id_field}' cannot be null for Qdrant writes")
        point["id"] = record[config.id_field]
    payload = _extract_payload_fields(
        record,
        id_field=config.id_field,
        vector_field=config.vector_field,
        payload_fields=config.payload_fields,
    )
    if payload:
        point["payload"] = _normalize_jsonable(payload)
    return point


def _write_points_iter(records: Iterable[Mapping[str, Any]], config: _QdrantWriteConfig) -> int:
    session = requests.Session()
    session.headers.update(config.headers)
    if config.api_key:
        session.headers.setdefault("api-key", config.api_key)
    url = _points_url(config.endpoint, config.collection)
    batch: list[Dict[str, Any]] = []
    total = 0
    collection_checked = False
    try:
        for record in records:
            batch.append(_point_from_record(record, config))
            if config.create_collection and not collection_checked:
                _ensure_collection_exists(session, config, batch[-1])
                collection_checked = True
            if len(batch) >= config.batch_size:
                _send_points_batch(session, url, batch, config)
                total += len(batch)
                batch.clear()
        if batch:
            _send_points_batch(session, url, batch, config)
            total += len(batch)
    finally:
        session.close()
    return total


def _send_points_batch(
    session: requests.Session,
    url: str,
    batch: Sequence[Mapping[str, Any]],
    config: _QdrantWriteConfig,
) -> None:
    # Build payload variants
    points_payload = {"points": list(batch), "wait": config.wait}
    batch_payload = _build_points_batch_payload(batch, wait=config.wait)
    flat_batch_payload = _build_flat_batch_payload(batch, wait=config.wait)

    # Determine payload attempt order
    if config.payload_format == "points":
        payload_attempts = [("points", points_payload)]
    elif config.payload_format == "batch":
        payload_attempts = [
            ("batch", batch_payload),
            ("flat-batch", flat_batch_payload),
            ("points", points_payload),
        ]
    else:  # auto
        payload_attempts = [
            ("points", points_payload),
            ("batch", batch_payload),
            ("flat-batch", flat_batch_payload),
        ]

    # Determine HTTP method order
    if config.write_method == "post":
        method_attempts = ["POST"]
    elif config.write_method == "put":
        method_attempts = ["PUT"]
    else:  # auto
        method_attempts = ["PUT", "POST"]

    last_exc: Optional[RuntimeError] = None
    response: Optional[Mapping[str, Any]] = None

    for method in method_attempts:
        for label, payload in payload_attempts:
            try:
                response = _perform_points_request(
                    session,
                    url,
                    payload,
                    timeout=config.timeout,
                    max_retries=config.max_retries,
                    backoff_factor=config.backoff_factor,
                    method=method,
                )
                _LOGGER.info(
                    "Qdrant write succeeded with method=%s payload_format=%s", method, label
                )
                break
            except RuntimeError as exc:
                message = str(exc).lower()
                if "missing field `ids`" in message:
                    _LOGGER.warning(
                        "Qdrant rejected %s payload via %s as missing ids; trying next payload format",
                        label,
                        method,
                    )
                    last_exc = exc
                    continue
                last_exc = exc
                # If method is auto, try next method; otherwise, re-raise
                if config.write_method == "auto" and method != method_attempts[-1]:
                    continue
                raise
        if response is not None:
            break

    if response is None:
        if last_exc:
            raise last_exc
        raise RuntimeError("Qdrant points write failed: no payload attempt succeeded")

    status = response.get("status")
    if status and str(status).lower() != "ok":
        raise RuntimeError(f"Qdrant returned a non-ok status: {status}")


class _QdrantDataSourceWriter(DataSourceWriter):
    def __init__(self, config: _QdrantWriteConfig) -> None:
        self._config = config

    def write(self, iterator: Iterator[Row]) -> WriterCommitMessage:
        _write_points_iter((row.asDict(recursive=True) for row in iterator), self._config)
        return WriterCommitMessage()

    def commit(self, messages: Sequence[Optional[WriterCommitMessage]]) -> None:
        return

    def abort(self, messages: Sequence[Optional[WriterCommitMessage]]) -> None:
        return


def write_qdrant_points(
    records: Iterable[Mapping[str, Any]],
    endpoint: Any,
    *,
    collection: str,
    id_field: Optional[str] = "id",
    vector_field: str = "vector",
    payload_fields: Optional[Sequence[str]] = None,
    wait: bool = True,
    batch_size: int = 128,
    api_key: Optional[str] = None,
    headers: Optional[Mapping[str, str]] = None,
    timeout: float = 30.0,
    max_retries: int = 3,
    backoff_factor: float = 0.5,
    create_collection: bool = False,
    distance: str = "Cosine",
    payload_format: str = "auto",
    write_method: str = "auto",
) -> int:
    """Write an iterable of records to a Qdrant collection via the HTTP API."""

    config_dict = {
        "endpoint": endpoint,
        "collection": collection,
        "api_key": api_key,
        "headers": headers or {},
        "timeout": timeout,
        "max_retries": max_retries,
        "backoff_factor": backoff_factor,
        "batch_size": batch_size,
        "wait": wait,
        "id_field": id_field,
        "vector_field": vector_field,
        "payload_fields": payload_fields,
        "create_collection": create_collection,
        "distance": distance,
        "payload_format": payload_format,
        "write_method": write_method,
    }
    config = _QdrantWriteConfig.from_dict(config_dict)
    return _write_points_iter(records, config)


def build_qdrant_write_config(
    endpoint: Any,
    *,
    collection: str,
    id_field: Optional[str] = "id",
    vector_field: str = "vector",
    payload_fields: Optional[Sequence[str]] = None,
    wait: bool = True,
    batch_size: int = 128,
    api_key: Optional[str] = None,
    headers: Optional[Mapping[str, str]] = None,
    timeout: float = 30.0,
    max_retries: int = 3,
    backoff_factor: float = 0.5,
    create_collection: bool = False,
    distance: str = "Cosine",
    payload_format: str = "auto",
    write_method: str = "auto",
    **overrides: Any,
) -> Dict[str, Any]:
    """Build the config payload used for Qdrant writes (DataFrameWriter options)."""

    config: Dict[str, Any] = {}
    for mapping in (overrides,):
        if mapping:
            config.update(mapping)

    config["endpoint"] = endpoint
    config["collection"] = collection
    config["api_key"] = api_key
    config["headers"] = headers or {}
    config["timeout"] = timeout
    config["max_retries"] = max_retries
    config["backoff_factor"] = backoff_factor
    config["batch_size"] = batch_size
    config["wait"] = wait
    config["id_field"] = id_field
    config["vector_field"] = vector_field
    config["payload_fields"] = payload_fields
    config["create_collection"] = create_collection
    config["distance"] = distance
    config["payload_format"] = payload_format
    config["write_method"] = write_method

    # Validate by constructing the resolved config; return raw dict for JSON serialization.
    _QdrantWriteConfig.from_dict(config)
    return config


__all__ = [
    "QdrantDataSource",
    "register_qdrant_data_source",
    "build_qdrant_config",
    "build_qdrant_write_config",
    "write_qdrant_points",
    "QDRANT_FORMAT",
    "QDRANT_CONFIG_OPTION",
    "QDRANT_SCHEMA_OPTION",
]
