"""Qdrant write path: configuration, point building, batch sending, and writer."""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Dict, Iterable, Iterator, Mapping, Optional, Sequence, Set

import requests
from pyspark.sql import Row
from pyspark.sql.datasource import DataSourceWriter, WriterCommitMessage

from .._http import normalize_jsonable as _normalize_jsonable
from .._http import validate_http_url as _validate_http_url

_LOGGER = logging.getLogger(__name__)


def _points_url(endpoint: str, collection: str) -> str:
    base = endpoint.rstrip("/")
    return f"{base}/collections/{collection}/points"


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
    config: "_QdrantWriteConfig",
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
