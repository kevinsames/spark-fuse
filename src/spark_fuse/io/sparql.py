"""SPARQL data source implemented on top of PySpark's Data Source API."""

from __future__ import annotations

import json
import logging
import math
import time
from dataclasses import dataclass
from typing import Any, Dict, Iterable, Iterator, List, Mapping, Optional, Sequence

import requests
from pyspark.sql import Row, SparkSession
from pyspark.sql.datasource import DataSource, DataSourceReader, InputPartition
from pyspark.sql.types import (
    BooleanType,
    DataType,
    DoubleType,
    LongType,
    StructField,
    StructType,
    StringType,
)

_LOGGER = logging.getLogger(__name__)
_DEFAULT_ACCEPT = "application/sparql-results+json"
SPARQL_CONFIG_OPTION = "spark.fuse.sparql.config"
SPARQL_SCHEMA_OPTION = "spark.fuse.sparql.schema"
_REGISTERED_SESSIONS: set[str] = set()
SPARQL_DATA_SOURCE_NAME = "spark-fuse-sparql"
_METADATA_KEYS = ("type", "datatype", "xml:lang")
_NUMERIC_TYPES = {
    "integer",
    "int",
    "long",
    "short",
    "byte",
    "nonpositiveinteger",
    "negativeinteger",
    "nonnegativeinteger",
    "positiveinteger",
    "unsignedbyte",
    "unsignedshort",
    "unsignedint",
    "unsignedlong",
}
_FLOATING_TYPES = {"decimal", "double", "float"}


def register_sparql_data_source(spark: SparkSession) -> None:
    session_id = spark.sparkContext.applicationId
    if session_id in _REGISTERED_SESSIONS:
        return
    spark.dataSource.register(SPARQLDataSource)
    _REGISTERED_SESSIONS.add(session_id)


def _coerce_literal(value: str, datatype: Optional[str]) -> Optional[Any]:
    if not datatype:
        return None
    dt = datatype.lower()
    if "#" in dt:
        dt = dt.split("#", 1)[1]

    if dt == "boolean":
        lowered = value.strip().lower()
        if lowered in {"true", "1"}:
            return True
        if lowered in {"false", "0"}:
            return False
        return None

    if dt in _NUMERIC_TYPES:
        try:
            return int(value)
        except (TypeError, ValueError):
            return None

    if dt in _FLOATING_TYPES:
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    return None


def _extract_value(entry: Any, *, coerce_types: bool) -> Any:
    if not isinstance(entry, Mapping):
        return entry
    if "value" not in entry:
        return None
    value = entry["value"]
    if coerce_types:
        coerced = _coerce_literal(str(value), entry.get("datatype"))
        if coerced is not None:
            return coerced
    return value


def _parse_results(
    payload: Mapping[str, Any],
    *,
    include_metadata: bool,
    metadata_suffix: str,
    coerce_types: bool,
) -> tuple[List[Dict[str, Any]], List[str]]:
    if "results" in payload:
        head_vars = payload.get("head", {}).get("vars", [])
        ordered_columns: List[str] = []
        if isinstance(head_vars, Sequence):
            for column in head_vars:
                if isinstance(column, str) and column not in ordered_columns:
                    ordered_columns.append(column)

        bindings = payload.get("results", {}).get("bindings", [])
        rows: List[Dict[str, Any]] = []
        if not isinstance(bindings, Sequence):
            bindings = []

        for binding in bindings:
            if not isinstance(binding, Mapping):
                continue
            row: Dict[str, Any] = {}
            for column in ordered_columns:
                row[column] = None

            for var_name, entry in binding.items():
                column_name = str(var_name)
                row[column_name] = _extract_value(entry, coerce_types=coerce_types)
                if column_name not in ordered_columns:
                    ordered_columns.append(column_name)

                if include_metadata and isinstance(entry, Mapping):
                    for meta_key in _METADATA_KEYS:
                        meta_column = f"{column_name}{metadata_suffix}{meta_key}"
                        row[meta_column] = entry.get(meta_key)
                        if meta_column not in ordered_columns:
                            ordered_columns.append(meta_column)

            rows.append(row)

        return rows, ordered_columns

    if "boolean" in payload:
        boolean_value = payload["boolean"]
        if isinstance(boolean_value, str):
            boolean_value = boolean_value.strip().lower() in {"true", "1"}
        else:
            boolean_value = bool(boolean_value)
        return [{"boolean": boolean_value}], ["boolean"]

    raise ValueError("SPARQL response must contain either 'results' or 'boolean'")


def _perform_request(
    session: requests.Session,
    config: "_SPARQLResolvedConfig",
    query: str,
) -> Mapping[str, Any]:
    attempts = max(config.max_retries, 0) + 1
    method = config.request_type
    payload_mode = config.payload_mode

    for attempt in range(attempts):
        try:
            request_kwargs: Dict[str, Any] = {"timeout": config.timeout}

            if method == "GET":
                get_params = dict(config.params)
                get_params[config.query_param] = query
                request_kwargs["params"] = get_params
                response = session.get(config.endpoint, **request_kwargs)
            else:
                if payload_mode == "json":
                    body = dict(config.params)
                    body[config.query_param] = query
                    request_kwargs["json"] = body
                elif payload_mode == "raw":
                    request_kwargs["data"] = query
                else:
                    body = dict(config.params)
                    body[config.query_param] = query
                    request_kwargs["data"] = body
                response = session.post(config.endpoint, **request_kwargs)

            if 200 <= response.status_code < 300:
                try:
                    return response.json()
                except ValueError as exc:
                    raise ValueError(f"Failed to decode SPARQL response JSON: {exc}") from exc

            _LOGGER.warning(
                "SPARQL endpoint returned HTTP %s for request (attempt %s/%s)",
                response.status_code,
                attempt + 1,
                attempts,
            )
        except requests.RequestException as exc:
            _LOGGER.warning(
                "SPARQL request failed on attempt %s/%s: %s",
                attempt + 1,
                attempts,
                exc,
            )

        if attempt < attempts - 1:
            delay = config.backoff_factor * (2**attempt)
            if delay > 0:
                time.sleep(delay)

    raise RuntimeError(f"SPARQL request to {config.endpoint} failed after {attempts} attempts")


def _collect_rows(
    config: "_SPARQLResolvedConfig",
    queries: Sequence[str],
) -> tuple[List[Dict[str, Any]], List[str]]:
    rows: List[Dict[str, Any]] = []
    column_order: List[str] = []
    session = requests.Session()
    session.headers.update(config.headers)
    if config.auth:
        session.auth = tuple(config.auth)  # type: ignore[assignment]
    try:
        for query in queries:
            payload = _perform_request(session, config, query)
            query_rows, columns = _parse_results(
                payload,
                include_metadata=config.include_metadata,
                metadata_suffix=config.metadata_suffix,
                coerce_types=config.coerce_types,
            )
            for column in columns:
                if column not in column_order:
                    column_order.append(column)
            rows.extend(query_rows)
    finally:
        session.close()
    return rows, column_order


def _build_schema_from_rows(
    column_order: Sequence[str],
    rows: Sequence[Mapping[str, Any]],
    metadata_suffix: str,
) -> StructType:
    fields: List[StructField] = []
    for column in column_order:
        values = [row.get(column) for row in rows if column in row]
        data_type = _infer_spark_type(column, values, metadata_suffix)
        fields.append(StructField(column, data_type, True))
    return StructType(fields)


def _infer_spark_type(column: str, values: Sequence[Any], metadata_suffix: str) -> DataType:
    if any(column.endswith(f"{metadata_suffix}{suffix}") for suffix in _METADATA_KEYS):
        return StringType()

    for value in values:
        if value is None:
            continue
        if isinstance(value, bool):
            return BooleanType()
        if isinstance(value, int) and not isinstance(value, bool):
            return LongType()
        if isinstance(value, float):
            return DoubleType()
        return StringType()
    return StringType()


def _chunk_queries(queries: Sequence[str], parallelism: int) -> List[List[str]]:
    if not queries:
        return []
    parallelism = max(parallelism, 1)
    chunk_size = max(int(math.ceil(len(queries) / parallelism)), 1)
    return [list(queries[idx : idx + chunk_size]) for idx in range(0, len(queries), chunk_size)]


@dataclass
class _SPARQLResolvedConfig:
    endpoint: str
    queries: List[str]
    params: Mapping[str, Any]
    headers: Mapping[str, str]
    auth: Optional[Sequence[str]]
    request_type: str
    payload_mode: str
    query_param: str
    include_metadata: bool
    metadata_suffix: str
    coerce_types: bool
    timeout: float
    max_retries: int
    backoff_factor: float
    parallelism: int

    @staticmethod
    def from_dict(data: Mapping[str, Any]) -> "_SPARQLResolvedConfig":
        endpoint = str(data["endpoint"])
        queries_value = data.get("queries") or []
        if isinstance(queries_value, str):
            queries = [queries_value]
        else:
            queries = [str(q) for q in queries_value]
        params = data.get("params")
        if not isinstance(params, Mapping):
            params = {}
        headers = data.get("headers")
        if isinstance(headers, Mapping):
            header_map = {str(k): str(v) for k, v in headers.items()}
        else:
            header_map = {}
        auth_value = data.get("auth")
        auth: Optional[Sequence[str]]
        if isinstance(auth_value, Sequence) and len(auth_value) == 2:
            auth = [str(auth_value[0]), str(auth_value[1])]
        else:
            auth = None
        request_type = str(data.get("request_type", "POST")).upper()
        payload_mode = str(data.get("payload_mode", "form")).lower()
        query_param = str(data.get("query_param", "query"))
        include_metadata = bool(data.get("include_metadata", False))
        metadata_suffix = str(data.get("metadata_suffix", "__"))
        coerce_types = bool(data.get("coerce_types", True))
        timeout = float(data.get("timeout", 30.0))
        max_retries = int(data.get("max_retries", 3))
        backoff_factor = float(data.get("backoff_factor", 0.5))
        parallelism = max(int(data.get("parallelism", len(queries) or 1)), 1)
        return _SPARQLResolvedConfig(
            endpoint=endpoint,
            queries=queries,
            params=params,
            headers=header_map,
            auth=auth,
            request_type=request_type,
            payload_mode=payload_mode,
            query_param=query_param,
            include_metadata=include_metadata,
            metadata_suffix=metadata_suffix,
            coerce_types=coerce_types,
            timeout=timeout,
            max_retries=max_retries,
            backoff_factor=backoff_factor,
            parallelism=parallelism,
        )


class _SPARQLInputPartition(InputPartition):
    def __init__(self, queries: Sequence[str]):
        super().__init__(list(queries))


class SPARQLDataSourceReader(DataSourceReader):
    def __init__(
        self,
        config: _SPARQLResolvedConfig,
        schema: StructType,
        partitions: Sequence[_SPARQLInputPartition],
    ) -> None:
        self._config = config
        self._schema = schema
        self._partitions = list(partitions)
        self._field_names = schema.fieldNames()

    def partitions(self) -> Sequence[InputPartition]:
        return self._partitions

    def read(self, partition: InputPartition) -> Iterator[Dict[str, Any]]:
        queries = partition.value if partition is not None else None
        if not queries:
            return iter(())
        rows, _ = _collect_rows(self._config, queries)
        return (self._dict_to_row(row) for row in rows)

    def _dict_to_row(self, row: Mapping[str, Any]) -> Row:
        data = {name: row.get(name) for name in self._field_names}
        return Row(**data)


class SPARQLDataSource(DataSource):
    @classmethod
    def name(cls) -> str:  # pragma: no cover - trivial accessor
        return SPARQL_DATA_SOURCE_NAME

    def __init__(self, options: Mapping[str, str]) -> None:
        super().__init__(options)
        raw_config = options.get(SPARQL_CONFIG_OPTION)
        if not raw_config:
            raise ValueError("SPARQL data source requires the config option to be provided")
        self._config = _SPARQLResolvedConfig.from_dict(json.loads(raw_config))
        schema_json = options.get(SPARQL_SCHEMA_OPTION)
        self._user_schema = StructType.fromJson(json.loads(schema_json)) if schema_json else None
        self._schema_cache: Optional[StructType] = None
        self._partitions: Optional[List[_SPARQLInputPartition]] = None

    def schema(self) -> StructType:
        if self._user_schema is not None:
            return self._user_schema
        if self._schema_cache is None:
            rows, columns = _collect_rows(self._config, self._config.queries)
            self._schema_cache = _build_schema_from_rows(
                columns, rows, self._config.metadata_suffix
            )
        return self._schema_cache

    def reader(self, schema: StructType) -> SPARQLDataSourceReader:
        if self._partitions is None:
            chunks = _chunk_queries(self._config.queries, self._config.parallelism)
            self._partitions = [_SPARQLInputPartition(chunk) for chunk in chunks] or [
                _SPARQLInputPartition([])
            ]
        return SPARQLDataSourceReader(self._config, schema, self._partitions)


def _as_sequence(value: Any) -> Iterable[Any]:
    if value is None:
        return []
    if isinstance(value, (str, bytes)):
        return [value]
    if isinstance(value, Mapping):
        raise TypeError("Value must be a non-mapping sequence")
    try:
        return list(value)  # type: ignore[arg-type]
    except TypeError as exc:
        raise TypeError("Value must be convertible to a sequence") from exc


def _validate_endpoint(path: str) -> bool:
    return isinstance(path, str) and path.startswith(("http://", "https://"))


def build_sparql_config(
    spark: SparkSession,
    source: Any,
    *,
    source_config: Optional[Mapping[str, Any]] = None,
    options: Optional[Mapping[str, Any]] = None,
    headers: Optional[Mapping[str, str]] = None,
    **kwargs: Any,
) -> Dict[str, Any]:
    """Build the options payload consumed by the SPARQL data source."""

    config: Dict[str, Any] = {}
    for mapping in (source_config, options, kwargs):
        if mapping:
            config.update(mapping)

    endpoint: Optional[str] = None
    queries: List[str] = []

    if isinstance(source, Mapping):
        endpoint = source.get("endpoint") or source.get("url") or source.get("path")
        if "query" in source:
            queries.append(str(source["query"]))
        if "queries" in source:
            queries.extend([str(q) for q in _as_sequence(source["queries"])])
    elif isinstance(source, str):
        endpoint = source
    elif source is not None:
        raise TypeError("SPARQL source must be a string endpoint or configuration mapping")

    endpoint = endpoint or config.get("endpoint") or config.get("url")
    if not isinstance(endpoint, str) or not _validate_endpoint(endpoint):
        raise ValueError("SPARQL reader requires an HTTP(S) endpoint URL")

    if "query" in config:
        queries.append(str(config["query"]))
    if "queries" in config:
        queries.extend([str(q) for q in _as_sequence(config["queries"])])

    queries = [query.strip() for query in queries if isinstance(query, str) and query.strip()]
    if not queries:
        raise ValueError("SPARQL reader requires at least one query to execute")

    params = config.get("params")
    if isinstance(params, Mapping):
        base_params: Mapping[str, Any] = params
    elif params is None:
        base_params = {}
    else:
        raise TypeError("SPARQL params configuration must be a mapping if provided")

    request_type = str(config.get("request_type", "POST")).upper()
    if request_type not in {"GET", "POST"}:
        raise ValueError("SPARQL request_type must be either 'GET' or 'POST'")

    payload_mode = str(config.get("payload_mode", "form")).lower()
    if payload_mode not in {"form", "json", "raw"}:
        raise ValueError("payload_mode must be one of {'form', 'json', 'raw'}")

    query_param = str(config.get("query_param", "query"))
    request_timeout = float(config.get("request_timeout", 30.0))
    max_retries = int(config.get("max_retries", 3))
    backoff_factor = float(config.get("retry_backoff_factor", 0.5))

    include_metadata = bool(config.get("include_metadata", False))
    metadata_suffix = str(config.get("metadata_suffix", "__"))
    coerce_types = bool(config.get("coerce_types", True))

    base_headers: Dict[str, str] = {"Accept": _DEFAULT_ACCEPT}
    if payload_mode == "raw":
        base_headers.setdefault("Content-Type", "application/sparql-query")
    for header_map in (config.get("headers"), headers):
        if isinstance(header_map, Mapping):
            base_headers.update({str(k): str(v) for k, v in header_map.items()})

    auth_value = config.get("auth")
    auth = None
    if isinstance(auth_value, Sequence) and len(auth_value) == 2:
        auth = [str(auth_value[0]), str(auth_value[1])]

    parallelism = max(int(config.get("parallelism", len(queries) or 1)), 1)

    payload_config = {
        "endpoint": endpoint,
        "queries": queries,
        "params": dict(base_params),
        "headers": base_headers,
        "auth": auth,
        "request_type": request_type,
        "payload_mode": payload_mode,
        "query_param": query_param,
        "include_metadata": include_metadata,
        "metadata_suffix": metadata_suffix,
        "coerce_types": coerce_types,
        "timeout": request_timeout,
        "max_retries": max_retries,
        "backoff_factor": backoff_factor,
        "parallelism": parallelism,
    }

    return payload_config


__all__ = [
    "SPARQLDataSource",
    "SPARQLDataSourceReader",
    "build_sparql_config",
    "register_sparql_data_source",
    "SPARQL_DATA_SOURCE_NAME",
    "SPARQL_CONFIG_OPTION",
    "SPARQL_SCHEMA_OPTION",
]
