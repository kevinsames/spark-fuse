"""SPARQL connector utilities for loading RDF query results via HTTP endpoints.

This module implements ``SPARQLReader`` which executes SPARQL queries against HTTP(S)
endpoints that expose the SPARQL Protocol and parses JSON results into Spark DataFrames.
"""

from __future__ import annotations

import logging
import time
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

import requests
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    BooleanType,
    DataType,
    DoubleType,
    LongType,
    StructField,
    StructType,
    StringType,
)

from .base import Connector
from .registry import register_connector


_LOGGER = logging.getLogger(__name__)
_DEFAULT_ACCEPT = "application/sparql-results+json"
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


def _extract_value(
    entry: Any,
    *,
    coerce_types: bool,
) -> Any:
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
) -> Tuple[List[Dict[str, Any]], List[str]]:
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
    endpoint: str,
    query: str,
    *,
    timeout: float,
    max_retries: int,
    backoff_factor: float,
    method: str,
    query_param: str,
    params: Mapping[str, Any],
    payload_mode: str,
) -> Mapping[str, Any]:
    attempts = max(int(max_retries), 0) + 1
    method = method.upper()
    payload_mode = payload_mode.lower()

    for attempt in range(attempts):
        try:
            request_kwargs: Dict[str, Any] = {"timeout": timeout}

            if method == "GET":
                get_params = dict(params)
                get_params[query_param] = query
                request_kwargs["params"] = get_params
                response = session.get(endpoint, **request_kwargs)
            else:
                if payload_mode == "json":
                    body = dict(params)
                    body[query_param] = query
                    request_kwargs["json"] = body
                elif payload_mode == "raw":
                    request_kwargs["data"] = query
                else:
                    body = dict(params)
                    body[query_param] = query
                    request_kwargs["data"] = body
                response = session.post(endpoint, **request_kwargs)

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
            delay = backoff_factor * (2**attempt)
            if delay > 0:
                time.sleep(delay)

    raise RuntimeError(f"SPARQL request to {endpoint} failed after {attempts} attempts")


@register_connector
class SPARQLReader(Connector):
    """Execute SPARQL queries against HTTP endpoints and load JSON results into Spark.

    The reader supports both ``GET`` and ``POST`` interactions with SPARQL services,
    applies configurable retry/backoff policies, and exposes helpers for optional
    literal type coercion and metadata capture. Responses are normalised into Spark
    rows so downstream transforms can work with RDF-derived datasets using standard
    DataFrame operations.
    """

    name = "sparql"

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
        """Execute one or more SPARQL queries and convert the results to a DataFrame.

        Parameters
        ----------
        spark:
            Active :class:`pyspark.sql.SparkSession` used to materialise the result rows.
        source:
            Either the endpoint URL string or a mapping that can include ``endpoint``,
            ``query`` / ``queries`` and other configuration values.
        fmt:
            Unused; provided to maintain a uniform interface with other connectors.
        schema:
            Optional schema to enforce on the output. When supplied, missing fields in
            the decoded rows are filled with ``None`` and Spark bypasses inference.
        source_config / options / kwargs:
            Additional settings that are merged together. Supported keys include
            ``query`` / ``queries``, ``params``, ``headers``, ``auth``,
            ``request_type``, ``payload_mode``, ``query_param``, ``include_metadata``,
            ``coerce_types``, retry controls, and Spark reader options.
        headers:
            Extra HTTP headers to apply on top of any supplied in ``source_config``.

        Returns
        -------
        pyspark.sql.DataFrame
            A DataFrame containing rows mapped from the SPARQL JSON results or ASK
            boolean payload.
        """
        config: Dict[str, Any] = {}
        if source_config:
            config.update(source_config)
        if options:
            config.update(options)
        if kwargs:
            config.update(kwargs)

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
            raise TypeError("SPARQL source must be a string endpoint or a mapping of configuration")

        endpoint = endpoint or config.get("endpoint") or config.get("url")
        if not isinstance(endpoint, str) or not self.validate_path(endpoint):
            raise ValueError("SPARQL reader requires an HTTP(S) endpoint URL")

        if "query" in config:
            queries.append(str(config["query"]))
        if "queries" in config:
            queries.extend([str(q) for q in _as_sequence(config["queries"])])

        queries = [q for q in (query.strip() for query in queries) if q]
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

        session = requests.Session()
        session.headers.update(base_headers)

        auth = config.get("auth")
        if isinstance(auth, Sequence) and len(auth) == 2:
            session.auth = (str(auth[0]), str(auth[1]))

        all_rows: List[Dict[str, Any]] = []
        column_order: List[str] = []

        try:
            for query in queries:
                payload = _perform_request(
                    session,
                    endpoint,
                    query,
                    timeout=request_timeout,
                    max_retries=max_retries,
                    backoff_factor=backoff_factor,
                    method=request_type,
                    query_param=query_param,
                    params=base_params,
                    payload_mode=payload_mode,
                )

                rows, columns = _parse_results(
                    payload,
                    include_metadata=include_metadata,
                    metadata_suffix=metadata_suffix,
                    coerce_types=coerce_types,
                )

                for column in columns:
                    if column not in column_order:
                        column_order.append(column)

                all_rows.extend(rows)
        finally:
            session.close()

        if schema is not None:
            schema_columns = [field.name for field in schema]
            normalized = [{col: row.get(col) for col in schema_columns} for row in all_rows]
            if normalized:
                return spark.createDataFrame(normalized, schema)
            empty_rdd = spark.sparkContext.emptyRDD()
            return spark.createDataFrame(empty_rdd, schema)

        if not column_order:
            column_order = sorted({column for row in all_rows for column in row.keys()})

        normalized_rows = [{col: row.get(col) for col in column_order} for row in all_rows]
        result_schema = _build_schema_from_rows(column_order, all_rows, metadata_suffix)

        if normalized_rows:
            return spark.createDataFrame(normalized_rows, schema=result_schema)

        empty_rdd = spark.sparkContext.emptyRDD()
        return spark.createDataFrame(empty_rdd, result_schema)

    def write(
        self,
        df: DataFrame,
        path: str,
        *,
        fmt: Optional[str] = None,
        mode: str = "error",
        **options: Any,
    ) -> None:
        raise NotImplementedError("SPARQLReader does not support write operations")


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


def _build_schema_from_rows(
    column_order: Sequence[str],
    rows: Sequence[Mapping[str, Any]],
    metadata_suffix: str,
) -> StructType:
    fields: list[StructField] = []
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


__all__ = ["SPARQLReader"]
