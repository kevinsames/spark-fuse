from __future__ import annotations

import logging
import os
import time
from typing import Any, Dict, Iterable, Mapping, Optional, Sequence, Tuple, Union

import requests

from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import DataType, StringType

__all__ = [
    "rename_columns",
    "with_constants",
    "cast_columns",
    "normalize_whitespace",
    "split_by_date_formats",
    "map_column_with_llm",
]


logger = logging.getLogger(__name__)


def _create_long_accumulator(sc, name: str):
    """Create a numeric accumulator, falling back when ``longAccumulator`` is unavailable."""

    if hasattr(sc, "longAccumulator"):
        return sc.longAccumulator(name)

    logger.debug("SparkContext.longAccumulator missing; using legacy accumulator for %s", name)
    return sc.accumulator(0)


def rename_columns(df: DataFrame, mapping: Mapping[str, str]) -> DataFrame:
    """Rename columns according to ``mapping`` while preserving column order.

    Raises:
        ValueError: If any source column is missing or the resulting columns collide.
    """
    if not mapping:
        return df

    missing = [name for name in mapping if name not in df.columns]
    if missing:
        raise ValueError(f"Cannot rename missing columns: {missing}")

    final_names = [mapping.get(name, name) for name in df.columns]
    if len(final_names) != len(set(final_names)):
        raise ValueError("Renaming results in duplicate column names")

    renamed = []
    for name in df.columns:
        new_name = mapping.get(name, name)
        column = F.col(name)
        if new_name != name:
            column = column.alias(new_name)
        renamed.append(column)
    return df.select(*renamed)


def with_constants(
    df: DataFrame,
    constants: Mapping[str, Any],
    *,
    overwrite: bool = False,
) -> DataFrame:
    """Add literal-valued columns using ``constants``.

    Args:
        constants: Mapping of column name to literal value.
        overwrite: Replace existing columns when ``True`` (default ``False``).

    Raises:
        ValueError: If attempting to add an existing column without ``overwrite``.
    """
    if not constants:
        return df

    if not overwrite:
        duplicates = [name for name in constants if name in df.columns]
        if duplicates:
            raise ValueError(f"Columns already exist: {duplicates}")

    result = df
    for name, value in constants.items():
        result = result.withColumn(name, F.lit(value))
    return result


TypeMapping = Mapping[str, Union[str, DataType]]


def cast_columns(df: DataFrame, type_mapping: TypeMapping) -> DataFrame:
    """Cast columns to new Spark SQL types.

    The ``type_mapping`` values may be ``str`` or ``DataType`` instances.

    Raises:
        ValueError: If any referenced column is missing.
    """
    if not type_mapping:
        return df

    missing = [name for name in type_mapping if name not in df.columns]
    if missing:
        raise ValueError(f"Cannot cast missing columns: {missing}")

    coerced = []
    for name in df.columns:
        if name in type_mapping:
            coerced.append(F.col(name).cast(type_mapping[name]).alias(name))
        else:
            coerced.append(F.col(name))
    return df.select(*coerced)


_DEFAULT_REGEX = r"\s+"


def normalize_whitespace(
    df: DataFrame,
    columns: Iterable[str],
    *,
    trim_ends: bool = True,
    pattern: str = _DEFAULT_REGEX,
    replacement: str = " ",
) -> DataFrame:
    """Collapse repeated whitespace in string columns.

    Args:
        columns: Iterable of column names to normalize. Duplicates are ignored.
        trim_ends: When ``True``, also ``trim`` the resulting string.
        pattern: Regex pattern to match; defaults to consecutive whitespace.
        replacement: Replacement string for the regex matches.

    Raises:
        TypeError: If ``columns`` is provided as a single ``str``.
        ValueError: If any referenced column is missing.
    """
    if isinstance(columns, str):
        raise TypeError("columns must be an iterable of column names, not a string")

    targets = list(dict.fromkeys(columns))
    if not targets:
        return df

    missing = [name for name in targets if name not in df.columns]
    if missing:
        raise ValueError(f"Cannot normalize missing columns: {missing}")

    result = df
    for name in targets:
        normalized = F.regexp_replace(F.col(name), pattern, replacement)
        if trim_ends:
            normalized = F.trim(normalized)
        result = result.withColumn(name, normalized)
    return result


_HANDLE_ERROR_MODES = {"null", "strict", "default"}


def split_by_date_formats(
    df: DataFrame,
    column: str,
    formats: Iterable[str],
    *,
    handle_errors: str = "null",
    default_value: Optional[str] = None,
    return_unmatched: bool = False,
    output_column: Optional[str] = None,
) -> Union[DataFrame, Tuple[DataFrame, DataFrame]]:
    """Split ``df`` into per-format partitions with safely parsed date columns.

    Args:
        column: Name of the string column containing date representations.
        formats: Iterable of date format strings, evaluated in order.
        handle_errors: Strategy for unmatched rows (``"null"``, ``"strict"``, ``"default"``).
        default_value: Fallback date string when ``handle_errors="default"``.
        return_unmatched: When ``True``, also return the unmatched rows DataFrame.
        output_column: Optional name for the parsed date column; defaults to ``f"{column}_date"``.

    Returns:
        The combined DataFrame containing all parsed rows.

        When ``return_unmatched`` is ``True``, also returns the unmatched rows
        DataFrame as a second element.

    Raises:
        TypeError: If ``formats`` is a string or contains non-string entries.
        ValueError: For missing columns, duplicate output column, invalid modes, or
            unmatched rows when in ``strict`` mode.
    """

    if column not in df.columns:
        raise ValueError(f"Column '{column}' not found in DataFrame")

    parsed_column = output_column or f"{column}_date"
    if parsed_column in df.columns and parsed_column != column:
        raise ValueError(f"Output column '{parsed_column}' already exists")

    if isinstance(formats, str):
        raise TypeError("formats must be an iterable of strings, not a string")

    format_list = list(dict.fromkeys(formats))
    if not format_list:
        raise ValueError("At least one date format must be provided")

    if any(not isinstance(fmt, str) for fmt in format_list):
        raise TypeError("Each format must be a string")

    mode = handle_errors.lower()
    if mode not in _HANDLE_ERROR_MODES:
        raise ValueError(f"Unsupported handle_errors mode '{handle_errors}'")

    if mode == "default" and default_value is None:
        raise ValueError("default_value must be provided when handle_errors='default'")

    parsed_expressions = [F.to_date(F.col(column), fmt) for fmt in format_list]
    if len(parsed_expressions) == 1:
        parsed_expr = parsed_expressions[0]
    else:
        parsed_expr = F.coalesce(*parsed_expressions)

    format_idx_expr = None
    for idx, expr in enumerate(parsed_expressions):
        candidate = F.when(expr.isNotNull(), F.lit(idx))
        format_idx_expr = (
            candidate if format_idx_expr is None else format_idx_expr.otherwise(candidate)
        )
    if format_idx_expr is None:
        format_idx_expr = F.lit(None)

    format_idx_column = f"__{column}_format_idx__"
    while format_idx_column in df.columns:
        format_idx_column = f"_{format_idx_column}"

    df_with_meta = df.withColumn(parsed_column, parsed_expr).withColumn(
        format_idx_column, format_idx_expr
    )

    partitions: list[DataFrame] = []
    for idx, _ in enumerate(format_list):
        group_df = df_with_meta.filter(F.col(format_idx_column) == idx).drop(format_idx_column)
        partitions.append(group_df)

    unmatched_df = df_with_meta.filter(F.col(format_idx_column).isNull()).drop(format_idx_column)

    if mode == "strict":
        if unmatched_df.limit(1).collect():
            raise ValueError("Unmatched rows detected while handle_errors='strict'")
    elif mode == "default":
        default_df = unmatched_df.withColumn(parsed_column, F.lit(default_value).cast("date"))
        partitions.append(default_df)
    else:
        partitions.append(unmatched_df)

    result_df = partitions[0]
    for part in partitions[1:]:
        result_df = result_df.unionByName(part)

    result: Union[DataFrame, Tuple[DataFrame, DataFrame]] = result_df
    if return_unmatched:
        result = (result_df, unmatched_df)
    return result


def _get_llm_api_config(model: str) -> Tuple[str, Dict[str, str], bool]:
    """Resolve API endpoint configuration for OpenAI or Azure OpenAI.

    Args:
        model: Chat model identifier. When using Azure OpenAI this corresponds to the
            deployment name that should be targeted.

    Returns:
        A triple ``(api_url, headers, use_azure)`` suitable for `requests.post`.
        ``use_azure`` is ``True`` when Azure OpenAI environment variables are present.

    Raises:
        RuntimeError: If the required API key cannot be found in the environment.

    Environment Variables:
        OPENAI_API_KEY: Standard OpenAI key.
        AZURE_OPENAI_KEY / AZURE_OPENAI_API_KEY: Azure OpenAI key alternatives.
        AZURE_OPENAI_ENDPOINT / OPENAI_API_BASE: Azure resource endpoint.
        AZURE_OPENAI_API_VERSION: Optional API version (defaults to ``2023-05-15``).
    """

    api_key = (
        os.getenv("OPENAI_API_KEY")
        or os.getenv("AZURE_OPENAI_KEY")
        or os.getenv("AZURE_OPENAI_API_KEY")
    )
    if not api_key:
        raise RuntimeError("LLM API key not found in environment variables.")

    azure_endpoint = os.getenv("AZURE_OPENAI_ENDPOINT") or os.getenv("OPENAI_API_BASE")
    if azure_endpoint:
        api_base = azure_endpoint.rstrip("/")
        api_version = os.getenv("AZURE_OPENAI_API_VERSION", "2023-05-15")
        deployment_name = model
        api_url = (
            f"{api_base}/openai/deployments/{deployment_name}/chat/completions"
            f"?api-version={api_version}"
        )
        headers = {"Content-Type": "application/json", "api-key": api_key}
        return api_url, headers, True

    api_url = "https://api.openai.com/v1/chat/completions"
    headers = {"Content-Type": "application/json", "Authorization": f"Bearer {api_key}"}
    return api_url, headers, False


def _fetch_llm_mapping(
    value: str,
    target_values: Sequence[str],
    api_url: str,
    headers: Dict[str, str],
    use_azure: bool,
    model: str,
    *,
    max_retries: int = 3,
    request_timeout: int = 30,
    temperature: Optional[float] = 0.0,
) -> Optional[str]:
    """Invoke the LLM API to map ``value`` to one of ``target_values``.

    Args:
        value: Raw value that should be normalized.
        target_values: Ordered collection of allowed canonical values.
        api_url: Endpoint returned by :func:`_get_llm_api_config`.
        headers: Prepared headers containing authentication details.
        use_azure: Indicates whether the request targets Azure OpenAI.
        model: Chat model or deployment name.
        max_retries: Maximum attempts before giving up.
        request_timeout: Per-request timeout (seconds) passed to ``requests``.
        temperature: Sampling temperature to include in the payload. Set to ``None`` to
            let the provider apply its default temperature (useful when certain models
            reject explicit values).

    Returns:
        The canonical value chosen by the model, or ``None`` when the model abstains or
        returns a value outside the permitted targets.

    Notes:
        - The function performs exponential back-off on rate limiting and server errors.
        - Only 200 responses are considered successful; other codes are logged and yield
          ``None``.
    """

    targets_str = ", ".join(f"'{target}'" for target in target_values)
    prompt = (
        f'Map the value "{value}" to one of the following categories: {targets_str}. '
        "If none apply, respond with 'None'."
    )

    active_temperature = temperature

    for attempt in range(1, max_retries + 1):
        payload: Dict[str, Any] = {
            "messages": [
                {"role": "system", "content": "You are a data normalization assistant."},
                {"role": "user", "content": prompt},
            ],
        }
        if active_temperature is not None:
            payload["temperature"] = active_temperature
        if not use_azure:
            payload["model"] = model

        try:
            response = requests.post(
                api_url,
                headers=headers,
                json=payload,
                timeout=request_timeout,
            )
        except requests.RequestException:
            logger.exception("Exception during LLM API call (attempt %s)", attempt)
            time.sleep(min(2**attempt, 60))
            continue

        if response.status_code == 429:
            logger.warning("Rate limit hit (HTTP 429). Backing off (attempt %s).", attempt)
            time.sleep(min(2**attempt, 60))
            continue

        if response.status_code == 400 and active_temperature is not None:
            try:
                error_payload = response.json()
                error_message = error_payload.get("error", {}).get("message", "")
            except ValueError:
                error_message = response.text
            if "temperature" in (error_message or "").lower():
                logger.warning(
                    "LLM rejected temperature=%s; retrying with provider default.",
                    active_temperature,
                )
                active_temperature = None
                time.sleep(min(2**attempt, 60))
                continue

        if 500 <= response.status_code < 600:
            logger.warning(
                "Server error %s on LLM call. Retrying (attempt %s).",
                response.status_code,
                attempt,
            )
            time.sleep(min(2**attempt, 60))
            continue

        if response.status_code != 200:
            logger.error(
                "LLM API call failed (status %s): %s",
                response.status_code,
                response.text,
            )
            return None

        try:
            payload_json = response.json()
            content = payload_json.get("choices", [{}])[0].get("message", {}).get("content", "")
        except ValueError:
            logger.exception("Failed to parse LLM response as JSON")
            return None

        mapped_value = content.strip().strip('"')
        if not mapped_value or mapped_value.lower() == "none":
            return None

        for target in target_values:
            if mapped_value.lower() == target.lower():
                return target

        logger.warning(
            "LLM returned '%s', which is not a valid target option; treating as unmapped.",
            mapped_value,
        )
        return None

    logger.error("LLM mapping failed after %s attempts", max_retries)
    return None


def map_column_with_llm(
    df: DataFrame,
    column: str,
    target_values: Union[Sequence[str], Mapping[str, Any]],
    *,
    model: str = "gpt-3.5-turbo",
    dry_run: bool = False,
    max_retries: int = 3,
    request_timeout: int = 30,
    temperature: Optional[float] = 0.0,
) -> DataFrame:
    """Map ``column`` values to ``target_values`` via a scalar PySpark UDF.

    The transformation applies a regular user-defined function across the column, keeping
    a per-executor in-memory cache to avoid duplicate LLM calls. Spark accumulators track
    mapping statistics. When ``dry_run=True`` the UDF performs case-insensitive matching
    only and yields ``None`` for unmatched rows without contacting the LLM. When targeting
    models that require provider-managed sampling behaviour, set ``temperature=None`` to
    omit the ``temperature`` parameter from LLM requests.

    Args:
        df: Input DataFrame whose values should be normalized.
        column: Source column containing the free-form text to map.
        target_values: List or mapping defining the set of canonical outputs. When a
            mapping is provided, its keys are treated as the canonical set.
        model: Chat model (or Azure deployment name) to query.
        dry_run: Skip external calls and simply echo canonical matches (useful for smoke
            testing and cost estimation).
        max_retries: Retry budget passed to :func:`_fetch_llm_mapping`.
        request_timeout: Timeout in seconds for each HTTP request.
        temperature: LLM sampling temperature. Use ``None`` to skip explicitly setting it
            (some provider models accept only their default temperature).

    Returns:
        A new DataFrame with an additional ``<column>_mapped`` string column containing
        the canonical value or ``None`` when no match is determined.

    Raises:
        ValueError: If the source column is missing or ``target_values`` is empty.
        TypeError: When ``target_values`` contains non-string entries.

    Notes:
        - The resulting DataFrame is cached to ensure logging the accumulator values does
          not trigger duplicate LLM requests.
        - Provide API credentials via the environment variables documented in
          :func:`_get_llm_api_config` before running with ``dry_run=False``.
    """

    if column not in df.columns:
        raise ValueError(f"Column '{column}' not found in DataFrame")

    if isinstance(target_values, Mapping):
        targets = list(dict.fromkeys(target_values.keys()))
    else:
        targets = list(dict.fromkeys(target_values))

    if not targets:
        raise ValueError("target_values must contain at least one entry")

    if not all(isinstance(target, str) for target in targets):
        raise TypeError("target_values entries must be strings")

    lookup: Dict[str, str] = {target.lower(): target for target in targets}
    target_list = list(lookup.values())

    api_url: Optional[str] = None
    headers: Dict[str, str] = {}
    use_azure = False

    if not dry_run:
        api_url, headers, use_azure = _get_llm_api_config(model)

    spark = df.sparkSession
    sc = spark.sparkContext
    calls_acc = _create_long_accumulator(sc, f"llm_api_calls_{column}")
    mapped_acc = _create_long_accumulator(sc, f"mapped_entries_{column}")
    unmapped_acc = _create_long_accumulator(sc, f"unmapped_entries_{column}")

    new_col_name = f"{column}_mapped"

    def _make_mapper():
        cache: Dict[str, Optional[str]] = {}

        def _map_value(raw_value: Any) -> Optional[str]:
            if raw_value is None:
                unmapped_acc.add(1)
                return None

            value_str = str(raw_value)
            if value_str.strip() == "":
                unmapped_acc.add(1)
                return None

            if dry_run:
                mapped_value = lookup.get(value_str.lower())
                if mapped_value is None:
                    unmapped_acc.add(1)
                else:
                    mapped_acc.add(1)
                return mapped_value

            if value_str in cache:
                mapped_value = cache[value_str]
            else:
                calls_acc.add(1)
                mapped_candidate = _fetch_llm_mapping(
                    value_str,
                    target_list,
                    api_url=api_url,  # type: ignore[arg-type]
                    headers=headers,
                    use_azure=use_azure,
                    model=model,
                    max_retries=max_retries,
                    request_timeout=request_timeout,
                    temperature=temperature,
                )
                if mapped_candidate is not None:
                    mapped_value = lookup.get(mapped_candidate.lower(), mapped_candidate)
                else:
                    mapped_value = None
                cache[value_str] = mapped_value

            if mapped_value is None:
                unmapped_acc.add(1)
            else:
                mapped_acc.add(1)
            return mapped_value

        return _map_value

    mapper_udf = F.udf(_make_mapper(), StringType())

    mapped_df = df.withColumn(new_col_name, mapper_udf(F.col(column))).cache()
    mapped_df.count()

    mapped_count = mapped_acc.value
    unmapped_count = unmapped_acc.value
    logger.info(
        "Mapping stats for column '%s': Mapped %s, Unmapped %s, API calls made %s.",
        column,
        mapped_count,
        unmapped_count,
        calls_acc.value,
    )

    return mapped_df
