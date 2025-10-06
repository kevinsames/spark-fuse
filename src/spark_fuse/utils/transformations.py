from __future__ import annotations

from typing import Any, Iterable, Mapping, Union

from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import DataType

__all__ = [
    "rename_columns",
    "with_constants",
    "cast_columns",
    "normalize_whitespace",
]


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
