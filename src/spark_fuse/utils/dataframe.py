from __future__ import annotations

from typing import Iterable

from pyspark.sql import DataFrame


def preview(df: DataFrame, n: int = 5) -> str:
    """Return a string preview of the dataframe head and schema."""
    rows = [r.asDict(recursive=True) for r in df.limit(n).collect()]
    schema = df.schema.simpleString()
    return f"rows={rows}\nschema={schema}"


def ensure_columns(df: DataFrame, required: Iterable[str]) -> DataFrame:
    """Validate that `df` contains all `required` columns.

    Raises a `ValueError` including the missing columns otherwise.
    """
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")
    return df
