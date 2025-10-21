from __future__ import annotations

import datetime as _dt

from typing import Iterable, Union

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

__all__ = [
    "preview",
    "ensure_columns",
    "create_date_dataframe",
    "create_time_dataframe",
]


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


def _ensure_date(value: Union[str, _dt.date, _dt.datetime]) -> _dt.date:
    if isinstance(value, _dt.datetime):
        return value.date()
    if isinstance(value, _dt.date):
        return value
    if isinstance(value, str):
        try:
            return _dt.datetime.strptime(value, "%Y-%m-%d").date()
        except ValueError as exc:  # pragma: no cover - defensive
            raise ValueError(f"Invalid date string {value!r}; expected 'YYYY-MM-DD'.") from exc
    raise TypeError("start_date/end_date must be str, datetime.date, or datetime.datetime")


def _ensure_time(value: Union[str, _dt.time, _dt.datetime]) -> _dt.time:
    if isinstance(value, _dt.datetime):
        return value.time()
    if isinstance(value, _dt.time):
        return value
    if isinstance(value, str):
        for fmt in ("%H:%M:%S", "%H:%M"):
            try:
                return _dt.datetime.strptime(value, fmt).time()
            except ValueError:
                continue
        raise ValueError(f"Invalid time string {value!r}; expected 'HH:MM:SS' or 'HH:MM'.")
    raise TypeError("start_time/end_time must be str, datetime.time, or datetime.datetime")


def create_date_dataframe(
    spark: SparkSession,
    start_date: Union[str, _dt.date, _dt.datetime],
    end_date: Union[str, _dt.date, _dt.datetime],
    *,
    date_col: str = "date",
) -> DataFrame:
    """Return contiguous dates between ``start_date`` and ``end_date`` inclusive with calendar attributes."""
    start = _ensure_date(start_date)
    end = _ensure_date(end_date)

    if end < start:
        raise ValueError("end_date must not be earlier than start_date")

    num_rows = (end - start).days + 1
    base_date_lit = F.lit(start.isoformat()).cast("date")

    df = (
        spark.range(0, num_rows)
        .withColumn("_offset", F.col("id").cast("int"))
        .withColumn(date_col, F.date_add(base_date_lit, F.col("_offset")))
        .drop("id", "_offset")
    )

    return (
        df.withColumn("year", F.year(F.col(date_col)))
        .withColumn("quarter", F.quarter(F.col(date_col)))
        .withColumn("month", F.month(F.col(date_col)))
        .withColumn("month_name", F.date_format(F.col(date_col), "MMMM"))
        .withColumn("week", F.weekofyear(F.col(date_col)))
        .withColumn("day", F.dayofmonth(F.col(date_col)))
        .withColumn("day_of_week", F.dayofweek(F.col(date_col)))
        .withColumn("day_name", F.date_format(F.col(date_col), "EEEE"))
    )


def create_time_dataframe(
    spark: SparkSession,
    start_time: Union[str, _dt.time, _dt.datetime],
    end_time: Union[str, _dt.time, _dt.datetime],
    *,
    interval_seconds: int = 60,
    time_col: str = "time",
) -> DataFrame:
    """Return evenly spaced times between ``start_time`` and ``end_time`` (inclusive) with clock units."""
    if interval_seconds <= 0:
        raise ValueError("interval_seconds must be a positive integer")

    start = _ensure_time(start_time)
    end = _ensure_time(end_time)
    if start.microsecond or end.microsecond:
        raise ValueError("start_time and end_time must not include microseconds")

    start_seconds = start.hour * 3600 + start.minute * 60 + start.second
    end_seconds = end.hour * 3600 + end.minute * 60 + end.second

    if end_seconds < start_seconds:
        raise ValueError("end_time must not be earlier than start_time within the same day")

    span_seconds = end_seconds - start_seconds
    steps, remainder = divmod(span_seconds, interval_seconds)
    if remainder != 0:
        raise ValueError("Time span must be evenly divisible by interval_seconds")

    df = spark.range(0, steps + 1).withColumn(
        "_seconds_since_midnight",
        (F.col("id") * F.lit(interval_seconds) + F.lit(start_seconds)).cast("int"),
    )

    hour_col = F.floor(F.col("_seconds_since_midnight") / 3600)
    minute_col = F.floor((F.col("_seconds_since_midnight") % 3600) / 60)
    second_col = F.col("_seconds_since_midnight") % 60

    df = (
        df.withColumn("hour", hour_col.cast("int"))
        .withColumn("minute", minute_col.cast("int"))
        .withColumn("second", second_col.cast("int"))
    )

    df = df.withColumn(
        time_col,
        F.format_string(
            "%02d:%02d:%02d",
            F.col("hour"),
            F.col("minute"),
            F.col("second"),
        ),
    )

    return df.drop("id", "_seconds_since_midnight")
