from __future__ import annotations

import datetime as _dt

import pytest

from spark_fuse.utils.dataframe import create_date_dataframe, create_time_dataframe


def _collect_dicts(df, order_by):
    return [r.asDict() for r in df.orderBy(order_by).collect()]


def test_create_date_dataframe_enriches_calendar_attributes(spark):
    df = create_date_dataframe(spark, "2024-01-01", "2024-01-03")
    rows = _collect_dicts(df, "date")

    assert len(rows) == 3
    assert rows[0]["date"] == _dt.date(2024, 1, 1)
    assert rows[0]["year"] == 2024
    assert rows[0]["quarter"] == 1
    assert rows[0]["month"] == 1
    assert rows[0]["month_name"] == "January"
    assert rows[0]["week"] == 1
    assert rows[0]["day"] == 1
    assert rows[0]["day_of_week"] == 2  # Spark: Sunday=1
    assert rows[0]["day_name"] == "Monday"

    assert rows[1]["date"] == _dt.date(2024, 1, 2)
    assert rows[2]["date"] == _dt.date(2024, 1, 3)


def test_create_date_dataframe_invalid_range_raises(spark):
    with pytest.raises(ValueError, match="end_date must not be earlier than start_date"):
        create_date_dataframe(spark, "2024-01-03", "2024-01-01")


def test_create_time_dataframe_enriches_clock_components(spark):
    df = create_time_dataframe(
        spark,
        start_time="00:00:00",
        end_time="00:02:00",
        interval_seconds=60,
    )
    rows = _collect_dicts(df, "time")

    assert [r["time"] for r in rows] == ["00:00:00", "00:01:00", "00:02:00"]
    assert rows[0]["hour"] == 0
    assert rows[0]["minute"] == 0
    assert rows[0]["second"] == 0
    assert rows[1]["hour"] == 0 and rows[1]["minute"] == 1 and rows[1]["second"] == 0


def test_create_time_dataframe_invalid_interval(spark):
    with pytest.raises(ValueError, match="interval_seconds must be a positive integer"):
        create_time_dataframe(spark, "00:00:00", "00:01:00", interval_seconds=0)

    with pytest.raises(ValueError, match="Time span must be evenly divisible"):
        create_time_dataframe(spark, "00:00:00", "00:01:30", interval_seconds=60)
