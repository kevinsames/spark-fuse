from __future__ import annotations

from datetime import date

import pytest

from spark_fuse.utils.transformations import (
    cast_columns,
    map_column_with_llm,
    normalize_whitespace,
    rename_columns,
    split_by_date_formats,
    with_constants,
)


def _rows_by_id(df, id_col="id"):
    return {row[id_col]: row.asDict(recursive=True) for row in df.collect()}


def test_rename_columns_success(spark):
    df = spark.createDataFrame([{"name": "Alice", "value": 1}])

    result = rename_columns(df, {"name": "full_name"})

    assert result.columns == ["full_name", "value"]
    rows = _rows_by_id(result, "value")
    assert rows[1]["full_name"] == "Alice"


def test_rename_columns_missing_raises(spark):
    df = spark.createDataFrame([{"name": "Alice"}])

    with pytest.raises(ValueError, match="Cannot rename missing"):
        rename_columns(df, {"missing": "other"})


def test_with_constants_add_and_overwrite(spark):
    df = spark.createDataFrame([{"id": 1, "country": "US"}])

    result = with_constants(df, {"source": "ingest"})
    rows = _rows_by_id(result)
    assert rows[1]["source"] == "ingest"

    overwritten = with_constants(df, {"country": "CA"}, overwrite=True)
    rows_overwrite = _rows_by_id(overwritten)
    assert rows_overwrite[1]["country"] == "CA"


def test_with_constants_existing_column_without_overwrite_raises(spark):
    df = spark.createDataFrame([{"id": 1}])

    with pytest.raises(ValueError, match="Columns already exist"):
        with_constants(df, {"id": 10})


def test_cast_columns_types(spark):
    df = spark.createDataFrame([{"id": "1", "flag": "true"}])

    result = cast_columns(df, {"id": "int", "flag": "boolean"})
    row = result.collect()[0]
    assert row["id"] == 1
    assert row["flag"] is True


def test_cast_columns_missing_source_raises(spark):
    df = spark.createDataFrame([{"id": "1"}])

    with pytest.raises(ValueError, match="Cannot cast missing"):
        cast_columns(df, {"missing": "int"})


def test_normalize_whitespace_basic(spark):
    df = spark.createDataFrame([{"id": 1, "text": "  hello   world  "}])

    result = normalize_whitespace(df, ["text"])
    rows = _rows_by_id(result)
    assert rows[1]["text"] == "hello world"


def test_normalize_whitespace_no_trim(spark):
    df = spark.createDataFrame([{"id": 1, "text": "foo\tbar"}])

    result = normalize_whitespace(df, ["text"], trim_ends=False, replacement="_")
    rows = _rows_by_id(result)
    assert rows[1]["text"] == "foo_bar"


def test_normalize_whitespace_requires_iterable(spark):
    df = spark.createDataFrame([{"id": 1, "text": " value "}])

    with pytest.raises(TypeError):
        normalize_whitespace(df, "text")


def test_split_by_date_formats_null_mode_with_unmatched(spark):
    df = spark.createDataFrame(
        [
            {"id": 1, "raw": "2023-01-01"},
            {"id": 2, "raw": "01/15/2023"},
            {"id": 3, "raw": "bad"},
        ]
    )

    result_df, unmatched_df = split_by_date_formats(
        df,
        "raw",
        ["yyyy-MM-dd", "MM/dd/yyyy"],
        return_unmatched=True,
    )

    rows = _rows_by_id(result_df)
    assert rows[1]["raw_date"] == date(2023, 1, 1)
    assert rows[2]["raw_date"] == date(2023, 1, 15)
    assert rows[3]["raw_date"] is None

    unmatched_rows = _rows_by_id(unmatched_df)
    assert set(unmatched_rows) == {3}
    assert unmatched_rows[3]["raw_date"] is None


def test_split_by_date_formats_default_mode_sets_fallback(spark):
    df = spark.createDataFrame(
        [
            {"id": 1, "raw": "2023-01-01"},
            {"id": 2, "raw": "bad"},
        ]
    )

    result_df, unmatched_df = split_by_date_formats(
        df,
        "raw",
        ["yyyy-MM-dd"],
        handle_errors="default",
        default_value="2000-01-01",
        return_unmatched=True,
    )

    rows = _rows_by_id(result_df)
    assert rows[1]["raw_date"] == date(2023, 1, 1)
    assert rows[2]["raw_date"] == date(2000, 1, 1)

    unmatched_rows = _rows_by_id(unmatched_df)
    assert unmatched_rows[2]["raw_date"] is None


def test_split_by_date_formats_default_mode_with_multiple_formats(spark):
    df = spark.createDataFrame(
        [
            {"id": 1, "raw": "01-01-1900"},
            {"id": 2, "raw": "15.02.2020"},
            {"id": 3, "raw": "00.00.0000"},
        ]
    )

    result_df, unmatched_df = split_by_date_formats(
        df,
        "raw",
        ["MM-dd-yyyy", "dd.MM.yyyy"],
        handle_errors="default",
        default_value="1900-01-01",
        return_unmatched=True,
    )

    rows = _rows_by_id(result_df)
    assert rows[1]["raw_date"] == date(1900, 1, 1)
    assert rows[2]["raw_date"] == date(2020, 2, 15)
    assert rows[3]["raw_date"] == date(1900, 1, 1)

    unmatched_rows = _rows_by_id(unmatched_df)
    assert set(unmatched_rows) == {3}
    assert unmatched_rows[3]["raw_date"] is None


def test_split_by_date_formats_default_mode_with_three_formats(spark):
    df = spark.createDataFrame(
        [
            {"id": 1, "raw": "2023-03-05"},
            {"id": 2, "raw": "05/06/2023"},
            {"id": 3, "raw": "17.08.2024"},
            {"id": 4, "raw": "not a date"},
        ]
    )

    result_df, unmatched_df = split_by_date_formats(
        df,
        "raw",
        ["yyyy-MM-dd", "MM/dd/yyyy", "dd.MM.yyyy"],
        handle_errors="default",
        default_value="1970-01-01",
        return_unmatched=True,
    )

    rows = _rows_by_id(result_df)
    assert rows[1]["raw_date"] == date(2023, 3, 5)
    assert rows[2]["raw_date"] == date(2023, 5, 6)
    assert rows[3]["raw_date"] == date(2024, 8, 17)
    assert rows[4]["raw_date"] == date(1970, 1, 1)

    unmatched_rows = _rows_by_id(unmatched_df)
    assert set(unmatched_rows) == {4}
    assert unmatched_rows[4]["raw_date"] is None


def test_split_by_date_formats_strict_mode_raises_on_unmatched(spark):
    df = spark.createDataFrame(
        [
            {"id": 1, "raw": "2023-01-01"},
            {"id": 2, "raw": "bad"},
        ]
    )

    with pytest.raises(ValueError, match="handle_errors='strict'"):
        split_by_date_formats(df, "raw", ["yyyy-MM-dd"], handle_errors="strict")


def test_split_by_date_formats_without_returning_unmatched(spark):
    df = spark.createDataFrame(
        [
            {"id": 1, "raw": "2023-03-05"},
            {"id": 2, "raw": "05/06/2023"},
        ]
    )

    result_df = split_by_date_formats(
        df,
        "raw",
        ["yyyy-MM-dd", "MM/dd/yyyy"],
    )

    rows = _rows_by_id(result_df)
    assert rows[1]["raw_date"] == date(2023, 3, 5)
    assert rows[2]["raw_date"] == date(2023, 5, 6)


def test_map_column_with_llm_dry_run_returns_none_for_unmatched(spark):
    df = spark.createDataFrame(
        [
            {"id": 1, "fruit": "Apple"},
            {"id": 2, "fruit": "bananna"},
            {"id": 3, "fruit": None},
        ]
    )

    result = map_column_with_llm(
        df,
        column="fruit",
        target_values=["Apple", "Banana", "Cherry"],
        dry_run=True,
    )

    rows = _rows_by_id(result)
    assert rows[1]["fruit_mapped"] == "Apple"
    assert rows[2]["fruit_mapped"] is None
    assert rows[3]["fruit_mapped"] is None
