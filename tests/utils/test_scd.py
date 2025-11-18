from __future__ import annotations

from pathlib import Path

import pytest


delta = pytest.importorskip("delta")  # ensure delta-spark is available when running tests

from spark_fuse.utils.scd import SCDMode, apply_scd, scd1_upsert, scd2_upsert  # noqa: E402


def _rows_by_key(df, key: str):
    return {r[key]: r.asDict() for r in df.collect()}


def test_scd1_upsert_dedup_and_update(spark, tmp_path: Path):
    target = str(tmp_path / "scd1_target")

    # Initial dataset with duplicates for id=1; keep latest by ts.
    df = spark.createDataFrame(
        [
            {"id": 1, "val": "a", "ts": 1},
            {"id": 1, "val": "b", "ts": 2},  # latest for id=1
            {"id": 2, "val": "x", "ts": 5},
        ]
    )

    scd1_upsert(
        spark,
        df,
        target,
        business_keys=["id"],
        tracked_columns=["val"],
        order_by=["ts"],
    )

    out = spark.read.format("delta").load(target)
    rows = _rows_by_key(out, "id")
    assert set(rows) == {1, 2}
    assert rows[1]["val"] == "b"  # dedup kept latest by ts

    # Second batch: update id=1, add id=3
    df2 = spark.createDataFrame(
        [
            {"id": 1, "val": "c", "ts": 3},
            {"id": 3, "val": "z", "ts": 1},
        ]
    )

    scd1_upsert(
        spark,
        df2,
        target,
        business_keys=["id"],
        tracked_columns=["val"],
        order_by=["ts"],
    )

    out2 = spark.read.format("delta").load(target)
    rows2 = _rows_by_key(out2, "id")
    assert set(rows2) == {1, 2, 3}
    assert rows2[1]["val"] == "c"  # updated in-place (SCD1)


def test_scd2_upsert_versioning(spark, tmp_path: Path):
    target = str(tmp_path / "scd2_target")

    df = spark.createDataFrame(
        [
            {"id": 1, "val": "a", "ts": 1},
            {"id": 1, "val": "b", "ts": 2},  # latest for id=1
            {"id": 2, "val": "x", "ts": 5},
        ]
    )

    scd2_upsert(
        spark,
        df,
        target,
        business_keys=["id"],
        tracked_columns=["val"],
        order_by=["ts"],
        load_ts_expr="to_timestamp('2020-01-01 00:00:00')",
    )

    out1 = spark.read.format("delta").load(target)
    assert out1.filter("id = 1").count() == 2  # history retained within a single batch
    assert out1.filter("is_current = true").count() == 2
    rows1 = _rows_by_key(out1.filter("is_current = true"), "id")
    assert rows1[1]["version"] == 2
    assert rows1[2]["version"] == 1
    assert rows1[1]["val"] == "b"
    assert out1.filter("id = 1 and is_current = false").count() == 1

    # Second batch: change id=1, add id=3; bump load timestamp
    df2 = spark.createDataFrame(
        [
            {"id": 1, "val": "c", "ts": 3},
            {"id": 3, "val": "z", "ts": 1},
        ]
    )

    scd2_upsert(
        spark,
        df2,
        target,
        business_keys=["id"],
        tracked_columns=["val"],
        order_by=["ts"],
        load_ts_expr="to_timestamp('2020-01-02 00:00:00')",
    )

    out2 = spark.read.format("delta").load(target)
    # Expect: id=1 has three versions (two closed, one current); id=2 has one, id=3 has one. Three currents total.
    assert out2.filter("is_current = true").count() == 3
    current = _rows_by_key(out2.filter("is_current = true"), "id")
    assert current[1]["version"] == 3
    assert current[1]["val"] == "c"
    # Closed rows for id=1 exist with non-null expiry
    closed_count = out2.filter(
        "id = 1 and is_current = false and effective_end_ts is not null"
    ).count()
    assert closed_count == 2


def test_scd2_upsert_multiple_versions_same_batch(spark, tmp_path: Path):
    target = str(tmp_path / "scd2_multi_batch")

    df = spark.createDataFrame(
        [
            {"id": 1, "val": "a", "ts": 1},
            {"id": 1, "val": "b", "ts": 2},
            {"id": 1, "val": "c", "ts": 3},
        ]
    )

    scd2_upsert(
        spark,
        df,
        target,
        business_keys=["id"],
        tracked_columns=["val"],
        order_by=["ts"],
        load_ts_expr="to_timestamp('2020-01-01 00:00:00')",
    )

    out = spark.read.format("delta").load(target)
    assert out.filter("id = 1").count() == 3
    assert out.filter("id = 1 and is_current = true").count() == 1
    versions = out.where("id = 1").orderBy("version").collect()
    assert [row.version for row in versions] == [1, 2, 3]
    assert versions[-1].val == "c"


def test_scd2_upsert_schema_evolution(spark, tmp_path: Path):
    target = str(tmp_path / "scd2_schema_evolution")

    df = spark.createDataFrame(
        [
            {"id": 1, "val": "a", "ts": 1},
        ]
    )

    scd2_upsert(
        spark,
        df,
        target,
        business_keys=["id"],
        tracked_columns=["val"],
        order_by=["ts"],
        load_ts_expr="to_timestamp('2020-01-01 00:00:00')",
    )

    df_with_new_col = spark.createDataFrame(
        [
            {"id": 1, "val": "b", "color": "red", "ts": 2},
        ]
    )

    scd2_upsert(
        spark,
        df_with_new_col,
        target,
        business_keys=["id"],
        tracked_columns=["val", "color"],
        order_by=["ts"],
        load_ts_expr="to_timestamp('2020-01-02 00:00:00')",
        allow_schema_evolution=True,
    )

    out = spark.read.format("delta").load(target)
    assert "color" in out.columns
    current = out.where("is_current = true").collect()[0]
    assert current["color"] == "red"
    previous = out.where("version = 1").collect()[0]
    assert previous["color"] is None


def test_scd1_upsert_schema_evolution(spark, tmp_path: Path):
    target = str(tmp_path / "scd1_schema_evolution")

    base = spark.createDataFrame(
        [
            {"id": 1, "val": "a"},
        ]
    )

    scd1_upsert(
        spark,
        base,
        target,
        business_keys=["id"],
        tracked_columns=["val"],
        allow_schema_evolution=True,
    )

    updates = spark.createDataFrame(
        [
            {"id": 1, "val": "b", "color": "red"},
        ]
    )

    scd1_upsert(
        spark,
        updates,
        target,
        business_keys=["id"],
        tracked_columns=["val", "color"],
        allow_schema_evolution=True,
    )

    out = spark.read.format("delta").load(target)
    assert "color" in out.columns
    rows = _rows_by_key(out, "id")
    assert rows[1]["color"] == "red"


def test_apply_scd_dispatch(spark, tmp_path: Path):
    # SCD1 via dispatcher
    target1 = str(tmp_path / "apply_scd_scd1")
    df1 = spark.createDataFrame([{"id": 1, "val": "a"}, {"id": 2, "val": "b"}])
    apply_scd(
        spark, df1, target1, scd_mode=SCDMode.SCD1, business_keys=["id"], tracked_columns=["val"]
    )
    out1 = spark.read.format("delta").load(target1)
    assert out1.count() == 2

    # SCD2 via dispatcher
    target2 = str(tmp_path / "apply_scd_scd2")
    df2 = spark.createDataFrame([{"id": 1, "val": "a"}, {"id": 2, "val": "b"}])
    apply_scd(
        spark,
        df2,
        target2,
        scd_mode=SCDMode.SCD2,
        business_keys=["id"],
        tracked_columns=["val"],
        load_ts_expr="to_timestamp('2020-01-01 00:00:00')",
    )
    out2 = spark.read.format("delta").load(target2)
    assert out2.filter("is_current = true").count() == 2
