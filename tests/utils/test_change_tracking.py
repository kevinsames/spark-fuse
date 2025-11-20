from __future__ import annotations

from pathlib import Path

import pytest

import spark_fuse.utils.change_tracking as change_tracking

delta = pytest.importorskip("delta")  # ensure delta-spark is available when running tests

from spark_fuse.utils.change_tracking import (  # noqa: E402
    ChangeTrackingMode,
    apply_change_tracking,
    current_only_upsert,
    track_history_upsert,
)


def _rows_by_key(df, key: str):
    return {r[key]: r.asDict() for r in df.collect()}


def test_current_only_upsert_dedup_and_update(spark, tmp_path: Path):
    target = str(tmp_path / "current_only_target")

    # Initial dataset with duplicates for id=1; keep latest by ts.
    df = spark.createDataFrame(
        [
            {"id": 1, "val": "a", "ts": 1},
            {"id": 1, "val": "b", "ts": 2},  # latest for id=1
            {"id": 2, "val": "x", "ts": 5},
        ]
    )

    current_only_upsert(
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

    current_only_upsert(
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
    assert rows2[1]["val"] == "c"  # updated in-place (current-only)


def test_track_history_upsert_versioning(spark, tmp_path: Path):
    target = str(tmp_path / "track_history_target")

    df = spark.createDataFrame(
        [
            {"id": 1, "val": "a", "ts": 1},
            {"id": 1, "val": "b", "ts": 2},  # latest for id=1
            {"id": 2, "val": "x", "ts": 5},
        ]
    )

    track_history_upsert(
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

    track_history_upsert(
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


def test_track_history_upsert_multiple_versions_same_batch(spark, tmp_path: Path):
    target = str(tmp_path / "track_history_multi_batch")

    df = spark.createDataFrame(
        [
            {"id": 1, "val": "a", "ts": 1},
            {"id": 1, "val": "b", "ts": 2},
            {"id": 1, "val": "c", "ts": 3},
        ]
    )

    track_history_upsert(
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


def test_track_history_upsert_schema_evolution(spark, tmp_path: Path):
    target = str(tmp_path / "track_history_schema_evolution")

    df = spark.createDataFrame(
        [
            {"id": 1, "val": "a", "ts": 1},
        ]
    )

    track_history_upsert(
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

    track_history_upsert(
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


def test_current_only_upsert_schema_evolution(spark, tmp_path: Path):
    target = str(tmp_path / "current_only_schema_evolution")

    base = spark.createDataFrame(
        [
            {"id": 1, "val": "a"},
        ]
    )

    current_only_upsert(
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

    current_only_upsert(
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


def test_apply_change_tracking_dispatch(spark, tmp_path: Path):
    # current-only via dispatcher
    target1 = str(tmp_path / "apply_change_tracking_current_only")
    df1 = spark.createDataFrame([{"id": 1, "val": "a"}, {"id": 2, "val": "b"}])
    apply_change_tracking(
        spark,
        df1,
        target1,
        change_tracking_mode=ChangeTrackingMode.CURRENT_ONLY,
        business_keys=["id"],
        tracked_columns=["val"],
    )
    out1 = spark.read.format("delta").load(target1)
    assert out1.count() == 2

    # track-history via dispatcher
    target2 = str(tmp_path / "apply_change_tracking_track_history")
    df2 = spark.createDataFrame([{"id": 1, "val": "a"}, {"id": 2, "val": "b"}])
    apply_change_tracking(
        spark,
        df2,
        target2,
        change_tracking_mode=ChangeTrackingMode.TRACK_HISTORY,
        business_keys=["id"],
        tracked_columns=["val"],
        load_ts_expr="to_timestamp('2020-01-01 00:00:00')",
    )
    out2 = spark.read.format("delta").load(target2)
    assert out2.filter("is_current = true").count() == 2


def test_apply_change_tracking_from_options_routes(monkeypatch, spark, tmp_path: Path):
    df = spark.createDataFrame([{"id": 1, "val": "a"}])
    target = str(tmp_path / "dispatch_options")
    called = {}

    def fake_current(*args, **kwargs):
        called["mode"] = "current"

    def fake_history(*args, **kwargs):
        called["mode"] = "history"

    monkeypatch.setattr(change_tracking, "current_only_upsert", fake_current)
    monkeypatch.setattr(change_tracking, "track_history_upsert", fake_history)

    change_tracking.apply_change_tracking_from_options(
        spark,
        df,
        target,
        options={
            "change_tracking_mode": "track_history",
            "track_history_options": {"business_keys": ["id"], "tracked_columns": ["val"]},
        },
    )
    assert called["mode"] == "history"


def test_change_tracking_writer_uses_apply(monkeypatch, spark, tmp_path: Path):
    df = spark.createDataFrame([{"id": 1, "val": "a"}])
    target = str(tmp_path / "writer_target")
    observed = {}

    def fake_apply(*, spark, source_df, target, options):
        observed["spark"] = spark
        observed["source"] = source_df
        observed["target"] = target
        observed["options"] = options

    monkeypatch.setattr(change_tracking, "apply_change_tracking_from_options", fake_apply)

    (
        df.write.change_tracking.options(
            change_tracking_mode="current_only",
            change_tracking_options={
                "business_keys": ["id"],
                "tracked_columns": ["val"],
            },
        ).table(target)
    )

    assert observed["spark"] is spark
    assert observed["source"].collect() == df.collect()
    assert observed["target"] == target
    assert observed["options"]["change_tracking_mode"] == "current_only"


def test_dataframe_change_tracking_property(monkeypatch, spark, tmp_path: Path):
    df = spark.createDataFrame([{"id": 1, "val": "a"}])
    target = str(tmp_path / "df_property_target")
    calls = []

    def fake_apply(*, spark, source_df, target, options):
        calls.append((spark, source_df, target, options))

    monkeypatch.setattr(change_tracking, "apply_change_tracking_from_options", fake_apply)

    df.change_tracking.options(
        change_tracking_mode="track_history",
        change_tracking_options={
            "business_keys": ["id"],
            "tracked_columns": ["val"],
            "load_ts_expr": "current_timestamp()",
        },
    ).table(target)

    assert len(calls) == 1
    spark_arg, source_arg, target_arg, opts = calls[0]
    assert spark_arg is spark
    assert source_arg.collect() == df.collect()
    assert target_arg == target
    assert opts["change_tracking_mode"] == "track_history"
