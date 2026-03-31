from __future__ import annotations

from pathlib import Path

import pytest

import spark_fuse.utils.change_tracking as change_tracking

delta = pytest.importorskip("delta")  # ensure delta-spark is available when running tests

from spark_fuse.utils.change_tracking import (  # noqa: E402
    ChangeTrackingMode,
    ChangeTrackingWriteBuilder,
    _ensure_mapping,
    _extract_tracking_kwargs_from_options,
    _is_delta_path,
    _normalize_option_key,
    _resolve_mode,
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
    assert set(r["id"] for r in out1.collect()) == {1, 2}

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
    assert set(r["id"] for r in out2.filter("is_current = true").collect()) == {1, 2}


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

    def fake_apply(*, spark, source_df, target, options, verbose=False):
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


def test_track_history_upsert_idempotent(spark, tmp_path: Path):
    """Calling track_history_upsert twice with identical data must produce no new rows."""
    target = str(tmp_path / "track_history_idempotent")

    df = spark.createDataFrame(
        [
            {"id": 1, "val": "a", "ts": 1},
            {"id": 2, "val": "x", "ts": 5},
        ]
    )

    common_kwargs = dict(
        business_keys=["id"],
        tracked_columns=["val"],
        order_by=["ts"],
        load_ts_expr="to_timestamp('2020-01-01 00:00:00')",
    )

    track_history_upsert(spark, df, target, **common_kwargs)
    out1 = spark.read.format("delta").load(target)
    count_after_first = out1.count()
    current_after_first = out1.filter("is_current = true").count()

    # Second call with identical data — must be a no-op
    track_history_upsert(spark, df, target, **common_kwargs)
    out2 = spark.read.format("delta").load(target)
    count_after_second = out2.count()
    current_after_second = out2.filter("is_current = true").count()

    assert (
        count_after_second == count_after_first
    ), f"Row count changed on idempotent call: {count_after_first} -> {count_after_second}"
    assert current_after_second == current_after_first
    # Every key must still have exactly one current row
    assert current_after_second == 2
    rows = _rows_by_key(out2.filter("is_current = true"), "id")
    assert rows[1]["version"] == 1
    assert rows[2]["version"] == 1


def test_dataframe_change_tracking_property(monkeypatch, spark, tmp_path: Path):
    df = spark.createDataFrame([{"id": 1, "val": "a"}])
    target = str(tmp_path / "df_property_target")
    calls = []

    def fake_apply(*, spark, source_df, target, options, verbose=False):
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


# ---------------------------------------------------------------------------
# 1. Input validation errors
# ---------------------------------------------------------------------------


def test_current_only_empty_business_keys(spark, tmp_path: Path):
    df = spark.createDataFrame([{"id": 1, "val": "a"}])
    with pytest.raises(ValueError, match="business_keys"):
        current_only_upsert(spark, df, str(tmp_path / "t"), business_keys=[])


def test_track_history_empty_business_keys(spark, tmp_path: Path):
    df = spark.createDataFrame([{"id": 1, "val": "a"}])
    with pytest.raises(ValueError, match="business_keys"):
        track_history_upsert(spark, df, str(tmp_path / "t"), business_keys=[])


def test_current_only_missing_business_keys_in_source(spark, tmp_path: Path):
    df = spark.createDataFrame([{"id": 1, "val": "a"}])
    with pytest.raises(ValueError, match="business_keys"):
        current_only_upsert(spark, df, str(tmp_path / "t"), business_keys=["nonexistent"])


def test_track_history_missing_business_keys_in_source(spark, tmp_path: Path):
    df = spark.createDataFrame([{"id": 1, "val": "a"}])
    with pytest.raises(ValueError, match="business_keys"):
        track_history_upsert(spark, df, str(tmp_path / "t"), business_keys=["nonexistent"])


def test_current_only_missing_tracked_columns_in_source(spark, tmp_path: Path):
    df = spark.createDataFrame([{"id": 1, "val": "a"}])
    with pytest.raises(ValueError, match="tracked_columns"):
        current_only_upsert(
            spark,
            df,
            str(tmp_path / "t"),
            business_keys=["id"],
            tracked_columns=["nonexistent"],
        )


def test_track_history_missing_tracked_columns_in_source(spark, tmp_path: Path):
    df = spark.createDataFrame([{"id": 1, "val": "a"}])
    with pytest.raises(ValueError, match="tracked_columns"):
        track_history_upsert(
            spark,
            df,
            str(tmp_path / "t"),
            business_keys=["id"],
            tracked_columns=["nonexistent"],
        )


# ---------------------------------------------------------------------------
# 2. Null key policy
# ---------------------------------------------------------------------------


def test_current_only_null_key_error_policy(spark, tmp_path: Path):
    df = spark.createDataFrame([{"id": None, "val": "a"}, {"id": 1, "val": "b"}])
    with pytest.raises(ValueError, match="Null business key"):
        current_only_upsert(spark, df, str(tmp_path / "t"), business_keys=["id"])


def test_current_only_null_key_drop_policy(spark, tmp_path: Path):
    target = str(tmp_path / "co_null_drop")
    df = spark.createDataFrame([{"id": None, "val": "a"}, {"id": 1, "val": "b"}])
    current_only_upsert(spark, df, target, business_keys=["id"], null_key_policy="drop")
    out = spark.read.format("delta").load(target)
    assert out.count() == 1
    assert out.collect()[0]["id"] == 1


def test_track_history_null_key_error_policy(spark, tmp_path: Path):
    df = spark.createDataFrame([{"id": None, "val": "a"}, {"id": 1, "val": "b"}])
    with pytest.raises(ValueError, match="Null business key"):
        track_history_upsert(
            spark,
            df,
            str(tmp_path / "t"),
            business_keys=["id"],
            load_ts_expr="to_timestamp('2020-01-01 00:00:00')",
        )


def test_track_history_null_key_drop_policy(spark, tmp_path: Path):
    target = str(tmp_path / "th_null_drop")
    df = spark.createDataFrame([{"id": None, "val": "a"}, {"id": 1, "val": "b"}])
    track_history_upsert(
        spark,
        df,
        target,
        business_keys=["id"],
        null_key_policy="drop",
        load_ts_expr="to_timestamp('2020-01-01 00:00:00')",
    )
    out = spark.read.format("delta").load(target)
    assert out.filter("is_current = true").count() == 1
    assert out.collect()[0]["id"] == 1


def test_current_only_invalid_null_key_policy(spark, tmp_path: Path):
    df = spark.createDataFrame([{"id": 1, "val": "a"}])
    with pytest.raises(ValueError, match="null_key_policy"):
        current_only_upsert(
            spark,
            df,
            str(tmp_path / "t"),
            business_keys=["id"],
            null_key_policy="ignore",
        )


# ---------------------------------------------------------------------------
# 3. create_if_not_exists=False
# ---------------------------------------------------------------------------


def test_current_only_no_create_if_not_exists(spark, tmp_path: Path):
    df = spark.createDataFrame([{"id": 1, "val": "a"}])
    with pytest.raises(ValueError, match="does not exist"):
        current_only_upsert(
            spark,
            df,
            str(tmp_path / "nonexistent"),
            business_keys=["id"],
            create_if_not_exists=False,
        )


def test_track_history_no_create_if_not_exists(spark, tmp_path: Path):
    df = spark.createDataFrame([{"id": 1, "val": "a"}])
    with pytest.raises(ValueError, match="does not exist"):
        track_history_upsert(
            spark,
            df,
            str(tmp_path / "nonexistent"),
            business_keys=["id"],
            create_if_not_exists=False,
            load_ts_expr="to_timestamp('2020-01-01 00:00:00')",
        )


# ---------------------------------------------------------------------------
# 4. Dedup without order_by (dropDuplicates path)
# ---------------------------------------------------------------------------


def test_current_only_dedup_without_order_by(spark, tmp_path: Path):
    target = str(tmp_path / "co_no_order_by")
    df = spark.createDataFrame(
        [
            {"id": 1, "val": "a"},
            {"id": 1, "val": "a"},  # exact duplicate
            {"id": 2, "val": "b"},
        ]
    )
    current_only_upsert(spark, df, target, business_keys=["id"], tracked_columns=["val"])
    out = spark.read.format("delta").load(target)
    assert out.count() == 2
    assert set(r["id"] for r in out.collect()) == {1, 2}


def test_track_history_without_order_by(spark, tmp_path: Path):
    target = str(tmp_path / "th_no_order_by")
    df = spark.createDataFrame(
        [
            {"id": 1, "val": "a"},
            {"id": 1, "val": "a"},  # exact duplicate
            {"id": 2, "val": "b"},
        ]
    )
    track_history_upsert(
        spark,
        df,
        target,
        business_keys=["id"],
        tracked_columns=["val"],
        load_ts_expr="to_timestamp('2020-01-01 00:00:00')",
    )
    out = spark.read.format("delta").load(target)
    assert out.filter("is_current = true").count() == 2
    assert set(r["id"] for r in out.filter("is_current = true").collect()) == {1, 2}


# ---------------------------------------------------------------------------
# 5. Default tracked_columns (None)
# ---------------------------------------------------------------------------


def test_current_only_default_tracked_columns(spark, tmp_path: Path):
    target = str(tmp_path / "co_default_tracked")
    df = spark.createDataFrame([{"id": 1, "val": "a", "extra": 10}])
    current_only_upsert(spark, df, target, business_keys=["id"])

    df2 = spark.createDataFrame([{"id": 1, "val": "b", "extra": 10}])
    current_only_upsert(spark, df2, target, business_keys=["id"])

    out = spark.read.format("delta").load(target)
    assert _rows_by_key(out, "id")[1]["val"] == "b"


def test_track_history_default_tracked_columns(spark, tmp_path: Path):
    target = str(tmp_path / "th_default_tracked")
    df = spark.createDataFrame([{"id": 1, "val": "a", "extra": 10}])
    track_history_upsert(
        spark,
        df,
        target,
        business_keys=["id"],
        load_ts_expr="to_timestamp('2020-01-01 00:00:00')",
    )

    df2 = spark.createDataFrame([{"id": 1, "val": "b", "extra": 10}])
    track_history_upsert(
        spark,
        df2,
        target,
        business_keys=["id"],
        load_ts_expr="to_timestamp('2020-01-02 00:00:00')",
    )

    out = spark.read.format("delta").load(target)
    assert out.filter("is_current = true").count() == 1
    assert out.filter("is_current = true").collect()[0]["version"] == 2
    assert out.filter("is_current = true").collect()[0]["id"] == 1


# ---------------------------------------------------------------------------
# 6. Empty source DataFrame
# ---------------------------------------------------------------------------


def test_track_history_empty_source(spark, tmp_path: Path):
    target = str(tmp_path / "th_empty_source")
    df = spark.createDataFrame([], "id INT, val STRING")
    # Should be a no-op — no exception and target not created
    track_history_upsert(
        spark,
        df,
        target,
        business_keys=["id"],
        load_ts_expr="to_timestamp('2020-01-01 00:00:00')",
    )
    assert not (tmp_path / "th_empty_source").exists()


def test_current_only_empty_source_creates_empty_target(spark, tmp_path: Path):
    target = str(tmp_path / "co_empty_source")
    df = spark.createDataFrame([], "id INT, val STRING")
    current_only_upsert(spark, df, target, business_keys=["id"])
    out = spark.read.format("delta").load(target)
    assert out.count() == 0


# ---------------------------------------------------------------------------
# 7. Idempotency for current_only
# ---------------------------------------------------------------------------


def test_current_only_idempotent(spark, tmp_path: Path):
    target = str(tmp_path / "co_idempotent")
    df = spark.createDataFrame([{"id": 1, "val": "a"}, {"id": 2, "val": "b"}])
    kwargs = dict(business_keys=["id"], tracked_columns=["val"])

    current_only_upsert(spark, df, target, **kwargs)
    count1 = spark.read.format("delta").load(target).count()

    current_only_upsert(spark, df, target, **kwargs)
    out2 = spark.read.format("delta").load(target)
    assert out2.count() == count1
    rows = _rows_by_key(out2, "id")
    assert rows[1]["val"] == "a"
    assert rows[2]["val"] == "b"


# ---------------------------------------------------------------------------
# 8. _resolve_mode unit tests
# ---------------------------------------------------------------------------


def test_resolve_mode_enum_passthrough():
    assert _resolve_mode(ChangeTrackingMode.CURRENT_ONLY) is ChangeTrackingMode.CURRENT_ONLY
    assert _resolve_mode(ChangeTrackingMode.TRACK_HISTORY) is ChangeTrackingMode.TRACK_HISTORY


def test_resolve_mode_int_values():
    assert _resolve_mode(1) is ChangeTrackingMode.CURRENT_ONLY
    assert _resolve_mode(2) is ChangeTrackingMode.TRACK_HISTORY


def test_resolve_mode_string_aliases():
    for alias in ("1", "current", "current_only", "currentonly"):
        assert _resolve_mode(alias) is ChangeTrackingMode.CURRENT_ONLY, alias
    for alias in ("2", "track_history", "trackhistory", "history"):
        assert _resolve_mode(alias) is ChangeTrackingMode.TRACK_HISTORY, alias


def test_resolve_mode_case_insensitive():
    assert _resolve_mode("Track_History") is ChangeTrackingMode.TRACK_HISTORY
    assert _resolve_mode(" CURRENT_ONLY ") is ChangeTrackingMode.CURRENT_ONLY


def test_resolve_mode_invalid_raises():
    for bad in (3, "foo", None):
        with pytest.raises((ValueError, TypeError)):
            _resolve_mode(bad)


# ---------------------------------------------------------------------------
# 9. _extract_tracking_kwargs_from_options unit tests
# ---------------------------------------------------------------------------


def test_extract_options_empty_raises():
    with pytest.raises(ValueError):
        _extract_tracking_kwargs_from_options({})


def test_extract_options_missing_mode_raises():
    with pytest.raises(ValueError, match="change_tracking_mode"):
        _extract_tracking_kwargs_from_options({"business_keys": ["id"]})


def test_extract_options_common_key():
    mode, kwargs = _extract_tracking_kwargs_from_options(
        {
            "change_tracking_mode": "current_only",
            "change_tracking_options": {"business_keys": ["id"]},
        }
    )
    assert mode is ChangeTrackingMode.CURRENT_ONLY
    assert kwargs == {"business_keys": ["id"]}


def test_extract_options_mode_specific_key():
    mode, kwargs = _extract_tracking_kwargs_from_options(
        {
            "change_tracking_mode": "track_history",
            "track_history_options": {"business_keys": ["id"], "tracked_columns": ["val"]},
        }
    )
    assert mode is ChangeTrackingMode.TRACK_HISTORY
    assert kwargs["business_keys"] == ["id"]

    mode2, kwargs2 = _extract_tracking_kwargs_from_options(
        {
            "change_tracking_mode": "current_only",
            "current_only_options": {"business_keys": ["id"]},
        }
    )
    assert mode2 is ChangeTrackingMode.CURRENT_ONLY
    assert kwargs2["business_keys"] == ["id"]


def test_extract_options_normalizes_keys():
    mode, _ = _extract_tracking_kwargs_from_options(
        {
            "  CHANGE_TRACKING_MODE  ": "current_only",
        }
    )
    assert mode is ChangeTrackingMode.CURRENT_ONLY


# ---------------------------------------------------------------------------
# 10. ChangeTrackingWriteBuilder unit tests
# ---------------------------------------------------------------------------


def test_builder_options_too_many_positional_args(spark):
    df = spark.createDataFrame([{"id": 1}])
    builder = ChangeTrackingWriteBuilder(df)
    with pytest.raises(TypeError):
        builder.options({"a": 1}, {"b": 2})


def test_builder_options_non_mapping_positional(spark):
    df = spark.createDataFrame([{"id": 1}])
    builder = ChangeTrackingWriteBuilder(df)
    with pytest.raises(TypeError):
        builder.options("not a dict")


def test_builder_option_single(spark):
    df = spark.createDataFrame([{"id": 1}])
    builder = ChangeTrackingWriteBuilder(df)
    builder.option("change_tracking_mode", "current_only")
    assert builder._options["change_tracking_mode"] == "current_only"


def test_builder_clear(spark):
    df = spark.createDataFrame([{"id": 1}])
    builder = ChangeTrackingWriteBuilder(df)
    builder.option("x", "y")
    builder.clear()
    assert builder._options == {}


def test_builder_table_clears_after_call(monkeypatch, spark, tmp_path: Path):
    df = spark.createDataFrame([{"id": 1, "val": "a"}])
    builder = ChangeTrackingWriteBuilder(df)
    builder.option("change_tracking_mode", "current_only")

    def fake_apply(*, spark, source_df, target, options, verbose=False):
        pass

    monkeypatch.setattr(change_tracking, "apply_change_tracking_from_options", fake_apply)
    builder.table(str(tmp_path / "dummy"))
    assert builder._options == {}


# ---------------------------------------------------------------------------
# 11. Helper function unit tests
# ---------------------------------------------------------------------------


def test_normalize_option_key_none_raises():
    with pytest.raises(ValueError):
        _normalize_option_key(None)


def test_normalize_option_key_strips_and_lowers():
    assert _normalize_option_key("  FOO  ") == "foo"


def test_ensure_mapping_none_returns_empty():
    assert _ensure_mapping(None, option_name="x") == {}


def test_ensure_mapping_non_mapping_raises():
    with pytest.raises(TypeError):
        _ensure_mapping("not a dict", option_name="x")


def test_is_delta_path():
    assert _is_delta_path("/some/path")
    assert _is_delta_path("dbfs:/path")
    assert not _is_delta_path("catalog.schema.table")
    assert not _is_delta_path("my_table")


# ---------------------------------------------------------------------------
# 12. load_ts_expr variants (track_history)
# ---------------------------------------------------------------------------


def test_track_history_load_ts_as_column(spark, tmp_path: Path):
    from pyspark.sql import functions as F

    target = str(tmp_path / "th_ts_column")
    df = spark.createDataFrame([{"id": 1, "val": "a"}])
    ts_col = F.to_timestamp(F.lit("2020-01-01 00:00:00"))
    track_history_upsert(
        spark,
        df,
        target,
        business_keys=["id"],
        tracked_columns=["val"],
        load_ts_expr=ts_col,
    )
    out = spark.read.format("delta").load(target)
    assert out.filter("is_current = true").count() == 1
    assert out.filter("is_current = true").collect()[0]["id"] == 1


def test_track_history_load_ts_default(spark, tmp_path: Path):
    target = str(tmp_path / "th_ts_default")
    df = spark.createDataFrame([{"id": 1, "val": "a"}])
    track_history_upsert(spark, df, target, business_keys=["id"], tracked_columns=["val"])
    out = spark.read.format("delta").load(target)
    assert out.filter("is_current = true").count() == 1
    assert out.filter("is_current = true").collect()[0]["id"] == 1
    assert out.filter("effective_start_ts is not null").count() == 1


# ---------------------------------------------------------------------------
# 13. Source DataFrame with pre-existing metadata columns
# ---------------------------------------------------------------------------


def test_current_only_source_has_row_hash(spark, tmp_path: Path):
    """row_hash present in source is overwritten by the computed hash."""
    target = str(tmp_path / "co_has_row_hash")
    df = spark.createDataFrame([{"id": 1, "val": "a", "row_hash": "old_hash_value"}])
    current_only_upsert(spark, df, target, business_keys=["id"], tracked_columns=["val"])
    out = spark.read.format("delta").load(target)
    row = out.collect()[0]
    assert row["row_hash"] != "old_hash_value"
    assert row["row_hash"] is not None


def test_current_only_source_has_history_fields(spark, tmp_path: Path):
    """History fields in source are excluded from merge write maps."""
    target = str(tmp_path / "co_has_history")
    base = spark.createDataFrame([{"id": 1, "val": "x"}])
    current_only_upsert(spark, base, target, business_keys=["id"], tracked_columns=["val"])

    df = spark.createDataFrame(
        [
            {
                "id": 1,
                "val": "updated",
                "effective_start_ts": "2020-01-01",
                "effective_end_ts": "2099-12-31",
                "is_current": False,
                "version": 99,
            }
        ]
    )
    current_only_upsert(spark, df, target, business_keys=["id"], tracked_columns=["val"])
    out = spark.read.format("delta").load(target)
    assert out.collect()[0]["val"] == "updated"
    # history fields from source must not have been added to target
    assert "is_current" not in out.columns
    assert "version" not in out.columns


def test_track_history_source_has_metadata_columns(spark, tmp_path: Path):
    """Managed metadata values are controlled by the upsert, not taken from source."""
    target = str(tmp_path / "th_has_metadata")
    schema = (
        "id INT, val STRING, effective_start_ts STRING, effective_end_ts STRING,"
        " is_current BOOLEAN, version BIGINT, row_hash STRING"
    )
    df = spark.createDataFrame(
        [(1, "a", "2020-01-01", None, False, 99, "old_hash")],
        schema,
    )
    track_history_upsert(
        spark,
        df,
        target,
        business_keys=["id"],
        tracked_columns=["val"],
        load_ts_expr="to_timestamp('2020-01-01 00:00:00')",
    )
    out = spark.read.format("delta").load(target)
    current = out.filter("is_current = true").collect()
    assert len(current) == 1
    assert current[0]["version"] == 1  # not 99 from source
    assert current[0]["id"] == 1


def test_track_history_source_has_row_hash(spark, tmp_path: Path):
    """row_hash in source is overwritten by computed hash."""
    target = str(tmp_path / "th_has_row_hash")
    df = spark.createDataFrame([{"id": 1, "val": "a", "row_hash": "old_hash"}])
    track_history_upsert(
        spark,
        df,
        target,
        business_keys=["id"],
        tracked_columns=["val"],
        load_ts_expr="to_timestamp('2020-01-01 00:00:00')",
    )
    out = spark.read.format("delta").load(target)
    assert out.collect()[0]["row_hash"] != "old_hash"
    assert out.collect()[0]["id"] == 1


def test_track_history_source_metadata_not_tracked_by_default(spark, tmp_path: Path):
    """Changing only metadata columns in source does not trigger a new version."""
    target = str(tmp_path / "th_meta_not_tracked")
    schema = (
        "id INT, val STRING, effective_start_ts STRING, effective_end_ts STRING,"
        " is_current BOOLEAN, version BIGINT, row_hash STRING"
    )
    df = spark.createDataFrame(
        [(1, "a", "2020-01-01", None, True, 99, "old_hash")],
        schema,
    )
    track_history_upsert(
        spark,
        df,
        target,
        business_keys=["id"],
        load_ts_expr="to_timestamp('2020-01-01 00:00:00')",
    )

    # Second batch: same val, only metadata columns changed
    df2 = spark.createDataFrame(
        [(1, "a", "2099-01-01", "2099-12-31", False, 100, "different_old_hash")],
        schema,
    )
    track_history_upsert(
        spark,
        df2,
        target,
        business_keys=["id"],
        load_ts_expr="to_timestamp('2020-01-02 00:00:00')",
    )

    out = spark.read.format("delta").load(target)
    assert out.filter("is_current = true").count() == 1
    assert out.filter("is_current = true").collect()[0]["version"] == 1
    assert out.filter("is_current = true").collect()[0]["id"] == 1


# ---------------------------------------------------------------------------
# 14. Composite business keys
# ---------------------------------------------------------------------------


def test_current_only_composite_business_keys(spark, tmp_path: Path):
    target = str(tmp_path / "co_composite_keys")
    df = spark.createDataFrame(
        [
            {"country": "US", "category": "A", "val": "x"},
            {"country": "US", "category": "B", "val": "y"},
            {"country": "UK", "category": "A", "val": "z"},
        ]
    )
    current_only_upsert(
        spark,
        df,
        target,
        business_keys=["country", "category"],
        tracked_columns=["val"],
    )
    out = spark.read.format("delta").load(target)
    assert out.count() == 3

    df2 = spark.createDataFrame([{"country": "US", "category": "A", "val": "updated"}])
    current_only_upsert(
        spark,
        df2,
        target,
        business_keys=["country", "category"],
        tracked_columns=["val"],
    )
    out2 = spark.read.format("delta").load(target)
    assert out2.count() == 3
    us_a = out2.filter("country = 'US' AND category = 'A'").collect()[0]
    assert us_a["val"] == "updated"


def test_track_history_composite_business_keys(spark, tmp_path: Path):
    target = str(tmp_path / "th_composite_keys")
    df = spark.createDataFrame(
        [
            {"country": "US", "category": "A", "val": "x"},
            {"country": "US", "category": "B", "val": "y"},
        ]
    )
    track_history_upsert(
        spark,
        df,
        target,
        business_keys=["country", "category"],
        tracked_columns=["val"],
        load_ts_expr="to_timestamp('2020-01-01 00:00:00')",
    )
    out = spark.read.format("delta").load(target)
    assert out.filter("is_current = true").count() == 2

    df2 = spark.createDataFrame([{"country": "US", "category": "A", "val": "updated"}])
    track_history_upsert(
        spark,
        df2,
        target,
        business_keys=["country", "category"],
        tracked_columns=["val"],
        load_ts_expr="to_timestamp('2020-01-02 00:00:00')",
    )
    out2 = spark.read.format("delta").load(target)
    assert out2.filter("is_current = true").count() == 2
    us_a_current = out2.filter("country = 'US' AND category = 'A' AND is_current = true").collect()[
        0
    ]
    assert us_a_current["version"] == 2


# ---------------------------------------------------------------------------
# 15. default_expiry_value
# ---------------------------------------------------------------------------


def test_track_history_default_expiry_value_string(spark, tmp_path: Path):
    """Open rows use the sentinel value; closed rows retain the load timestamp."""
    target = str(tmp_path / "th_default_expiry_string")

    df = spark.createDataFrame([{"id": 1, "val": "a"}])
    track_history_upsert(
        spark,
        df,
        target,
        business_keys=["id"],
        tracked_columns=["val"],
        load_ts_expr="to_timestamp('2020-01-01 00:00:00')",
        default_expiry_value="9999-12-31",
    )

    out1 = spark.read.format("delta").load(target)
    current = out1.filter("is_current = true").collect()
    assert len(current) == 1
    assert current[0]["effective_end_ts"] is not None
    assert current[0]["effective_end_ts"].year == 9999

    # Second batch: trigger a version change
    df2 = spark.createDataFrame([{"id": 1, "val": "b"}])
    track_history_upsert(
        spark,
        df2,
        target,
        business_keys=["id"],
        tracked_columns=["val"],
        load_ts_expr="to_timestamp('2020-01-02 00:00:00')",
        default_expiry_value="9999-12-31",
    )

    out2 = spark.read.format("delta").load(target)
    # Closed row should have the load timestamp, not 9999
    closed = out2.filter("is_current = false").collect()
    assert len(closed) == 1
    assert closed[0]["effective_end_ts"].year == 2020
    # New current row should have the sentinel
    new_current = out2.filter("is_current = true").collect()
    assert len(new_current) == 1
    assert new_current[0]["effective_end_ts"].year == 9999


def test_track_history_default_expiry_value_column(spark, tmp_path: Path):
    """default_expiry_value accepts a PySpark Column expression."""
    from pyspark.sql import functions as F

    target = str(tmp_path / "th_default_expiry_column")
    df = spark.createDataFrame([{"id": 1, "val": "a"}])
    sentinel = F.to_timestamp(F.lit("9999-12-31"))
    track_history_upsert(
        spark,
        df,
        target,
        business_keys=["id"],
        tracked_columns=["val"],
        load_ts_expr="to_timestamp('2020-01-01 00:00:00')",
        default_expiry_value=sentinel,
    )

    out = spark.read.format("delta").load(target)
    current = out.filter("is_current = true").collect()
    assert len(current) == 1
    assert current[0]["effective_end_ts"].year == 9999


def test_track_history_default_expiry_value_none_is_null(spark, tmp_path: Path):
    """Default behavior (None) still writes NULL for open rows."""
    target = str(tmp_path / "th_default_expiry_none")
    df = spark.createDataFrame([{"id": 1, "val": "a"}])
    track_history_upsert(
        spark,
        df,
        target,
        business_keys=["id"],
        tracked_columns=["val"],
        load_ts_expr="to_timestamp('2020-01-01 00:00:00')",
    )

    out = spark.read.format("delta").load(target)
    current = out.filter("is_current = true").collect()
    assert len(current) == 1
    assert current[0]["effective_end_ts"] is None
