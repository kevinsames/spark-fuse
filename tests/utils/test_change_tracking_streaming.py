from __future__ import annotations

from pathlib import Path
from typing import Any, Dict
from unittest.mock import MagicMock

import pytest

import spark_fuse.utils.change_tracking as change_tracking
import spark_fuse.utils.change_tracking_streaming as ct_streaming

delta = pytest.importorskip("delta")  # skip entire module if delta-spark unavailable


from spark_fuse.utils.change_tracking_streaming import (  # noqa: E402
    StreamingChangeTrackingBuilder,
    StreamingChangeTrackingWriter,
    streaming_change_tracking_writer,
)


# ---------------------------------------------------------------------------
# Unit tests — no Spark streaming needed
# ---------------------------------------------------------------------------


def test_streaming_writer_stores_options():
    writer = StreamingChangeTrackingWriter(
        target="my_target",
        options={"change_tracking_mode": "current_only", "business_keys": ["id"]},
    )
    assert writer._target == "my_target"
    assert writer._options["change_tracking_mode"] == "current_only"


def test_streaming_writer_options_are_copied():
    opts = {"change_tracking_mode": "current_only", "business_keys": ["id"]}
    writer = StreamingChangeTrackingWriter(target="t", options=opts)
    opts["extra"] = "mutated"
    assert "extra" not in writer._options


def test_streaming_writer_call_delegates(monkeypatch):
    calls: list[Dict[str, Any]] = []

    def fake_apply(spark, source_df, target, *, change_tracking_mode, verbose=False, **kwargs):
        calls.append(
            {"source_df": source_df, "target": target, "mode": change_tracking_mode, **kwargs}
        )

    monkeypatch.setattr(ct_streaming, "apply_change_tracking", fake_apply)

    batch_df = MagicMock()
    batch_df.sparkSession = MagicMock()

    writer = StreamingChangeTrackingWriter(
        target="tgt",
        options={"change_tracking_mode": "current_only", "business_keys": ["id"]},
    )
    writer(batch_df, 0)

    assert len(calls) == 1
    assert calls[0]["target"] == "tgt"
    assert calls[0]["source_df"] is batch_df
    assert calls[0]["mode"] == "current_only"
    assert calls[0]["business_keys"] == ["id"]


def test_streaming_writer_passes_independent_options_copy(monkeypatch):
    """Each __call__ invocation gets its own options copy (no cross-batch mutation)."""
    received: list[Dict[str, Any]] = []

    def fake_apply(spark, source_df, target, *, change_tracking_mode, verbose=False, **kwargs):
        kwargs["__mutated__"] = True
        received.append({"change_tracking_mode": change_tracking_mode, **kwargs})

    monkeypatch.setattr(ct_streaming, "apply_change_tracking", fake_apply)

    batch_df = MagicMock()
    batch_df.sparkSession = MagicMock()

    writer = StreamingChangeTrackingWriter(
        target="tgt",
        options={"change_tracking_mode": "current_only", "business_keys": ["id"]},
    )
    writer(batch_df, 0)
    writer(batch_df, 1)

    # Writer's own options must not be polluted between calls
    assert "__mutated__" not in writer._options
    assert len(received) == 2


def test_streaming_builder_rejects_batch_df():
    batch_df = MagicMock()
    batch_df.isStreaming = False
    with pytest.raises(ValueError, match="streaming DataFrame"):
        StreamingChangeTrackingBuilder(batch_df)


def test_streaming_change_tracking_writer_fn():
    """streaming_change_tracking_writer raises ValueError on batch DataFrame."""
    batch_df = MagicMock()
    batch_df.isStreaming = False
    with pytest.raises(ValueError, match="streaming DataFrame"):
        streaming_change_tracking_writer(batch_df)


def test_builder_option_and_options_fluent():
    builder = MagicMock(spec=StreamingChangeTrackingBuilder)
    builder._options = {}
    builder.option = StreamingChangeTrackingBuilder.option.__get__(builder)  # type: ignore[assignment]

    # Replace with a real builder via __new__ to avoid Spark session requirement
    class FakeDF:
        isStreaming = True

    b = StreamingChangeTrackingBuilder.__new__(StreamingChangeTrackingBuilder)
    b._df = FakeDF()
    b._options = {}
    b.option("change_tracking_mode", "current_only")
    assert b._options["change_tracking_mode"] == "current_only"

    b.options({"business_keys": ["id"]}, extra="val")
    assert b._options["business_keys"] == ["id"]
    assert b._options["extra"] == "val"

    b.clear()
    assert b._options == {}


def test_builder_options_rejects_multiple_positional():
    class FakeDF:
        isStreaming = True

    b = StreamingChangeTrackingBuilder.__new__(StreamingChangeTrackingBuilder)
    b._df = FakeDF()
    b._options = {}
    with pytest.raises(TypeError, match="at most one"):
        b.options({"a": 1}, {"b": 2})


def test_builder_options_rejects_non_mapping():
    class FakeDF:
        isStreaming = True

    b = StreamingChangeTrackingBuilder.__new__(StreamingChangeTrackingBuilder)
    b._df = FakeDF()
    b._options = {}
    with pytest.raises(TypeError, match="mapping"):
        b.options("not-a-dict")


# ---------------------------------------------------------------------------
# Integration tests — require Delta + Spark streaming
# ---------------------------------------------------------------------------


def _write_delta(spark, rows, schema, path: str) -> None:
    spark.createDataFrame(rows, schema).write.format("delta").mode("overwrite").save(path)


def _append_delta(spark, rows, schema, path: str) -> None:
    spark.createDataFrame(rows, schema).write.format("delta").mode("append").save(path)


def _run_streaming_query(spark, src_path: str, tgt_path: str, chk_path: str, **ct_options):
    """Start a streaming query with change tracking and wait for completion."""
    change_tracking.enable_change_tracking_accessors(force=True)
    query = (
        spark.readStream.format("delta")
        .load(src_path)
        .writeStream.change_tracking.options(**ct_options)
        .toTable(tgt_path, checkpoint=chk_path, trigger={"availableNow": True})
    )
    query.awaitTermination()


def test_streaming_current_only_basic(spark, tmp_path: Path):
    src = str(tmp_path / "src")
    tgt = str(tmp_path / "tgt")
    chk = str(tmp_path / "chk")

    _write_delta(spark, [(1, "a"), (2, "b")], "id INT, val STRING", src)
    _run_streaming_query(
        spark,
        src,
        tgt,
        chk,
        change_tracking_mode="current_only",
        business_keys=["id"],
    )

    result = spark.read.format("delta").load(tgt)
    assert result.count() == 2
    rows = {r["id"]: r["val"] for r in result.select("id", "val").collect()}
    assert rows == {1: "a", 2: "b"}


def test_streaming_track_history_basic(spark, tmp_path: Path):
    src = str(tmp_path / "src")
    tgt = str(tmp_path / "tgt")
    chk = str(tmp_path / "chk")

    _write_delta(spark, [(1, "a")], "id INT, val STRING", src)
    _run_streaming_query(
        spark,
        src,
        tgt,
        chk,
        change_tracking_mode="track_history",
        business_keys=["id"],
    )

    result = spark.read.format("delta").load(tgt)
    assert result.count() == 1
    row = result.collect()[0]
    assert row["id"] == 1
    assert row["is_current"] is True
    assert row["version"] == 1


def test_streaming_idempotent_reprocess(spark, tmp_path: Path):
    """Restarting the query from the same checkpoint must not create duplicate rows."""
    src = str(tmp_path / "src")
    tgt = str(tmp_path / "tgt")
    chk = str(tmp_path / "chk")

    _write_delta(spark, [(1, "a"), (2, "b")], "id INT, val STRING", src)

    # First run
    _run_streaming_query(
        spark,
        src,
        tgt,
        chk,
        change_tracking_mode="track_history",
        business_keys=["id"],
    )
    count_after_first = spark.read.format("delta").load(tgt).count()

    # Second run from same checkpoint — no new source data
    _run_streaming_query(
        spark,
        src,
        tgt,
        chk,
        change_tracking_mode="track_history",
        business_keys=["id"],
    )
    count_after_second = spark.read.format("delta").load(tgt).count()

    assert count_after_first == count_after_second == 2


def test_streaming_multi_microbatch_history(spark, tmp_path: Path):
    """New source data between runs produces a new version, not a duplicate."""
    src = str(tmp_path / "src")
    tgt = str(tmp_path / "tgt")
    chk = str(tmp_path / "chk")

    _write_delta(spark, [(1, "a")], "id INT, val STRING", src)
    _run_streaming_query(
        spark,
        src,
        tgt,
        chk,
        change_tracking_mode="track_history",
        business_keys=["id"],
    )

    # Update — append a new version of id=1 to the source table
    _append_delta(spark, [(1, "b")], "id INT, val STRING", src)
    _run_streaming_query(
        spark,
        src,
        tgt,
        chk,
        change_tracking_mode="track_history",
        business_keys=["id"],
    )

    result = spark.read.format("delta").load(tgt)
    rows = result.collect()
    assert result.count() == 2  # version 1 (expired) + version 2 (current)

    versions = {r["version"]: r for r in rows}
    assert versions[1]["is_current"] is False
    assert versions[2]["is_current"] is True
    assert versions[2]["val"] == "b"


def test_streaming_writer_options_forwarded(monkeypatch, spark, tmp_path: Path):
    """Verify the writer forwards target and options correctly for each micro-batch."""
    src = str(tmp_path / "src")
    chk = str(tmp_path / "chk")
    tgt = str(tmp_path / "tgt")

    _write_delta(spark, [(1, "a")], "id INT, val STRING", src)

    calls: list[Dict[str, Any]] = []
    real_apply = ct_streaming.apply_change_tracking

    def capturing_apply(
        spark_session, source_df, target, *, change_tracking_mode, verbose=False, **kwargs
    ):
        calls.append({"target": target, "mode": change_tracking_mode, **kwargs})
        real_apply(
            spark_session,
            source_df,
            target,
            change_tracking_mode=change_tracking_mode,
            verbose=verbose,
            **kwargs,
        )

    monkeypatch.setattr(ct_streaming, "apply_change_tracking", capturing_apply)

    change_tracking.enable_change_tracking_accessors(force=True)
    query = (
        spark.readStream.format("delta")
        .load(src)
        .writeStream.change_tracking.options(
            change_tracking_mode="current_only",
            business_keys=["id"],
        )
        .toTable(tgt, checkpoint=chk, trigger={"availableNow": True})
    )
    query.awaitTermination()

    assert len(calls) >= 1
    assert all(c["target"] == tgt for c in calls)
    assert all(c["mode"] == "current_only" for c in calls)


def test_datastream_writer_accessor_registered(spark, tmp_path: Path):
    """df.writeStream.change_tracking is a StreamingChangeTrackingBuilder."""
    src = str(tmp_path / "src")
    _write_delta(spark, [(1, "a")], "id INT, val STRING", src)

    change_tracking.enable_change_tracking_accessors(force=True)
    streaming_df = spark.readStream.format("delta").load(src)
    builder = streaming_df.writeStream.change_tracking
    assert isinstance(builder, StreamingChangeTrackingBuilder)
