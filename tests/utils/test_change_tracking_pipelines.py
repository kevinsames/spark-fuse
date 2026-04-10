"""Tests for change_tracking_pipelines.

pyspark.pipelines is not available in standard PySpark test environments; all tests
mock the dp module so behaviour can be verified without a Spark Declarative Pipelines runtime.
"""

from __future__ import annotations

import sys
import types
from typing import Any, Callable, Dict, List
from unittest.mock import MagicMock, patch

import pytest


# ---------------------------------------------------------------------------
# Fake pyspark.pipelines module injected into sys.modules for all tests
# ---------------------------------------------------------------------------


class _FakeDP:
    """Minimal fake pyspark.pipelines that records decorator calls."""

    def __init__(self):
        self.table_calls: List[Dict[str, Any]] = []
        self.append_flow_calls: List[Dict[str, Any]] = []
        self.create_streaming_table_calls: List[Dict[str, Any]] = []

    def table(self, **kwargs) -> Callable:
        self.table_calls.append(kwargs)

        def decorator(fn):
            return fn

        return decorator

    def append_flow(self, **kwargs) -> Callable:
        self.append_flow_calls.append(kwargs)

        def decorator(fn):
            return fn

        return decorator

    def create_streaming_table(self, name: str, **kwargs) -> None:
        self.create_streaming_table_calls.append({"name": name, **kwargs})


@pytest.fixture()
def fake_dp():
    """Inject a fresh FakeDP into sys.modules for the duration of each test."""
    dp = _FakeDP()
    module = types.ModuleType("pyspark.pipelines")
    module.table = dp.table
    module.append_flow = dp.append_flow
    module.create_streaming_table = dp.create_streaming_table

    # Also expose dp object for assertion convenience
    module._dp = dp

    sys.modules["pyspark.pipelines"] = module
    # Ensure fresh import of the module under test so lazy import picks up the fake
    import importlib

    import spark_fuse.utils.change_tracking_pipelines as ct_pipelines

    importlib.reload(ct_pipelines)
    yield dp, ct_pipelines
    del sys.modules["pyspark.pipelines"]


# ---------------------------------------------------------------------------
# create_change_tracking_table
# ---------------------------------------------------------------------------


def test_create_change_tracking_table_forwards_name(fake_dp):
    dp, ct = fake_dp
    ct.create_change_tracking_table("my_table")
    assert len(dp.create_streaming_table_calls) == 1
    assert dp.create_streaming_table_calls[0]["name"] == "my_table"


def test_create_change_tracking_table_forwards_kwargs(fake_dp):
    dp, ct = fake_dp
    ct.create_change_tracking_table(
        "my_table",
        comment="test table",
        table_properties={"delta.enableChangeDataFeed": "true"},
        partition_cols=["region"],
    )
    call = dp.create_streaming_table_calls[0]
    assert call["comment"] == "test table"
    assert call["table_properties"] == {"delta.enableChangeDataFeed": "true"}
    assert call["partition_cols"] == ["region"]


def test_create_change_tracking_table_omits_none_kwargs(fake_dp):
    """None values must not be forwarded to avoid overriding SDP defaults."""
    dp, ct = fake_dp
    ct.create_change_tracking_table("tbl", schema=None, comment=None)
    call = dp.create_streaming_table_calls[0]
    assert "schema" not in call
    assert "comment" not in call


# ---------------------------------------------------------------------------
# change_tracking_flow
# ---------------------------------------------------------------------------


def test_change_tracking_flow_registers_append_flow(fake_dp):
    dp, ct = fake_dp

    @ct.change_tracking_flow(
        target="my_target",
        change_tracking_mode="current_only",
        business_keys=["id"],
    )
    def my_flow():
        return MagicMock()

    assert len(dp.append_flow_calls) == 1
    assert dp.append_flow_calls[0]["target"] == "my_target"


def test_change_tracking_flow_custom_name(fake_dp):
    dp, ct = fake_dp

    @ct.change_tracking_flow(
        target="my_target",
        change_tracking_mode="current_only",
        business_keys=["id"],
        name="custom_flow",
    )
    def my_flow():
        return MagicMock()

    assert dp.append_flow_calls[0]["name"] == "custom_flow"


def test_change_tracking_flow_once_flag(fake_dp):
    dp, ct = fake_dp

    @ct.change_tracking_flow(
        target="my_target",
        change_tracking_mode="current_only",
        business_keys=["id"],
        once=True,
    )
    def my_flow():
        return MagicMock()

    assert dp.append_flow_calls[0].get("once") is True


def test_change_tracking_flow_omits_name_when_none(fake_dp):
    dp, ct = fake_dp

    @ct.change_tracking_flow(
        target="my_target",
        change_tracking_mode="current_only",
        business_keys=["id"],
    )
    def my_flow():
        return MagicMock()

    assert "name" not in dp.append_flow_calls[0]


def test_change_tracking_flow_creates_writer_with_correct_options(fake_dp, monkeypatch):
    """StreamingChangeTrackingWriter must be created with the right target and mode."""
    dp, ct = fake_dp

    created_writers: List[Dict[str, Any]] = []
    import spark_fuse.utils.change_tracking_streaming as ct_streaming

    original_cls = ct_streaming.StreamingChangeTrackingWriter

    class CapturingWriter(original_cls):
        def __init__(self, *, target, options):
            created_writers.append({"target": target, "options": dict(options)})
            super().__init__(target=target, options=options)

    monkeypatch.setattr(ct_streaming, "StreamingChangeTrackingWriter", CapturingWriter)
    # Reload so the patched class is used during decoration
    import importlib

    importlib.reload(ct)

    @ct.change_tracking_flow(
        target="tgt",
        change_tracking_mode="track_history",
        business_keys=["id", "region"],
        tracked_columns=["val"],
    )
    def my_flow():
        return MagicMock()

    assert len(created_writers) == 1
    w = created_writers[0]
    assert w["target"] == "tgt"
    assert w["options"]["change_tracking_mode"] == "track_history"
    assert w["options"]["business_keys"] == ["id", "region"]
    assert w["options"]["tracked_columns"] == ["val"]


# ---------------------------------------------------------------------------
# change_tracking_table
# ---------------------------------------------------------------------------


def test_change_tracking_table_creates_streaming_table(fake_dp):
    dp, ct = fake_dp

    @ct.change_tracking_table(
        target="catalog.schema.tgt",
        change_tracking_mode="track_history",
        business_keys=["id"],
    )
    def my_table():
        return MagicMock()

    assert len(dp.create_streaming_table_calls) == 1
    assert dp.create_streaming_table_calls[0]["name"] == "catalog.schema.tgt"


def test_change_tracking_table_uses_name_over_target_for_table(fake_dp):
    dp, ct = fake_dp

    @ct.change_tracking_table(
        target="tgt",
        change_tracking_mode="current_only",
        business_keys=["id"],
        name="explicit_name",
    )
    def my_table():
        return MagicMock()

    assert dp.create_streaming_table_calls[0]["name"] == "explicit_name"
    assert dp.append_flow_calls[0]["target"] == "explicit_name"


def test_change_tracking_table_registers_append_flow(fake_dp):
    dp, ct = fake_dp

    @ct.change_tracking_table(
        target="tgt",
        change_tracking_mode="current_only",
        business_keys=["id"],
    )
    def my_table():
        return MagicMock()

    assert len(dp.append_flow_calls) == 1
    assert dp.append_flow_calls[0]["target"] == "tgt"


def test_change_tracking_table_forwards_table_properties(fake_dp):
    dp, ct = fake_dp

    @ct.change_tracking_table(
        target="tgt",
        change_tracking_mode="current_only",
        business_keys=["id"],
        comment="my table",
        table_properties={"prop": "val"},
        partition_cols=["dt"],
    )
    def my_table():
        return MagicMock()

    call = dp.create_streaming_table_calls[0]
    assert call["comment"] == "my table"
    assert call["table_properties"] == {"prop": "val"}
    assert call["partition_cols"] == ["dt"]


# ---------------------------------------------------------------------------
# Missing pyspark.pipelines error
# ---------------------------------------------------------------------------


def test_missing_pyspark_pipelines_raises_clear_error():
    """All three entry points must raise ImportError with a useful message when SDP absent."""
    import spark_fuse.utils.change_tracking_pipelines as ct

    def _raise_import_error():
        raise ImportError("No module named 'pyspark.pipelines'")

    with patch.object(ct, "_get_dp", _raise_import_error):
        with pytest.raises(ImportError, match="pyspark.pipelines"):
            ct.create_change_tracking_table("tbl")

        with pytest.raises(ImportError, match="pyspark.pipelines"):

            @ct.change_tracking_flow(
                target="tgt",
                change_tracking_mode="current_only",
                business_keys=["id"],
            )
            def flow():
                return MagicMock()

        with pytest.raises(ImportError, match="pyspark.pipelines"):

            @ct.change_tracking_table(
                target="tgt",
                change_tracking_mode="current_only",
                business_keys=["id"],
            )
            def table():
                return MagicMock()
