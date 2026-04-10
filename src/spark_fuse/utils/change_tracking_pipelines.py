"""pyspark.pipelines (Spark Declarative Pipelines) integration for change tracking.

Provides decorator helpers that combine ``pyspark.pipelines`` table/flow registration
with the change tracking merge semantics from :mod:`spark_fuse.utils.change_tracking`.

Requires PySpark >= 4.0 with ``pyspark.pipelines`` available (Spark Declarative Pipelines).
Each decorator lazily imports ``pyspark.pipelines`` so this module can be imported in
environments that do not have SDP without raising an error at module load time.

Example::

    import pyspark.pipelines as dp
    from spark_fuse.utils.change_tracking_pipelines import (
        change_tracking_table,
        change_tracking_flow,
        create_change_tracking_table,
    )

    # Single-decorator pattern — creates table + flow in one step
    @change_tracking_table(
        target="catalog.schema.target",
        change_tracking_mode="track_history",
        business_keys=["id"],
    )
    def my_table():
        return spark.readStream.table("source")


    # Two-step pattern — explicit table + flow
    create_change_tracking_table("target")

    @change_tracking_flow(
        target="target",
        change_tracking_mode="current_only",
        business_keys=["id"],
    )
    def my_flow():
        return spark.readStream.table("source")
"""

from __future__ import annotations

import functools
from typing import Any, Callable, Dict, Iterable, Optional, Sequence, Union

__all__ = [
    "change_tracking_table",
    "change_tracking_flow",
    "create_change_tracking_table",
]

_PIPELINES_IMPORT_ERROR = (
    "pyspark.pipelines is required for declarative pipeline integration. "
    "Ensure you are running PySpark >= 4.0 with Spark Declarative Pipelines support."
)


def _get_dp():
    """Lazily import pyspark.pipelines, raising a clear error when absent."""
    try:
        import pyspark.pipelines as dp

        return dp
    except ImportError as exc:
        raise ImportError(_PIPELINES_IMPORT_ERROR) from exc


def _build_ct_options(
    change_tracking_mode: Union[str, int],
    business_keys: Sequence[str],
    tracked_columns: Optional[Iterable[str]],
    **extra_kwargs: Any,
) -> Dict[str, Any]:
    opts: Dict[str, Any] = {
        "change_tracking_mode": change_tracking_mode,
        "business_keys": list(business_keys),
    }
    if tracked_columns is not None:
        opts["tracked_columns"] = list(tracked_columns)
    opts.update(extra_kwargs)
    return opts


def create_change_tracking_table(
    name: str,
    *,
    schema: Optional[Any] = None,
    partition_cols: Optional[Sequence[str]] = None,
    table_properties: Optional[Dict[str, str]] = None,
    comment: Optional[str] = None,
) -> None:
    """Create a streaming table target for use with :func:`change_tracking_flow`.

    Thin wrapper around ``pyspark.pipelines.create_streaming_table`` that passes through
    all arguments unchanged.

    Args:
        name: Target streaming table name.
        schema: Optional schema (``StructType`` or DDL string).
        partition_cols: Optional partition column names.
        table_properties: Optional Delta table properties.
        comment: Optional table comment.
    """
    dp = _get_dp()

    kwargs: Dict[str, Any] = {}
    if schema is not None:
        kwargs["schema"] = schema
    if partition_cols is not None:
        kwargs["partition_cols"] = list(partition_cols)
    if table_properties is not None:
        kwargs["table_properties"] = table_properties
    if comment is not None:
        kwargs["comment"] = comment

    dp.create_streaming_table(name, **kwargs)


def change_tracking_flow(
    *,
    target: str,
    change_tracking_mode: Union[str, int],
    business_keys: Sequence[str],
    tracked_columns: Optional[Iterable[str]] = None,
    name: Optional[str] = None,
    once: bool = False,
    **change_tracking_kwargs: Any,
) -> Callable:
    """Decorator that registers a ``pyspark.pipelines`` append flow with change tracking.

    The decorated function must return a streaming ``DataFrame``. The decorator wraps it
    with ``@dp.append_flow`` and applies change tracking merge semantics via
    :class:`~spark_fuse.utils.change_tracking_streaming.StreamingChangeTrackingWriter`
    inside a ``foreachBatch`` callback.

    The target streaming table must already exist (e.g. created via
    :func:`create_change_tracking_table`) before the pipeline runs.

    Args:
        target: Target streaming table name.
        change_tracking_mode: ``"current_only"`` or ``"track_history"`` (Type 1 / Type 2).
        business_keys: Column name(s) that uniquely identify a record.
        tracked_columns: Columns to hash for change detection. Defaults to all non-key columns.
        name: Optional explicit flow name (defaults to the decorated function name).
        once: When ``True``, the flow runs only once per pipeline update.
        **change_tracking_kwargs: Additional keyword arguments forwarded to
            :func:`~spark_fuse.utils.change_tracking.current_only_upsert` or
            :func:`~spark_fuse.utils.change_tracking.track_history_upsert`.

    Returns:
        A decorator that registers the function as a change-tracking append flow.
    """

    def decorator(fn: Callable) -> Callable:
        dp = _get_dp()

        from .change_tracking_streaming import StreamingChangeTrackingWriter

        ct_options = _build_ct_options(
            change_tracking_mode=change_tracking_mode,
            business_keys=business_keys,
            tracked_columns=tracked_columns,
            **change_tracking_kwargs,
        )
        batch_writer = StreamingChangeTrackingWriter(target=target, options=ct_options)

        flow_kwargs: Dict[str, Any] = {"target": target}
        if name is not None:
            flow_kwargs["name"] = name
        if once:
            flow_kwargs["once"] = once

        @dp.append_flow(**flow_kwargs)
        @functools.wraps(fn)
        def wrapper():
            source_df = fn()
            return source_df.writeStream.foreachBatch(batch_writer)

        return wrapper

    return decorator


def change_tracking_table(
    *,
    target: str,
    change_tracking_mode: Union[str, int],
    business_keys: Sequence[str],
    tracked_columns: Optional[Iterable[str]] = None,
    name: Optional[str] = None,
    comment: Optional[str] = None,
    table_properties: Optional[Dict[str, str]] = None,
    partition_cols: Optional[Sequence[str]] = None,
    schema: Optional[Any] = None,
    **change_tracking_kwargs: Any,
) -> Callable:
    """Decorator that combines streaming table creation with a change tracking flow.

    Convenience wrapper equivalent to calling :func:`create_change_tracking_table` followed
    by :func:`change_tracking_flow`. The decorated function must return a streaming DataFrame.

    Args:
        target: Target streaming table name (also used as the ``dp.table`` name unless
            ``name`` is provided).
        change_tracking_mode: ``"current_only"`` or ``"track_history"`` (Type 1 / Type 2).
        business_keys: Column name(s) that uniquely identify a record.
        tracked_columns: Columns to hash for change detection. Defaults to all non-key columns.
        name: Optional explicit table/flow name (defaults to ``target``).
        comment: Optional table comment forwarded to ``create_streaming_table``.
        table_properties: Optional Delta table properties forwarded to ``create_streaming_table``.
        partition_cols: Optional partition columns forwarded to ``create_streaming_table``.
        schema: Optional schema forwarded to ``create_streaming_table``.
        **change_tracking_kwargs: Additional keyword arguments forwarded to the underlying
            change tracking upsert function.

    Returns:
        A decorator that registers the function as a change-tracking pipeline table.
    """

    def decorator(fn: Callable) -> Callable:
        dp = _get_dp()

        from .change_tracking_streaming import StreamingChangeTrackingWriter

        # Create the streaming table target
        table_kwargs: Dict[str, Any] = {}
        if schema is not None:
            table_kwargs["schema"] = schema
        if partition_cols is not None:
            table_kwargs["partition_cols"] = list(partition_cols)
        if table_properties is not None:
            table_kwargs["table_properties"] = table_properties
        if comment is not None:
            table_kwargs["comment"] = comment
        dp.create_streaming_table(name or target, **table_kwargs)

        ct_options = _build_ct_options(
            change_tracking_mode=change_tracking_mode,
            business_keys=business_keys,
            tracked_columns=tracked_columns,
            **change_tracking_kwargs,
        )
        batch_writer = StreamingChangeTrackingWriter(target=target, options=ct_options)

        @dp.append_flow(target=name or target)
        @functools.wraps(fn)
        def wrapper():
            source_df = fn()
            return source_df.writeStream.foreachBatch(batch_writer)

        return wrapper

    return decorator
