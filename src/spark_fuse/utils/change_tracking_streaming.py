from __future__ import annotations

from typing import Any, Dict, Mapping, Optional

from pyspark.sql import DataFrame
from pyspark.sql.streaming import StreamingQuery

from .change_tracking import apply_change_tracking

__all__ = [
    "StreamingChangeTrackingWriter",
    "StreamingChangeTrackingBuilder",
    "streaming_change_tracking_writer",
]


class StreamingChangeTrackingWriter:
    """Callable for use with ``foreachBatch`` that applies change tracking to each micro-batch.

    Pass an instance to ``DataStreamWriter.foreachBatch()`` or use the fluent
    :class:`StreamingChangeTrackingBuilder` API via ``df.writeStream.change_tracking``.

    Args:
        target: Target table name or Delta path.
        options: Flat change tracking options dict — must include ``change_tracking_mode`` and
            ``business_keys`` plus any other kwargs accepted by the underlying upsert function.
    """

    def __init__(self, *, target: str, options: Dict[str, Any]) -> None:
        self._target = target
        self._options = dict(options)

    def __call__(self, batch_df: DataFrame, batch_id: int) -> None:
        spark = batch_df.sparkSession
        opts = dict(self._options)
        mode = opts.pop("change_tracking_mode")
        verbose = opts.pop("verbose", False)
        apply_change_tracking(
            spark,
            batch_df,
            self._target,
            change_tracking_mode=mode,
            verbose=verbose,
            **opts,
        )


class StreamingChangeTrackingBuilder:
    """Fluent API for applying change tracking to a streaming DataFrame via ``foreachBatch``.

    Obtained via ``df.writeStream.change_tracking`` after calling
    :func:`~spark_fuse.utils.change_tracking.enable_change_tracking_accessors`.

    Example::

        from spark_fuse.utils import enable_change_tracking_accessors
        enable_change_tracking_accessors()

        query = (
            spark.readStream.format("delta").load("/src")
            .writeStream.change_tracking
            .options(change_tracking_mode="track_history", business_keys=["id"])
            .toTable("/tgt", checkpoint="/chk", trigger={"availableNow": True})
        )
        query.awaitTermination()
    """

    def __init__(self, df: DataFrame) -> None:
        if not df.isStreaming:
            raise ValueError(
                "StreamingChangeTrackingBuilder requires a streaming DataFrame. "
                "Use df.change_tracking (without writeStream) for batch DataFrames."
            )
        self._df = df
        self._options: Dict[str, Any] = {}

    def option(self, key: str, value: Any) -> "StreamingChangeTrackingBuilder":
        """Set a single change tracking option."""
        self._options[str(key)] = value
        return self

    def options(self, *args: Mapping[str, Any], **kwargs: Any) -> "StreamingChangeTrackingBuilder":
        """Set multiple change tracking options.

        Accepts an optional positional mapping and/or keyword arguments, mirroring the
        batch :class:`~spark_fuse.utils.change_tracking.ChangeTrackingWriteBuilder` API.
        """
        if len(args) > 1:
            raise TypeError("options() accepts at most one positional mapping argument.")
        if args:
            mapping = args[0]
            if not isinstance(mapping, Mapping):
                raise TypeError("options() positional argument must be a mapping/dict.")
            for key, value in mapping.items():
                self.option(key, value)
        for key, value in kwargs.items():
            self.option(key, value)
        return self

    def clear(self) -> "StreamingChangeTrackingBuilder":
        """Clear all accumulated options."""
        self._options.clear()
        return self

    def toTable(
        self,
        name: str,
        checkpoint: str,
        *,
        verbose: bool = False,
        output_mode: str = "update",
        trigger: Optional[Dict[str, Any]] = None,
        query_name: Optional[str] = None,
        **streaming_options: Any,
    ) -> StreamingQuery:
        """Start a streaming query that writes to ``name`` using change tracking semantics.

        Each micro-batch is processed by :class:`StreamingChangeTrackingWriter` via
        ``foreachBatch``, which delegates to the existing batch change tracking helpers.
        The built-in row-hash idempotency guard ensures exactly-once semantics against
        Delta sinks even on microbatch replay.

        Args:
            name: Target table name or Delta path.
            checkpoint: Checkpoint location for fault tolerance and exactly-once delivery.
            verbose: Pass ``True`` to enable INFO-level logging inside the change tracking helpers.
            output_mode: Streaming output mode (default ``"update"``).
            trigger: Trigger options passed as keyword arguments to ``DataStreamWriter.trigger()``,
                e.g. ``{"availableNow": True}`` for one-shot processing.
            query_name: Optional name for the resulting :class:`~pyspark.sql.streaming.StreamingQuery`.
            **streaming_options: Extra options forwarded to ``DataStreamWriter.option()``.

        Returns:
            The started :class:`~pyspark.sql.streaming.StreamingQuery`.
        """
        batch_writer = StreamingChangeTrackingWriter(
            target=name,
            options={**self._options, "verbose": verbose},
        )
        try:
            writer = self._df.writeStream
            writer = writer.option("checkpointLocation", checkpoint)
            writer = writer.outputMode(output_mode)
            for k, v in streaming_options.items():
                writer = writer.option(k, v)
            if trigger is not None:
                writer = writer.trigger(**trigger)
            if query_name is not None:
                writer = writer.queryName(query_name)
            return writer.foreachBatch(batch_writer).start()
        finally:
            self.clear()


def streaming_change_tracking_writer(df: DataFrame) -> StreamingChangeTrackingBuilder:
    """Return a :class:`StreamingChangeTrackingBuilder` for the given streaming DataFrame.

    Used internally by the ``df.writeStream.change_tracking`` accessor registered via
    :func:`~spark_fuse.utils.change_tracking.enable_change_tracking_accessors`.
    """
    return StreamingChangeTrackingBuilder(df)
