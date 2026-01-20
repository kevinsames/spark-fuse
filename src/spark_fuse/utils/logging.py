from __future__ import annotations

import time
from typing import Callable, Dict, Iterable, Optional, Protocol, Sequence, Union

from pyspark.sql import SparkSession

from pydantic import BaseModel, Field, field_validator
from rich.console import Console
from rich.theme import Theme
from tqdm.auto import tqdm


_console: Optional[Console] = None


def console() -> Console:
    """Return a shared Rich Console instance with basic theming."""
    global _console
    if _console is None:
        _console = Console(theme=Theme({"info": "cyan", "warn": "yellow", "error": "bold red"}))
    return _console


_DEFAULT_SPARK_LOGGERS: Sequence[str] = (
    "org.apache.spark.storage",  # Shuffle spill diagnostics, memory store details.
    "org.apache.spark.scheduler",  # Stage progress updates.
    "org.apache.spark.shuffle",  # Shuffle write/read details.
)


class LogEventRecord(BaseModel):
    label: str = Field(min_length=1)
    event: Optional[str] = None
    step: int = Field(ge=0)
    total: int = Field(ge=1)
    elapsed_seconds: float = Field(ge=0.0)
    total_elapsed_seconds: float = Field(ge=0.0)
    timestamp: float = Field(ge=0.0)

    @field_validator("label")
    @classmethod
    def _validate_label(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("label must be non-empty")
        return value

    @field_validator("event", mode="before")
    @classmethod
    def _normalize_event(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return None
        text = str(value).strip()
        return text.upper() or None


class LogEventWriter(Protocol):
    def write(self, event: LogEventRecord) -> None: ...


LogEventSink = Union[Callable[[LogEventRecord], None], LogEventWriter]
LogEventSinks = Union[LogEventSink, Sequence[LogEventSink]]


def create_progress_tracker(
    total_steps: int,
    *,
    event_sinks: Optional[LogEventSinks] = None,
) -> Dict[str, object]:
    """Return a simple progress tracker structure."""
    tracker = {
        "current": 0,
        "total": float(total_steps),
        "start": time.perf_counter(),
        "last": None,
        "bar": None,
    }
    if event_sinks is not None:
        tracker["event_sinks"] = _normalize_event_sinks(event_sinks)
    return tracker


def _format_event_label(label: str, event: Optional[str]) -> str:
    if not event:
        return label
    event_label = event.strip().upper()
    if not event_label:
        return label
    return f"{event_label}: {label}"


def _normalize_event_sinks(sinks: Optional[LogEventSinks]) -> Sequence[LogEventSink]:
    if sinks is None:
        return ()
    if isinstance(sinks, (list, tuple)):
        return list(sinks)
    return (sinks,)


def _emit_log_event(record: LogEventRecord, sinks: Sequence[LogEventSink]) -> None:
    for sink in sinks:
        if hasattr(sink, "write"):
            sink.write(record)
        elif callable(sink):
            sink(record)
        else:
            raise TypeError(f"Unsupported log event sink: {sink!r}")


def log_event(
    tracker: Dict[str, object],
    logger: Console,
    label: str,
    *,
    event: Optional[str] = None,
    advance: int = 1,
    sinks: Optional[LogEventSinks] = None,
) -> None:
    """Advance a progress tracker, emit a validated event record, and log with tqdm."""

    now = time.perf_counter()
    last = tracker.get("last") or tracker["start"]

    advance_by = int(advance)
    current = int(tracker.get("current", 0))
    if advance_by:
        current += advance_by

    total = int(tracker.get("total") or 1)

    elapsed = now - float(last)
    total_elapsed = now - float(tracker["start"])

    record = LogEventRecord(
        label=label,
        event=event,
        step=current,
        total=total,
        elapsed_seconds=elapsed,
        total_elapsed_seconds=total_elapsed,
        timestamp=time.time(),
    )

    event_sinks = _normalize_event_sinks(sinks if sinks is not None else tracker.get("event_sinks"))
    if event_sinks:
        _emit_log_event(record, event_sinks)

    tracker["current"] = current
    tracker["last"] = now

    bar = tracker.get("bar")
    if bar is None:
        bar = tqdm(
            total=total,
            file=logger.file,
            dynamic_ncols=True,
        )
        tracker["bar"] = bar
    elif bar.total != total:
        bar.total = total

    bar.set_description_str(_format_event_label(label, record.event))
    bar.set_postfix_str(f"+{elapsed:.2f}s, total {total_elapsed:.2f}s")
    if advance_by:
        bar.update(advance_by)
    else:
        bar.refresh()

    if current >= total:
        bar.close()
        tracker["bar"] = None


def log_start(
    tracker: Dict[str, object],
    logger: Console,
    label: str,
    *,
    advance: int = 1,
    sinks: Optional[LogEventSinks] = None,
) -> None:
    log_event(tracker, logger, label, event="start", advance=advance, sinks=sinks)


def log_end(
    tracker: Dict[str, object],
    logger: Console,
    label: str,
    *,
    advance: int = 1,
    sinks: Optional[LogEventSinks] = None,
) -> None:
    log_event(tracker, logger, label, event="end", advance=advance, sinks=sinks)


def log_error(
    tracker: Dict[str, object],
    logger: Console,
    label: str,
    *,
    advance: int = 1,
    sinks: Optional[LogEventSinks] = None,
) -> None:
    log_event(tracker, logger, label, event="error", advance=advance, sinks=sinks)


def log_warn(
    tracker: Dict[str, object],
    logger: Console,
    label: str,
    *,
    advance: int = 1,
    sinks: Optional[LogEventSinks] = None,
) -> None:
    log_event(tracker, logger, label, event="warn", advance=advance, sinks=sinks)


def log_info(
    tracker: Dict[str, object],
    logger: Console,
    label: str,
    *,
    advance: int = 1,
    sinks: Optional[LogEventSinks] = None,
) -> None:
    log_event(tracker, logger, label, event="info", advance=advance, sinks=sinks)


def log_debug(
    tracker: Dict[str, object],
    logger: Console,
    label: str,
    *,
    advance: int = 1,
    sinks: Optional[LogEventSinks] = None,
) -> None:
    log_event(tracker, logger, label, event="debug", advance=advance, sinks=sinks)


def log_fatal(
    tracker: Dict[str, object],
    logger: Console,
    label: str,
    *,
    advance: int = 1,
    sinks: Optional[LogEventSinks] = None,
) -> None:
    log_event(tracker, logger, label, event="fatal", advance=advance, sinks=sinks)


def log_trace(
    tracker: Dict[str, object],
    logger: Console,
    label: str,
    *,
    advance: int = 1,
    sinks: Optional[LogEventSinks] = None,
) -> None:
    log_event(tracker, logger, label, event="trace", advance=advance, sinks=sinks)


def enable_spark_logging(
    spark: SparkSession,
    *,
    level: str = "INFO",
    categories: Optional[Iterable[str]] = None,
) -> None:
    """Promote Spark log verbosity so shuffle spilling and scheduler details surface.

    Spark's default log level is ``WARN``, which hides shuffle spill diagnostics,
    broadcast cache messages, and other executor hints. This helper raises the log
    level both through the public ``SparkContext.setLogLevel`` API and directly on
    the underlying Log4j loggers that emit the spill messages.

    Args:
        spark: Active ``SparkSession`` instance.
        level: Target log level (case insensitive), defaults to ``"INFO"``.
        categories: Optional iterable of Log4j logger names to tune. When omitted,
            a curated set covering storage, scheduler, and shuffle components is used.
    """

    sc = spark.sparkContext
    sc.setLogLevel(level.upper())

    jvm = getattr(spark, "_jvm", None)
    if jvm is None:
        return

    log_manager = jvm.org.apache.log4j.LogManager
    log4j_level = jvm.org.apache.log4j.Level.toLevel(level.upper())

    for name in categories or _DEFAULT_SPARK_LOGGERS:
        logger = log_manager.getLogger(name)
        if logger is not None:
            logger.setLevel(log4j_level)
