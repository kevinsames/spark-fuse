from __future__ import annotations

import time
from typing import Dict, Iterable, Optional, Sequence

from pyspark.sql import SparkSession

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


def create_progress_tracker(total_steps: int) -> Dict[str, object]:
    """Return a simple progress tracker structure."""
    return {
        "current": 0,
        "total": float(total_steps),
        "start": time.perf_counter(),
        "last": None,
        "bar": None,
    }


def _format_event_label(label: str, event: Optional[str]) -> str:
    if not event:
        return label
    event_label = event.strip().upper()
    if not event_label:
        return label
    return f"{event_label}: {label}"


def log_event(
    tracker: Dict[str, object],
    logger: Console,
    label: str,
    *,
    event: Optional[str] = None,
    advance: int = 1,
) -> None:
    """Advance a progress tracker and log elapsed timings with tqdm."""

    now = time.perf_counter()
    last = tracker.get("last") or tracker["start"]

    advance_by = int(advance)
    current = int(tracker.get("current", 0))
    if advance_by:
        current += advance_by
    tracker["current"] = current
    tracker["last"] = now

    total = int(tracker.get("total") or 1)

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

    elapsed = now - float(last)
    total_elapsed = now - float(tracker["start"])

    bar.set_description_str(_format_event_label(label, event))
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
) -> None:
    log_event(tracker, logger, label, event="start", advance=advance)


def log_end(
    tracker: Dict[str, object],
    logger: Console,
    label: str,
    *,
    advance: int = 1,
) -> None:
    log_event(tracker, logger, label, event="end", advance=advance)


def log_error(
    tracker: Dict[str, object],
    logger: Console,
    label: str,
    *,
    advance: int = 1,
) -> None:
    log_event(tracker, logger, label, event="error", advance=advance)


def log_warn(
    tracker: Dict[str, object],
    logger: Console,
    label: str,
    *,
    advance: int = 1,
) -> None:
    log_event(tracker, logger, label, event="warn", advance=advance)


def log_info(
    tracker: Dict[str, object],
    logger: Console,
    label: str,
    *,
    advance: int = 1,
) -> None:
    log_event(tracker, logger, label, event="info", advance=advance)


def log_debug(
    tracker: Dict[str, object],
    logger: Console,
    label: str,
    *,
    advance: int = 1,
) -> None:
    log_event(tracker, logger, label, event="debug", advance=advance)


def log_fatal(
    tracker: Dict[str, object],
    logger: Console,
    label: str,
    *,
    advance: int = 1,
) -> None:
    log_event(tracker, logger, label, event="fatal", advance=advance)


def log_trace(
    tracker: Dict[str, object],
    logger: Console,
    label: str,
    *,
    advance: int = 1,
) -> None:
    log_event(tracker, logger, label, event="trace", advance=advance)


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
