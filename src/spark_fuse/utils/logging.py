from __future__ import annotations

from typing import Iterable, Optional, Sequence

from pyspark.sql import SparkSession

from rich.console import Console
from rich.theme import Theme


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
