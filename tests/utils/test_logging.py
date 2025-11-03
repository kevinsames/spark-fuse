from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from spark_fuse.utils.logging import enable_spark_logging


class FakeLogger:
    def __init__(self) -> None:
        self.level = None

    def setLevel(self, level) -> None:  # noqa: N802 (Log4j uses camelCase)
        self.level = level


class FakeLogManager:
    def __init__(self) -> None:
        self._loggers = {}

    def getLogger(self, name):  # noqa: N802
        return self._loggers.setdefault(name, FakeLogger())


class FakeLevel:
    @staticmethod
    def toLevel(name):  # noqa: N802
        return name.upper()


def build_fake_jvm() -> SimpleNamespace:
    manager = FakeLogManager()
    log4j = SimpleNamespace(LogManager=manager, Level=FakeLevel)
    apache = SimpleNamespace(log4j=log4j)
    org = SimpleNamespace(apache=apache)
    return SimpleNamespace(org=org)


@pytest.mark.parametrize("level", ["INFO", "info"])
def test_enable_spark_logging_sets_context_level(level: str) -> None:
    sc = MagicMock()
    spark = SimpleNamespace(sparkContext=sc, _jvm=None)

    enable_spark_logging(spark, level=level)

    sc.setLogLevel.assert_called_once_with(level.upper())


def test_enable_spark_logging_configures_default_loggers() -> None:
    sc = MagicMock()
    jvm = build_fake_jvm()
    spark = SimpleNamespace(sparkContext=sc, _jvm=jvm)

    enable_spark_logging(spark, level="debug")

    sc.setLogLevel.assert_called_once_with("DEBUG")

    manager = jvm.org.apache.log4j.LogManager
    expected_categories = {
        "org.apache.spark.storage",
        "org.apache.spark.scheduler",
        "org.apache.spark.shuffle",
    }
    assert set(manager._loggers) == expected_categories
    for category in expected_categories:
        logger = manager.getLogger(category)
        assert logger.level == "DEBUG"


def test_enable_spark_logging_custom_categories_only() -> None:
    sc = MagicMock()
    jvm = build_fake_jvm()
    spark = SimpleNamespace(sparkContext=sc, _jvm=jvm)
    categories = ["org.example.first", "org.example.second"]

    enable_spark_logging(spark, level="warn", categories=categories)

    sc.setLogLevel.assert_called_once_with("WARN")
    manager = jvm.org.apache.log4j.LogManager
    assert set(manager._loggers) == set(categories)
    for name in categories:
        assert manager._loggers[name].level == "WARN"
