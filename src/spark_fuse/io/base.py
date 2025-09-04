from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, ClassVar, Optional

from pyspark.sql import DataFrame, SparkSession


class Connector(ABC):
    """Abstract base class for IO connectors."""

    #: Short identifier used for registry lookups
    name: ClassVar[str]

    @abstractmethod
    def validate_path(self, path: str) -> bool:
        """Return True if the given path/URI is supported by this connector."""

    @abstractmethod
    def read(self, spark: SparkSession, path: str, *, fmt: Optional[str] = None, **options: Any) -> DataFrame:
        """Read a dataset from the given path using the connector."""

    @abstractmethod
    def write(
        self,
        df: DataFrame,
        path: str,
        *,
        fmt: Optional[str] = None,
        mode: str = "error",
        **options: Any,
    ) -> None:
        """Write a dataset to the given path using the connector."""

