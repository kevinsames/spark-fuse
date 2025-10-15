from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, ClassVar, Mapping, Optional

from pyspark.sql import DataFrame, SparkSession


class Connector(ABC):
    """Abstract base class for IO connectors.

    Connector implementations must define a class attribute `name` and implement
    `validate_path`, `read`, and `write`.
    """

    #: Short identifier used for registry lookups
    name: ClassVar[str]

    @abstractmethod
    def validate_path(self, path: str) -> bool:
        """Return True if the given path/URI is supported by this connector."""

    @abstractmethod
    def read(
        self,
        spark: SparkSession,
        source: Any,
        *,
        fmt: Optional[str] = None,
        schema: Optional[Any] = None,
        source_config: Optional[Mapping[str, Any]] = None,
        options: Optional[Mapping[str, Any]] = None,
        **kwargs: Any,
    ) -> DataFrame:
        """Load a dataset from the given source and return a Spark DataFrame.

        Args:
            spark: Active ``SparkSession``.
            source: Canonical identifier for the data source (e.g. filesystem path,
                database table name, REST endpoint, etc.).
            fmt: Optional format hint (e.g. ``delta``, ``csv``) for connectors that
                support multiple serialization formats.
            schema: Optional explicit schema to enforce. Connectors may ignore this
                parameter if schema enforcement is not supported.
            source_config: Connector-specific configuration describing how to read
                from ``source`` (e.g. pagination settings for REST APIs, credentials,
                or authentication hints).
            options: Additional connector options. This can be used for Spark read
                options or arbitrary connector-specific parameters.
            **kwargs: Future extension point for connector-specific keyword arguments.

        Returns:
            pyspark.sql.DataFrame: The loaded dataset.
        """

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
