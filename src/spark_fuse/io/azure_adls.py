from __future__ import annotations

import re
from typing import Any, Mapping, Optional

from pyspark.sql import DataFrame, SparkSession

from .base import Connector
from .registry import register_connector


_ABFSS_RE = re.compile(r"^abfss://[^@]+@[^/]+/.+")


@register_connector
class ADLSGen2Connector(Connector):
    """Connector for Azure Data Lake Storage Gen2 using `abfss://` URIs.

    - Supports reading and writing Delta (default), Parquet, and CSV.
    - Path must match `abfss://<container>@<account>.dfs.core.windows.net/<path>`.
    """

    name = "adls"

    def validate_path(self, path: str) -> bool:
        """Return True if the path is a valid ADLS Gen2 `abfss://` URI."""
        return bool(_ABFSS_RE.match(path))

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
        """Read a dataset from ADLS Gen2.

        Args:
            spark: Active `SparkSession`.
            source: `abfss://` location to read from.
            fmt: Optional format override: `delta` (default), `parquet`, or `csv`.
            schema: Optional schema for structured reads.
            source_config: Unused for ADLS, accepted for interface compatibility.
            options: Additional Spark read options.
        """
        if not isinstance(source, str):
            raise TypeError("ADLSGen2Connector.read expects 'source' to be a string path")
        path = source
        if not self.validate_path(path):
            raise ValueError(f"Invalid ADLS Gen2 path: {path}")
        opts = dict(options or {})
        fmt = (fmt or opts.pop("format", None) or "delta").lower()
        reader = spark.read
        if schema is not None:
            reader = reader.schema(schema)
        if opts:
            reader = reader.options(**opts)
        if fmt == "delta":
            return reader.format("delta").load(path)
        elif fmt in {"parquet", "csv"}:
            return reader.format(fmt).load(path)
        else:
            raise ValueError(f"Unsupported format for ADLS: {fmt}")

    def write(
        self,
        df: DataFrame,
        path: str,
        *,
        fmt: Optional[str] = None,
        mode: str = "error",
        **options: Any,
    ) -> None:
        """Write a dataset to ADLS Gen2.

        Args:
            df: DataFrame to write.
            path: `abfss://` output location.
            fmt: Optional format override: `delta` (default), `parquet`, or `csv`.
            mode: Save mode, e.g. `error`, `overwrite`, `append`.
            **options: Additional Spark write options.
        """
        if not self.validate_path(path):
            raise ValueError(f"Invalid ADLS Gen2 path: {path}")
        fmt = (fmt or options.pop("format", None) or "delta").lower()
        writer = df.write.mode(mode).options(**options)
        if fmt == "delta":
            writer.format("delta").save(path)
        elif fmt in {"parquet", "csv"}:
            writer.format(fmt).save(path)
        else:
            raise ValueError(f"Unsupported format for ADLS: {fmt}")
