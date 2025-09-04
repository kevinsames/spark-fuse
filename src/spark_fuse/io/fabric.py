from __future__ import annotations

import re
from typing import Any, Optional

from pyspark.sql import DataFrame, SparkSession

from .base import Connector
from .registry import register_connector


_ONELAKE_SCHEME = re.compile(r"^onelake://[^/]+/.+")
_ONELAKE_ABFSS = re.compile(r"^abfss://[^@]+@onelake\.dfs\.fabric\.microsoft\.com/.+")


@register_connector
class FabricLakehouseConnector(Connector):
    """Connector for Microsoft Fabric Lakehouses via OneLake URIs.

    Accepts either `onelake://...` URIs or `abfss://...@onelake.dfs.fabric.microsoft.com/...`.
    Supports Delta (default), Parquet, and CSV.
    """

    name = "fabric"

    def validate_path(self, path: str) -> bool:
        """Return True if the path looks like a valid OneLake URI."""
        return bool(_ONELAKE_SCHEME.match(path) or _ONELAKE_ABFSS.match(path))

    def read(
        self, spark: SparkSession, path: str, *, fmt: Optional[str] = None, **options: Any
    ) -> DataFrame:
        """Read a dataset from a Fabric OneLake-backed location.

        Args:
            spark: Active `SparkSession`.
            path: OneLake or abfss-on-OneLake path.
            fmt: Optional format override: `delta` (default), `parquet`, or `csv`.
            **options: Additional Spark read options.
        """
        if not self.validate_path(path):
            raise ValueError(f"Invalid Fabric OneLake path: {path}")
        fmt = (fmt or options.pop("format", None) or "delta").lower()
        reader = spark.read.options(**options)
        if fmt == "delta":
            return reader.format("delta").load(path)
        elif fmt in {"parquet", "csv"}:
            return reader.format(fmt).load(path)
        else:
            raise ValueError(f"Unsupported format for Fabric: {fmt}")

    def write(
        self,
        df: DataFrame,
        path: str,
        *,
        fmt: Optional[str] = None,
        mode: str = "error",
        **options: Any,
    ) -> None:
        """Write a dataset to a Fabric OneLake-backed location.

        Args:
            df: DataFrame to write.
            path: Output OneLake path.
            fmt: Optional format override: `delta` (default), `parquet`, or `csv`.
            mode: Save mode, e.g. `error`, `overwrite`, `append`.
            **options: Additional Spark write options.
        """
        if not self.validate_path(path):
            raise ValueError(f"Invalid Fabric OneLake path: {path}")
        fmt = (fmt or options.pop("format", None) or "delta").lower()
        writer = df.write.mode(mode).options(**options)
        if fmt == "delta":
            writer.format("delta").save(path)
        elif fmt in {"parquet", "csv"}:
            writer.format(fmt).save(path)
        else:
            raise ValueError(f"Unsupported format for Fabric: {fmt}")
