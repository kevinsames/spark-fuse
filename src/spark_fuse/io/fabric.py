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
    name = "fabric"

    def validate_path(self, path: str) -> bool:
        return bool(_ONELAKE_SCHEME.match(path) or _ONELAKE_ABFSS.match(path))

    def read(
        self, spark: SparkSession, path: str, *, fmt: Optional[str] = None, **options: Any
    ) -> DataFrame:
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
