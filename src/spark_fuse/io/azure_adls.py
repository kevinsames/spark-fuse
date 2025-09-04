from __future__ import annotations

import re
from typing import Any, Optional

from pyspark.sql import DataFrame, SparkSession

from .base import Connector
from .registry import register_connector


_ABFSS_RE = re.compile(r"^abfss://[^@]+@[^/]+/.+")


@register_connector
class ADLSGen2Connector(Connector):
    name = "adls"

    def validate_path(self, path: str) -> bool:
        return bool(_ABFSS_RE.match(path))

    def read(self, spark: SparkSession, path: str, *, fmt: Optional[str] = None, **options: Any) -> DataFrame:
        if not self.validate_path(path):
            raise ValueError(f"Invalid ADLS Gen2 path: {path}")
        fmt = (fmt or options.pop("format", None) or "delta").lower()
        reader = spark.read.options(**options)
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

