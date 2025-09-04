from __future__ import annotations

import json
import os
from typing import Any, Dict, Optional

import requests
from pyspark.sql import DataFrame, SparkSession

from .base import Connector
from .registry import register_connector


@register_connector
class DatabricksDBFSConnector(Connector):
    name = "databricks"

    def validate_path(self, path: str) -> bool:
        return path.startswith("dbfs:/")

    def read(
        self, spark: SparkSession, path: str, *, fmt: Optional[str] = None, **options: Any
    ) -> DataFrame:
        if not self.validate_path(path):
            raise ValueError(f"Invalid DBFS path: {path}")
        fmt = (fmt or options.pop("format", None) or "delta").lower()
        reader = spark.read.options(**options)
        if fmt == "delta":
            return reader.format("delta").load(path)
        elif fmt in {"parquet", "csv"}:
            return reader.format(fmt).load(path)
        else:
            raise ValueError(f"Unsupported format for Databricks: {fmt}")

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
            raise ValueError(f"Invalid DBFS path: {path}")
        fmt = (fmt or options.pop("format", None) or "delta").lower()
        writer = df.write.mode(mode).options(**options)
        if fmt == "delta":
            writer.format("delta").save(path)
        elif fmt in {"parquet", "csv"}:
            writer.format(fmt).save(path)
        else:
            raise ValueError(f"Unsupported format for Databricks: {fmt}")


def databricks_submit_job(
    payload: Dict[str, Any], *, host: Optional[str] = None, token: Optional[str] = None
) -> Dict[str, Any]:
    """Submit a job run to Databricks using the 2.1 Runs Submit API.

    Environment variables `DATABRICKS_HOST` and `DATABRICKS_TOKEN` are used if not provided.
    Returns the parsed JSON response or raises for HTTP errors.
    """
    host = host or os.environ.get("DATABRICKS_HOST")
    token = token or os.environ.get("DATABRICKS_TOKEN")
    if not host or not token:
        raise ValueError("DATABRICKS_HOST and DATABRICKS_TOKEN must be set to submit jobs")

    url = host.rstrip("/") + "/api/2.1/jobs/runs/submit"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    resp = requests.post(url, headers=headers, data=json.dumps(payload), timeout=60)
    resp.raise_for_status()
    return resp.json()
