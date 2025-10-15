from __future__ import annotations

import json
import os
from typing import Any, Dict, Mapping, Optional, Tuple

import requests
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from .base import Connector
from .registry import register_connector
from .utils import as_seq, as_bool


@register_connector
class DatabricksDBFSConnector(Connector):
    """Connector for Databricks DBFS paths and Unity/Hive tables.

    Supports reading and writing Delta (default), Parquet, and CSV for DBFS paths.
    """

    name = "databricks"

    def validate_path(self, path: str) -> bool:
        """Return True if the input looks like a DBFS path or Databricks table identifier."""
        if not isinstance(path, str):
            return False
        value = path.strip()
        if not value:
            return False
        return value.startswith("dbfs:/") or self._is_table_identifier(value)

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
        """Read a dataset from Databricks storage or catalogs.

        Args:
            spark: Active `SparkSession`.
            source: Either a `dbfs:/` location, a table identifier string, or a mapping containing
                table/path metadata.
            fmt: Optional format override for DBFS reads (`delta` by default). Ignored for tables.
            schema: Optional schema to enforce for file reads. Not supported for table reads.
            source_config: Connector-specific settings (e.g., `catalog`, `schema`, `table`, `path`).
            options: Additional Spark read options.
        """
        path, table = self._normalize_source(source, source_config)
        opts = dict(options or {})

        if path:
            fmt_value = (fmt or opts.pop("format", None) or "delta").lower()
            reader = spark.read
            if schema is not None:
                reader = reader.schema(schema)
            if opts:
                reader = reader.options(**opts)
            if fmt_value == "delta":
                return reader.format("delta").load(path)
            if fmt_value in {"parquet", "csv"}:
                return reader.format(fmt_value).load(path)
            raise ValueError(f"Unsupported format for Databricks: {fmt_value}")

        if schema is not None:
            raise ValueError("schema parameter is not supported when reading Databricks tables")
        fmt_option = fmt or opts.pop("format", None)
        if fmt_option:
            raise ValueError("fmt/format options are not applicable when reading Databricks tables")
        reader = spark.read
        if opts:
            reader = reader.options(**opts)
        return reader.table(table)

    def _normalize_source(
        self,
        source: Any,
        source_config: Optional[Mapping[str, Any]],
    ) -> Tuple[Optional[str], Optional[str]]:
        """Return a tuple of (dbfs_path, table_identifier) based on the provided inputs."""
        config: Dict[str, Any] = {}
        if isinstance(source_config, Mapping):
            config.update(source_config)

        raw_source = source
        if isinstance(raw_source, Mapping):
            config.update(raw_source)
            raw_source = config.get("path") or config.get("table") or config.get("name")

        path_value = config.get("path")
        table_value = config.get("table") or config.get("name")

        if isinstance(raw_source, str):
            stripped = raw_source.strip()
            if stripped:
                if stripped.startswith("dbfs:/"):
                    path_value = stripped
                elif self._is_table_identifier(stripped):
                    table_value = stripped
                else:
                    raise ValueError(f"Unsupported Databricks source '{raw_source}'")
        elif raw_source is not None and not isinstance(raw_source, Mapping):
            raise TypeError(
                "Databricks source must be a dbfs:/ path string, table identifier string, or mapping."
            )

        if path_value and table_value:
            raise ValueError("Provide either a DBFS path or a table identifier, not both")

        if path_value is not None:
            if not isinstance(path_value, str):
                raise TypeError("DBFS path must be a string")
            path_value = path_value.strip()
            if not path_value.startswith("dbfs:/"):
                raise ValueError(
                    "Only dbfs:/ paths are supported when reading from the Databricks file system"
                )
            return path_value, None

        if table_value is not None:
            if not isinstance(table_value, str):
                raise TypeError("Table identifier must be a string")
            table_value = table_value.strip()
            if not table_value:
                raise ValueError("Table identifier must be non-empty")

            catalog = config.get("catalog")
            schema_name = config.get("schema") or config.get("database")
            parts = table_value.split(".")
            if len(parts) >= 3:
                table_identifier = table_value
            elif len(parts) == 2:
                if catalog:
                    table_identifier = ".".join([str(catalog).strip(), table_value])
                else:
                    table_identifier = table_value
            else:
                segments = [
                    str(segment).strip()
                    for segment in (catalog, schema_name, table_value)
                    if segment
                ]
                table_identifier = ".".join(segments) if segments else table_value

            if not self._is_table_identifier(table_identifier):
                raise ValueError(f"Invalid table identifier: {table_identifier}")
            return None, table_identifier

        raise ValueError(
            "Databricks connector requires either a dbfs:/ path or a table identifier via the "
            "`source` argument or `source_config`."
        )

    @staticmethod
    def _is_table_identifier(value: str) -> bool:
        if not isinstance(value, str):
            return False
        trimmed = value.strip()
        if not trimmed or trimmed.startswith("dbfs:/"):
            return False
        if "://" in trimmed or "/" in trimmed:
            return False
        parts = trimmed.split(".")
        if not 1 <= len(parts) <= 3:
            return False
        return all(parts)

    def write(
        self,
        df: DataFrame,
        path: str,
        *,
        fmt: Optional[str] = None,
        mode: str = "error",
        **options: Any,
    ) -> None:
        """Write a dataset to DBFS.

        Args:
            df: DataFrame to write.
            path: Output DBFS location.
            fmt: Optional format override: `delta` (default), `parquet`, or `csv`.
            mode: Save mode, e.g. `error`, `overwrite`, `append`.
            **options: Additional Spark write options.
        """
        if not self.validate_path(path):
            raise ValueError(f"Invalid DBFS path: {path}")
        fmt = (fmt or options.pop("format", None) or "delta").lower()
        # When writing Delta, use SCD upsert helpers
        if fmt == "delta":
            # Import at call time to avoid importing Delta libs unless needed
            from ..utils.scd import SCDMode, apply_scd

            scd_mode_opt = (
                options.pop("scd_mode", None) or options.pop("scd", None) or "scd2"
            ).upper()
            if scd_mode_opt not in {"SCD1", "SCD2"}:
                raise ValueError("scd_mode must be 'SCD1' or 'SCD2'")
            scd_mode = SCDMode[scd_mode_opt]

            business_keys = options.pop("business_keys", None)
            if business_keys is None:
                raise ValueError("business_keys must be provided for SCD writes to Delta")
            if not business_keys or len(business_keys) == 0:
                raise ValueError(
                    "business_keys must be a non-empty sequence for SCD writes to Delta"
                )

            tracked_columns = as_seq(options.pop("tracked_columns", None))
            dedupe_keys = as_seq(options.pop("dedupe_keys", None))
            order_by = as_seq(options.pop("order_by", None))

            # SCD2-specific options (also used by SCD1 for hash_col)
            effective_col = options.pop("effective_col", "effective_start_ts")
            expiry_col = options.pop("expiry_col", "effective_end_ts")
            current_col = options.pop("current_col", "is_current")
            version_col = options.pop("version_col", "version")
            hash_col = options.pop("hash_col", "row_hash")

            load_ts_expr_opt = options.pop("load_ts_expr", None)
            load_ts_expr = F.expr(load_ts_expr_opt) if isinstance(load_ts_expr_opt, str) else None

            null_key_policy = options.pop("null_key_policy", "error")
            create_if_not_exists = as_bool(options.pop("create_if_not_exists", True), True)

            kwargs: Dict[str, Any] = {
                "business_keys": business_keys,
                "tracked_columns": tracked_columns,
                "dedupe_keys": dedupe_keys,
                "order_by": order_by,
                "hash_col": hash_col,
                "null_key_policy": null_key_policy,
                "create_if_not_exists": create_if_not_exists,
            }

            if scd_mode == SCDMode.SCD2:
                kwargs.update(
                    {
                        "effective_col": effective_col,
                        "expiry_col": expiry_col,
                        "current_col": current_col,
                        "version_col": version_col,
                        "load_ts_expr": load_ts_expr,
                    }
                )

            # Use DataFrame's session
            spark = df.sparkSession
            apply_scd(
                spark,
                df,
                path,
                scd_mode=scd_mode,
                **{k: v for k, v in kwargs.items() if v is not None},
            )
        elif fmt in {"parquet", "csv"}:
            writer = df.write.mode(mode).options(**options)
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
