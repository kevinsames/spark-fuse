from __future__ import annotations

import re
from typing import Any, Optional, Sequence

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

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
        if fmt == "delta":
            from ..utils.scd import SCDMode, apply_scd

            def _as_seq(val: Any) -> Optional[Sequence[str]]:
                if val is None:
                    return None
                if isinstance(val, str):
                    return [s.strip() for s in val.split(",") if s.strip()]
                return list(val)

            def _as_bool(val: Any, default: bool) -> bool:
                if val is None:
                    return default
                if isinstance(val, bool):
                    return val
                if isinstance(val, str):
                    v = val.strip().lower()
                    if v in {"1", "true", "yes", "y"}:
                        return True
                    if v in {"0", "false", "no", "n"}:
                        return False
                return bool(val)

            scd_mode_opt = (
                options.pop("scd_mode", None) or options.pop("scd", None) or "scd2"
            ).upper()
            if scd_mode_opt not in {"SCD1", "SCD2"}:
                raise ValueError("scd_mode must be 'SCD1' or 'SCD2'")
            scd_mode = SCDMode[scd_mode_opt]

            business_keys = options.pop("business_keys", None)
            if business_keys is None:
                raise ValueError("business_keys must be provided for SCD writes to Delta")
            business_keys = _as_seq(business_keys)  # type: ignore
            assert business_keys and len(business_keys) > 0

            tracked_columns = _as_seq(options.pop("tracked_columns", None))
            dedupe_keys = _as_seq(options.pop("dedupe_keys", None))
            order_by = _as_seq(options.pop("order_by", None))

            effective_col = options.pop("effective_col", "effective_start_ts")
            expiry_col = options.pop("expiry_col", "effective_end_ts")
            current_col = options.pop("current_col", "is_current")
            version_col = options.pop("version_col", "version")
            hash_col = options.pop("hash_col", "row_hash")

            load_ts_expr_opt = options.pop("load_ts_expr", None)
            load_ts_expr = F.expr(load_ts_expr_opt) if isinstance(load_ts_expr_opt, str) else None

            null_key_policy = options.pop("null_key_policy", "error")
            create_if_not_exists = _as_bool(options.pop("create_if_not_exists", True), True)

            kwargs: dict[str, Any] = {
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
            raise ValueError(f"Unsupported format for Fabric: {fmt}")
