import os
from typing import Dict, Optional

from pyspark.sql import SparkSession


def detect_environment() -> str:
    """Detect a likely runtime environment: databricks, fabric, or local.

    Heuristics only; callers should not rely on this for security decisions.
    """
    if os.environ.get("DATABRICKS_RUNTIME_VERSION") or os.environ.get("DATABRICKS_CLUSTER_ID"):
        return "databricks"
    if os.environ.get("FABRIC_ENVIRONMENT") or os.environ.get("MS_FABRIC"):
        return "fabric"
    return "local"


def _apply_delta_configs(builder: SparkSession.Builder) -> SparkSession.Builder:
    # Configure Delta if possible; on Databricks this is already set.
    builder = builder.config(
        "spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension"
    ).config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    # Best-effort: if delta provides a helper, let it enrich the builder.
    try:
        from delta import configure_spark_with_delta_pip  # type: ignore

        builder = configure_spark_with_delta_pip(builder)
    except Exception:
        # Do not fail locally; the configs above are typically enough when delta-spark is installed.
        pass
    return builder


def create_session(
    app_name: str = "spark-fuse",
    *,
    master: Optional[str] = None,
    extra_configs: Optional[Dict[str, str]] = None,
) -> SparkSession:
    """Create a SparkSession with Delta configs and light Azure defaults.

    - Uses `local[2]` when no master is provided and not on Databricks or Fabric.
    - Applies Delta extensions; works both on Databricks and local delta-spark.
    - Accepts `extra_configs` to inject environment-specific credentials.
    """
    env = detect_environment()

    builder = SparkSession.builder.appName(app_name)
    if master:
        builder = builder.master(master)
    elif env == "local":
        builder = builder.master("local[2]")

    builder = _apply_delta_configs(builder)

    # Minimal IO friendliness. Advanced auth must come via extra_configs or cluster env.
    builder = builder.config("spark.sql.shuffle.partitions", "8")

    if extra_configs:
        for k, v in extra_configs.items():
            builder = builder.config(k, v)

    spark = builder.getOrCreate()
    return spark

