import os
from typing import Dict, Optional

from pyspark.sql import SparkSession

# Recommended Delta Lake package per PySpark minor version.
# Keep this map aligned with the Delta Lake compatibility matrix.
# Summary (as of 2025-09):
# - PySpark 3.3 → delta-spark 2.3.x
# - PySpark 3.4 → delta-spark 2.4.x
# - PySpark 3.5 → delta-spark 3.2.x
# You can override the choice with the environment variable
# SPARK_FUSE_DELTA_VERSION if you need a specific version.
DELTA_PYSPARK_COMPAT: Dict[str, str] = {
    "3.3": "2.3.0",
    "3.4": "2.4.0",
    "3.5": "3.2.0",
}


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
    """Attach Delta configs and add a compatible Delta Lake package.

    Uses a simple compatibility map between PySpark and delta-spark to avoid
    runtime class mismatches. An override can be provided via the environment
    variable `SPARK_FUSE_DELTA_VERSION`.
    """
    builder = builder.config(
        "spark.sql.extensions",
        "io.delta.sql.DeltaSparkSessionExtension",
    ).config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )

    # Choose a delta-spark version compatible with the local PySpark runtime.
    delta_ver = os.environ.get("SPARK_FUSE_DELTA_VERSION")
    if not delta_ver:
        try:
            import pyspark  # type: ignore

            ver = pyspark.__version__
            major, minor, *_ = ver.split(".")
            key = f"{major}.{minor}"
            delta_ver = DELTA_PYSPARK_COMPAT.get(key, "3.2.0")
        except Exception:
            # Fallback that works for recent Spark
            delta_ver = "3.2.0"

    # Append io.delta package; assume Scala 2.12 for Spark 3.x
    pkg = f"io.delta:delta-spark_2.12:{delta_ver}"
    builder = builder.config(
        "spark.jars.packages",
        pkg
        if os.environ.get("SPARK_JARS_PACKAGES") is None
        else os.environ.get("SPARK_JARS_PACKAGES") + "," + pkg,
    )

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
