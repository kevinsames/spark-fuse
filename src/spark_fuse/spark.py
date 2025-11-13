import os
import sys
from pathlib import Path
from typing import Dict, Optional

from pyspark.sql import SparkSession

# Recommended Delta Lake package per PySpark minor version.
# Keep this map aligned with the Delta Lake compatibility matrix.
# Summary (as of 2025-09):
# - PySpark 3.3 → delta-spark 2.3.x
# - PySpark 3.4 → delta-spark 2.4.x
# - PySpark 3.5 → delta-spark 3.2.x
# - PySpark 4.0 → delta-spark 4.0.x
# You can override the choice with the environment variable
# SPARK_FUSE_DELTA_VERSION if you need a specific version, and
# SPARK_FUSE_DELTA_SCALA_SUFFIX to force a Scala binary (e.g., 2.13).
DELTA_PYSPARK_COMPAT: Dict[str, str] = {
    "3.3": "2.3.0",
    "3.4": "2.4.0",
    "3.5": "3.2.0",
    "4.0": "4.0.0",
}
DEFAULT_DELTA_VERSION = "4.0.0"
LEGACY_DELTA_VERSION = "3.2.0"
DEFAULT_SCALA_SUFFIX = "2.13"
LEGACY_SCALA_SUFFIX = "2.12"


def _detect_scala_binary(pyspark_module) -> Optional[str]:
    """Best-effort Scala binary detection based on bundled jars."""

    try:
        jars_dir = Path(pyspark_module.__file__).resolve().parent / "jars"
        matches = sorted(jars_dir.glob("scala-library-*.jar"))
        if not matches:
            return None
        version = matches[0].name.split("scala-library-")[-1].split(".jar")[0]
        major_minor = version.split(".")[:2]
        return ".".join(major_minor)
    except Exception:
        return None


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
    runtime class mismatches. Overrides can be provided via the environment
    variables `SPARK_FUSE_DELTA_VERSION` and `SPARK_FUSE_DELTA_SCALA_SUFFIX`.
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
    scala_suffix = os.environ.get("SPARK_FUSE_DELTA_SCALA_SUFFIX")
    if not (delta_ver and scala_suffix):
        try:
            import pyspark  # type: ignore

            ver = pyspark.__version__
            major, minor, *_ = ver.split(".")
            key = f"{major}.{minor}"
            try:
                major_int = int(major)
            except ValueError:
                major_int = 4

            modern_runtime = major_int >= 4
            default_delta = DEFAULT_DELTA_VERSION if modern_runtime else LEGACY_DELTA_VERSION
            default_scala = DEFAULT_SCALA_SUFFIX if modern_runtime else LEGACY_SCALA_SUFFIX

            if not delta_ver:
                delta_ver = DELTA_PYSPARK_COMPAT.get(key, default_delta)

            if not scala_suffix:
                detected_scala = _detect_scala_binary(pyspark)
                scala_suffix = detected_scala or default_scala
        except Exception:
            # Fallback that works for recent Spark
            delta_ver = delta_ver or DEFAULT_DELTA_VERSION
            scala_suffix = scala_suffix or DEFAULT_SCALA_SUFFIX

    delta_ver = delta_ver or DEFAULT_DELTA_VERSION
    scala_suffix = scala_suffix or DEFAULT_SCALA_SUFFIX

    # Append io.delta package, matching the Scala binary for the detected Spark runtime.
    pkg = f"io.delta:delta-spark_{scala_suffix}:{delta_ver}"
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

    python_exec = os.environ.get("PYSPARK_PYTHON", sys.executable)
    driver_python = os.environ.get("PYSPARK_DRIVER_PYTHON", python_exec)

    active = SparkSession.getActiveSession()
    if active is not None:
        try:
            current_exec = active.sparkContext.pythonExec
        except Exception:
            current_exec = None
        if current_exec and os.path.realpath(current_exec) != os.path.realpath(python_exec):
            active.stop()

    builder = SparkSession.builder.appName(app_name)
    if master:
        builder = builder.master(master)
    elif env == "local":
        builder = builder.master("local[2]")

    builder = builder.config("spark.pyspark.python", python_exec).config(
        "spark.pyspark.driver.python", driver_python
    )

    builder = _apply_delta_configs(builder)

    # Minimal IO friendliness. Advanced auth must come via extra_configs or cluster env.
    builder = builder.config("spark.sql.shuffle.partitions", "8")

    if extra_configs:
        for k, v in extra_configs.items():
            builder = builder.config(k, v)

    spark = builder.getOrCreate()
    return spark
