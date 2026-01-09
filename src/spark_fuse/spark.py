import os
import re
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


_SCALA_VERSION_RE = re.compile(r"(\\d+\\.\\d+)")
_SCALA_SUFFIX_RE = re.compile(r"_(\\d+\\.\\d+)")
_SCALA_LIBRARY_RE = re.compile(r"scala-library-(\\d+\\.\\d+)")
_SPARK_CORE_RE = re.compile(r"spark-core_(\\d+\\.\\d+)-(\\d+\\.\\d+(?:\\.\\d+)?)\\.jar")


def _find_spark_jars_dir(pyspark_module) -> Optional[Path]:
    spark_home = os.environ.get("SPARK_HOME")
    candidates = []
    if spark_home:
        candidates.append(Path(spark_home).resolve() / "jars")
    try:
        from pyspark.find_spark_home import _find_spark_home  # type: ignore

        detected_home = _find_spark_home()
        if detected_home:
            candidates.append(Path(detected_home).resolve() / "jars")
    except Exception:
        pass
    module_file = getattr(pyspark_module, "__file__", None)
    if module_file:
        candidates.append(Path(module_file).resolve().parent / "jars")
    module_paths = getattr(pyspark_module, "__path__", None)
    if module_paths:
        for path in module_paths:
            candidates.append(Path(path).resolve() / "jars")
    for candidate in candidates:
        if candidate.is_dir():
            return candidate
    return None


def _detect_delta_package_version() -> Optional[str]:
    try:
        from importlib import metadata
    except ImportError:  # pragma: no cover - Python <3.8
        return None

    try:
        return metadata.version("delta-spark")
    except metadata.PackageNotFoundError:
        return None


def _extract_scala_binary(jar_name: str) -> Optional[str]:
    for pattern in (_SCALA_SUFFIX_RE, _SCALA_LIBRARY_RE, _SCALA_VERSION_RE):
        match = pattern.search(jar_name)
        if match:
            return match.group(1)
    return None


def _detect_scala_binary(pyspark_module) -> Optional[str]:
    """Best-effort Scala binary detection based on Spark jars."""

    try:
        jars_dir = _find_spark_jars_dir(pyspark_module)
        if not jars_dir:
            return None

        # Prefer spark-core to reflect the actual Spark build's Scala binary.
        for pattern in ("spark-core_*.jar", "scala-library-*.jar"):
            matches = sorted(jars_dir.glob(pattern))
            if not matches:
                continue
            scala_binary = _extract_scala_binary(matches[0].name)
            if scala_binary:
                return scala_binary
        return None
    except Exception:
        return None


def _detect_spark_version(pyspark_module) -> Optional[str]:
    """Best-effort Spark version detection from bundled jars."""

    try:
        jars_dir = _find_spark_jars_dir(pyspark_module)
        if not jars_dir:
            return None
        for jar in sorted(jars_dir.glob("spark-core_*.jar")):
            match = _SPARK_CORE_RE.match(jar.name)
            if match:
                return match.group(2)
        return None
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

            ver = _detect_spark_version(pyspark) or pyspark.__version__
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
                if modern_runtime:
                    # Keep Delta aligned to the exact Spark runtime for 4.x patch releases.
                    delta_ver = ver
                else:
                    delta_ver = _detect_delta_package_version() or DELTA_PYSPARK_COMPAT.get(
                        key, default_delta
                    )

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
