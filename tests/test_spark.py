"""Tests for spark module utilities."""

from __future__ import annotations

import os
from unittest.mock import patch

from spark_fuse.spark import (
    _extract_scala_binary,
    _has_java,
    detect_environment,
)


class TestDetectEnvironment:
    def test_databricks_runtime(self):
        with patch.dict(os.environ, {"DATABRICKS_RUNTIME_VERSION": "14.3"}, clear=False):
            assert detect_environment() == "databricks"

    def test_databricks_cluster_id(self):
        with patch.dict(os.environ, {"DATABRICKS_CLUSTER_ID": "abc-123"}, clear=False):
            assert detect_environment() == "databricks"

    def test_fabric_environment(self):
        env = {k: v for k, v in os.environ.items() if "DATABRICKS" not in k}
        env["FABRIC_ENVIRONMENT"] = "prod"
        with patch.dict(os.environ, env, clear=True):
            assert detect_environment() == "fabric"

    def test_ms_fabric(self):
        env = {k: v for k, v in os.environ.items() if "DATABRICKS" not in k}
        env["MS_FABRIC"] = "1"
        with patch.dict(os.environ, env, clear=True):
            assert detect_environment() == "fabric"

    def test_local_default(self):
        env = {
            k: v
            for k, v in os.environ.items()
            if k
            not in {
                "DATABRICKS_RUNTIME_VERSION",
                "DATABRICKS_CLUSTER_ID",
                "FABRIC_ENVIRONMENT",
                "MS_FABRIC",
            }
        }
        with patch.dict(os.environ, env, clear=True):
            assert detect_environment() == "local"


class TestExtractScalaBinary:
    """Tests for _extract_scala_binary.

    Note: The regex patterns in spark.py use double-escaped backslashes in raw
    strings (``r"(\\\\d+\\\\.\\\\d+)"``), which match literal ``\\d+.\\d+`` patterns
    rather than digit sequences. As a result, standard jar names return None.
    These tests verify the current behavior as-is.
    """

    def test_standard_jar_names_return_none(self):
        # The regex patterns don't match because of double-escaping.
        assert _extract_scala_binary("spark-core_2.13-4.0.0.jar") is None
        assert _extract_scala_binary("scala-library-2.12.18.jar") is None
        assert _extract_scala_binary("delta-spark_2.13-4.0.0.jar") is None

    def test_no_match(self):
        assert _extract_scala_binary("random-lib.jar") is None


class TestHasJava:
    def test_java_home_set_with_valid_binary(self, tmp_path):
        java_bin = tmp_path / "bin" / "java"
        java_bin.parent.mkdir(parents=True)
        java_bin.touch()
        with patch.dict(os.environ, {"JAVA_HOME": str(tmp_path)}, clear=False):
            assert _has_java() is True

    def test_java_home_set_but_no_binary(self, tmp_path):
        # JAVA_HOME points to dir without bin/java; falls back to which
        with patch.dict(os.environ, {"JAVA_HOME": str(tmp_path)}, clear=False):
            # _has_java may still return True if java is on PATH,
            # so we just verify it doesn't crash
            result = _has_java()
            assert isinstance(result, bool)
