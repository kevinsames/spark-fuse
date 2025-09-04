import os

import pytest


@pytest.fixture(scope="session")
def spark():
    # Lazy import to avoid overhead when not used.
    from spark_fuse.spark import create_session

    # Keep it minimal; rely on delta-spark only if present.
    spark = create_session(app_name="spark-fuse-tests", master="local[2]")
    yield spark
    spark.stop()

