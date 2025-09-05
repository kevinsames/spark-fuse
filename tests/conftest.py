import os
import pytest


@pytest.fixture(scope="session")
def spark():
    # Lazy import to avoid overhead when not used.
    from spark_fuse.spark import create_session

    # Keep it minimal; rely on delta-spark only if present.
    # Ensure local networking is used for Spark in constrained environments.
    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
    os.environ.setdefault("SPARK_LOCAL_HOSTNAME", "localhost")

    spark = create_session(
        app_name="spark-fuse-tests",
        master="local[2]",
        extra_configs={
            # Bind to localhost to avoid network/port issues in CI or sandboxes
            "spark.driver.bindAddress": "127.0.0.1",
            "spark.driver.host": "localhost",
            # Disable UI to avoid binding extra ports
            "spark.ui.enabled": "false",
            # Be resilient to port contention
            "spark.port.maxRetries": "64",
        },
    )
    yield spark
    spark.stop()
