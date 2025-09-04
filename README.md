spark-fuse
================

![CI](https://github.com/kevinsames/spark-fuse/actions/workflows/ci.yml/badge.svg)
![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)

spark-fuse is an open-source toolkit for PySpark — providing utilities, connectors, and tools to fuse your data workflows across Azure Storage (ADLS Gen2), Databricks, Microsoft Fabric Lakehouses (via OneLake/Delta), Unity Catalog, and Hive Metastore.

Features
- Connectors for ADLS Gen2 (`abfss://`), Fabric OneLake (`onelake://` or `abfss://...onelake.dfs.fabric.microsoft.com/...`), and Databricks DBFS (`dbfs:/`).
- Unity Catalog and Hive Metastore helpers to create catalogs/schemas and register external Delta tables.
- SparkSession helpers with sensible defaults and environment detection (Databricks/Fabric/local).
- Typer-powered CLI: list connectors, preview datasets, register tables, submit Databricks jobs.

Installation
- Create a virtual environment (recommended)
  - macOS/Linux:
    - `python3 -m venv .venv`
    - `source .venv/bin/activate`
    - `python -m pip install --upgrade pip`
  - Windows (PowerShell):
    - `python -m venv .venv`
    - `.\\.venv\\Scripts\\Activate.ps1`
    - `python -m pip install --upgrade pip`
- From source (dev): `pip install -e ".[dev]"`
- From PyPI: `pip install spark-fuse`

Quickstart
1) Create a SparkSession with helpful defaults
```python
from spark_fuse.spark import create_session
spark = create_session(app_name="spark-fuse-quickstart")
```

2) Read a Delta table from ADLS or OneLake
```python
from spark_fuse.io.azure_adls import ADLSGen2Connector

df = ADLSGen2Connector().read(spark, "abfss://container@account.dfs.core.windows.net/path/to/delta")
df.show(5)
```

3) Register an external table in Unity Catalog
```python
from spark_fuse.catalogs import unity

unity.create_catalog(spark, "analytics")
unity.create_schema(spark, catalog="analytics", schema="core")
unity.register_external_delta_table(
    spark,
    catalog="analytics",
    schema="core",
    table="events",
    location="abfss://container@account.dfs.core.windows.net/path/to/delta",
)
```

CLI Usage
- `spark-fuse --help`
- `spark-fuse connectors`
- `spark-fuse read --path abfss://container@account.dfs.core.windows.net/path/to/delta --show 5`
- `spark-fuse uc-create --catalog analytics --schema core`
- `spark-fuse uc-register-table --catalog analytics --schema core --table events --path abfss://.../delta`
- `spark-fuse hive-register-external --database analytics_core --table events --path abfss://.../delta`
- `spark-fuse fabric-register --table lakehouse_table --path onelake://workspace/lakehouse/Tables/events`
- `spark-fuse databricks-submit --json job.json`

CI
- GitHub Actions runs ruff and pytest for Python 3.9–3.11.

License
- Apache 2.0
