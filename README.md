spark-fuse
================

![CI](https://github.com/kevinsames/spark-fuse/actions/workflows/ci.yml/badge.svg)
![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)

spark-fuse is an open-source toolkit for PySpark — providing utilities, connectors, and tools to fuse your data workflows across Azure Storage (ADLS Gen2), Databricks, Microsoft Fabric Lakehouses (via OneLake/Delta), Unity Catalog, Hive Metastore, and JSON-centric REST APIs.

Features
- Connectors for ADLS Gen2 (`abfss://`), Fabric OneLake (`onelake://` or `abfss://...onelake.dfs.fabric.microsoft.com/...`), Databricks DBFS and catalog tables, and REST APIs (JSON).
- Unity Catalog and Hive Metastore helpers to create catalogs/schemas and register external Delta tables.
- SparkSession helpers with sensible defaults and environment detection (Databricks/Fabric/local).
- LLM-powered semantic column normalization that batches API calls and caches responses.
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
- From PyPI: `pip install "spark-fuse>=0.2.1"`

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

3) Load paginated REST API responses
```python
from spark_fuse.io.rest_api import RestAPIReader

reader = RestAPIReader()
config = {
    "records_field": "results",
    "pagination": {"mode": "response", "field": "next", "max_pages": 2},
}
pokemon = reader.read(spark, "https://pokeapi.co/api/v2/pokemon", source_config=config)
pokemon.select("name").show(5)
```

4) Register an external table in Unity Catalog
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

LLM-Powered Column Mapping
```python
from spark_fuse.utils.transformations import map_column_with_llm

standard_values = ["Apple", "Banana", "Cherry"]
mapped_df = map_column_with_llm(
    df,
    column="fruit",
    target_values=standard_values,
    model="o4-mini",
    temperature=None,
)
mapped_df.select("fruit", "fruit_mapped").show()
```

Set `dry_run=True` to inspect how many rows already match without spending LLM tokens. Configure your OpenAI or Azure OpenAI credentials with the usual environment variables before running live mappings. Some provider models only accept their default sampling configuration—pass `temperature=None` to omit the parameter when needed. This helper ships with spark-fuse 0.2.0 and later.

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
