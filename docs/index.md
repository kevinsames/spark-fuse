# spark-fuse

spark-fuse is an open-source toolkit for PySpark — providing utilities, connectors, and tools to fuse your data workflows across Azure Storage (ADLS Gen2), Databricks, Microsoft Fabric Lakehouses (via OneLake/Delta), Unity Catalog, and Hive Metastore.

## Features
- Connectors for ADLS Gen2 (`abfss://`), Fabric OneLake (`onelake://` or `abfss://...onelake.dfs.fabric.microsoft.com/...`), and Databricks DBFS (`dbfs:/`).
- Unity Catalog and Hive Metastore helpers to create catalogs/schemas and register external Delta tables.
- SparkSession helpers with sensible defaults and environment detection (Databricks/Fabric/local).
- DataFrame utilities for previews, name management, casts, whitespace cleanup, and resilient date parsing.
- Typer-powered CLI: list connectors, preview datasets, register tables, submit Databricks jobs.

## Quickstart
```python
from spark_fuse.spark import create_session
spark = create_session(app_name="spark-fuse-quickstart")

from spark_fuse.io.azure_adls import ADLSGen2Connector

df = ADLSGen2Connector().read(spark, "abfss://container@account.dfs.core.windows.net/path/to/delta")
df.show(5)

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

## CLI
- `spark-fuse connectors`
- `spark-fuse read --path <uri> --show 5`
- `spark-fuse uc-create --catalog <name> --schema <name>`
- `spark-fuse uc-register-table --catalog <c> --schema <s> --table <t> --path <uri>`
- `spark-fuse hive-register-external --database <db> --table <t> --path <uri>`
- `spark-fuse fabric-register --table <t> --path <onelake-or-abfss-on-onelake>`
- `spark-fuse databricks-submit --json job.json`

See the Install and CLI pages for more.
