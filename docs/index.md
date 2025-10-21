# spark-fuse

spark-fuse is an open-source toolkit for PySpark — providing utilities, connectors, and tools to fuse your data workflows across Azure Storage (ADLS Gen2), Databricks, Microsoft Fabric Lakehouses (via OneLake/Delta), Unity Catalog, Hive Metastore, and REST APIs.

## Features
- Connectors for ADLS Gen2 (`abfss://`), Fabric OneLake (`onelake://` or `abfss://...onelake.dfs.fabric.microsoft.com/...`), Databricks DBFS and catalog tables, and REST APIs (JSON).
- Unity Catalog and Hive Metastore helpers to create catalogs/schemas and register external Delta tables.
- SparkSession helpers with sensible defaults and environment detection (Databricks/Fabric/local).
- DataFrame utilities for previews, name management, casts, whitespace cleanup, resilient date parsing, calendar/time dimensions, and LLM-backed semantic column mapping.
- Typer-powered CLI: list connectors, preview datasets, register tables, submit Databricks jobs.

## Quickstart
```python
from spark_fuse.spark import create_session
spark = create_session(app_name="spark-fuse-quickstart")

from spark_fuse.io.azure_adls import ADLSGen2Connector

df = ADLSGen2Connector().read(spark, "abfss://container@account.dfs.core.windows.net/path/to/delta")
df.show(5)

from spark_fuse.io.rest_api import RestAPIReader

reader = RestAPIReader()
pokemon = reader.read(
    spark,
    "https://pokeapi.co/api/v2/pokemon",
    source_config={
        "records_field": "results",
        "pagination": {"mode": "response", "field": "next", "max_pages": 2},
    },
)
pokemon.select("name").show(5)

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

### LLM-powered semantic mapping
```python
from spark_fuse.utils.transformations import map_column_with_llm

targets = ["Apple", "Banana", "Cherry"]
normalized = map_column_with_llm(
    df,
    column="fruit",
    target_values=targets,
    model="o4-mini",
    temperature=None,
)
normalized.select("fruit", "fruit_mapped").show()
```



#### Azure OpenAI token estimate

For sizing, assume:

- 2,000 rows with ~50 characters each (≈50 tokens).

- `target_values` totalling 100 characters (≈25 tokens).

- `o4-mini` model with `temperature=None`.

Each request sends ≈75 input tokens and returns ≈10 tokens, totalling ≈85 tokens per row.

At 2,000 rows ⇒ about 170k tokens. Multiply by your provider's per-token price to estimate cost; adjust partitions or dry-run to manage quota.

As a reference, OpenAI currently lists o4-mini input tokens at $0.0006 per 1K tokens and output tokens at $0.0024 per 1K tokens.
- Input: 150K tokens × $0.0006 ≈ $0.09
- Output: 20K tokens × $0.0024 ≈ $0.048
- Approximate total: $0.14 for the batch

Azure OpenAI pricing may differ; always confirm with your subscription's rate card before running large workloads.

Use `dry_run=True` during development to avoid external API calls until credentials and prompts are ready. Some models only accept their default sampling configuration—use `temperature=None` to omit the parameter when required. The LLM mapper is available starting in spark-fuse 0.2.0.

### Calendar and time dimensions
```python
from spark_fuse.utils.dataframe import create_date_dataframe, create_time_dataframe

dates = create_date_dataframe(spark, "2024-01-01", "2024-01-07")
times = create_time_dataframe(spark, "08:00:00", "12:00:00", interval_seconds=1800)

dates.select("date", "year", "week", "day_name").show()
times.select("time", "hour", "minute").show()
```

Try the interactive `notebooks/date_time_dimensions_demo.ipynb` notebook to explore the helpers end-to-end.

## CLI
- `spark-fuse connectors`
- `spark-fuse read --path <uri> --show 5`
- `spark-fuse uc-create --catalog <name> --schema <name>`
- `spark-fuse uc-register-table --catalog <c> --schema <s> --table <t> --path <uri>`
- `spark-fuse hive-register-external --database <db> --table <t> --path <uri>`
- `spark-fuse fabric-register --table <t> --path <onelake-or-abfss-on-onelake>`
- `spark-fuse databricks-submit --json job.json`

See the Install and CLI pages for more.
