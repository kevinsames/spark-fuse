# CLI

spark-fuse provides a Typer-based CLI. Examples:

- `spark-fuse connectors`
- `spark-fuse read --path abfss://container@account.dfs.core.windows.net/path/to/delta --show 5`
- `spark-fuse fabric-register --table lakehouse_table --path onelake://workspace/lakehouse/Tables/events`
- `spark-fuse databricks-submit --json job.json`

Run `spark-fuse --help` for the full command reference.
