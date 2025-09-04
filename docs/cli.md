# CLI

spark-fuse provides a Typer-based CLI. Examples:

- `spark-fuse connectors`
- `spark-fuse read --path abfss://container@account.dfs.core.windows.net/path/to/delta --show 5`
- `spark-fuse uc-create --catalog analytics --schema core`
- `spark-fuse uc-register-table --catalog analytics --schema core --table events --path abfss://.../delta`
- `spark-fuse hive-register-external --database analytics_core --table events --path abfss://.../delta`
- `spark-fuse fabric-register --table lakehouse_table --path onelake://workspace/lakehouse/Tables/events`
- `spark-fuse databricks-submit --json job.json`

Run `spark-fuse --help` for the full command reference.
