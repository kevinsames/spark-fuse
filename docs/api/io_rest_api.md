# REST API Data Source

Use `build_rest_api_config` in combination with `spark.read.format("spark-fuse-rest")` to ingest
JSON-centric REST endpoints, including query-based and response-driven pagination, retry handling,
and Spark-native schema inference.

::: spark_fuse.io.rest_api.build_rest_api_config

::: spark_fuse.io.rest_api.RestAPIDataSource
