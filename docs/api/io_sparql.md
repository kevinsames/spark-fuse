# SPARQL Data Source

Use `build_sparql_config` with `spark.read.format("spark-fuse-sparql")` to run SPARQL queries
against HTTP endpoints, apply retry/backoff policies, and map result bindings (including optional
metadata) into Spark DataFrames.

::: spark_fuse.io.sparql.build_sparql_config

::: spark_fuse.io.sparql.SPARQLDataSource
