# Qdrant Data Source

Use `build_qdrant_config` with `spark.read.format("spark-fuse-qdrant")` to scroll a collection over
HTTP (payload and vector selection, pagination/limits, retries). For writes, construct
`build_qdrant_write_config` and `df.write.format("spark-fuse-qdrant")` to batch points into
Qdrant, or call `write_qdrant_points` directly from Python.

::: spark_fuse.io.qdrant.build_qdrant_config

::: spark_fuse.io.qdrant.build_qdrant_write_config

::: spark_fuse.io.qdrant.write_qdrant_points

::: spark_fuse.io.qdrant.QdrantDataSource
