# Qdrant Data Source

The `spark-fuse-qdrant` connector streams points from (and into) Qdrant over the HTTP API.
Reading uses the Scroll endpoint with optional payload/vector selection, pagination, and retries;
writing batches points to the Points endpoint with configurable payload extraction.

## Reading from Qdrant

Use `build_qdrant_config` with `spark.read.format(QDRANT_FORMAT)` after registering the data
source. The connector can infer the schema from returned points or consume an explicit schema.

```python
import json
from spark_fuse.io import (
    QDRANT_CONFIG_OPTION,
    QDRANT_FORMAT,
    build_qdrant_config,
    register_qdrant_data_source,
)

register_qdrant_data_source(spark)
config = build_qdrant_config(
    spark,
    endpoint="http://localhost:6333",
    collection="demo",
    with_vectors=True,
    limit=200,
)

df = (
    spark.read.format(QDRANT_FORMAT)
    .option(QDRANT_CONFIG_OPTION, json.dumps(config))
    .load()
)
df.show(5)
```

Set `infer_schema=False` in the config when you want to provide a schema explicitly (pass
`QDRANT_SCHEMA_OPTION` with `schema.json` in the DataFrame reader options).

### Reader options

| Option | Type / Default | Description |
| --- | --- | --- |
| `endpoint` | string, required | Base HTTP URL for Qdrant (must start with `http://` or `https://`). |
| `collection` | string, required | Target collection to scroll. |
| `api_key` | string, optional | Adds `api-key` header when provided. |
| `headers` | mapping, optional | Extra headers merged into every request. |
| `timeout` | float, `30.0` | Request timeout in seconds. |
| `max_retries` | int, `3` | Retry attempts per request. |
| `backoff_factor` | float, `0.5` | Exponential backoff multiplier between retries. |
| `with_payload` | bool \| str \| sequence \| mapping, `True` | Controls the `with_payload` scroll flag. `True` includes all payload, `False` drops it, strings/sequences select payload keys, mappings pass through advanced payload selectors. |
| `with_vectors` | bool \| str \| sequence, `False` | Controls the `with_vectors` scroll flag. `True` includes all vectors, strings/sequences select named vectors. |
| `limit` | int, optional | Total point cap; must be positive. |
| `page_size` | int, `128` | Points per scroll page (clipped to `limit` when set). |
| `max_pages` | int, optional | Maximum number of pages to request. |
| `filter` | mapping, optional | Qdrant filter object sent with every scroll request. |
| `offset` | any, optional | Scroll offset token to start from. |
| `infer_schema` | bool, `True` | When `False`, an explicit schema is required via `QDRANT_SCHEMA_OPTION`. |

## Writing to Qdrant

Build a writer config with `build_qdrant_write_config` and pass it to
`df.write.format(QDRANT_FORMAT)`, or call `write_qdrant_points` directly for non-Spark workflows.

```python
import json
from spark_fuse.io import (
    QDRANT_CONFIG_OPTION,
    QDRANT_FORMAT,
    build_qdrant_write_config,
    register_qdrant_data_source,
)

register_qdrant_data_source(spark)
write_config = build_qdrant_write_config(
    endpoint="http://localhost:6333",
    collection="demo",
    vector_field="embedding",
    payload_fields=["text", "source"],
)

(
    df.write.format(QDRANT_FORMAT)
    .option(QDRANT_CONFIG_OPTION, json.dumps(write_config))
    .mode("append")
    .save()
)
```

By default, the writer pulls payload columns from all fields except the vector (and `id` if used);
set `payload_fields` to restrict which columns become payload.

### Writer options

| Option | Type / Default | Description |
| --- | --- | --- |
| `endpoint` | string, required | Base HTTP URL for Qdrant (must start with `http://` or `https://`). |
| `collection` | string, required | Target collection to write into. |
| `api_key` | string, optional | Adds `api-key` header when provided. |
| `headers` | mapping, optional | Extra headers merged into every request. |
| `timeout` | float, `30.0` | Request timeout in seconds. |
| `max_retries` | int, `3` | Retry attempts per batch. |
| `backoff_factor` | float, `0.5` | Exponential backoff multiplier between retries. |
| `batch_size` | int, `128` | Number of points sent per HTTP request. |
| `wait` | bool, `True` | Passes the `wait` flag to Qdrant to block until the write is applied. |
| `id_field` | string \| None, `"id"` | Column to use as the point ID. Set to `None` to let Qdrant assign IDs. |
| `vector_field` | string, `"vector"` | Column containing the vector to index; required in every record. |
| `payload_fields` | string \| sequence, optional | If set, only these columns are sent as payload. When omitted, all non-ID and non-vector columns become payload. |
| `create_collection` | bool, `False` | When true, auto-creates the collection (using the first point to infer vector size) if a GET on the collection returns 404. |
| `distance` | string, `"Cosine"` | Distance metric to use when creating a collection automatically. |
| `payload_format` | string, `"auto"` | Payload encoding for writes: `points` (list-of-points), `batch` (ids/vectors/payloads arrays), or `auto` (try points then fall back to batch on 400 “missing ids”). |
| `write_method` | string, `"auto"` | HTTP method for writes: `put`, `post`, or `auto` (tries PUT then POST). |

## API Reference

::: spark_fuse.io.qdrant.build_qdrant_config

::: spark_fuse.io.qdrant.build_qdrant_write_config

::: spark_fuse.io.qdrant.write_qdrant_points

::: spark_fuse.io.qdrant.QdrantDataSource
