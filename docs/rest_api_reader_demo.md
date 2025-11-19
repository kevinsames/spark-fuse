# REST API Data Source Demo

The REST API data source ingests paginated JSON endpoints into Spark DataFrames with optional request throttling and retry support.

```python
import json
from spark_fuse.spark import create_session
from spark_fuse.io import (
    REST_API_CONFIG_OPTION,
    REST_API_FORMAT,
    build_rest_api_config,
    register_rest_data_source,
)

spark = create_session(app_name="spark-fuse-rest-demo")
register_rest_data_source(spark)

payload = build_rest_api_config(
    spark,
    "https://pokeapi.co/api/v2/pokemon",
    source_config={
        "request_type": "GET",  # switch to "POST" when the API expects a payload
        "records_field": "results",
        "pagination": {"mode": "response", "field": "next", "max_pages": 2},
    },
)

pokemon = (
    spark.read.format(REST_API_FORMAT)
    .option(REST_API_CONFIG_OPTION, json.dumps(payload))
    .load()
)

pokemon.select("name").show(5)
```

Highlights:

- Cursor-based (`response`) and offset-based (`token`) pagination.
- Optional request headers and query parameters.
- Issue `GET` or `POST` calls by setting `request_type`, attaching payloads with `request_body`.
- Built-in retry/backoff controls.
- Optional `include_response_payload` column to capture the full server JSON per row.

## Notebook walkthrough

See the data source in action with additional configuration examples:

- [REST API Data Source Demo](https://github.com/kevinsames/spark-fuse/blob/main/notebooks/demos/rest_api_reader_demo.ipynb)
