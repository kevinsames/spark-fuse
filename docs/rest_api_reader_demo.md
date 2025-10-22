# REST API Reader Demo

The REST API connector ingests paginated JSON endpoints into Spark DataFrames with optional request throttling and retry support.

```python
from spark_fuse.spark import create_session
from spark_fuse.io.rest_api import RestAPIReader

spark = create_session(app_name="spark-fuse-rest-demo")

reader = RestAPIReader()
pokemon = reader.read(
    spark,
    "https://pokeapi.co/api/v2/pokemon",
    source_config={
        "request_type": "GET",  # switch to "POST" when the API expects a payload
        "records_field": "results",
        "pagination": {"mode": "response", "field": "next", "max_pages": 2},
    },
)

pokemon.select("name").show(5)
```

Highlights:

- Cursor-based (`response`) and offset-based (`token`) pagination.
- Optional request headers and query parameters.
- Issue `GET` or `POST` calls by setting `request_type`, attaching payloads with `request_body`.
- Built-in retry/backoff controls.

## Notebook walkthrough

See the connector in action with additional configuration examples:

- [REST API Reader Demo](https://github.com/kevinsames/spark-fuse/blob/main/notebooks/rest_api_reader_demo.ipynb)
