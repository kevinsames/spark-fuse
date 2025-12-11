spark-fuse
================

![CI](https://github.com/kevinsames/spark-fuse/actions/workflows/ci.yml/badge.svg)
![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)

spark-fuse is an open-source toolkit for PySpark — providing utilities, data sources, and tools to fuse your data workflows across JSON-centric REST APIs and SPARQL endpoints.

Features
- Data sources for REST APIs (JSON payloads with pagination/retry support), SPARQL services, and Qdrant collections (read/write).
- SparkSession helpers with sensible defaults and environment detection (databricks/fabric/local heuristics retained for legacy jobs).
- DataFrame utilities for previews, schema checks, and ready-made date/time dimensions (daily calendar attributes and clock buckets).
- LLM-powered semantic column normalization and LangChain-backed embedding generation (with optional text splitters) that batch work to limit API calls.
- Similarity partitioning toolkit with modular embedding preparation, clustering, and representative selection utilities.
- Change-tracking helpers to write current-only or history-preserving datasets with concise options.
- Typer-powered CLI: list data sources and preview datasets via the REST/SPARQL helpers.

Installation
- Create a virtual environment (recommended)
  - macOS/Linux:
    - `python3 -m venv .venv`
    - `source .venv/bin/activate`
    - `python -m pip install --upgrade pip`
  - Windows (PowerShell):
    - `python -m venv .venv`
    - `.\\.venv\\Scripts\\Activate.ps1`
    - `python -m pip install --upgrade pip`
- From source (dev): `pip install -e ".[dev]"`
- From PyPI: `pip install "spark-fuse>=1.0.2"`

Quickstart
1) Create a SparkSession with helpful defaults
```python
from spark_fuse.spark import create_session
spark = create_session(app_name="spark-fuse-quickstart")
```

2) Load paginated REST API responses
```python
import json
from spark_fuse.io import (
    REST_API_CONFIG_OPTION,
    REST_API_FORMAT,
    build_rest_api_config,
    register_rest_data_source,
)

register_rest_data_source(spark)
config = build_rest_api_config(
    spark,
    "https://pokeapi.co/api/v2/pokemon",
    source_config={
        "request_type": "GET",  # switch to "POST" for endpoints that require a body
        "records_field": "results",
        "pagination": {"mode": "response", "field": "next", "max_pages": 2},
        "params": {"limit": 20},
    },
)
pokemon = (
    spark.read.format(REST_API_FORMAT)
    .option(REST_API_CONFIG_OPTION, json.dumps(config))
    .load()
)
pokemon.select("name").show(5)
```
Need to hit a POST endpoint? Set `"request_type": "POST"` and attach your payload with
`"request_body": {...}` (defaults to JSON encoding—add `"request_body_type": "data"` for form bodies).
Flip on `"include_response_payload": True` to add a `response_payload` column with the raw server JSON.

3) Query a SPARQL endpoint
```python
sparql_query = """
PREFIX wd: <http://www.wikidata.org/entity/>
PREFIX wdt: <http://www.wikidata.org/prop/direct/>

SELECT ?pokemon ?pokemonLabel ?pokedexNumber WHERE {
  ?pokemon wdt:P31 wd:Q3966183 .
  ?pokemon wdt:P1685 ?pokedexNumber .
}
LIMIT 5
"""

from spark_fuse.io import (
    SPARQL_CONFIG_OPTION,
    SPARQL_DATA_SOURCE_NAME,
    build_sparql_config,
    register_sparql_data_source,
)

register_sparql_data_source(spark)
sparql_options = build_sparql_config(
    spark,
    "https://query.wikidata.org/sparql",
    source_config={
        "query": sparql_query,
        "request_type": "POST",
    "headers": {"User-Agent": "spark-fuse-demo/1.0 (contact@example.com)"},
    },
)
sparql_df = (
    spark.read.format(SPARQL_DATA_SOURCE_NAME)
    .option(SPARQL_CONFIG_OPTION, json.dumps(sparql_options))
    .load()
)
if sparql_df.rdd.isEmpty():
    print("Endpoint unavailable — adjust the query or check your network.")
else:
    sparql_df.show(5, truncate=False)
```

4) Write to a Qdrant collection
```python
from spark_fuse.io import (
    QDRANT_CONFIG_OPTION,
    QDRANT_FORMAT,
    build_qdrant_write_config,
    register_qdrant_data_source,
)

register_qdrant_data_source(spark)
write_cfg = build_qdrant_write_config(
    "http://localhost:6333",
    collection="pokemon",
    id_field="id",
    vector_field="embedding",
    payload_fields=["name", "type"],
)
df.write.format(QDRANT_FORMAT).option(QDRANT_CONFIG_OPTION, json.dumps(write_cfg)).save()
```

5) Generate embeddings with LangChain (optionally split text)
```python
from langchain_openai import OpenAIEmbeddings
from langchain_text_splitters import RecursiveCharacterTextSplitter
from spark_fuse.utils.transformations import with_langchain_embeddings

splitter = RecursiveCharacterTextSplitter(chunk_size=256, chunk_overlap=32)
embedded = with_langchain_embeddings(
    df,
    input_col="text",
    embeddings=lambda: OpenAIEmbeddings(model="text-embedding-3-small"),
    text_splitter=splitter,
    output_col="embedding",
    aggregation="mean",
    batch_size=16,
)
embedded.select("text", "embedding").show(3, truncate=False)
```
Pass a factory (`lambda: OpenAIEmbeddings(...)`) when the client cannot be pickled or needs executor-local setup. Provide a LangChain text splitter to chunk long documents before embedding; chunk vectors are combined with the chosen `aggregation` strategy (`mean` or `first`). Install `langchain-core`, `langchain-openai`, and `langchain-text-splitters` to use this helper.

6) Build date/time dimensions with rich attributes
```python
from spark_fuse.utils.dataframe import create_date_dataframe, create_time_dataframe

date_dim = create_date_dataframe(spark, "2024-01-01", "2024-01-07")
time_dim = create_time_dataframe(spark, "00:00:00", "23:59:00", interval_seconds=60)

date_dim.select("date", "year", "week", "day_name").show()
time_dim.select("time", "hour", "minute").show(5)
```
Check out `notebooks/demos/date_time_dimensions_demo.ipynb` for an interactive walkthrough.

7) Partition embeddings and pick representatives
```python
from spark_fuse.similarity import (
    CosineSimilarity,
    IdentityEmbeddingGenerator,
    KMeansPartitioner,
    MaxColumnChoice,
    SimilarityPipeline,
)

pipeline = SimilarityPipeline(
    embedding_generator=IdentityEmbeddingGenerator(input_col="embedding"),
    partitioner=KMeansPartitioner(k=3, seed=7),
    similarity_metric=CosineSimilarity(embedding_col="embedding"),
    choice_function=MaxColumnChoice(column="score"),
)

clustered = pipeline.run(df)
representatives = pipeline.select_representatives(clustered)
```
See `docs/guides/similarity_partitioning_demo.md` for a walkthrough and `notebooks/demos/similarity_pipeline_demo.ipynb` for an
interactive companion.

LLM-Powered Column Mapping
```python
from spark_fuse.utils.transformations import map_column_with_llm

standard_values = ["Apple", "Banana", "Cherry"]
mapped_df = map_column_with_llm(
    df,
    column="fruit",
    target_values=standard_values,
    model="o4-mini",
    temperature=None,
)
mapped_df.select("fruit", "fruit_mapped").show()
```

Set `dry_run=True` to inspect how many rows already match without spending LLM tokens. Configure your OpenAI or Azure OpenAI credentials with the usual environment variables before running live mappings. Some provider models only accept their default sampling configuration—pass `temperature=None` to omit the parameter when needed. The helper is available across spark-fuse 0.2.0 and later, including the 1.0.x series.

CLI Usage
- `spark-fuse --help`
- `spark-fuse datasources`
- `spark-fuse read --format rest --path https://pokeapi.co/api/v2/pokemon --config rest.json --show 5`

CI
- GitHub Actions runs ruff and pytest for Python 3.9–3.11.

License
- Apache 2.0
