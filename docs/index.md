# spark-fuse

spark-fuse is an open-source toolkit for PySpark — providing utilities, data sources, and tools to fuse your data workflows across REST APIs and SPARQL endpoints.

## Features
- Data sources for REST APIs (JSON payloads with pagination/retry support) and SPARQL services.
- SparkSession helpers with sensible defaults and environment detection (Databricks/Fabric/local heuristics retained for legacy jobs).
- DataFrame utilities for previews, name management, casts, whitespace cleanup, resilient date parsing, calendar/time dimensions, and LLM-backed semantic column mapping.
- LangChain-based embedding helper with optional text splitting and chunk aggregation.
- Change-tracking helpers to capture current-only or history-preserving datasets with concise writer options.
- Similarity partitioning toolkit with modular embedding preparation, clustering, and representative selection utilities.
- Typer-powered CLI: list data sources and preview datasets via the REST/SPARQL helpers.

## Quickstart

1) Create a SparkSession
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
rest_config = build_rest_api_config(
    spark,
    "https://pokeapi.co/api/v2/pokemon",
    source_config={
        "records_field": "results",
        "pagination": {"mode": "response", "field": "next", "max_pages": 2},
    },
)
pokemon = (
    spark.read.format(REST_API_FORMAT)
    .option(REST_API_CONFIG_OPTION, json.dumps(rest_config))
    .load()
)
pokemon.select("name").show(5)
```

3) Query a SPARQL endpoint
```python
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
        "query": """
        PREFIX wd: <http://www.wikidata.org/entity/>
        PREFIX wdt: <http://www.wikidata.org/prop/direct/>

        SELECT ?pokemon ?pokemonLabel ?pokedexNumber WHERE {
          ?pokemon wdt:P31 wd:Q3966183 .
          ?pokemon wdt:P1685 ?pokedexNumber .
        }
        LIMIT 5
        """,
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

4) LLM-powered semantic mapping
```python
from spark_fuse.utils.llm import map_column_with_llm

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

Use `dry_run=True` during development to avoid external API calls until credentials and prompts are ready. Some models only accept their default sampling configuration—use `temperature=None` to omit the parameter when required. The LLM mapper is available across spark-fuse 0.2.0 and later, including the 1.0.x releases.

5) LangChain embeddings (with optional text splitting)
```python
from langchain_openai import OpenAIEmbeddings
from langchain_text_splitters import RecursiveCharacterTextSplitter
from spark_fuse.utils.llm import with_langchain_embeddings

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
embedded.select("text", "embedding").show(2, truncate=False)
```
Pass a factory when the client is not picklable; chunk long documents with a LangChain splitter and combine chunk vectors using `aggregation` (`mean` or `first`). Install `langchain-core`, `langchain-openai`, and `langchain-text-splitters` to use this helper.

6) Calendar and time dimensions
```python
from spark_fuse.utils.dataframe import create_date_dataframe, create_time_dataframe

dates = create_date_dataframe(spark, "2024-01-01", "2024-01-07")
times = create_time_dataframe(spark, "08:00:00", "12:00:00", interval_seconds=1800)

dates.select("date", "year", "week", "day_name").show()
times.select("time", "hour", "minute").show()
```

Try the interactive `notebooks/demos/date_time_dimensions_demo.ipynb` notebook to explore the helpers end-to-end.

7) Similarity partitioning
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

The `docs/guides/similarity_partitioning_demo.md` guide walks through the workflow, and `notebooks/demos/similarity_pipeline_demo.ipynb` provides an interactive companion.

## CLI
- `spark-fuse datasources`
- `spark-fuse read --format <rest|sparql> --path <uri> --config config.json --show 5`

See the Install and CLI pages for more.
