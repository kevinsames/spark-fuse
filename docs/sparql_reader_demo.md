# SPARQL Reader Demo

`SPARQLReader` executes SPARQL queries over HTTP(S) endpoints and returns the results as Spark
DataFrames. The companion notebook `notebooks/demos/sparql_reader_demo.ipynb` walks through a live query
against the public Wikidata service and includes a small fallback dataset so the example remains
useful when the endpoint is unavailable (for example in air-gapped environments).

## Prerequisites
- A running `SparkSession`.
- Outbound network access to your SPARQL endpoint (unless you rely on the built-in fallback rows).
- A descriptive `User-Agent` header that complies with the endpoint's usage policy. Wikidata
  requires contact information in the header string.

## Basic usage
```python
from spark_fuse.io.sparql import SPARQLReader

reader = SPARQLReader()
endpoint = "https://query.wikidata.org/sparql"

sample_query = """
PREFIX wd: <http://www.wikidata.org/entity/>
PREFIX wdt: <http://www.wikidata.org/prop/direct/>

SELECT ?pokemon ?pokemonLabel ?pokedexNumber WHERE {
  ?pokemon wdt:P31 wd:Q3966183 .
  ?pokemon wdt:P1685 ?pokedexNumber .
}
ORDER BY ASC(?pokedexNumber)
LIMIT 10
""".strip()

config = {
    "query": sample_query,
    "request_type": "POST",
    "include_metadata": True,
    "metadata_suffix": "__",
    "coerce_types": True,
    "headers": {"User-Agent": "spark-fuse-demo/0.3 (contact@example.com)"},
}

pokemon_df = reader.read(spark, endpoint, source_config=config)
pokemon_df.printSchema()
if pokemon_df.rdd.isEmpty():
    print("Endpoint unavailable — using built-in fallback rows.")
else:
    pokemon_df.show(5, truncate=False)
```

### Boolean queries (ASK)
`SPARQLReader` also supports ASK queries that return a boolean payload.

```python
ask_df = reader.read(
    spark,
    {"endpoint": endpoint, "query": "ASK WHERE { wd:Q3966183 wdt:P31 wd:Q1656682 }"},
    source_config={
        "request_type": "POST",
        "headers": {"User-Agent": "spark-fuse-demo/0.3 (contact@example.com)"},
    },
)
ask_df.show()
```

## Handling empty responses
Some environments may block outbound requests. When the reader cannot retrieve any rows, you can
still test downstream transformations by supplying fallback data:

```python
if pokemon_df.rdd.isEmpty():
    fallback_rows = [
        {"pokemon": "bulbasaur", "pokemonLabel": "Bulbasaur", "pokedexNumber": "001"},
        {"pokemon": "charmander", "pokemonLabel": "Charmander", "pokedexNumber": "004"},
        {"pokemon": "squirtle", "pokemonLabel": "Squirtle", "pokedexNumber": "007"},
    ]
    pokemon_df = spark.createDataFrame(fallback_rows, schema=pokemon_df.schema)
```

The demo notebook includes this logic so you can follow along even when the live Wikidata query is
not reachable.
