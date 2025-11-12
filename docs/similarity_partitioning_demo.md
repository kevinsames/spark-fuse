# Similarity Partitioning Demo

This guide introduces the similarity pipeline primitives included with `spark-fuse`. The pipeline lets you reuse existing embedding columns or plug in custom generators, normalize vectors for a chosen metric, cluster rows, and optionally select a representative per cluster.

## Prerequisites

- PySpark 3.4+ with access to the `spark-fuse` package (version 0.4.0 or newer).
- The demo snippet below runs locally; adapt the Spark session to match your cluster configuration when deploying.
- Sample descriptions are embedded on the fly for brevity. In production, feed vectors produced by your preferred embedding workflow or swap in a different generator.

## 1. Start Spark

```python
import os, sys
from spark_fuse.spark import create_session

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
spark = create_session(app_name="spark-fuse-similarity-demo", master="local[2]")
```

## 2. Prepare example data

```python
sample = [
    (1, "Crunchy Red Apple", 4.7),
    (2, "Sweet Gala Apple", 4.9),
    (3, "Fresh Cavendish Banana", 4.6),
    (4, "Ripe Plantain", 4.5),
    (5, "Classic Spiral Notebook", 4.4),
]
columns = ["product_id", "description", "score"]
df = spark.createDataFrame(sample, columns)
```

## 3. Configure the pipeline

```python
from spark_fuse.similarity import (
    CosineSimilarity,
    KMeansPartitioner,
    MaxColumnChoice,
    SentenceEmbeddingGenerator,
    SimilarityPipeline,
)

pipeline = SimilarityPipeline(
    embedding_generator=SentenceEmbeddingGenerator(
        input_col="description",
        model_name="sentence-transformers/all-MiniLM-L6-v2",
        normalize=True,
        device="cpu",
        use_vectorized=False,
        prefer_stub=False,
    ),
    partitioner=KMeansPartitioner(k=3, seed=7),
    similarity_metric=CosineSimilarity(embedding_col="embedding"),
    choice_function=MaxColumnChoice(column="score"),
)
```
If you are unsure how many clusters to request in advance, switch to
``AutoKMeansPartitioner`` and specify the desired rows per cluster instead:

```python
from spark_fuse.similarity import AutoKMeansPartitioner

partitioner = AutoKMeansPartitioner(target_partition_size=10_000, seed=7)
```

If your environment cannot import `sentence-transformers`, the generator falls back to a lightweight deterministic stub so the demo keeps running. Install the real dependency (and PyTorch) when you need production-grade embeddings.

## 4. Cluster and pick representatives

```python
clustered = pipeline.run(df)
clustered.select("product_id", "cluster_id", "description").orderBy("cluster_id").show(truncate=False)

representatives = pipeline.select_representatives(clustered)
representatives.select("cluster_id", "product_id", "description", "score").orderBy("cluster_id").show(truncate=False)
```

## 5. Clean up

```python
spark.stop()
```

## Notebook companion

The repository ships with `notebooks/demos/similarity_pipeline_demo.ipynb`, which mirrors the steps above and can be executed interactively to explore different cluster parameters and representative strategies.

### Using Hugging Face sentence models

When raw text needs to be embedded on the fly, switch to `SentenceEmbeddingGenerator`, which wraps Hugging Face `sentence-transformers` models:

```python
from spark_fuse.similarity import SentenceEmbeddingGenerator

text_embedder = SentenceEmbeddingGenerator(
    input_col="description",
    model_name="sentence-transformers/all-MiniLM-L6-v2",
    batch_size=32,
    normalize=True,
    prefer_stub=False,
)
```

The generator depends on the `sentence-transformers` package, which is installed alongside `spark-fuse`.

Vectorized pandas UDFs can speed up inference, but they require pandas and numpy on every Spark executor. Leave `use_vectorized=False` (default) when you prefer the pure PySpark UDF fallback, or set it to `True` once your cluster has the dependencies installed.

Set `prefer_stub=True` when you explicitly want the deterministic hash-based fallback without attempting to import Hugging Face models. This is useful on constrained environments where importing `sentence-transformers` could terminate the Python worker.
