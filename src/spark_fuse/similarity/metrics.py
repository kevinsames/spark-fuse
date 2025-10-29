"""
Similarity metric helpers.

Metrics typically need the embedding column expressed as ``VectorUDT`` so that
Spark MLlib transformers can consume it. The helper routines below provide that
conversion and optional normalization.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Tuple

try:
    from pyspark.ml.feature import Normalizer
except ImportError:  # pragma: no cover - worker environments missing numpy.__config__
    # Some sandboxed environments cannot import numpy.__config__ due to missing optional
    # metadata. Inject a minimal stub so PySpark's import succeeds.
    import types
    import sys

    if "numpy.__config__" not in sys.modules:
        stub = types.ModuleType("numpy.__config__")

        def _noop_show_config(*args, **kwargs):
            return None

        stub.show_config = _noop_show_config
        sys.modules["numpy.__config__"] = stub

    from pyspark.ml.feature import Normalizer
from pyspark.ml.functions import array_to_vector
from pyspark.ml.linalg import VectorUDT
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType


def _ensure_vector_column(df: DataFrame, source: str, target: str) -> Tuple[DataFrame, str]:
    """
    Ensure that ``target`` is a ``VectorUDT`` column derived from ``source``.

    Returns the possibly modified DataFrame and the name of the vector column.
    """

    dtype = df.schema[source].dataType

    if isinstance(dtype, VectorUDT):
        if source == target:
            return df, target
        return df.withColumn(target, F.col(source)), target

    if isinstance(dtype, ArrayType):
        vectorized = df.withColumn(target, array_to_vector(F.col(source)))
        return vectorized, target

    raise TypeError(f"Column {source} must be an array or VectorUDT; found {dtype}")


@dataclass
class SimilarityMetric(ABC):
    """
    Base class for similarity metrics.

    ``prepare`` may modify the DataFrame (e.g. normalization) and should return
    both the transformed DataFrame and the name of the vector column that
    downstream components should use.
    """

    embedding_col: str = "embedding"

    @abstractmethod
    def prepare(self, df: DataFrame) -> Tuple[DataFrame, str]:
        """Return the transformed DataFrame and vector column name."""


@dataclass
class CosineSimilarity(SimilarityMetric):
    """
    Normalize embeddings to unit length for cosine similarity calculations.

    Produces a new column (``prepared_col``) that holds the normalized vectors.
    """

    prepared_col: str = "embedding_unit"

    def prepare(self, df: DataFrame) -> Tuple[DataFrame, str]:
        df_vector, vector_col = _ensure_vector_column(df, self.embedding_col, self.embedding_col)
        normalizer = Normalizer(inputCol=vector_col, outputCol=self.prepared_col, p=2.0)
        normalized = normalizer.transform(df_vector)
        return normalized, self.prepared_col


@dataclass
class EuclideanDistance(SimilarityMetric):
    """
    Pass-through metric for Euclidean distance.

    Ensures the embedding column is a ``VectorUDT`` and returns it unchanged.
    """

    def prepare(self, df: DataFrame) -> Tuple[DataFrame, str]:
        df_vector, vector_col = _ensure_vector_column(df, self.embedding_col, self.embedding_col)
        return df_vector, vector_col
