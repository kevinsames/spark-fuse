"""
Partitioner implementations that assign cluster identifiers.

The initial implementation wraps Spark MLlib's ``KMeans`` algorithm and relies on
prepared feature vectors provided by the embedding and metric stages.
"""

from __future__ import annotations

import math
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Optional

try:
    from pyspark.ml.clustering import KMeans
except ImportError:
    import types
    import sys

    if "numpy.__config__" not in sys.modules:
        stub = types.ModuleType("numpy.__config__")

        def _noop_show_config(*args, **kwargs):
            return None

        stub.show_config = _noop_show_config
        sys.modules["numpy.__config__"] = stub

    from pyspark.ml.clustering import KMeans
from pyspark.sql import DataFrame

from .metrics import _ensure_vector_column


@dataclass
class Partitioner(ABC):
    """
    Base interface for partitioners.

    ``partition`` should append a cluster identifier column and return the
    augmented DataFrame.
    """

    output_col: str = "cluster_id"

    @abstractmethod
    def partition(self, df: DataFrame, features_col: Optional[str] = None) -> DataFrame:
        """Assign cluster identifiers to each row of the DataFrame."""


@dataclass
class KMeansPartitioner(Partitioner):
    """
    Wrapper around Spark MLlib's KMeans clustering.

    Parameters
    ----------
    k:
        Number of clusters to fit.
    max_iter:
        Maximum number of EM iterations.
    seed:
        Optional random seed for deterministic runs.
    features_col:
        Explicit features column to use. When omitted the partitioner relies on
        the column provided at runtime, defaulting to ``embedding`` when none is
        supplied.
    """

    k: int = 8
    max_iter: int = 20
    seed: Optional[int] = None
    features_col: Optional[str] = None

    def partition(self, df: DataFrame, features_col: Optional[str] = None) -> DataFrame:
        column = features_col or self.features_col or "embedding"
        df, vector_col = _ensure_vector_column(df, column, column)

        estimator = (
            KMeans(k=self.k, maxIter=self.max_iter, seed=self.seed)
            .setFeaturesCol(vector_col)
            .setPredictionCol(self.output_col)
        )
        model = estimator.fit(df)
        return model.transform(df)


@dataclass
class AutoKMeansPartitioner(Partitioner):
    """
    Dynamically infer the number of partitions and fit KMeans accordingly.

    Instead of fixing ``k`` ahead of time, this partitioner derives it from the
    size of the input DataFrame with a target number of rows per partition.

    Parameters
    ----------
    target_partition_size:
        Desired number of rows per cluster. Larger values produce fewer clusters.
    min_partitions:
        Lower bound for the inferred cluster count.
    max_partitions:
        Optional upper bound for the inferred cluster count.
    max_iter:
        Maximum number of EM iterations.
    seed:
        Optional random seed for deterministic runs.
    features_col:
        Explicit features column to use; defaults to the runtime column or
        ``embedding`` when omitted.
    """

    target_partition_size: int = 5_000
    min_partitions: int = 1
    max_partitions: Optional[int] = None
    max_iter: int = 20
    seed: Optional[int] = None
    features_col: Optional[str] = None

    def __post_init__(self) -> None:
        if self.target_partition_size <= 0:
            raise ValueError("target_partition_size must be a positive integer")
        if self.min_partitions < 1:
            raise ValueError("min_partitions must be at least 1")
        if self.max_partitions is not None and self.max_partitions < self.min_partitions:
            raise ValueError("max_partitions must be >= min_partitions")

    def _infer_partition_count(self, row_count: int) -> int:
        if row_count <= 0:
            return self.min_partitions

        partitions = math.ceil(row_count / self.target_partition_size)
        partitions = max(self.min_partitions, partitions)

        if self.max_partitions is not None:
            partitions = min(self.max_partitions, partitions)

        return partitions

    def partition(self, df: DataFrame, features_col: Optional[str] = None) -> DataFrame:
        column = features_col or self.features_col or "embedding"
        df_vector, vector_col = _ensure_vector_column(df, column, column)

        inferred_k = self._infer_partition_count(df_vector.count())
        estimator = (
            KMeans(k=inferred_k, maxIter=self.max_iter, seed=self.seed)
            .setFeaturesCol(vector_col)
            .setPredictionCol(self.output_col)
        )
        model = estimator.fit(df_vector)
        return model.transform(df_vector)
