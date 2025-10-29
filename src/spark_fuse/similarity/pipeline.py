"""
High-level pipeline that orchestrates embedding generation, metric preparation,
partitioning, and representative selection.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from pyspark.sql import DataFrame

from .choices import ChoiceFunction
from .embedding import EmbeddingGenerator
from .metrics import SimilarityMetric
from .partitioners import Partitioner


@dataclass
class SimilarityPipeline:
    """
    Compose the similarity partitioning workflow.

    Parameters
    ----------
    embedding_generator:
        Produces the embedding column; defaults to ``IdentityEmbeddingGenerator``.
    partitioner:
        Assigns cluster identifiers.
    similarity_metric:
        Optional metric used to prepare embeddings prior to clustering.
    choice_function:
        Optional representative selection step. When provided,
        :meth:`select_representatives` can be used to obtain one row per cluster.
    """

    embedding_generator: EmbeddingGenerator
    partitioner: Partitioner
    similarity_metric: Optional[SimilarityMetric] = None
    choice_function: Optional[ChoiceFunction] = None

    def run(self, df: DataFrame) -> DataFrame:
        """
        Execute embedding generation, metric preparation, and partitioning.

        Returns a DataFrame with the cluster identifier appended.
        """

        df_with_embeddings = self.embedding_generator.transform(df)
        features_col = None
        if self.similarity_metric is not None:
            df_prepared, prepared_col = self.similarity_metric.prepare(df_with_embeddings)
            features_col = prepared_col
        else:
            df_prepared = df_with_embeddings

        clustered = self.partitioner.partition(df_prepared, features_col)
        return clustered

    def select_representatives(self, clustered_df: DataFrame) -> DataFrame:
        """
        Apply the configured choice function to obtain representatives.
        """

        if self.choice_function is None:
            raise ValueError("choice_function is not configured for this pipeline")
        return self.choice_function.select(clustered_df)
