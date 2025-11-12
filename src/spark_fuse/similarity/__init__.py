"""
Similarity partitioning toolkit for PySpark.

This package exposes modular components that handle embedding preparation,
similarity metric configuration, clustering, and representative selection.
"""

from .embedding import EmbeddingGenerator, IdentityEmbeddingGenerator, SentenceEmbeddingGenerator
from .metrics import CosineSimilarity, EuclideanDistance, SimilarityMetric
from .partitioners import AutoKMeansPartitioner, KMeansPartitioner, Partitioner
from .choices import ChoiceFunction, FirstItemChoice, MaxColumnChoice
from .pipeline import SimilarityPipeline

__all__ = [
    "AutoKMeansPartitioner",
    "ChoiceFunction",
    "CosineSimilarity",
    "EmbeddingGenerator",
    "EuclideanDistance",
    "FirstItemChoice",
    "IdentityEmbeddingGenerator",
    "KMeansPartitioner",
    "MaxColumnChoice",
    "Partitioner",
    "SentenceEmbeddingGenerator",
    "SimilarityMetric",
    "SimilarityPipeline",
]
