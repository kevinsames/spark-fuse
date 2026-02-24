"""Tests for similarity metrics."""

from __future__ import annotations

import pytest

from pyspark.ml.linalg import DenseVector
from pyspark.sql.types import ArrayType, FloatType, StructField, StructType

from spark_fuse.similarity.metrics import (
    CosineSimilarity,
    EuclideanDistance,
    _ensure_vector_column,
)


class TestEnsureVectorColumn:
    def test_array_converted_to_vector(self, spark):
        schema = StructType([StructField("embedding", ArrayType(FloatType()), True)])
        df = spark.createDataFrame([([1.0, 0.0],)], schema=schema)

        result, col_name = _ensure_vector_column(df, "embedding", "embedding")
        assert col_name == "embedding"
        row = result.collect()[0]
        assert isinstance(row["embedding"], DenseVector)

    def test_vector_column_passthrough(self, spark):
        df = spark.createDataFrame([(DenseVector([1.0, 0.0]),)], schema=["embedding"])
        result, col_name = _ensure_vector_column(df, "embedding", "embedding")
        assert col_name == "embedding"
        row = result.collect()[0]
        assert isinstance(row["embedding"], DenseVector)

    def test_invalid_type_raises(self, spark):
        df = spark.createDataFrame([("not_a_vector",)], schema=["embedding"])
        with pytest.raises(TypeError, match="must be an array or VectorUDT"):
            _ensure_vector_column(df, "embedding", "embedding")


class TestCosineSimilarity:
    def test_produces_unit_vectors(self, spark):
        schema = StructType([StructField("embedding", ArrayType(FloatType()), True)])
        df = spark.createDataFrame([([3.0, 4.0],)], schema=schema)

        metric = CosineSimilarity(embedding_col="embedding")
        result, prepared_col = metric.prepare(df)

        assert prepared_col == "embedding_unit"
        row = result.collect()[0]
        vec = row[prepared_col]
        # L2 norm should be ~1.0
        norm = sum(v**2 for v in vec) ** 0.5
        assert abs(norm - 1.0) < 1e-6


class TestEuclideanDistance:
    def test_passthrough_converts_to_vector(self, spark):
        schema = StructType([StructField("embedding", ArrayType(FloatType()), True)])
        df = spark.createDataFrame([([1.0, 2.0],)], schema=schema)

        metric = EuclideanDistance(embedding_col="embedding")
        result, col_name = metric.prepare(df)

        assert col_name == "embedding"
        row = result.collect()[0]
        assert isinstance(row["embedding"], DenseVector)
