"""Tests for representative selection (choice functions)."""

from __future__ import annotations

import pytest

from spark_fuse.similarity.choices import FirstItemChoice, MaxColumnChoice


class TestFirstItemChoice:
    def test_selects_one_per_cluster(self, spark):
        df = spark.createDataFrame(
            [
                {"cluster_id": 0, "name": "a", "value": 1},
                {"cluster_id": 0, "name": "b", "value": 2},
                {"cluster_id": 1, "name": "c", "value": 3},
            ]
        )
        choice = FirstItemChoice(cluster_col="cluster_id")
        result = choice.select(df)
        assert result.count() == 2

    def test_custom_order_by(self, spark):
        df = spark.createDataFrame(
            [
                {"cluster_id": 0, "name": "b", "value": 2},
                {"cluster_id": 0, "name": "a", "value": 1},
            ]
        )
        choice = FirstItemChoice(cluster_col="cluster_id", order_by=["name"])
        result = choice.select(df)
        rows = [r.asDict() for r in result.collect()]
        assert len(rows) == 1
        assert rows[0]["name"] == "a"

    def test_missing_cluster_col_raises(self, spark):
        df = spark.createDataFrame([{"name": "a"}])
        choice = FirstItemChoice(cluster_col="missing")
        with pytest.raises(ValueError, match="missing"):
            choice.select(df)


class TestMaxColumnChoice:
    def test_selects_max_value_per_cluster(self, spark):
        df = spark.createDataFrame(
            [
                {"cluster_id": 0, "name": "a", "score": 10},
                {"cluster_id": 0, "name": "b", "score": 20},
                {"cluster_id": 1, "name": "c", "score": 5},
            ]
        )
        choice = MaxColumnChoice(cluster_col="cluster_id", column="score")
        result = choice.select(df)
        rows = {r["cluster_id"]: r.asDict() for r in result.collect()}
        assert rows[0]["name"] == "b"
        assert rows[1]["name"] == "c"

    def test_empty_column_raises(self, spark):
        df = spark.createDataFrame([{"cluster_id": 0}])
        choice = MaxColumnChoice(cluster_col="cluster_id", column="")
        with pytest.raises(ValueError, match="column must be provided"):
            choice.select(df)

    def test_missing_column_raises(self, spark):
        df = spark.createDataFrame([{"cluster_id": 0, "value": 1}])
        choice = MaxColumnChoice(cluster_col="cluster_id", column="missing")
        with pytest.raises(ValueError, match="missing"):
            choice.select(df)
