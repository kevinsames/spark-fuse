"""
Representative selection helpers.

Choice functions operate on clustered DataFrames and emit one row per cluster.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Iterable, Optional

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


@dataclass
class ChoiceFunction(ABC):
    """Base interface for representative selection."""

    cluster_col: str = "cluster_id"

    @abstractmethod
    def select(self, df: DataFrame) -> DataFrame:
        """Return one representative row per cluster."""


@dataclass
class FirstItemChoice(ChoiceFunction):
    """
    Select the first row in each cluster.

    Optionally accepts an ordering specification; otherwise Spark's natural
    ordering is used.
    """

    order_by: Optional[Iterable] = None

    def select(self, df: DataFrame) -> DataFrame:
        if self.cluster_col not in df.columns:
            raise ValueError(f"cluster column '{self.cluster_col}' missing from DataFrame")

        if self.order_by is None:
            order_columns = [self.cluster_col]
        else:
            order_columns = list(self.order_by)

        window = Window.partitionBy(self.cluster_col).orderBy(*order_columns)
        ranked = df.withColumn("_rn", F.row_number().over(window))
        return ranked.filter(F.col("_rn") == 1).drop("_rn")


@dataclass
class MaxColumnChoice(ChoiceFunction):
    """
    Select the row with the largest value for ``column`` in each cluster.
    """

    column: str = ""

    def select(self, df: DataFrame) -> DataFrame:
        if not self.column:
            raise ValueError("column must be provided for MaxColumnChoice")
        if self.column not in df.columns:
            raise ValueError(f"column '{self.column}' missing from DataFrame")

        window = Window.partitionBy(self.cluster_col).orderBy(F.col(self.column).desc())
        ranked = df.withColumn("_rn", F.row_number().over(window))
        return ranked.filter(F.col("_rn") == 1).drop("_rn")
