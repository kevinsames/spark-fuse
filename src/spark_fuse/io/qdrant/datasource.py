"""Qdrant PySpark DataSource registration and entry-point class."""

from __future__ import annotations

import json
from typing import Mapping, Optional

from pyspark.sql import SparkSession
from pyspark.sql.datasource import DataSource
from pyspark.sql.types import StructType

from .reader import QdrantDataSourceReader, _QdrantResolvedConfig, _infer_schema_from_points
from .writer import _QdrantDataSourceWriter, _QdrantWriteConfig

QDRANT_CONFIG_OPTION = "spark.fuse.qdrant.config"
QDRANT_SCHEMA_OPTION = "spark.fuse.qdrant.schema"
QDRANT_FORMAT = "spark-fuse-qdrant"

_REGISTERED_SESSIONS: set[str] = set()


def register_qdrant_data_source(spark: SparkSession) -> None:
    """Register the Qdrant data source with the given SparkSession."""
    session_id = spark.sparkContext.applicationId
    if session_id in _REGISTERED_SESSIONS:
        return
    spark.dataSource.register(QdrantDataSource)
    _REGISTERED_SESSIONS.add(session_id)


class QdrantDataSource(DataSource):
    @classmethod
    def name(cls) -> str:  # pragma: no cover - trivial accessor
        return QDRANT_FORMAT

    def __init__(self, options: Mapping[str, str]) -> None:
        super().__init__(options)
        raw_config = options.get(QDRANT_CONFIG_OPTION)
        if not raw_config:
            raise ValueError("Qdrant data source requires the config option")
        config_data = json.loads(raw_config)

        self._read_config = _QdrantResolvedConfig.from_dict(config_data)
        self._write_config = _QdrantWriteConfig.from_dict(config_data)

        schema_json = options.get(QDRANT_SCHEMA_OPTION)
        self._user_schema = StructType.fromJson(json.loads(schema_json)) if schema_json else None
        self._schema_cache: Optional[StructType] = None

    def schema(self) -> StructType:
        if self._user_schema is not None:
            return self._user_schema
        if self._schema_cache is None:
            self._schema_cache = _infer_schema_from_points(self._read_config)
        return self._schema_cache

    def reader(self, schema: StructType) -> QdrantDataSourceReader:
        return QdrantDataSourceReader(self._read_config, schema)

    def writer(self, schema: StructType, overwrite: bool) -> _QdrantDataSourceWriter:
        return _QdrantDataSourceWriter(self._write_config)
