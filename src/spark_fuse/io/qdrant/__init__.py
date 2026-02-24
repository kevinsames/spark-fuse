"""Qdrant data source for PySpark.

This package splits the reader and writer concerns into separate modules while
maintaining a single public API surface.
"""

from .datasource import (
    QDRANT_CONFIG_OPTION,
    QDRANT_FORMAT,
    QDRANT_SCHEMA_OPTION,
    QdrantDataSource,
    register_qdrant_data_source,
)
from .reader import (
    QdrantDataSourceReader,
    build_qdrant_config,
)
from .writer import (
    _QdrantDataSourceWriter,
    _QdrantWriteConfig,
    build_qdrant_write_config,
    write_qdrant_points,
)

__all__ = [
    "QDRANT_CONFIG_OPTION",
    "QDRANT_FORMAT",
    "QDRANT_SCHEMA_OPTION",
    "QdrantDataSource",
    "QdrantDataSourceReader",
    "_QdrantDataSourceWriter",
    "_QdrantWriteConfig",
    "build_qdrant_config",
    "build_qdrant_write_config",
    "register_qdrant_data_source",
    "write_qdrant_points",
]
