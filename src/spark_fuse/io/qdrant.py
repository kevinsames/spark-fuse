from __future__ import annotations

import re
from typing import Any, Optional

from pyspark.sql import DataFrame, SparkSession

from .base import Connector
from .registry import register_connector


_QDRANT_RE = re.compile(r"^qdrant(\+http|\+https)?://[^/]+/.+")


@register_connector
class QdrantConnector(Connector):
    """Qdrant vector DB connector (stub).

    Path format: `qdrant://host:port/collection` or `qdrant+https://host/collection`.
    Requires optional dependency `qdrant-client` for real IO.
    """

    name = "qdrant"

    def validate_path(self, path: str) -> bool:
        return bool(_QDRANT_RE.match(path))

    def read(
        self, spark: SparkSession, path: str, *, fmt: Optional[str] = None, **options: Any
    ) -> DataFrame:
        raise NotImplementedError(
            "Qdrant read is not implemented in the stub. Install 'qdrant-client' and use a specialized reader."
        )

    def write(
        self,
        df: DataFrame,
        path: str,
        *,
        fmt: Optional[str] = None,
        mode: str = "append",
        **options: Any,
    ) -> None:
        raise NotImplementedError(
            "Qdrant write is not implemented in the stub. Install 'qdrant-client' and implement upsert per collection schema."
        )
