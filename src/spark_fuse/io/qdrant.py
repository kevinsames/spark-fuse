from __future__ import annotations

import re
from typing import Any, Mapping, Optional

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
        """Return True if `path` looks like a supported Qdrant URI."""
        return bool(_QDRANT_RE.match(path))

    def read(
        self,
        spark: SparkSession,
        source: Any,
        *,
        fmt: Optional[str] = None,
        schema: Optional[Any] = None,
        source_config: Optional[Mapping[str, Any]] = None,
        options: Optional[Mapping[str, Any]] = None,
        **kwargs: Any,
    ) -> DataFrame:
        """Read vectors/payloads from Qdrant (not yet implemented)."""
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
        """Write vectors/payloads to Qdrant (not yet implemented)."""
        raise NotImplementedError(
            "Qdrant write is not implemented in the stub. Install 'qdrant-client' and implement upsert per collection schema."
        )
