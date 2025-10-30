"""
Embedding generation primitives.

The base abstractions focus on producing a column that can be consumed by a
similarity metric or partitioner. Initial implementations cover the common
scenario where embeddings already exist on the DataFrame.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass
import hashlib
import math
import warnings
from typing import Dict, Tuple, Optional

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import pandas_udf, udf
from pyspark.sql.types import ArrayType, FloatType

_SENTENCE_TRANSFORMER_CACHE: Dict[Tuple[str, Optional[str]], object] = {}
_FALLBACK_WARNING_EMITTED: Dict[Tuple[str, Optional[str]], bool] = {}


def _hash_to_vector(value: Optional[str], dims: int = 16) -> list[float]:
    """
    Deterministically map ``value`` to a vector of length ``dims``.

    Used when `sentence-transformers` is unavailable so demos keep working.
    """

    if not value:
        return [0.0] * dims

    digest = hashlib.sha256(value.encode("utf-8")).digest()
    needed = dims * 4
    buffer = (digest * ((needed + len(digest) - 1) // len(digest)))[:needed]

    vector = []
    for idx in range(0, needed, 4):
        chunk = buffer[idx : idx + 4]
        integer = int.from_bytes(chunk, byteorder="big", signed=False)
        vector.append(integer / 0xFFFFFFFF)
    return vector


def _normalize_vector(values: list[float]) -> list[float]:
    norm = math.sqrt(sum(v * v for v in values))
    if norm == 0.0:
        return [float(v) for v in values]
    return [float(v / norm) for v in values]


def _build_sentence_stub(model_name: str) -> object:
    class _StubModel:
        def encode(self, values, batch_size: int = 32, normalize_embeddings: bool = True):
            vectors = []
            for item in values:
                vector = _hash_to_vector(item, dims=16)
                if normalize_embeddings:
                    vector = _normalize_vector(vector)
                vectors.append(vector)
            return vectors

    return _StubModel()


def _warn_sentence_fallback(key: Tuple[str, Optional[str]], exc: Exception) -> None:
    if not _FALLBACK_WARNING_EMITTED.get(key):
        warnings.warn(
            "sentence-transformers could not be loaded "
            f"for model '{key[0]}': {exc}. Falling back to a deterministic "
            "hash-based stub so the pipeline demo can still run.",
            RuntimeWarning,
            stacklevel=2,
        )
        _FALLBACK_WARNING_EMITTED[key] = True


@dataclass
class EmbeddingGenerator(ABC):
    """
    Base interface for producing an embedding column.

    Subclasses must implement :meth:`transform` and write the resulting vectors
    to ``output_col``. Implementations may add helper columns but should avoid
    dropping user data.
    """

    output_col: str = "embedding"

    @abstractmethod
    def transform(self, df: DataFrame) -> DataFrame:
        """Return a DataFrame with the requested embedding column."""


@dataclass
class IdentityEmbeddingGenerator(EmbeddingGenerator):
    """
    Copy or alias an existing column as the embedding column.

    Parameters
    ----------
    input_col:
        Name of the column that already contains embeddings.
    drop_input:
        When ``True`` and the generator aliases the column, the original column
        will be dropped. Defaults to ``False`` to keep the source column.
    """

    input_col: str = "features"
    drop_input: bool = False

    def transform(self, df: DataFrame) -> DataFrame:
        if self.input_col == self.output_col:
            return df

        df_with_embedding = df.withColumn(self.output_col, F.col(self.input_col))
        if self.drop_input:
            return df_with_embedding.drop(self.input_col)
        return df_with_embedding


def _load_sentence_model(model_name: str, device: Optional[str]) -> object:
    key = (model_name, device)
    if key in _SENTENCE_TRANSFORMER_CACHE:
        return _SENTENCE_TRANSFORMER_CACHE[key]

    import os
    import sys

    debug = os.environ.get("SPARK_FUSE_DEBUG_SENTENCE_EMBEDDING") == "1"

    # Default to offline mode so environments without network access can rely on cached models.
    os.environ.setdefault("HF_HUB_OFFLINE", "1")
    os.environ.setdefault("TRANSFORMERS_OFFLINE", "1")
    os.environ.setdefault("OMP_NUM_THREADS", "1")
    os.environ.setdefault("MKL_NUM_THREADS", "1")
    os.environ.setdefault("TOKENIZERS_PARALLELISM", "false")
    os.environ.setdefault("PYTHONFAULTHANDLER", "1")
    os.environ.setdefault("PYTORCH_ENABLE_BACKTRACE", "1")
    os.environ.setdefault("PYTORCH_ENABLE_MPS_FALLBACK", "1")
    os.environ.setdefault("CUDA_VISIBLE_DEVICES", "")
    os.environ.setdefault("KMP_DUPLICATE_LIB_OK", "TRUE")
    os.environ.setdefault("KMP_INIT_AT_FORK", "FALSE")
    os.environ.setdefault("KMP_BLOCKTIME", "0")

    if debug:
        print(
            f"[SentenceEmbeddingGenerator] loading model {model_name} device={device} python={sys.executable}",
            flush=True,
        )

    try:
        from sentence_transformers import SentenceTransformer

        try:  # pragma: no cover - torch optional
            import torch

            torch.set_num_threads(1)
            torch.set_num_interop_threads(1)
        except Exception:
            pass
    except Exception as exc:  # pragma: no cover - dependency error path
        _warn_sentence_fallback(key, exc)
        model = _build_sentence_stub(model_name)
    else:
        try:
            if device is None:
                model = SentenceTransformer(model_name)
            else:
                model = SentenceTransformer(model_name, device=device)
            if debug:
                print(
                    f"[SentenceEmbeddingGenerator] model {model_name} loaded (device={device}) -> actual {getattr(model, 'device', 'unknown')}",
                    flush=True,
                )
        except Exception as exc:  # pragma: no cover - runtime import issues
            _warn_sentence_fallback(key, exc)
            model = _build_sentence_stub(model_name)

    _SENTENCE_TRANSFORMER_CACHE[key] = model
    return model


@dataclass
class SentenceEmbeddingGenerator(EmbeddingGenerator):
    """
    Generate embeddings using Hugging Face `sentence-transformers` models.

    Parameters
    ----------
    input_col:
        Name of the column containing raw text.
    model_name:
        Identifier of the model to load, e.g. ``sentence-transformers/all-MiniLM-L6-v2``.
    batch_size:
        Batch size for encoding calls.
    normalize:
        Whether to L2-normalize embeddings returned by the model.
    device:
        Optional device string forwarded to ``SentenceTransformer`` (for example ``"cuda"``).
    drop_input:
        Drop the original ``input_col`` once the embedding column is added.
    prefer_stub:
        When ``True`` the deterministic hash-based stub is used instead of loading
        ``sentence-transformers``. Helpful on environments where importing the real
        dependency is unreliable.
    """

    input_col: str = "text"
    model_name: str = "sentence-transformers/all-MiniLM-L6-v2"
    batch_size: int = 32
    normalize: bool = True
    device: Optional[str] = None
    drop_input: bool = False
    use_vectorized: bool = False
    prefer_stub: bool = False

    def transform(self, df: DataFrame) -> DataFrame:
        model_name = self.model_name
        device = self.device
        batch_size = self.batch_size
        normalize = self.normalize
        input_col = self.input_col
        output_col = self.output_col
        cache_key = (model_name, device)

        if self.prefer_stub:
            _SENTENCE_TRANSFORMER_CACHE[cache_key] = _build_sentence_stub(model_name)
        else:
            # Prime the cache on the driver so worker failures surface upfront.
            _load_sentence_model(model_name, device)

        if self.use_vectorized:
            try:
                import pandas as pd

                Series = pd.Series

                @pandas_udf(ArrayType(FloatType()))
                def _encode(text_series: Series) -> Series:
                    model = _load_sentence_model(model_name, device)
                    filled = text_series.fillna("")
                    vectors = model.encode(
                        filled.tolist(),
                        batch_size=batch_size,
                        normalize_embeddings=normalize,
                    )
                    return pd.Series([list(map(float, vec)) for vec in vectors])

                transformed = df.withColumn(output_col, _encode(F.col(input_col)))
            except ImportError:
                transformed = None
            except Exception:  # pragma: no cover - safety net for worker envs lacking numpy
                transformed = None

            if transformed is not None:
                if self.drop_input:
                    return transformed.drop(input_col)
                return transformed

        def _encode_row(value: Optional[str]) -> Optional[list]:
            import sys
            import os

            debug_local = os.environ.get("SPARK_FUSE_DEBUG_SENTENCE_EMBEDDING") == "1"
            try:
                import numpy  # type: ignore
            except Exception:
                numpy_info = "unavailable"
            else:
                numpy_info = getattr(numpy, "__version__", "unknown")
            if debug_local and not hasattr(_encode_row, "_diag_printed"):
                print(
                    f"[SentenceEmbeddingGenerator] worker python={sys.executable} numpy={numpy_info}",
                    flush=True,
                )
                setattr(_encode_row, "_diag_printed", True)

            model = _load_sentence_model(model_name, device)
            text = value if value is not None else ""
            if debug_local:
                print("[SentenceEmbeddingGenerator] starting encode", flush=True)
            embeddings = model.encode(
                [text],
                batch_size=1,
                normalize_embeddings=normalize,
            )
            if debug_local:
                print("[SentenceEmbeddingGenerator] encode finished", flush=True)
            if embeddings is None:
                return None
            if len(embeddings) == 0:
                return None
            vector = embeddings[0]
            return [float(x) for x in vector]

        encode_udf = udf(_encode_row, ArrayType(FloatType()))
        transformed = df.withColumn(output_col, encode_udf(F.col(input_col)))
        if self.drop_input:
            return transformed.drop(input_col)
        return transformed
