# Refactoring Summary

This document describes the structural refactoring applied to the `spark-fuse` codebase.
No business logic was changed -- all modifications are purely organizational.

---

## Goals

1. **Rename & reorganize** files into a clearer directory structure
2. **Break up large files** into single-responsibility modules
3. **Improve naming conventions** for consistency
4. **Add tests** for previously uncovered modules
5. **Update imports** after reorganization

---

## Changes

### 1. Extracted shared IO HTTP utilities (`io/_http.py`)

**Why:** `_normalize_jsonable`, `_validate_http_url`, and HTTP retry logic were
duplicated across `rest_api.py`, `sparql.py`, and `qdrant.py`.

**New file:** `src/spark_fuse/io/_http.py` (~100 lines)

- `normalize_jsonable()` -- recursively normalize values into JSON-serializable form
- `validate_http_url()` -- check that a string starts with `http://` or `https://`
- `perform_request_with_retry()` -- exponential-backoff retry wrapper for HTTP requests

**Updated files:**
- `io/rest_api.py` -- imports from `_http.py`, local duplicates removed
- `io/sparql.py` -- no longer duplicates retry logic

---

### 2. Split `transformations.py` into DataFrame ops + LLM module

**Why:** The 798-line `utils/transformations.py` mixed pure DataFrame operations
with external LLM API integration (OpenAI / Azure). These are independent concerns.

**New file:** `src/spark_fuse/utils/llm.py` (~510 lines)

Extracted functions:
- `with_langchain_embeddings()`
- `map_column_with_llm()`
- `_get_llm_api_config()`
- `_fetch_llm_mapping()`
- `_create_long_accumulator()`

**Kept in `transformations.py`** (~290 lines):
- `rename_columns()`
- `with_constants()`
- `cast_columns()`
- `normalize_whitespace()`
- `split_by_date_formats()`

**Import path:** Use `from spark_fuse.utils.llm import map_column_with_llm`
(and `with_langchain_embeddings`). All notebooks, docs, and tests have been
updated to the new paths.

---

### 3. Split `qdrant.py` (1,080 lines) into a package

**Why:** The monolithic file mixed read, write, and DataSource registration
concerns. The reader (~350 lines) and writer (~500 lines) are independent.

**New package:** `src/spark_fuse/io/qdrant/`

| File | Contents |
|------|----------|
| `__init__.py` | Re-exports the full public API |
| `reader.py` | `_QdrantResolvedConfig`, scroll iteration, schema inference, `QdrantDataSourceReader`, `build_qdrant_config()` |
| `writer.py` | `_QdrantWriteConfig`, point building, batch sending, collection management, `_QdrantDataSourceWriter`, `write_qdrant_points()`, `build_qdrant_write_config()` |
| `datasource.py` | `QdrantDataSource`, `register_qdrant_data_source()`, format constants |

**Deleted:** `src/spark_fuse/io/qdrant.py` (replaced by package)

**Import path:** `qdrant/__init__.py` re-exports all public symbols so
`from spark_fuse.io.qdrant import ...` continues to work as before.

---

### 4. Renamed `utils/logging.py` to `utils/progress.py`

**Why:** `logging.py` collides with Python's stdlib `logging` module, which
can cause subtle import issues. The file's actual content is progress tracking
and Spark event logging, not general-purpose logging.

**Renamed files:**
- `src/spark_fuse/utils/logging.py` -> `src/spark_fuse/utils/progress.py`
- `tests/utils/test_logging.py` -> `tests/utils/test_progress.py`

**Updated imports in:** `tools/cli.py`, test files, all notebooks and docs

---

### 5. Standardized naming conventions

- Changed `logger = logging.getLogger(...)` to `_LOGGER = logging.getLogger(...)`
  in `utils/transformations.py` and `utils/llm.py` to match the convention used
  across all IO modules.

---

### 6. Added tests for uncovered modules

| Test file | Module covered | Tests added |
|-----------|---------------|-------------|
| `tests/io/test_http.py` | `io/_http.py` | 13 tests for `normalize_jsonable()` and `validate_http_url()` |
| `tests/io/test_utils.py` | `io/utils.py` | 17 tests for `as_seq()` and `as_bool()` |
| `tests/test_spark.py` | `spark.py` | 7 tests for `detect_environment()`, `_extract_scala_binary()`, `_has_java()` |
| `tests/similarity/test_choices.py` | `similarity/choices.py` | 6 tests for `FirstItemChoice`, `MaxColumnChoice` |
| `tests/similarity/test_metrics.py` | `similarity/metrics.py` | 5 tests for `_ensure_vector_column`, `CosineSimilarity`, `EuclideanDistance` |
| `tests/utils/test_llm.py` | `utils/llm.py` | 5 tests for `_get_llm_api_config()` |

**Total new tests:** 53 (bringing the suite from 66 to 119 tests)

---

## Files Summary

### New files
- `src/spark_fuse/io/_http.py`
- `src/spark_fuse/io/qdrant/__init__.py`
- `src/spark_fuse/io/qdrant/reader.py`
- `src/spark_fuse/io/qdrant/writer.py`
- `src/spark_fuse/io/qdrant/datasource.py`
- `src/spark_fuse/utils/llm.py`
- `tests/io/test_http.py`
- `tests/io/test_utils.py`
- `tests/test_spark.py`
- `tests/similarity/test_choices.py`
- `tests/similarity/test_metrics.py`
- `tests/utils/test_llm.py`
- `docs/api/utils_llm.md`
- `REFACTOR.md`

### Renamed files
- `src/spark_fuse/utils/logging.py` -> `src/spark_fuse/utils/progress.py`
- `tests/utils/test_logging.py` -> `tests/utils/test_progress.py`

### Deleted files
- `src/spark_fuse/io/qdrant.py` (replaced by `io/qdrant/` package)

### Modified files
- `src/spark_fuse/io/__init__.py` (qdrant imports updated)
- `src/spark_fuse/io/rest_api.py` (uses shared `_http` helpers)
- `src/spark_fuse/utils/__init__.py` (added `llm` module)
- `src/spark_fuse/utils/transformations.py` (LLM functions extracted)
- `src/spark_fuse/tools/cli.py` (import path: `logging` -> `progress`)
- `tests/utils/test_transformations.py` (LLM imports from `utils.llm`)
- `notebooks/demos/llm_mapping_demo.ipynb` (import from `utils.llm`)
- `notebooks/demos/langchain_embeddings_demo.ipynb` (import from `utils.llm`)
- `notebooks/demos/spark_logging_demo.ipynb` (import from `utils.progress`)
- `notebooks/templates/llm_mapping_template.ipynb` (imports from `utils.llm` and `utils.progress`)
- `notebooks/templates/data_processing_template.ipynb` (import from `utils.progress`)
- `README.md` (import paths updated)
- `docs/index.md` (import paths updated)
- `docs/api/utils_logging.md` (references `utils.progress`)
- `docs/api/utils_transformations.md` (LLM members removed)
- `docs/guides/llm_mapping_demo.md` (import from `utils.llm`)
- `CHANGELOG.md` (module path updated)
- `docs/project/changelog.md` (module path updated)
- `docs/project/releases/v1.0.2.md` (module path updated)

---

## Verification

- **119 tests pass** (`pytest`)
- **0 lint errors** (`ruff check`)
- **0 format issues** (`ruff format --check`)
- All notebooks, docs, and tests updated to new import paths
