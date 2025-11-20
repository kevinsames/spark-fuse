# Changelog

All notable changes to this project will be documented in this file.

The format is based on Keep a Changelog, and this project adheres to Semantic Versioning.

## [Unreleased]
### Removed
- ADLS Gen2, Fabric, and Databricks connectors plus their CLI commands/documentation, focusing IO on REST and SPARQL sources.
- `read_rest_api` and `read_sparql` helpers—use the Spark Data Source API directly with the new config builders.
- Visualization helpers exposed via `spark_fuse.utils.visualization` and the dedicated demo notebook; lean on project-specific pandas/matplotlib code when rendering data.
### Changed
- CLI `datasources` command replaces `connectors`, and `spark-fuse read` now requires `--format rest|sparql` to select the desired data source.
- SCD helpers have been fully renamed to the change-tracking API (`spark_fuse.utils.change_tracking`) with `change_tracking_mode=current_only|track_history` and the new ``DataFrame.write.change_tracking`` sugar.

## [1.0.1] - 2025-11-18
### Added
- Extended test coverage for `scd2_upsert` batches containing multiple updates per key and a demo
  notebook example that highlights the resulting historical versions.
- Both `scd1_upsert` and `scd2_upsert` now accept `allow_schema_evolution=True` to automatically add
  new columns when the upstream dataset evolves.

### Changed
- `scd2_upsert` sequences duplicate business keys within a single batch to ensure every change is
  recorded with an incremented version rather than collapsing to the most recent row.

### Documentation
- Documented the intra-batch sequencing behavior in the SCD guide and notebook so teams understand
  how history is preserved even when upstream data is not pre-deduplicated.

## [1.0.0] - 2025-11-16
### Changed
- Require PySpark 4.x (and delta-spark 4.x) in the Python package metadata and auto-detect the Scala
  binary when configuring Delta Lake jars, with an escape hatch via `SPARK_FUSE_DELTA_SCALA_SUFFIX`.

### Fixed
- `split_by_date_formats` now relies on `try_to_timestamp` when available so PySpark 4 no longer raises
  ANSI parsing errors for invalid rows; unmatched rows are still surfaced per the chosen mode.

### Removed
- Deprecated catalog helpers (Unity/Hive) and their CLI commands, documentation, and tests.
- Dropped the experimental Qdrant connector stub and the optional `qdrant` dependency extra.

### Documentation
- Updated install/prerequisite docs and demos to reference PySpark 4 and current Java requirements.

## [0.4.0] - 2025-11-15
### Added
- Similarity partitioning primitives, including embedding, metric, partitioner, and representative-selection helpers exposed under `spark_fuse.similarity`.
- `SentenceEmbeddingGenerator` integrates Hugging Face `sentence-transformers` models for on-the-fly text embeddings.
- Demo notebook (`notebooks/demos/similarity_pipeline_demo.ipynb`) illustrating an end-to-end clustering workflow.
- Documentation updates covering the similarity toolkit and linking to the interactive notebook.
- Core dependencies now include `numpy`, `pandas`, and `sentence-transformers` to support on-demand embedding generation.

## [0.3.2] - 2025-10-22
### Added
- SPARQL connector with metadata-aware schema handling, retry configuration, and type coercion.
- Demo notebook showcasing Wikidata queries with a fallback sample for offline environments.
- Documentation updates covering SPARQL usage, including a new guide and API reference entry.

### Fixed
- Explicit schema derivation for SPARQL result sets prevents Spark from emitting `NullType`
  inference errors when metadata columns contain only `None` values.

## [0.3.0] - 2025-09-09
### Added
- Date and time dimension helpers (`create_date_dataframe`, `create_time_dataframe`) with calendar and clock attributes.
- Unit tests covering the new DataFrame utilities.
- Demo notebook (`notebooks/demos/date_time_dimensions_demo.ipynb`) showcasing the dimension builders in action.

### Documentation
- README and site homepage updated with calendar/time examples and notebook reference.

## [0.2.1] - 2025-09-08
### Added
- REST API connector with pagination, retry handling, and distributed ingestion support.
- Example notebook showcasing the REST connector against the public PokeAPI service.
- Test coverage for REST API pagination paths using mocked HTTP responses.

### Changed
- Databricks connector can now read from Unity Catalog and Hive Metastore tables in addition to DBFS paths.
- Documentation highlights REST ingestion and updated Databricks capabilities.
- Bumped minimum recommended package version in install guides and README.

## [0.2.0] - 2025-09-07
### Added
- `map_column_with_llm` runs as a scalar PySpark UDF with per-executor caching, retry-aware LLM calls, and support for leaving `temperature` unset when providers require their defaults.
- Databricks notebook demo for the LLM mapping pipeline, including optional secret scope integration for credentials.
- `sitecustomize.py` to automatically expose the repository `src/` directory on `sys.path` during local development.

### Changed
- Documentation and examples now highlight the LLM mapper, including dry-run guidance and provider-specific temperature tips.
- Notebook samples default to `o4-mini` with `temperature=None` to align with current OpenAI requirements.

### Removed
- Unused pandas dependency now that the mapper no longer relies on pandas UDFs.

## [0.1.9] - 2025-09-06
### Added
- LLM-powered `map_column_with_llm` transformation for semantic column normalization with batch processing, caching, and retry-aware API integration.

### Documentation
- Highlighted the LLM mapping helper in the README, site landing page, and API reference.

## [0.1.8] - 2025-09-06
### Fixed
- Corrected `scd2_upsert` documentation parameters so MkDocs strict builds no longer fail.

### Documentation
- Ensured SCD API reference renders cleanly by aligning docstring argument annotations with the function signature.

## [0.1.7] - 2025-09-06
### Added
- Transformation utilities module offering column renaming, casting, literal injection, whitespace cleanup, and multi-format date parsing helpers.
- Test coverage for the transformation utilities, including `split_by_date_formats` error modes.

### Documentation
- Added API reference page for transformation utilities and highlighted the new helpers in the feature overview.

## [0.1.6] - 2025-09-05
### Changed
- Rename module `spark_fuse.utils.scd2` to `spark_fuse.utils.scd` (update imports accordingly).
- Databricks and Fabric connectors route Delta writes through SCD helpers (`apply_scd`) when `fmt='delta'`.
- IO connectors: extract shared option parsers (`as_seq`, `as_bool`) to `spark_fuse.io.utils` to remove duplication.

### Added
- Tests for SCD utilities: SCD1 de-dup/update, SCD2 versioning, and dispatcher.

### Fixed
- `scd2_upsert`: accept SQL string for `load_ts_expr`; compute next version using historical max per key.
- Spark session helper: add Delta Lake package via version map to match local PySpark, avoiding classpath mismatches.
- Test fixture: bind Spark driver to localhost and disable UI to avoid port issues in CI/sandboxes.

## [0.1.5] - 2025-09-04
### Added
- Documentation site: MkDocs + Material theme under `docs/`, with GitHub Pages workflow to build and deploy.

### Changed
- Packaging metadata: set `Homepage`/`Documentation` URLs to the GitHub Pages site.

## [0.1.4] - 2025-09-04
### Changed
- CI/Release: switch to PyPI Trusted Publishing (OIDC) with attestations enabled.
- CI/Release: publish workflow runs only on GitHub Release "published" events.

## [0.1.3] - 2025-09-04
### Changed
- CI/Release: publish via `pypi` environment using `PYPI_API_TOKEN` (API token auth) and disable attestations warnings.

## [0.1.2] - 2025-09-04
### Changed
- CI/Release: trigger publish workflow on tag pushes matching `v*`.
- CI/Release: validate built artifacts with `twine check` before upload.

## [0.1.1] - 2025-09-04
### Fixed
- Packaging: fix Hatch src-layout config for editable installs (`pip install -e .[dev]`).

### Added
- SCD2 Delta writer helper `scd2_upsert` in `spark_fuse.utils.scd`.
- Qdrant connector stub (`qdrant://` URIs) and optional extra `qdrant` (`qdrant-client>=1.8`).
- Project docs: GOVERNANCE.md, CODE_OF_CONDUCT.md, CONTRIBUTING.md, SECURITY.md, INSTALL, MAINTAINERS, authors.md, roadmap.md.

### Changed
- CI/Release: publish workflow targets protected `pypi` environment; switch to Trusted Publishing.

## [0.1.0] - 2024-09-01
### Added
- Connectors: ADLS Gen2 (`abfss://`), Microsoft Fabric OneLake (`onelake://` and `abfss://…onelake…`), and Databricks DBFS (`dbfs:/`).
- Catalog helpers: Unity Catalog (create catalog/schema, register external Delta) and Hive Metastore (create database, register external Delta).
- Spark helper: `create_session` with Delta configuration and environment detection (Databricks/Fabric/local).
- CLI (`spark-fuse`):
  - `connectors` — list available connectors
  - `read` — load and preview dataset
  - `uc-create`, `uc-register-table` — Unity Catalog ops
  - `hive-register-external` — Hive external table registration
  - `fabric-register` — register external Delta table backed by OneLake
  - `databricks-submit` — submit job via REST API
- Tests: registry resolution, path validation (ADLS/Fabric), and SQL generation (UC/Hive).
- CI: Python 3.9–3.11 matrix, ruff + pytest. Pre-commit hooks and Makefile.

[Unreleased]: https://github.com/kevinsames/spark-fuse/compare/v1.0.1...HEAD
[1.0.1]: https://github.com/kevinsames/spark-fuse/compare/v1.0.0...v1.0.1
[1.0.0]: https://github.com/kevinsames/spark-fuse/compare/v0.4.0...v1.0.0
[0.4.0]: https://github.com/kevinsames/spark-fuse/compare/v0.3.2...v0.4.0
[0.3.2]: https://github.com/kevinsames/spark-fuse/compare/v0.3.0...v0.3.2
[0.3.0]: https://github.com/kevinsames/spark-fuse/compare/v0.2.1...v0.3.0
[0.2.1]: https://github.com/kevinsames/spark-fuse/compare/v0.2.0...v0.2.1
[0.2.0]: https://github.com/kevinsames/spark-fuse/compare/v0.1.9...v0.2.0
[0.1.9]: https://github.com/kevinsames/spark-fuse/compare/v0.1.8...v0.1.9
[0.1.8]: https://github.com/kevinsames/spark-fuse/compare/v0.1.7...v0.1.8
[0.1.7]: https://github.com/kevinsames/spark-fuse/compare/v0.1.6...v0.1.7
[0.1.6]: https://github.com/kevinsames/spark-fuse/compare/v0.1.5...v0.1.6
[0.1.5]: https://github.com/kevinsames/spark-fuse/compare/v0.1.4...v0.1.5
[0.1.4]: https://github.com/kevinsames/spark-fuse/compare/v0.1.3...v0.1.4
[0.1.3]: https://github.com/kevinsames/spark-fuse/compare/v0.1.2...v0.1.3
[0.1.2]: https://github.com/kevinsames/spark-fuse/compare/v0.1.1...v0.1.2
[0.1.1]: https://github.com/kevinsames/spark-fuse/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/kevinsames/spark-fuse/releases/tag/v0.1.0
