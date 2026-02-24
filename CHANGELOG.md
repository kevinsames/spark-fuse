# Changelog

All notable changes to this project will be documented in this file.

The format is based on Keep a Changelog, and this project adheres to Semantic Versioning.

## [Unreleased]

## [1.2.0] - 2026-02-24
### Added
- `with_langchain_embeddings` transformation to batch LangChain embedding calls into a Spark column, with optional LangChain text splitter support and configurable chunk aggregation.

### Removed
- ADLS Gen2, Microsoft Fabric, and Databricks connectors along with their CLI commands/documentation; the IO package now focuses on REST and SPARQL sources.
- `read_rest_api` and `read_sparql` helpers; use the Spark Data Source API directly via the new config builders and options.
- Visualization helpers under `spark_fuse.utils.visualization` and the accompanying demo notebook now that users can rely on bespoke pandas/matplotlib workflows outside the library.

### Changed
- CLI `connectors` command has been replaced by `datasources`, and `spark-fuse read` now requires an explicit `--format` (`rest` or `sparql`).
- Renamed the SCD helpers to the change-tracking API (`spark_fuse.utils.change_tracking`) with `change_tracking_mode=current_only|track_history`, new option names, and a ``DataFrame.write.change_tracking`` entry point.

## [1.0.2] - 2025-12-01
### Added
- Exported lightweight progress helpers (`create_progress_tracker`, `log_progress`) from `spark_fuse.utils.progress` so notebooks can log elapsed time consistently.
- Template data-processing notebook now inspects Delta logs before/after writes and supports change-tracking writes with schema evolution.

### Changed
- Default template write path renamed to `/tmp/spark_fuse/orders_enriched_ct` and uses `DataFrame.write.change_tracking` when Delta is available, falling back to Parquet otherwise.

## [1.0.1] - 2025-11-18
### Added
- Unit coverage for `scd2_upsert` scenarios where multiple changes for a key arrive in the same
  batch, plus a corresponding example in the demo notebook to visualize the full history.
- Optional `allow_schema_evolution` flag on both `scd1_upsert` and `scd2_upsert` to let Delta
  automatically add new columns when the incoming dataset evolves.

### Changed
- `scd2_upsert` now sequences duplicate business keys within a batch so each intermediate state is
  written with its own version rather than only keeping the latest row from the source feed.

### Documentation
- Noted the intra-batch sequencing behavior in the SCD documentation/demo so teams know that the
  helper preserves every version even when the upstream dataset is not pre-collapsed.

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

## [0.3.2] - 2025-10-22
### Added
- SPARQL reader for HTTP endpoints with optional metadata capture, type coercion, and retry/backoff
  handling, plus a demo notebook and API documentation.

### Fixed
- Ensure SPARQL reader builds explicit Spark schemas so metadata columns containing only ``None``
  values no longer trigger ``NullType`` inference errors.

### Documentation
- Added SPARQL reader guide, updated README/site feature lists, and noted the demo notebook's
  fallback sample for offline environments.

## [0.3.1] - 2025-10-22
### Added
- REST API reader supports configurable request types (`GET`/`POST`), payloads via `request_body`,
  and optional response capture (`include_response_payload`), with updated notebook and documentation examples.

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

[Unreleased]: https://github.com/kevinsames/spark-fuse/compare/v1.2.0...HEAD
[1.2.0]: https://github.com/kevinsames/spark-fuse/compare/v1.0.2...v1.2.0
[1.0.2]: https://github.com/kevinsames/spark-fuse/compare/v1.0.1...v1.0.2
[1.0.1]: https://github.com/kevinsames/spark-fuse/compare/v1.0.0...v1.0.1
[1.0.0]: https://github.com/kevinsames/spark-fuse/compare/v0.3.2...v1.0.0
[0.3.2]: https://github.com/kevinsames/spark-fuse/compare/v0.3.1...v0.3.2
[0.3.1]: https://github.com/kevinsames/spark-fuse/compare/v0.3.0...v0.3.1
[0.1.6]: https://github.com/kevinsames/spark-fuse/compare/v0.1.5...v0.1.6
[0.1.5]: https://github.com/kevinsames/spark-fuse/compare/v0.1.4...v0.1.5
[0.1.4]: https://github.com/kevinsames/spark-fuse/compare/v0.1.3...v0.1.4
[0.1.3]: https://github.com/kevinsames/spark-fuse/compare/v0.1.2...v0.1.3
[0.1.2]: https://github.com/kevinsames/spark-fuse/compare/v0.1.1...v0.1.2
[0.1.1]: https://github.com/kevinsames/spark-fuse/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/kevinsames/spark-fuse/releases/tag/v0.1.0
