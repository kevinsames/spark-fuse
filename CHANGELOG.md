# Changelog

All notable changes to this project will be documented in this file.

The format is based on Keep a Changelog, and this project adheres to Semantic Versioning.

## [Unreleased]

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
- SCD2 Delta writer helper `scd2_upsert` in `spark_fuse.utils.scd2`.
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

[Unreleased]: https://github.com/kevinsames/spark-fuse/compare/v0.1.5...HEAD
[0.1.5]: https://github.com/kevinsames/spark-fuse/compare/v0.1.4...v0.1.5
[0.1.4]: https://github.com/kevinsames/spark-fuse/compare/v0.1.3...v0.1.4
[0.1.3]: https://github.com/kevinsames/spark-fuse/compare/v0.1.2...v0.1.3
[0.1.2]: https://github.com/kevinsames/spark-fuse/compare/v0.1.1...v0.1.2
[0.1.1]: https://github.com/kevinsames/spark-fuse/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/kevinsames/spark-fuse/releases/tag/v0.1.0
