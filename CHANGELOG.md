# Changelog

All notable changes to this project will be documented in this file.

The format is based on Keep a Changelog, and this project adheres to Semantic Versioning.

## [Unreleased]
### Added
- SCD2 Delta writer helper `scd2_upsert` in `spark_fuse.utils.scd2`.
- Qdrant connector stub (`qdrant://` URIs) and optional extra `qdrant` (`qdrant-client>=1.8`).
- Project docs: GOVERNANCE.md, CODE_OF_CONDUCT.md, CONTRIBUTING.md, SECURITY.md, INSTALL, MAINTAINERS, authors.md, roadmap.md.

### Changed
- Packaging: publish workflow targets protected `pypi` environment.

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

[Unreleased]: https://github.com/your-org/spark-fuse/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/your-org/spark-fuse/releases/tag/v0.1.0
