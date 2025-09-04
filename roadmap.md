# spark-fuse Roadmap

This document outlines planned work for spark-fuse. Timelines and scope may shift based on user feedback and maintainer bandwidth. Items are grouped into milestones with rough sequencing.

## Milestone 0 — 0.1.0 (Initial release)
- Connectors: ADLS Gen2 (`abfss://`), Fabric OneLake (`onelake://` and `abfss://…onelake…`), Databricks DBFS (`dbfs:/`).
- Catalogs: Unity Catalog (create catalog/schema, register external Delta), Hive Metastore (create database, register external Delta).
- Spark helpers: Delta configuration + environment detection.
- CLI: connectors list, read preview, UC/Hive/Fabric registration, Databricks job submit.
- DevX: tests, ruff, pre-commit, CI matrix, publishing workflow.

Status: completed.

## Milestone 1 — 0.2.0 (Config + Auth polish)
- Add Pydantic-based config models (profiles) for connector and catalog operations.
- Support loading config from YAML/env; profile selection via CLI flag.
- Document recommended ABFS OAuth configs for SPN and MSI; helper to inject minimal Spark configs.
- Improve Fabric path handling and normalization (canonicalize `onelake://` ↔ `abfss://…onelake…`).

## Milestone 2 — 0.3.0 (Databricks API expansion)
- API client wrappers: `runs/get`, `runs/get-output`, simple polling `--wait` for `databricks-submit`.
- Basic helpers for `clusters/list`, `jobs/create` (opt-in; minimal surface area).
- CLI UX: pretty status output, error surfacing, and `--host/--token` overrides.

## Milestone 3 — 0.4.0 (Catalog management)
- Unity Catalog: manage external locations and storage credentials via SQL helpers.
- Unity Catalog: grant/revoke privileges helpers (tables, schemas, catalogs).
- Hive: location utilities and table existence checks; safer create/register flows.
 - Delta utilities: SCD Type 2 writer helper to upsert/merge DataFrames into Delta tables using business keys, effective/expiry timestamps, and current-row flags.

## Milestone 4 — 0.5.0 (Plugin system)
- Connector auto-discovery via Python entry points (`spark_fuse.connectors`).
- Registry diagnostics: show source package and version for each connector.
 - Qdrant connector: read/write vectors and payloads, basic similarity search, and minimal admin utilities (collection create/upsert) via REST/gRPC.

## Milestone 5 — 0.6.0 (Docs + Examples)
- Docs site (MkDocs Material) with walkthroughs for ADLS, Fabric, and UC flows.
- Example notebooks and CLI recipes.
- Architecture and contribution guides for new connectors.

## Milestone 6 — 0.7.0 (Quality + Compatibility)
- Optional structured logging and better error types.
- Broader compatibility testing: Python 3.12 and Spark 3.5.
- CI enhancements (caching, selective test runs) and nightly lint/test.

## Nice-to-haves / Investigation
- Fabric Lakehouse admin APIs integration (as they stabilize).
- Optional telemetry (anonymous usage metrics) gated by explicit opt-in.
- Containerized dev environment (Devcontainer) and minimal Docker images for examples.

## Backward Compatibility
- Follow SemVer; avoid breaking public CLI flags/APIs in minor releases.
- Document deprecations with at least one minor release of overlap.

## How to Influence the Roadmap
- Open or upvote issues tagged `enhancement` or `roadmap`.
- Propose RFCs for larger changes (see `GOVERNANCE.md`).

---
If you want a specific item prioritized, please open an issue and describe the use case. Contributions are welcome!
