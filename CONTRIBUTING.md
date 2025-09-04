# Contributing to spark-fuse

Thank you for your interest in contributing! This guide covers the local dev setup, coding standards, tests, and how to propose changes.

## Getting Started
- Prerequisites:
  - Python 3.9+
  - Java JDK 8–11 (required by PySpark)
- Create and activate a virtual environment (recommended):
  - macOS/Linux:
    - `python3 -m venv .venv`
    - `source .venv/bin/activate`
  - Windows (PowerShell):
    - `python -m venv .venv`
    - `.\\.venv\\Scripts\\Activate.ps1`
- Clone/fork the repo and install dev deps:
  - `python -m pip install --upgrade pip`
  - `pip install -e ".[dev]"`
  - (optional) `pre-commit install`

## Dev Tasks
- Lint: `ruff check src tests`
- Format: `ruff format src tests`
- Tests: `pytest`
- Run all: `make lint && make test`

## Project Layout
- Package: `src/spark_fuse/...`
- CLI entrypoint: `src/spark_fuse/tools/cli.py` (`spark-fuse`)
- Connectors: `src/spark_fuse/io/`
- Catalog helpers: `src/spark_fuse/catalogs/`
- Spark helpers: `src/spark_fuse/spark.py`
- Tests: `tests/`

## Style and Standards
- Use type hints and keep functions small and focused.
- Follow existing patterns and naming; keep public APIs stable.
- Ruff rules (line length 100) enforced in CI.
- Add/adjust tests alongside changes.

## Adding a Connector
1. Create a module under `src/spark_fuse/io/your_connector.py`.
2. Subclass `Connector` from `io/base.py` and implement:
   - `name: ClassVar[str]`
   - `validate_path(path: str) -> bool`
   - `read(spark, path, *, fmt=None, **options)`
   - `write(df, path, *, fmt=None, mode="error", **options)`
3. Register with `@register_connector` decorator.
4. Add tests in `tests/io/` (path validation and basic behavior).
5. Optionally wire a CLI command in `tools/cli.py`.

## Catalog Utilities
- Unity Catalog helpers live in `catalogs/unity.py`.
- Hive Metastore helpers live in `catalogs/hive.py`.
- Prefer building SQL with minimal quoting helpers; add unit tests for generated SQL in `tests/catalogs/`.

## CLI Commands
- Implement Typer commands in `tools/cli.py`.
- Keep output friendly and concise (Rich tables/panels welcome).
- Add short examples to `README.md` if introducing new UX.

## Testing Notes
- Uses a local Spark session (`local[2]`) for tests.
- Avoid network calls; mock them when needed (e.g., Databricks REST).
- Keep tests fast and deterministic.

## Documentation
- Update `README.md` when adding user-facing functionality.
- Add yourself to `authors.md` if you make significant contributions.
- Follow the tone: concise, practical, and example-driven.

## Releasing
- Bump `version` in `pyproject.toml`.
- Create a GitHub Release (tag, e.g., `v0.1.1`).
- The workflow `.github/workflows/publish.yml` builds and publishes to PyPI.
- PyPI token is stored in the protected environment `pypi` as `PYPI_API_TOKEN`.

## Code of Conduct
- This project follows the Contributor Covenant. See `CODE_OF_CONDUCT.md`.

## License
- By contributing, you agree that your contributions are licensed under the Apache 2.0 License.
