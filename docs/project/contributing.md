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
- Data sources: `src/spark_fuse/io/`
- Spark helpers: `src/spark_fuse/spark.py`
- Tests: `tests/`

## Style and Standards
- Use type hints and keep functions small and focused.
- Follow existing patterns and naming; keep public APIs stable.
- Ruff rules (line length 100) enforced in CI.
- Add/adjust tests alongside changes.

## Adding a Data Source
1. Create a module under `src/spark_fuse/io/your_datasource.py`.
2. Implement a `pyspark.sql.datasource.DataSource` subclass (and reader) mirroring the REST/SPARQL examples.
3. Provide helper functions (e.g., `register_<name>_data_source`, `read_<name>`) that hide the JSON plumbing required by Spark.
4. Add tests in `tests/io/` that spin up a mock service and exercise the helper function.
5. Document the new data source in the README, site landing page, and `docs/api/`.

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
