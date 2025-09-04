spark-fuse — Install Guide
==========================

Prerequisites
- Python: 3.9+
- Java: JDK 8 or 11 (required by PySpark 3.4.x)
- OS packages: build tools to compile native deps if needed

Virtual Environment (recommended)
- macOS/Linux:
  - `python3 -m venv .venv`
  - `source .venv/bin/activate`
  - `python -m pip install --upgrade pip`
- Windows (PowerShell):
  - `python -m venv .venv`
  - `.\\.venv\\Scripts\\Activate.ps1`
  - `python -m pip install --upgrade pip`

Quick Install (PyPI)
- `pip install spark-fuse`

Development Install (editable)
- `python -m pip install --upgrade pip`
- `pip install -e ".[dev]"`

Verify Installation
- `python -c "import spark_fuse; print(spark_fuse.__version__)"`
- `spark-fuse --help`

Java Setup Notes
- macOS (Homebrew): `brew install openjdk@11` then add to your shell:
  - `export JAVA_HOME="$(/usr/libexec/java_home -v 11)"`
- Linux: install OpenJDK 11 using your distro’s package manager and set `JAVA_HOME` accordingly.

Optional: Authentication and Environment
- Azure ADLS Gen2 (abfss://): set environment variables for a Service Principal and pass Spark configs as needed.
  - Env vars: `AZURE_TENANT_ID`, `AZURE_CLIENT_ID`, `AZURE_CLIENT_SECRET`
  - Example Spark configs via code:
    - `fs.azure.account.auth.type.<account>.dfs.core.windows.net=OAuth`
    - `fs.azure.account.oauth.provider.type.<account>.dfs.core.windows.net=org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider`
    - `fs.azure.account.oauth2.client.id.<account>.dfs.core.windows.net=$AZURE_CLIENT_ID`
    - `fs.azure.account.oauth2.client.secret.<account>.dfs.core.windows.net=$AZURE_CLIENT_SECRET`
    - `fs.azure.account.oauth2.client.endpoint.<account>.dfs.core.windows.net=https://login.microsoftonline.com/$AZURE_TENANT_ID/oauth2/token`
- Databricks REST submit:
  - Env vars: `DATABRICKS_HOST`, `DATABRICKS_TOKEN`
  - CLI: `spark-fuse databricks-submit --json payload.json`
- Microsoft Fabric (OneLake): use `onelake://...` or `abfss://...@onelake.dfs.fabric.microsoft.com/...` URIs. Access is managed by your Fabric workspace identity.

Minimal Usage Example
```python
from spark_fuse.spark import create_session
from spark_fuse.io.registry import connector_for_path

spark = create_session(app_name="spark-fuse-demo")
path = "abfss://container@account.dfs.core.windows.net/path/to/delta"
conn = connector_for_path(path)
df = conn.read(spark, path)  # delta by default
df.show(5)
```

Testing and Linting
- Run tests: `pytest`
- Lint: `ruff check src tests`
- Format: `ruff format src tests`

Publishing (Maintainers)
- Bump version in `pyproject.toml:1`
- Create GitHub Release (tag `vX.Y.Z`)
- Workflow `.github/workflows/publish.yml:1` builds and uploads to PyPI using the protected `pypi` environment (`PYPI_API_TOKEN`).
