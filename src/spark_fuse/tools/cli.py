from __future__ import annotations

import json
from pathlib import Path
from typing import Optional

import typer
from rich import box
from rich.panel import Panel
from rich.table import Table

from .. import __version__
from ..catalogs import hive, unity
from ..io import azure_adls  # noqa: F401  # Ensure registration side-effects
from ..io import databricks as dbr  # noqa: F401
from ..io import fabric as fabric_io  # noqa: F401
from ..io import qdrant as qdrant_io  # noqa: F401
from ..io.registry import connector_for_path, list_connectors
from ..spark import create_session
from ..utils.logging import console


app = typer.Typer(name="spark-fuse", help="PySpark toolkit: connectors, catalogs, CLI tools")


@app.callback()
def _main(
    version: Optional[bool] = typer.Option(None, "--version", is_flag=True, help="Show version"),
):
    if version:
        console().print(f"spark-fuse {__version__}")
        raise typer.Exit(code=0)


@app.command("connectors")
def connectors_cmd():
    """List available connector plugins."""
    table = Table(title="Connectors", box=box.SIMPLE_HEAVY)
    table.add_column("name")
    for name in list_connectors():
        table.add_row(name)
    console().print(table)


@app.command("read")
def read_cmd(
    path: str = typer.Option(..., help="Dataset path/URI"),
    fmt: Optional[str] = typer.Option(None, help="Format override (delta/parquet/csv)"),
    show: int = typer.Option(5, help="Show N rows after load"),
):
    """Load and preview a dataset using the connector inferred from the path."""
    connector = connector_for_path(path)
    if not connector:
        console().print(Panel.fit(f"No connector found for: {path}", style="error"))
        raise typer.Exit(code=2)

    spark = create_session(app_name="spark-fuse-read")
    df = connector.read(spark, path, fmt=fmt)
    console().print(
        Panel.fit(f"Loaded with connector: {connector.name}", title="Info", style="info")
    )
    df.show(show, truncate=False)
    console().print(f"Schema: {df.schema.simpleString()}")


@app.command("uc-create")
def uc_create_cmd(
    catalog: str = typer.Option(..., help="Unity Catalog name"),
    schema: Optional[str] = typer.Option(None, help="Schema to create inside the catalog"),
):
    """Create a Unity Catalog and optionally a schema."""
    spark = create_session(app_name="spark-fuse-uc-create")
    unity.create_catalog(spark, catalog)
    if schema:
        unity.create_schema(spark, catalog=catalog, schema=schema)
    console().print(Panel.fit("Unity Catalog create completed", style="info"))


@app.command("uc-register-table")
def uc_register_table_cmd(
    catalog: str = typer.Option(..., help="Unity Catalog name"),
    schema: str = typer.Option(..., help="Schema name"),
    table: str = typer.Option(..., help="Table name"),
    path: str = typer.Option(
        ..., help="External Delta location (abfss:// or dbfs:/ or onelake://)"
    ),
):
    """Register an external Delta table in Unity Catalog."""
    spark = create_session(app_name="spark-fuse-uc-register")
    unity.register_external_delta_table(
        spark, catalog=catalog, schema=schema, table=table, location=path
    )
    console().print(Panel.fit("Unity Catalog registration completed", style="info"))


@app.command("hive-register-external")
def hive_register_external_cmd(
    database: str = typer.Option(..., help="Hive database name"),
    table: str = typer.Option(..., help="Table name"),
    path: str = typer.Option(..., help="External Delta location"),
):
    """Register an external Delta table in Hive Metastore."""
    spark = create_session(app_name="spark-fuse-hive-register")
    hive.register_external_delta_table(spark, database=database, table=table, location=path)
    console().print(Panel.fit("Hive registration completed", style="info"))


@app.command("fabric-register")
def fabric_register_cmd(
    table: str = typer.Option(..., help="Table name to create in current catalog/database"),
    path: str = typer.Option(
        ...,
        help="OneLake Delta path (onelake:// or abfss://...onelake.dfs.fabric.microsoft.com/...)",
    ),
):
    """Register an external Delta table backed by a Fabric OneLake location."""
    spark = create_session(app_name="spark-fuse-fabric-register")
    # Simple SQL compatible with Spark on Fabric/Delta.
    sql = f"CREATE TABLE IF NOT EXISTS `{table}` USING DELTA LOCATION '{path}'"
    spark.sql(sql)
    console().print(Panel.fit("Fabric table registration completed", style="info"))


@app.command("databricks-submit")
def databricks_submit_cmd(
    json_path: str = typer.Option(..., "--json", help="Path to JSON payload or inline JSON string"),
    host: Optional[str] = typer.Option(None, help="Override DATABRICKS_HOST"),
    token: Optional[str] = typer.Option(None, help="Override DATABRICKS_TOKEN"),
):
    """Submit a job to Databricks via REST API (Runs Submit)."""
    payload_text: str
    p = Path(json_path)
    if p.exists():
        payload_text = p.read_text()
    else:
        payload_text = json_path
    payload = json.loads(payload_text)

    resp = dbr.databricks_submit_job(payload, host=host, token=token)
    run_id = resp.get("run_id")
    console().print(Panel.fit(f"Job submitted: run_id={run_id}", style="info"))


if __name__ == "__main__":
    app()
